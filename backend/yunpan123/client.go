package yunpan123

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/backend/yunpan123/tokenmanager"
)

// APIClient 封装了对 123 云盘 API 的所有请求
type APIClient struct {
	// 用于元数据、JSON API请求的客户端，超时时间较短
	metaClient *http.Client
	// 用于文件上传/下载的客户端，超时时间很长或没有超时
	dataClient *http.Client

	BaseURL      string
	TokenManager *tokenmanager.Manager
}

// NewAPIClient 创建一个新的 APIClient 实例
func NewAPIClient(baseURL string, tm *tokenmanager.Manager) *APIClient {
	// rclone的fshttp.NewClient会根据rclone的全局配置（如--timeout）创建客户端
	// 我们在这里创建两个，一个用于元数据，一个用于数据传输
	// 如果用户没有设置 --timeout，默认是 5 分钟，对于元数据来说可能太长
	// 但我们可以依赖 context 的超时来控制。
	// 对于数据传输，我们需要一个更长的超时，所以我们直接从rclone配置创建。
	return &APIClient{
		metaClient:   fshttp.NewClient(fs.Config),
		dataClient:   fshttp.NewClient(fs.Config), // dataClient也使用rclone的配置，因为--timeout通常用于数据传输
		BaseURL:      baseURL,
		TokenManager: tm,
	}
}

// baseResponse 是所有API响应共有的基础结构，用于解析code和msg
type baseResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// do sends a request and handles retries for JSON-based APIs (metadata).
// It reads the response body to check for API-level errors in the JSON.
func (c *APIClient) do(ctx context.Context, req *http.Request) (*http.Response, []byte, error) {
	var (
		resp      *http.Response
		respBody  []byte
		err       error
		lastError error
	)

	// rclone的pacer会自动处理重试的退避逻辑
	pacer := fs.NewPacer(ctx)

	for i := 0; i < fs.Config.Retries; i++ {
		// 添加最新的认证头
		token := c.TokenManager.GetToken()
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Platform", "open_platform")

		// 使用元数据客户端发送请求
		resp, err = c.metaClient.Do(req.WithContext(ctx))
		if err != nil {
			lastError = fmt.Errorf("request failed: %w", err)
			fs.Debugf(nil, "API metadata request error (attempt %d/%d): %v", i+1, fs.Config.Retries, err)
			if pacer.Retry() {
				continue
			}
			break // 不再重试
		}

		// 读取响应体，因为我们需要检查JSON中的code
		respBody, err = io.ReadAll(resp.Body)
		resp.Body.Close() // 立即关闭
		if err != nil {
			lastError = fmt.Errorf("failed to read response body: %w", err)
			fs.Debugf(nil, "API metadata response read error (attempt %d/%d): %v", i+1, fs.Config.Retries, err)
			if pacer.Retry() {
				continue
			}
			break
		}

		// 解析基础响应，检查code
		var baseResp baseResponse
		if jsonErr := json.Unmarshal(respBody, &baseResp); jsonErr != nil {
			// 如果无法解析JSON，可能是一个非标准的错误页面（如网关错误）
			lastError = fmt.Errorf("API request failed with status %s and non-JSON body: %s", resp.Status, string(respBody))
			fs.Debugf(nil, "API metadata response non-JSON error (attempt %d/%d): %v", i+1, fs.Config.Retries, lastError)
			// 对于服务器端错误（5xx），可以重试
			if resp.StatusCode >= 500 && pacer.Retry() {
				continue
			}
			break
		}

		// 根据JSON中的code处理重试逻辑
		switch baseResp.Code {
		case 0: // 成功
			return resp, respBody, nil
		case 401: // Token 过期
			fs.Logf(nil, "[APIClient] Received API code 401 (Unauthorized). Refreshing token...")
			refreshErr := c.TokenManager.GetAndStoreToken("/renew_token")
			if refreshErr != nil {
				return nil, nil, fmt.Errorf("failed to renew token after API code 401: %w", refreshErr)
			}
			// Token已刷新，立即重试
			continue
		case 429: // 速率限制
			fs.Logf(nil, "[APIClient] Received API code 429 (Too Many Requests). Pacer will handle backoff.")
			pacer.Pace(resp.StatusCode) // 即使是200 OK，也用pacer来退避
			continue
		default: // 其他API错误
			lastError = fmt.Errorf("API returned an error: code=%d, msg='%s'", baseResp.Code, baseResp.Message)
			// 对于某些错误，可能不想重试，但为了简单起见，我们让pacer决定
			fs.Debugf(nil, "API metadata request returned error (attempt %d/%d): %v", i+1, fs.Config.Retries, lastError)
			if pacer.Retry() {
				continue
			}
			break
		}
	}

	if lastError == nil {
		lastError = fserrors.ErrMaxTriesExceeded
	}
	return nil, nil, lastError
}

// doUpload sends a request for data transfer (upload). It does not read the response body by default.
// It uses fshttp.Do for robust, streaming-safe retries.
func (c *APIClient) doUpload(ctx context.Context, req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error

	addAuthHeaders := func(req *http.Request) {
		token := c.TokenManager.GetToken()
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Platform", "open_platform")
	}

	shouldRetry := func(resp *http.Response, err error) (bool, error) {
		if err != nil {
			// 对于网络错误，rclone的默认行为是重试
			return fs.ShouldRetry(ctx, resp, err)
		}

		// 对于上传，我们必须读取响应体来检查JSON code
		bodyBytes, readErr := io.ReadAll(resp.Body)
		resp.Body.Close() // 立即关闭
		if readErr != nil {
			return false, fmt.Errorf("failed to read upload response body for retry check: %w", readErr)
		}
		// 把body重新放回去，以便上层可以再次读取
		resp.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		var baseResp baseResponse
		if jsonErr := json.Unmarshal(bodyBytes, &baseResp); jsonErr != nil {
			// 如果无法解析，则按HTTP状态码处理
			return fs.ShouldRetry(ctx, resp, err)
		}

		switch baseResp.Code {
		case 401:
			fs.Logf(nil, "[APIClient-Upload] Received API code 401. Refreshing token...")
			if refreshErr := c.TokenManager.GetAndStoreToken("/renew_token"); refreshErr != nil {
				return false, fmt.Errorf("failed to renew token for upload: %w", refreshErr)
			}
			return true, nil // 需要重试
		case 429:
			fs.Logf(nil, "[APIClient-Upload] Received API code 429. Pacer will handle backoff.")
			// 在 fshttp.Do 外部没有 pacer，但 fs.ShouldRetry 会处理 429 状态码
			return fs.ShouldRetry(ctx, resp, err)
		case 0: // 成功
			return false, nil // 不需要重试
		default:
			// 其他API错误，认为是不可恢复的
			return false, fmt.Errorf("upload API returned an error: code=%d, msg='%s'", baseResp.Code, baseResp.Message)
		}
	}

	addAuthHeaders(req)
	// 使用rclone的fshttp.Do，它能正确处理流式请求的重试
	resp, err = fshttp.Do(ctx, c.dataClient, req, shouldRetry)
	return resp, err
}