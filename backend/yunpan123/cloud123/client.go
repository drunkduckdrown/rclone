package cloud123

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rclone/rclone/fs/log"
	// 导入你的 tokenmanager 包，路径需要与你的 go.mod 模块路径一致
	"github.com/rclone/rclone/backend/yunpan123/tokenmanager"
)

// APIClient 封装了对 123 云盘 API 的所有请求，并处理鉴权和重试
type APIClient struct {
	BaseURL      string
	HTTPClient   *http.Client
	TokenManager *tokenmanager.Manager // 你的 token 管理器实例
}

// NewAPIClient 创建一个新的 APIClient 实例
func NewAPIClient(baseURL string, tm *tokenmanager.Manager) *APIClient {
	return &APIClient{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second, // 默认超时，可根据需要调整
		},
		TokenManager: tm,
	}
}

// Do 发送一个 HTTP 请求，并处理鉴权刷新和重试逻辑
// requestBodyBytes 用于在重试时重建请求体，对于GET/HEAD请求可以为nil
func (c *APIClient) Do(ctx context.Context, req *http.Request, requestBodyBytes []byte) (*http.Response, error) {
	maxRetries := 5 // 最大重试次数
	for i := 0; i < maxRetries; i++ {
		// 在每次重试前，确保请求头包含最新的 token
		token := c.TokenManager.GetToken()
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Platform", "open_platform") // *** 新增：添加通用请求头 ***

		// 如果是 POST/PUT 请求且有请求体，每次重试前需要重新设置 Body
		// req.Body 是 io.ReadCloser，只能读一次
		if requestBodyBytes != nil {
			req.Body = io.NopCloser(bytes.NewReader(requestBodyBytes))
			req.ContentLength = int64(len(requestBodyBytes))
		} else if req.GetBody != nil { // For requests created with http.NewRequestWithContext or similar, if GetBody is set
			// If GetBody is set, it means the body can be re-read.
			// This is useful for large files when the original source can be reset.
			bodyReadCloser, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("failed to get request body for retry: %w", err)
			}
			req.Body = bodyReadCloser
		}

		// 使用带 context 的请求，以便 rclone 可以取消长时间运行的操作
		resp, err := c.HTTPClient.Do(req.WithContext(ctx))

		// 检查网络错误
		if err != nil {
			log.Debugf(nil, "[APIClient] API request network error (attempt %d/%d): %v", i+1, maxRetries, err)
			if i < maxRetries-1 { // 不是最后一次尝试
				time.Sleep(time.Duration(1<<(i)) * 100 * time.Millisecond) // 指数退避
				continue
			}
			return nil, fmt.Errorf("API request failed after %d retries due to network error: %w", maxRetries, err)
		}

		switch resp.StatusCode {
		case http.StatusUnauthorized: // 401
			log.Warningf(nil, "[APIClient] Received 401 Unauthorized. Attempting to refresh token via /renew_token...")
			// 关闭当前的响应体，以便可以重用连接
			func() {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()

			// 触发你的鉴权更新逻辑：调用 /renew_token 强制服务端刷新
			err := c.TokenManager.GetAndStoreToken("/renew_token") // 调用 /renew_token 接口
			if err != nil {
				return nil, fmt.Errorf("failed to renew token after 401: %w", err)
			}
			// Token 已更新，继续循环，重试请求
			continue

		case http.StatusTooManyRequests: // 429
			log.Warningf(nil, "[APIClient] Received 429 Too Many Requests. Waiting (attempt %d/%d)...", i+1, maxRetries)
			// 关闭当前的响应体
			func() {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()

			waitTime := time.Duration(1<<(i)) * time.Second // 指数退避 (1s, 2s, 4s...)
			if waitTime > 30*time.Second { // 设置最大等待时间
				waitTime = 30 * time.Second
			}
			log.Warningf(nil, "[APIClient] Waiting %s before retry.", waitTime)
			time.Sleep(waitTime)
			continue // 继续循环，重试请求

		case http.StatusOK, http.StatusCreated, http.StatusNoContent, http.StatusPartialContent:
			// 2xx 成功状态码
			return resp, nil

		default:
			// 其他非 2xx 错误，直接返回
			bodyBytes, _ := io.ReadAll(resp.Body) // 尝试读取响应体以获取更多错误信息
			func() {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}() // 关闭响应体
			return nil, fmt.Errorf("API request failed with status %d: %s, body: %s", resp.StatusCode, resp.Status, string(bodyBytes))
		}
	}

	return nil, fmt.Errorf("max retries (%d) exceeded for request to %s", maxRetries, req.URL.Path)
}