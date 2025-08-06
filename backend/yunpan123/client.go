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
func NewAPIClient(ctx context.Context, baseURL string, tm *tokenmanager.Manager) *APIClient {
	// 对于数据传输，我们需要一个更长的超时
	return &APIClient{
		metaClient:             &http.Client{
									// 设置一个总的请求超时。
									Timeout: 8 * time.Second,},
		uploadClient:           &http.Client{
									// 我们给一个更宽裕的时间
									Timeout: 10 * time.Minute,

									Transport: &http.Transport{
										DialContext: (&net.Dialer{
											Timeout:   10 * time.Second, // 连接时间可以稍长一点
											KeepAlive: 90 * time.Second,
										}).DialContext,

										TLSHandshakeTimeout: 10 * time.Second,

										// 响应头超时也需要更长，因为服务器可能需要一些时间来接收和校验分片。
										ResponseHeaderTimeout: 60 * time.Second,

										// 允许更多的并发上传连接
										MaxIdleConns:        100,
										MaxIdleConnsPerHost: 20, // 假设你可能并发上传多个分片
										IdleConnTimeout:     90 * time.Second,
										
										ExpectContinueTimeout: 1 * time.Second,},},
		downloadClient:         &http.Client{
									// !!! 关键：不设置总超时 (Timeout: 0) !!!
									// 因为我们不知道文件有多大，下载可能需要几小时。
									// 超时控制完全交给底层的Transport。
									Timeout: 0, 

									Transport: &http.Transport{
										// 连接和握手依然要快，快速判断服务是否可用
										DialContext: (&net.Dialer{
											Timeout:   10 * time.Second,
											KeepAlive: 90 * time.Second,
										}).DialContext,
										TLSHandshakeTimeout: 10 * time.Second,

										// 核心：设置一个合理的响应头超时。
										// 一旦服务器开始发送数据，我们就可以一直等下去。
										ResponseHeaderTimeout: 60 * time.Second, // 1分钟内服务器必须开始响应

										// 下载任务通常是长连接，可以设置较长的空闲超时
										MaxIdleConns:        100,
										MaxIdleConnsPerHost: 10,
										IdleConnTimeout:     90 * time.Second,},},
		BaseURL:      baseURL,
		TokenManager: tm,
	}
}

// Do sends a request for JSON-based APIs (metadata).
func (c *APIClient) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	var (
		resp      *http.Response
		err       error
		lastError error
	)

	// 添加最新的认证头
	token := c.TokenManager.GetToken()
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Platform", "open_platform")

	// 使用元数据客户端发送请求
	resp, err = c.metaClient.Do(req)
	if err != nil {
		lastError = fmt.Errorf("request failed: %w", err)
		fs.Debugf(nil, "API metadata request error : %v", err)
	}
	
	return resp, err
}

// DoUpload sends a request for data transfer (upload). It does not read the response body by default.
func (c *APIClient) DoUpload(ctx context.Context, req *http.Request) (*http.Response, error) {
	var (
		resp      *http.Response
		err       error
		lastError error
	)

	// 添加最新的认证头
	token := c.TokenManager.GetToken()
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Platform", "open_platform")
	
	// 使用上传客户端发送请求
	resp, err = c.uploadClient.Do(req)
	if err != nil {
		lastError = fmt.Errorf("request failed: %w", err)
		fs.Debugf(nil, "API fileupload request error : %v", err)
	}
	return resp, err
}

// DoDownload sends a request for data transfer (download). It does not read the response body by default.
func (c *APIClient) DoDownload(ctx context.Context, req *http.Request) (*http.Response, error) {
	var (
		resp      *http.Response
		err       error
		lastError error
	)
	
	// 使用下载客户端发送请求
	resp, err = c.downloadClient.Do(req)
	if err != nil {
		lastError = fmt.Errorf("request failed: %w", err)
		fs.Debugf(nil, "API filedownload request error : %v", err)
	}
	return resp, err
}