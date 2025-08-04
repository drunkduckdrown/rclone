package tokenmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	//"github.com/rclone/rclone/fs/log"
	"github.com/rclone/rclone/fs"
)

// TokenResponse 结构体，用于解析云函数返回的 JSON
type TokenResponse struct {
	AccessToken string `json:"accessToken"`
	ExpiredAt   string `json:"expiredAt"` // RFC3339 格式的时间字符串
}

// Manager 负责 token 的获取、存储和刷新
type Manager struct {
	cloudFunctionURL    string // 基础 URL，例如 https://your-domain.com
	cloudFunctionAuthToken string // 新名称：云函数鉴权令牌
	accessToken         string
	expireTime          time.Time // 解析后的过期时间
	mu                  sync.RWMutex // 保护 accessToken 和 expireTime
	httpClient          *http.Client // 用于调用云函数的 HTTP 客户端
	stopChan            chan struct{} // 用于停止自动刷新协程
}

// NewManager 创建一个新的 TokenManager 实例
func NewManager(cloudFunctionURL, cloudFunctionAuthToken string) *Manager { // 参数名称也更新
	return &Manager{
		cloudFunctionURL:    cloudFunctionURL,
		cloudFunctionAuthToken: cloudFunctionAuthToken, // 新名称
		httpClient: &http.Client{
			Timeout: 10 * time.Second, // 调用云函数超时时间
		},
		stopChan: make(chan struct{}),
	}
}

// callCloudFunction 发送请求到云函数并解析响应
func (m *Manager) callCloudFunction(endpoint string) (*TokenResponse, error) {
	fullURL := fmt.Sprintf("%s%s", m.cloudFunctionURL, endpoint)
	fs.Debugf(nil, "[TokenManager] Calling cloud function: %s", fullURL)

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create token request: %w", err)
	}

	// *** 关键修改：在请求头中添加 Bearer 鉴权令牌 ***
	req.Header.Set("Authorization", "Bearer "+m.cloudFunctionAuthToken)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call cloud function %s: %w", endpoint, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("cloud function %s returned non-200 status: %d - %s", endpoint, resp.StatusCode, string(bodyBytes))
	}

	var tokenResp TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("failed to decode token response from %s: %w", endpoint, err)
	}

	return &tokenResp, nil
}

// GetAndStoreToken 从云函数获取 token 并更新本地存储
// endpoint 参数可以是 "/get_token" 或 "/renew_token"
func (m *Manager) GetAndStoreToken(endpoint string) error {
	tokenResp, err := m.callCloudFunction(endpoint)
	if err != nil {
		return err
	}

	// 解析 expiredAt 字符串为 time.Time
	parsedTime, err := time.Parse(time.RFC3339, tokenResp.ExpiredAt)
	if err != nil {
		return fmt.Errorf("failed to parse expiredAt time '%s': %w", tokenResp.ExpiredAt, err)
	}

	m.mu.Lock()
	m.accessToken = tokenResp.AccessToken
	// 提前5分钟过期，给网络延迟和处理留出余地
	//m.expireTime = parsedTime.Add(-5 * time.Minute)
	m.expireTime = parsedTime
	m.mu.Unlock()

	fs.Debugf(nil, "[TokenManager] Token updated successfully from %s, expires at %s (actual: %s)",
		endpoint, m.expireTime.Format(time.RFC3339), parsedTime.Format(time.RFC3339))
	return nil
}

// GetToken 返回当前有效的 token
func (m *Manager) GetToken() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.accessToken
}

// IsTokenExpiredSoon 检查 token 是否即将过期 (例如，距离过期前半小时)
func (m *Manager) IsTokenExpiredSoon() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// 如果本地没有 token，也算作即将过期，需要获取
	if m.accessToken == "" || m.expireTime.IsZero() {
		return true
	}
	// 距离过期前半小时
	return time.Now().After(m.expireTime.Add(-30 * time.Minute))
}

// StartAutoRefresh 启动一个 goroutine 定期检查并刷新 token
func (m *Manager) StartAutoRefresh(ctx context.Context) {
	ticker := time.NewTicker(25 * time.Minute) // 每25分钟检查一次
	defer ticker.Stop()

	fs.Debugf(nil, "[TokenManager] Auto-refresh goroutine started.")

	for {
		select {
		case <-ticker.C:
			if m.IsTokenExpiredSoon() {
				fs.Debugf(nil, "[TokenManager] Token expiring soon or not set, attempting auto-refresh via /get_token...")
				if err := m.GetAndStoreToken("/get_token"); err != nil {
					fs.Errorf(nil, "[TokenManager] Auto-refresh failed: %v", err)
				}
			}
		case <-m.stopChan:
			fs.Debugf(nil, "[TokenManager] Auto-refresh stopped via stopChan.")
			return
		case <-ctx.Done(): // rclone context 取消时停止
			fs.Debugf(nil, "[TokenManager] Context cancelled, stopping auto-refresh.")
			return
		}
	}
}

// StopAutoRefresh 停止自动刷新协程
func (m *Manager) StopAutoRefresh() {
	close(m.stopChan)
}