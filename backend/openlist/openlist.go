// Package openlist implements an rclone backend for OpenList.
package openlist

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	defaultPacerMinSleep = fs.Duration(50 * time.Millisecond)
	maxSleep             = fs.Duration(2 * time.Second)
	decayConstant        = 2 // bigger for slower decay, exponential
	// API endpoint constants from OpenList documentation.
	apiLogin  = "/api/auth/login/hash"
	apiList   = "/api/fs/list"
	apiGet    = "/api/fs/get"
	apiMe     = "/api/me"
	apiMkdir  = "/api/fs/mkdir"
	apiRemove = "/api/fs/remove"
	// 新增：用于高级功能的API端点
	apiMove   = "/api/fs/move"
	apiCopy   = "/api/fs/copy"
	apiRename = "/api/fs/rename"
	apiForm   = "/api/fs/form" // 用于获取直传URL
)

// Register the backend with rclone
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "openlist",
		Description: "OpenList",
		NewFs:       NewFs,
		Options: []fs.Option{
			{
				Name:     "url",
				Help:     "URL of the OpenList server (e.g. https://your-openlist.example.com)",
				Required: true,
			},
			{
				Name:     "username",
				Help:     "Username for OpenList",
				Required: false,
			},
			{
				Name:       "password",
				Help:       "Password for OpenList",
				Required:   false,
				IsPassword: true,
			},
			{
				Name:     "root_path",
				Help:     "Root path within the OpenList server",
				Required: false,
				Default:  "/",
			},
			{
				Name:     "cf_server",
				Help:     "URL of the Cloudflare solver server (e.g. alist-helper)",
				Required: false,
				Default:  "",
			},
			{
				Name:     "pacer_min_sleep",
				Help:     "Minimum sleep time between API requests",
				Advanced: true,
				Default:  defaultPacerMinSleep,
			},
			{
				Name:     "otp_code",
				Help:     "Two-factor authentication code",
				Default:  "",
				Advanced: true,
			},
			{
				Name:     "meta_pass",
				Help:     "Meta password for listing encrypted directories",
				Default:  "",
				Advanced: true,
			},
			{
				Name:     config.ConfigEncoding,
				Help:     config.ConfigEncodingHelp,
				Advanced: true,
				Default: (encoder.EncodeLtGt |
					encoder.EncodeLeftSpace |
					encoder.EncodeCtl |
					encoder.EncodeSlash |
					encoder.EncodeRightSpace |
					encoder.EncodeInvalidUtf8),
			},
			{
				Name:     "user_agent",
				Help:     "Custom User-Agent string to use (overridden by Cloudflare if cf_server is set)",
				Advanced: true,
				Default:  "",
			},
		},
	})
}

// Options defines the configuration for this backend.
type Options struct {
	URL           string      `config:"url"`
	Username      string      `config:"username"`
	Password      string      `config:"password"`
	PacerMinSleep fs.Duration `config:"pacer_min_sleep"`
	OTPCode       string      `config:"otp_code"`
	MetaPass      string      `config:"meta_pass"`
	RootPath      string      `config:"root_path"`
	CfServer      string      `config:"cf_server"`
	UserAgent     string      `config:"user_agent"`
}

// Fs represents a remote OpenList server.
type Fs struct {
	name            string
	root            string
	opt             Options
	features        *fs.Features
	token           string
	tokenMu         sync.Mutex
	srv             *rest.Client
	pacer           *fs.Pacer
	fileListCacheMu sync.Mutex
	fileListCache   map[string]listResponse

	userPermission int
	// cfCookies and cfCookieExpiry store Cloudflare cookies per host.
	cfCookies      map[string]*http.Cookie
	cfCookieExpiry map[string]time.Time
	cfUserAgent    string
	cfMu           sync.Mutex

	// The underlying HTTP client used to build rest.Client.
	httpClient *http.Client
}

// API response structures.
type loginResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Token string `json:"token"`
	} `json:"data"`
}

type meResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Permission int `json:"permission"`
	} `json:"data"`
}

type fileInfo struct {
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	IsDir    bool      `json:"is_dir"`
	Modified time.Time `json:"modified"`
	HashInfo *struct {
		MD5    string `json:"md5,omitempty"`
		SHA1   string `json:"sha1,omitempty"`
		SHA256 string `json:"sha256,omitempty"`
	} `json:"hash_info"`
	RawURL string `json:"raw_url"`
}

type listResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Content []fileInfo `json:"content"`
		Total   int        `json:"total"`
	} `json:"data"`
}

type requestResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Object describes an OpenList object.
type Object struct {
	fs        *Fs
	remote    string
	size      int64
	modTime   time.Time
	md5sum    string
	sha1sum   string
	sha256sum string
}

// 新增：用于 /api/fs/form 接口的响应结构体
type formResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		URL    string      `json:"url"`
		Header http.Header `json:"header"`
		Method string      `json:"method"` // 通常是 PUT 或 POST
	} `json:"data"`
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the Fs
func (f *Fs) String() string {
	return fmt.Sprintf("OpenList remote %s:%s", f.name, f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// newClientWithPacer creates an HTTP client using fs.AddConfig to override the
// User-Agent from Options.
func newClientWithPacer(ctx context.Context, opt *Options) *http.Client {
	newCtx, ci := fs.AddConfig(ctx)
	ci.UserAgent = opt.UserAgent
	return fshttp.NewClient(newCtx)
}

// NewFs constructs an Fs from the path, container:path.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}

	opt.URL = strings.TrimSuffix(opt.URL, "/")
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	if opt.RootPath != "" && opt.RootPath != "/" {
		root = path.Join(opt.RootPath, root)
	}

	f := &Fs{
		name:           name,
		root:           root,
		opt:            *opt,
		fileListCache:  make(map[string]listResponse),
		cfCookies:      make(map[string]*http.Cookie),
		cfCookieExpiry: make(map[string]time.Time),
	}

	if f.opt.CfServer != "" {
		if err := f.fetchUserAgent(ctx); err != nil {
			fs.Infof(ctx, "Warning: failed to fetch CF user agent: %v", err)
		} else {
			fs.Infof(ctx, "Using CF user agent: %s", f.cfUserAgent)
		}
	}

	client := newClientWithPacer(ctx, opt)
	f.httpClient = client
	f.srv = rest.NewClient(client).SetRoot(opt.URL)
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(opt.PacerMinSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))

	if f.opt.Username != "" && f.opt.Password != "" {
		if err := f.login(ctx); err != nil {
			return nil, fmt.Errorf("login failed: %w", err)
		}
	} else {
		f.token = ""
	}

	var meResp meResponse
	err := f.doCFRequestMust(ctx, "GET", apiMe, nil, &meResp)
	if err != nil && f.opt.CfServer != "" {
		if fetchErr := f.fetchCloudflare(ctx, f.opt.URL); fetchErr == nil {
			err = f.doCFRequestMust(ctx, "GET", apiMe, nil, &meResp)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve user permissions: %w", err)
	}
	f.userPermission = meResp.Data.Permission

	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)

	// ========================================================================
	// FIXED: This is the corrected section that resolves the compilation errors.
	// ========================================================================
	var getResp struct {
		Data fileInfo `json:"data"`
	}
	err = f.doCFRequestMust(ctx, "POST", apiGet, map[string]string{"path": f.root}, &getResp)
	if err == nil && !getResp.Data.IsDir {
		// It's a file. Adjust the root to its parent directory.
		newRoot := path.Dir(f.root)
		// The root of the Fs is now the parent directory.
		f.root = newRoot
		// Return fs.ErrorIsFile to signal to the rclone core that the path was a file.
		// The core will then wrap this Fs to present it as a single file remote.
		return f, fs.ErrorIsFile
	}
	// If it's a directory or an error occurred (e.g., path not found),
	// return the Fs object as is. The rclone core will handle other errors.
	return f, nil
}

// IMPORTANT: This password hashing function is taken directly from the AList backend.
// OpenList, as a fork, MIGHT use a different salt. If authentication fails,
// this is the most likely place to fix. The salt is the string appended
// to the password before hashing. You may need to find the correct salt
// from the OpenList source code or documentation.
// The original AList salt is "-https://github.com/alist-org/alist".
func (f *Fs) makePasswordHash(password string) string {
	password += "-https://github.com/alist-org/alist"
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

// login performs authentication and stores the token.
func (f *Fs) login(ctx context.Context) error {
	f.tokenMu.Lock()
	defer f.tokenMu.Unlock()

	if f.opt.Username == "" || f.opt.Password == "" {
		return nil
	}
	pw, err := obscure.Reveal(f.opt.Password)
	if err != nil {
		return fmt.Errorf("password decode failed - did you obscure it?: %w", err)
	}
	data := map[string]string{
		"username": f.opt.Username,
		"password": f.makePasswordHash(pw),
		"otpcode":  f.opt.OTPCode,
	}
	var loginResp loginResponse
	if err := f.doCFRequestMust(ctx, "POST", apiLogin, data, &loginResp); err != nil {
		return err
	}
	f.token = loginResp.Data.Token
	fs.Debugf(f, "Login successful, token received.")
	return nil
}

// domainMatch checks if a host matches a cookie domain according to RFC 6265
func (f *Fs) domainMatch(host, cookieDomain string) bool {
	if cookieDomain == "" {
		return host == cookieDomain
	}
	if strings.HasPrefix(cookieDomain, ".") {
		cookieDomain = cookieDomain[1:]
	}
	return host == cookieDomain || strings.HasSuffix(host, "."+cookieDomain)
}

// findCookiesForHost returns all matching cookies for the given host
func (f *Fs) findCookiesForHost(host string) []*http.Cookie {
	var cookies []*http.Cookie
	now := time.Now()

	for domain, cookie := range f.cfCookies {
		expiry, ok := f.cfCookieExpiry[domain]
		if !ok || now.After(expiry) {
			continue
		}
		if f.domainMatch(host, domain) {
			cookies = append(cookies, cookie)
		}
	}
	return cookies
}

// doCFRequest is the central function for all HTTP requests.
// It handles auth tokens, Cloudflare cookies, and retries.
func (f *Fs) doCFRequest(req *http.Request) (*http.Response, error) {
	apiBase, err := url.Parse(f.opt.URL)
	if err == nil && req.URL.Host == apiBase.Host && f.token != "" {
		req.Header.Set("Authorization", f.token)
	}

	if f.opt.CfServer != "" {
		f.cfMu.Lock()
		host := req.URL.Host
		matchingCookies := f.findCookiesForHost(host)
		if len(matchingCookies) == 0 || time.Now().After(f.cfCookieExpiry[matchingCookies[0].Domain].Add(-1*time.Minute)) {
			if err := f.fetchCloudflare(req.Context(), req.URL.String()); err != nil {
				f.cfMu.Unlock()
				return nil, fmt.Errorf("failed to refresh CF cookies: %w", err)
			}
			matchingCookies = f.findCookiesForHost(host)
		}
		for _, cookie := range matchingCookies {
			req.AddCookie(cookie)
		}
		f.cfMu.Unlock()
	}

	var clientFunc func(*http.Request) (*http.Response, error)
	if err == nil && req.URL.Host == apiBase.Host {
		clientFunc = f.srv.Do
	} else {
		clientFunc = f.httpClient.Do
	}

	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		var errPacer error
		resp, errPacer = clientFunc(req)
		return shouldRetry(resp, errPacer)
	})
	if err != nil {
		return nil, err
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	_ = resp.Body.Close()

	if resp.StatusCode == 403 && f.opt.CfServer != "" {
		f.cfMu.Lock()
		if err := f.fetchCloudflare(req.Context(), req.URL.String()); err != nil {
			f.cfMu.Unlock()
			return nil, fmt.Errorf("failed to refresh CF cookies on 403: %w", err)
		}
		f.cfMu.Unlock()
		newReq, err := http.NewRequestWithContext(req.Context(), req.Method, req.URL.String(), nil)
		if err != nil {
			return nil, err
		}
		for k, v := range req.Header {
			newReq.Header[k] = v
		}
		return f.doCFRequest(newReq)
	}

	resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	return resp, nil
}

// shouldRetry returns true if err != nil or the HTTP status code is 429 or 5xx.
func shouldRetry(resp *http.Response, err error) (bool, error) {
	if err != nil {
		return true, err
	}
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return true, fmt.Errorf("got status code %d", resp.StatusCode)
	}
	return false, nil
}

// doCFRequestStream is for streaming response bodies (e.g., downloads).
func (f *Fs) doCFRequestStream(req *http.Request) (*http.Response, error) {
	apiBase, err := url.Parse(f.opt.URL)
	if err == nil && req.URL.Host == apiBase.Host && f.token != "" {
		req.Header.Set("Authorization", f.token)
	}

	if f.opt.CfServer != "" {
		f.cfMu.Lock()
		host := req.URL.Host
		matchingCookies := f.findCookiesForHost(host)
		if len(matchingCookies) == 0 || (len(matchingCookies) > 0 && time.Now().After(f.cfCookieExpiry[matchingCookies[0].Domain].Add(-1*time.Minute))) {
			if err := f.fetchCloudflare(req.Context(), req.URL.String()); err != nil {
				f.cfMu.Unlock()
				return nil, fmt.Errorf("failed to refresh CF cookies: %w", err)
			}
			matchingCookies = f.findCookiesForHost(host)
		}
		for _, cookie := range matchingCookies {
			req.AddCookie(cookie)
		}
		f.cfMu.Unlock()
	}

	var clientFunc func(*http.Request) (*http.Response, error)
	if err == nil && req.URL.Host == apiBase.Host {
		clientFunc = f.srv.Do
	} else {
		clientFunc = f.httpClient.Do
	}

	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		var errPacer error
		resp, errPacer = clientFunc(req)
		return shouldRetry(resp, errPacer)
	})
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 403 {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if f.opt.CfServer != "" {
			f.cfMu.Lock()
			if err := f.fetchCloudflare(req.Context(), req.URL.String()); err != nil {
				f.cfMu.Unlock()
				return nil, fmt.Errorf("failed to refresh CF cookies on 403: %w", err)
			}
			f.cfMu.Unlock()
		}
		newReq, err := http.NewRequestWithContext(req.Context(), req.Method, req.URL.String(), nil)
		if err != nil {
			return nil, err
		}
		for k, v := range req.Header {
			newReq.Header[k] = v
		}
		return f.doCFRequestStream(newReq)
	}

	return resp, nil
}

// doCFRequestMust performs a request and unmarshals the JSON response.
func (f *Fs) doCFRequestMust(ctx context.Context, method, endpoint string, data, response interface{}) error {
	var reqBody io.Reader
	if data != nil {
		jsonData, err := json.Marshal(data)
		if err != nil {
			return err
		}
		reqBody = bytes.NewBuffer(jsonData)
	}
	req, err := http.NewRequestWithContext(ctx, method, f.opt.URL+endpoint, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/plain, */*")

	resp, err := f.doCFRequest(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(bodyBytes, response); err != nil {
		return fmt.Errorf("failed to unmarshal json response: %w. Response body: %s", err, string(bodyBytes))
	}

	// Check for API-level errors
	if err := f.handleResponse(response); err != nil {
		if err.Error() == "unauthorized access" && (f.opt.Username != "" && f.opt.Password != "") {
			fs.Debugf(f, "Token expired or invalid, attempting to re-login.")
			if loginErr := f.login(ctx); loginErr != nil {
				return fmt.Errorf("token renewal failed: %w (original error: %v)", loginErr, err)
			}
			// Retry the original request after successful login
			fs.Debugf(f, "Re-login successful, retrying original request.")
			return f.doCFRequestMust(ctx, method, endpoint, data, response)
		}
		return err
	}
	return nil
}

// handleResponse checks the API response for error codes.
func (f *Fs) handleResponse(response interface{}) error {
	v := reflect.ValueOf(response).Elem()
	if v.Kind() != reflect.Struct {
		return nil
	}
	codeField := v.FieldByName("Code")
	messageField := v.FieldByName("Message")
	if !codeField.IsValid() || !messageField.IsValid() {
		return nil
	}
	code := codeField.Int()
	message := messageField.String()
	if code != 200 {
		if code == 401 {
			return fmt.Errorf("unauthorized access")
		}
		return fmt.Errorf("API error: %s (code: %d)", message, code)
	}
	return nil
}

// fileInfoToDirEntry converts an API fileInfo to an rclone DirEntry.
func (f *Fs) fileInfoToDirEntry(item fileInfo, dir string) fs.DirEntry {
	remote := path.Join(dir, item.Name)
	if item.IsDir {
		return fs.NewDir(remote, item.Modified)
	}
	var md5sum, sha1sum, sha256sum string
	if item.HashInfo != nil {
		md5sum = item.HashInfo.MD5
		sha1sum = item.HashInfo.SHA1
		sha256sum = item.HashInfo.SHA256
	}
	return &Object{
		fs:        f,
		remote:    remote,
		size:      item.Size,
		modTime:   item.Modified,
		md5sum:    md5sum,
		sha1sum:   sha1sum,
		sha256sum: sha256sum,
	}
}

// List lists the objects and directories in dir.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	if cached, ok := f.getCachedList(dir); ok {
		for _, item := range cached.Data.Content {
			entries = append(entries, f.fileInfoToDirEntry(item, dir))
		}
		return entries, nil
	}
	data := map[string]interface{}{
		"path":     path.Join(f.root, dir),
		"per_page": 0, // 0 means all
		"page":     1,
		"password": f.opt.MetaPass,
	}
	if f.userPermission >= 2 { // Admin or owner
		data["refresh"] = true
	}
	var listResp listResponse
	if err = f.doCFRequestMust(ctx, "POST", apiList, data, &listResp); err != nil {
		return nil, err
	}
	f.setCachedList(dir, listResp)
	for _, item := range listResp.Data.Content {
		entries = append(entries, f.fileInfoToDirEntry(item, dir))
	}
	return entries, nil
}

// 修改：Put 函数现在实现了直传到存储。
// 它首先向 OpenList 请求一个预签名的 URL，然后将数据直接上传到该 URL。
// 这确保了上传流量不会经过云函数。
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	remote := src.Remote()
	size := src.Size()
	modTime := src.ModTime(ctx)

	// 第1步：从 OpenList 获取直传 URL
	fs.Debugf(f, "正在为 %s 请求直传 URL", remote)
	fullPath := path.Join(f.root, remote)
	var formResp formResponse
	err := f.doCFRequestMust(ctx, "POST", apiForm, map[string]string{"path": fullPath}, &formResp)
	if err != nil {
		return nil, fmt.Errorf("获取上传表单URL失败: %w", err)
	}
	if formResp.Data.URL == "" {
		return nil, fmt.Errorf("API 未返回上传 URL")
	}

	uploadURL := formResp.Data.URL
	uploadMethod := formResp.Data.Method
	if uploadMethod == "" {
		uploadMethod = "PUT" // 如果API未指定，则默认为 PUT
	}

	fs.Debugf(f, "已收到直传 URL: %s, 方法: %s", uploadURL, uploadMethod)

	// 第2步：将文件直接上传到存储提供商的 URL
	req, err := http.NewRequestWithContext(ctx, uploadMethod, uploadURL, in)
	if err != nil {
		return nil, fmt.Errorf("创建直传请求失败: %w", err)
	}
	req.ContentLength = size
	req.Header.Set("Content-Length", fmt.Sprintf("%d", size))

	// 复制 /api/fs/form 响应中提供的请求头
	for key, values := range formResp.Data.Header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	// 如果 form API 没有提供，确保设置了 Content-Type
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	// 使用标准的 HTTP 客户端执行此请求，因为它不访问 OpenList API
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("直传失败: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取直传响应体失败: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("直传失败，状态码 %d: %s", resp.StatusCode, string(bodyBytes))
	}

	fs.Debugf(f, "文件 %s 直传成功", remote)

	// 使父目录的缓存失效
	parentDir := path.Dir(remote)
	f.invalidateCache(parentDir)

	// 创建并返回新的对象
	return &Object{
		fs:      f,
		remote:  remote,
		size:    size,
		modTime: modTime,
	}, nil
}

// 新增：Move 函数，实现单个对象的服务器端移动。
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcFs, ok := src.Fs().(*Fs)
	if !ok {
		return nil, fmt.Errorf("无法从不同的远程类型 %T 移动对象", src.Fs())
	}
	if srcFs.name != f.name {
		return nil, fmt.Errorf("无法从不同的远程 %s:%s 移动对象", srcFs.name, srcFs.root)
	}

	srcPath := src.Remote()
	dstPath := remote

	srcDir := path.Join(f.root, path.Dir(srcPath))
	dstDir := path.Join(f.root, path.Dir(dstPath))
	srcName := path.Base(srcPath)
	dstName := path.Base(dstPath)

	// 情况1：在同一目录内重命名
	if srcDir == dstDir {
		fs.Debugf(f, "在目录 %s 中，重命名 %s 为 %s", srcDir, srcName, dstName)
		data := map[string]string{
			"path": path.Join(srcDir, srcName),
			"name": dstName,
		}
		var renameResp requestResponse
		err := f.doCFRequestMust(ctx, "POST", apiRename, data, &renameResp)
		if err != nil {
			return nil, fmt.Errorf("重命名失败: %w", err)
		}
	} else {
		// 情况2：移动到不同目录
		fs.Debugf(f, "移动 %s 从 %s 到 %s", srcName, srcDir, dstDir)
		data := map[string]interface{}{
			"src_dir": srcDir,
			"dst_dir": dstDir,
			"names":   []string{srcName},
		}
		var moveResp requestResponse
		err := f.doCFRequestMust(ctx, "POST", apiMove, data, &moveResp)
		if err != nil {
			return nil, fmt.Errorf("移动失败: %w", err)
		}

		// 如果文件名也改变了，在移动后执行重命名
		if srcName != dstName {
			fs.Debugf(f, "移动后，在目录 %s 中，重命名 %s 为 %s", dstDir, srcName, dstName)
			renameData := map[string]string{
				"path": path.Join(dstDir, srcName),
				"name": dstName,
			}
			var renameResp requestResponse
			err := f.doCFRequestMust(ctx, "POST", apiRename, renameData, &renameResp)
			if err != nil {
				// 理想情况下应尝试移回，但暂时只返回错误
				return nil, fmt.Errorf("移动后重命名失败: %w", err)
			}
		}
	}

	f.invalidateCache(path.Dir(srcPath))
	f.invalidateCache(path.Dir(dstPath))

	// 创建一个新对象来代表移动后的文件
	newObj, err := f.NewObject(ctx, dstPath)
	if err != nil {
		// 如果出错，则根据源信息回退创建对象
		return &Object{
			fs:      f,
			remote:  dstPath,
			size:    src.Size(),
			modTime: src.ModTime(ctx),
		}, nil
	}
	return newObj, nil
}

// 新增：Copy 函数，实现单个对象的服务器端复制。
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcFs, ok := src.Fs().(*Fs)
	if !ok {
		return nil, fmt.Errorf("无法从不同的远程类型 %T 复制对象", src.Fs())
	}
	if srcFs.name != f.name {
		return nil, fmt.Errorf("无法从不同的远程 %s:%s 复制对象", srcFs.name, srcFs.root)
	}

	srcPath := src.Remote()
	dstPath := remote

	srcDir := path.Join(f.root, path.Dir(srcPath))
	dstDir := path.Join(f.root, path.Dir(dstPath))
	srcName := path.Base(srcPath)
	dstName := path.Base(dstPath)

	fs.Debugf(f, "复制 %s 从 %s 到 %s", srcName, srcDir, dstDir)
	data := map[string]interface{}{
		"src_dir": srcDir,
		"dst_dir": dstDir,
		"names":   []string{srcName},
	}
	var copyResp requestResponse
	err := f.doCFRequestMust(ctx, "POST", apiCopy, data, &copyResp)
	if err != nil {
		return nil, fmt.Errorf("复制失败: %w", err)
	}

	// 如果文件名也改变了，在复制后执行重命名
	if srcName != dstName {
		fs.Debugf(f, "复制后，在目录 %s 中，重命名 %s 为 %s", dstDir, srcName, dstName)
		renameData := map[string]string{
			"path": path.Join(dstDir, srcName),
			"name": dstName,
		}
		var renameResp requestResponse
		err := f.doCFRequestMust(ctx, "POST", apiRename, renameData, &renameResp)
		if err != nil {
			return nil, fmt.Errorf("复制后重命名失败: %w", err)
		}
	}

	f.invalidateCache(path.Dir(dstPath))

	newObj, err := f.NewObject(ctx, dstPath)
	if err != nil {
		return &Object{
			fs:      f,
			remote:  dstPath,
			size:    src.Size(),
			modTime: src.ModTime(ctx),
		}, nil
	}
	return newObj, nil
}

// Mkdir creates a directory.
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	data := map[string]string{
		"path": path.Join(f.root, dir),
	}
	var mkdirResp requestResponse
	err := f.doCFRequestMust(ctx, "POST", apiMkdir, data, &mkdirResp)
	if err == nil {
		f.invalidateCache(path.Dir(dir))
	}
	return err
}

// Rmdir removes an empty directory.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	// AList's remove API can remove an empty directory by specifying its name in a parent dir.
	parent := path.Dir(dir)
	name := path.Base(dir)
	data := map[string]interface{}{
		"dir":   path.Join(f.root, parent),
		"names": []string{name},
	}
	var removeResp requestResponse
	err := f.doCFRequestMust(ctx, "POST", apiRemove, data, &removeResp)
	if err == nil {
		f.invalidateCache(dir)
		f.invalidateCache(parent)
	}
	return err
}

// Purge removes a directory and all its contents.
func (f *Fs) Purge(ctx context.Context, dir string) error {
	// This is not directly supported by the simple remove API.
	// Rclone will call this on a directory, so we can treat it like Rmdir.
	// AList's WebDAV handles this, but the core API is more basic.
	// For simplicity, we just try to remove the directory itself.
	// If it's not empty, the API will likely return an error.
	return f.Rmdir(ctx, dir)
}

// --- Object Methods ---

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info { return o.fs }

// Remote returns the remote path
func (o *Object) Remote() string { return o.remote }

// Size returns the size of the object
func (o *Object) Size() int64 { return o.size }

// ModTime returns the modification time
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }

// SetModTime is not supported
func (o *Object) SetModTime(ctx context.Context, t time.Time) error { return fs.ErrorCantSetModTime }

// Storable returns true as this is a storable object
func (o *Object) Storable() bool { return true }

// Open retrieves the raw download URL and streams the file content.
// This is the key function for download traffic passthrough.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	data := map[string]string{
		"path": path.Join(o.fs.root, o.remote),
	}
	var getResp struct {
		Data struct {
			RawURL string `json:"raw_url"`
		} `json:"data"`
	}
	if err := o.fs.doCFRequestMust(ctx, "POST", apiGet, data, &getResp); err != nil {
		return nil, fmt.Errorf("failed to get raw_url: %w", err)
	}
	if getResp.Data.RawURL == "" {
		return nil, fmt.Errorf("API did not return a raw_url for %s", o.remote)
	}

	fs.Debugf(o, "Opening from raw_url: %s", getResp.Data.RawURL)

	req, err := http.NewRequestWithContext(ctx, "GET", getResp.Data.RawURL, nil)
	if err != nil {
		return nil, err
	}
	fs.FixRangeOption(options, o.size)
	fs.OpenOptionAddHTTPHeaders(req.Header, options)
	if o.size == 0 {
		delete(req.Header, "Range")
	}

	// Use the streaming helper
	response, err := o.fs.doCFRequestStream(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		_ = response.Body.Close()
		return nil, fmt.Errorf("failed to open object: status code %d on raw_url", response.StatusCode)
	}
	return response.Body, nil
}

// Update updates the object with new content
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	_, err := o.fs.Put(ctx, in, src, options...)
	return err
}

// Remove removes the object
func (o *Object) Remove(ctx context.Context) error {
	data := map[string]interface{}{
		"dir":   path.Join(o.fs.root, path.Dir(o.remote)),
		"names": []string{path.Base(o.remote)},
	}
	var removeResp requestResponse
	if err := o.fs.doCFRequestMust(ctx, "POST", apiRemove, data, &removeResp); err != nil {
		return err
	}
	o.fs.invalidateCache(path.Dir(o.remote))
	return nil
}

// Hash returns the hash of a type
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	switch ty {
	case hash.MD5:
		return o.md5sum, nil
	case hash.SHA1:
		return o.sha1sum, nil
	case hash.SHA256:
		return o.sha256sum, nil
	default:
		return "", hash.ErrUnsupported
	}
}

// String returns a string representation of the object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// --- Fs utility methods ---

// Hashes returns the supported hash types.
func (f *Fs) Hashes() hash.Set {
	return hash.NewHashSet(hash.MD5, hash.SHA1, hash.SHA256)
}

// Precision returns the modification time precision.
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// NewObject finds the Object at remote.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	dir := path.Dir(remote)
	if dir == "." {
		dir = ""
	}
	entries, err := f.List(ctx, dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Remote() == remote {
			if obj, ok := entry.(*Object); ok {
				return obj, nil
			}
		}
	}
	return nil, fs.ErrorObjectNotFound
}

// getCachedList retrieves a cached directory listing.
func (f *Fs) getCachedList(dir string) (listResponse, bool) {
	f.fileListCacheMu.Lock()
	defer f.fileListCacheMu.Unlock()
	cached, ok := f.fileListCache[dir]
	return cached, ok
}

// setCachedList caches a directory listing.
func (f *Fs) setCachedList(dir string, resp listResponse) {
	f.fileListCacheMu.Lock()
	defer f.fileListCacheMu.Unlock()
	f.fileListCache[dir] = resp
}

// invalidateCache deletes the cached listing for a directory.
func (f *Fs) invalidateCache(dir string) {
	f.fileListCacheMu.Lock()
	defer f.fileListCacheMu.Unlock()
	delete(f.fileListCache, dir)
}

// fetchCloudflare contacts the CF server to obtain cookies.
func (f *Fs) fetchCloudflare(ctx context.Context, targetURL string) error {
	reqURL := fmt.Sprintf("%s/get-cookies?url=%s", f.opt.CfServer, url.QueryEscape(targetURL))
	resp, err := http.Get(reqURL)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	var cookieMap map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&cookieMap); err != nil {
		return err
	}
	if len(cookieMap) == 0 {
		return fmt.Errorf("no cookies received from cf_server")
	}

	var cookieName, cookieValue string
	for k, v := range cookieMap {
		cookieName = k
		cookieValue = v
		break
	}

	parsed, err := url.Parse(targetURL)
	if err != nil {
		return err
	}
	host := parsed.Host
	expiry := time.Now().Add(30 * time.Minute)
	cfCookie := &http.Cookie{Name: cookieName, Value: cookieValue, Domain: host, Path: "/", Expires: expiry}

	f.cfCookies[host] = cfCookie
	f.cfCookieExpiry[host] = expiry
	return nil
}

// fetchUserAgent retrieves the user agent from the CF server.
func (f *Fs) fetchUserAgent(ctx context.Context) error {
	reqURL := fmt.Sprintf("%s/get-ua", f.opt.CfServer)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	var data struct {
		UserAgent string `json:"user_agent"`
		Error     string `json:"error"`
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		return err
	}
	if data.Error != "" {
		return fmt.Errorf("cfserver error: %s", data.Error)
	}
	f.cfUserAgent = data.UserAgent
	f.opt.UserAgent = data.UserAgent
	return nil
}

// --- 接口满足性检查 ---
// 修改：将 Mover 和 Copier 添加到检查列表中。
var (
	_ fs.Fs     = (*Fs)(nil)
	_ fs.Object = (*Object)(nil)
	_ fs.Mover  = (*Fs)(nil) // 新增
	_ fs.Copier = (*Fs)(nil) // 新增
)
