package cloud123

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"
	"encoding/json"
	"net/http"
	"net/url" // 新增导入
	"path"    // 新增导入
	"strconv" // 新增导入
	"strings" // 新增导入
	"sync"
	"errors"
	"mime/multipart"
	"crypto/md5"
	"encoding/hex"
	
	

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/config/configmap"
	//"github.com/rclone/rclone/fs/log"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"

	// 导入你的 tokenmanager 包，路径需要与你的 go.mod 模块路径一致
	"github.com/rclone/rclone/backend/yunpan123/tokenmanager"
)

// Fs represents the 123 cloud drive backend
type Fs struct {
	name     string            // rclone remote 的名称 (例如 "my123pan")
	root     string            // 用户配置的根路径 (例如 "/MyFiles")
	pacer    *pacer.Pacer      // rclone 提供的限速器，用于控制 API 请求频率
	client   *APIClient      // 你的 123 云盘 API 客户端
	tokenMgr *tokenmanager.Manager // 你的 token 管理器实例
	features *fs.Features // rclone 后端支持的特性
	
	pathCache    map[string]*cacheEntry // *** 路径对应的id的缓存 ***
	pathCacheMu  sync.RWMutex
	cacheTTL     time.Duration // *** 新增：缓存的生命周期 ***
	
	uploadDomain          string
	uploadDomainExpiresAt time.Time // 上传域名的过期时间
	uploadDomainMu        sync.Mutex  // 保护上传域名的读写
}

// cacheEntry 存储目录ID和其过期时间
type cacheEntry struct {
	id        int64
	expiresAt time.Time
}

// NewFs initializes the 123 cloud drive backend
// 这是 rclone 调用以创建文件系统实例的入口
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// 1. 从 configmap.Mapper 中读取配置参数
	cloudFunctionURL := m.Get("cloud_function_url")
	cloudFunctionAuthToken := m.Get("cloud_function_auth_token") // 新名称
	apiBaseURL := m.Get("api_base_url")

	// 检查必要参数是否已提供
	if cloudFunctionURL == "" {
		return nil, fmt.Errorf("cloud_function_url is not set in rclone config")
	}
	if cloudFunctionAuthToken == "" { // 新名称
		return nil, fmt.Errorf("cloud_function_auth_token is not set in rclone config")
	}
	if apiBaseURL == "" {
		return nil, fmt.Errorf("api_base_url is not set in rclone config")
	}

	// 2. 初始化你的 TokenManager
	fs.Debugf(nil, "[123CloudFs] Initializing TokenManager with URL: %s", cloudFunctionURL)
	tokenMgr := tokenmanager.NewManager(cloudFunctionURL, cloudFunctionAuthToken) // 传递新名称的参数

	// 3. 首次获取 token (使用 /get_token)
	fs.Debugf(nil, "[123CloudFs] Attempting to get initial token from cloud function...")
	err := tokenMgr.GetAndStoreToken("/get_token")
	if err != nil {
		return nil, fmt.Errorf("failed to get initial token from cloud function: %w", err)
	}

	// 4. 启动后台 token 刷新协程
	go tokenMgr.StartAutoRefresh(ctx)
	fs.Debugf(nil, "[123CloudFs] Token auto-refresh goroutine started.")

	// 5. 初始化你的 API 客户端
	fs.Debugf(nil, "[123CloudFs] Initializing APIClient with BaseURL: %s", apiBaseURL)
	apiClient := NewAPIClient(apiBaseURL, tokenMgr)

	// 6. 初始化 Fs 结构体
	f := &Fs{
		name:     name,
		root:     root,
		pacer:    pacer.New().SetMinSleep(10 * time.Millisecond),
		client:   apiClient,
		tokenMgr: tokenMgr,
		pathCache:    make(map[string]*cacheEntry),
		cacheTTL:     10 * time.Second, // *** 设置缓存TTL为10秒 ***
	}
	// 根目录的缓存永不过期，或者给一个很长的过期时间
	f.pathCache[""] = &cacheEntry{id: 0, expiresAt: time.Now().Add(50 * time.Year)}
	
	// 启动后台缓存清理协程
	go f.startCacheCleaner(ctx)


	// 7. 定义后端支持的特性 (后续会详细实现)
	f.features = (&fs.Features{
		CanListR:      false,
		ReadMimeType:  false,
		Put:           f.put,
		Mkdir:         f.mkdir,
		Rmdir:         f.rmdir,
		Delete:        f.delete,
		Purge:         f.purge,
		Move:          f.move,
		// Rename:        f.rename,
		About:         f.about,
	}).Fill(ctx, f)

	fs.Debugf(nil, "[123CloudFs] Backend initialized successfully.")
	return f, nil
}

// ------------------------------------------------------------------------------------
// 以下是 Fs 结构体需要实现的核心 rclone 接口方法
// 暂时只是空壳，后续逐步填充

// startCacheCleaner 定期清理过期的缓存条目
func (f *Fs) startCacheCleaner(ctx context.Context) {
	// 每隔一段时间（例如5分钟）运行一次清理
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.pathCacheMu.Lock()
			initialSize := len(f.pathCache)
			for path, entry := range f.pathCache {
				// 不要清理根目录
				if path != "" && time.Now().After(entry.expiresAt) {
					delete(f.pathCache, path)
				}
			}
			cleanedSize := len(f.pathCache)
			f.pathCacheMu.Unlock()
			if initialSize > cleanedSize {
				fs.Debugf(f, "[PathCache] Cleaned %d expired entries from cache. Current size: %d", initialSize-cleanedSize, cleanedSize)
			}
		case <-ctx.Done(): // 当 rclone 退出时，停止协程
			fs.Debugf(f, "[PathCache] Cache cleaner stopped.")
			return
		}
	}
}

// listDir 获取指定目录ID的一页内容，并机会主义地更新路径缓存
func (f *Fs) listDir(ctx context.Context, parentID int64, parentPath string, lastFileID int64) (*FileListV2Response, error) {
	// 构建请求参数
	q := url.Values{}
	q.Set("parentFileId", strconv.FormatInt(parentID, 10))
	q.Set("limit", "100")
	if lastFileID > 0 {
		q.Set("lastFileId", strconv.FormatInt(lastFileID, 10))
	}

	// 构建请求
	apiEndpoint := "/api/v2/file/list"
	req, err := http.NewRequestWithContext(ctx, "GET", f.client.BaseURL+apiEndpoint+"?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create list request: %w", err)
	}

	// 发送请求
	resp, err := f.client.Do(ctx, req, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to call list api: %w", err)
	}
	defer resp.Body.Close()

	// 解析 JSON
	var respData FileListV2Response
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return nil, fmt.Errorf("failed to decode list response: %w", err)
	}

	// 检查业务状态码
	if respData.Code != 0 {
		return nil, fmt.Errorf("list api returned an error: code=%d, msg='%s'", respData.Code, respData.Message)
	}

	// *** 核心：集中化的缓存写入逻辑 ***
	f.pathCacheMu.Lock()
	defer f.pathCacheMu.Unlock()
	for i := range respData.Data.FileList {
		fileInfo := &respData.Data.FileList[i]
		if fileInfo.Type == 1 { // 只缓存目录
			remotePath := path.Join(parentPath, fileInfo.Filename)
			entry, exists := f.pathCache[remotePath]
			if !exists || time.Now().After(entry.expiresAt) {
				fs.Debugf(f, "[PathCache] Caching/Updating path '%s' -> ID %d from listDir", remotePath, fileInfo.FileId)
				f.pathCache[remotePath] = &cacheEntry{
					id:        fileInfo.FileId,
					expiresAt: time.Now().Add(f.cacheTTL),
				}
			}
		}
	}

	return &respData, nil
}

// pathToID 将一个路径字符串解析为其对应的目录ID，并使用带TTL的缓存
func (f *Fs) pathToID(ctx context.Context, dirPath string) (int64, error) {
	dirPath = strings.TrimRight(dirPath, "/")

	// 1. 检查缓存
	f.pathCacheMu.RLock()
	entry, found := f.pathCache[dirPath]
	f.pathCacheMu.RUnlock()

	if found && time.Now().Before(entry.expiresAt) {
		fs.Debugf(f, "[PathCache] Cache hit for path '%s' -> ID %d", dirPath, entry.id)
		return entry.id, nil
	}

	// 2. 缓存未命中或已过期，查找父路径
	parentPath, baseName := path.Split(strings.TrimRight(dirPath, "/"))
	parentPath = strings.TrimRight(parentPath, "/")

	parentID, err := f.pathToID(ctx, parentPath) // 递归调用
	if err != nil {
		return 0, err
	}

	// 3. 从已知的父ID开始查找，调用新的 listDir
	lastFileID := int64(0)
	for {
		// *** 注意这里的调用变化：传入了 parentPath ***
		respData, err := f.listDir(ctx, parentID, parentPath, lastFileID)
		if err != nil {
			return 0, err
		}

		// 在返回的数据中寻找我们需要的那个目录
		for _, fileInfo := range respData.Data.FileList {
			if fileInfo.Filename == baseName && fileInfo.Type == 1 {
				// 找到了！listDir 已经帮我们把这个 ID 和它的同级目录都缓存了。
				return fileInfo.FileId, nil
			}
		}

		lastFileID = respData.Data.LastFileId
		if lastFileID == -1 {
			return 0, fs.ErrorDirNotFound
		}
	}
}

// List the objects and directories in dir into entries
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	fs.Debugf(f, "[123CloudFs] Listing directory: %s", dir)

	// 1. 将路径解析为目录ID (此步骤会利用缓存)
	parentID, err := f.pathToID(ctx, dir)
	if err != nil {
		return nil, err
	}
	fs.Debugf(f, "[123CloudFs] Path '%s' resolved to ID: %d", dir, parentID)

	// 2. 循环分页获取所有条目
	entries = fs.DirEntries{}
	lastFileID := int64(0)
	for {
		respData, err := f.listDir(ctx, parentID, dir, lastFileID)
		if err != nil {
			return nil, err
		}

		// 3. 遍历当前页的 fileList，将 API 对象转换为 rclone 对象
		for i := range respData.Data.FileList {
			fileInfo := &respData.Data.FileList[i]

			if fileInfo.Trashed != 0 {
				continue // 跳过回收站中的文件
			}

			remotePath := path.Join(dir, fileInfo.Filename)

			if fileInfo.Type == 1 { // 这是一个目录
				// *** 核心修改：解析目录的 updateAt 时间 ***
				modTime, err := time.Parse("2006-01-02 15:04:05", fileInfo.UpdateAt)
				if err != nil {
					fs.Errorf(f, "Failed to parse directory mod time '%s' for %s: %v", fileInfo.UpdateAt, remotePath, err)
					modTime = time.Now() // 解析失败时给一个默认值
				}

				// 使用解析出的真实修改时间
				d := fs.NewDir(remotePath, modTime).SetID(strconv.FormatInt(fileInfo.FileId, 10))
				entries = append(entries, d)
			} else { // 这是一个文件
				o, err := newObject(ctx, f, remotePath, fileInfo)
				if err != nil {
					fs.Errorf(f, "Failed to create object for %s: %v", remotePath, err)
					continue
				}
				entries = append(entries, o)
			}
		}

		// 4. 检查是否需要继续分页
		lastFileID = respData.Data.LastFileId
		if lastFileID == -1 {
			break // 已经是最后一页
		}
	}

	return entries, nil
}

// Name returns the name of the remote
func (f *Fs) Name() string {
	return f.name
}

// Root returns the root path of the remote
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("123 Cloud Drive root '%s'", f.root)
}

// Features returns the optional features of the Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision returns the precision of modtimes in this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns the supported hash types
func (f *Fs) Hashes() fs.HashSet {
	return fs.HashSet(0)
}

// Pacer returns the pacer for this Fs
func (f *Fs) Pacer() *pacer.Pacer {
	return f.pacer
}

// About gets quota information from the Fs
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	fs.Debugf(nil, "[123CloudFs] Getting About information...")

	// 1. 构建请求
	// Endpoint 来自你的文档
	apiEndpoint := "/api/v1/user/info"
	req, err := http.NewRequestWithContext(ctx, "GET", f.client.BaseURL+apiEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create about request: %w", err)
	}

	// 2. 发送请求 (client.Do 会自动添加 Authorization 和 Platform 头)
	resp, err := f.client.Do(ctx, req, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to call about api: %w", err)
	}
	defer resp.Body.Close()

	// 3. 解析 JSON 响应
	var respData UserInfoResponse // 使用我们定义的结构体
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return nil, fmt.Errorf("failed to decode about response: %w", err)
	}

	// 4. 检查业务状态码 (文档中 code=0 表示成功)
	if respData.Code != 0 {
		return nil, fmt.Errorf("about api returned an error: code=%d, msg='%s'", respData.Code, respData.Msg)
	}

	// 5. 创建并填充 rclone 的 fs.Usage 结构体
	totalSpace := respData.Data.SpacePermanent + respData.Data.SpaceTemp
	usedSpace := respData.Data.SpaceUsed

	usage := &fs.Usage{
		Total: fs.NewUsageValue(totalSpace), // 总空间 = 永久空间 + 临时空间
		Used:  fs.NewUsageValue(usedSpace),  // 已用空间
	}

	fs.Debugf(nil, "[123CloudFs] About successful: Total=%s, Used=%s", usage.Total, usage.Used)
	return usage, nil
}

// NewObject finds the Object at remote.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fs.Debugf(nil, "[123CloudFs] Getting object metadata for: %s", remote)
	return nil, fs.ErrorNotImplemented
}

// Mkdir makes the directory
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	fs.Debugf(f, "Mkdir called for path: %s", dir)

	// 1. 分割路径，获取父目录路径和新目录名称
	parentPath, newDirName := path.Split(strings.TrimRight(dir, "/"))
	parentPath = strings.TrimRight(parentPath, "/")

	// 2. 获取父目录的ID
	parentID, err := f.pathToID(ctx, parentPath)
	if err != nil {
		fs.Errorf(f, "Failed to find parent directory for Mkdir: %v", err)
		return err // 如果父目录不存在，则无法创建
	}

	// 3. 检查目录是否已存在 (这是必需的，因为API不允许重名创建)
	_, err = f.pathToID(ctx, dir)
	if err == nil {
		fs.Debugf(f, "Directory '%s' already exists, returning nil as per rclone spec.", dir)
		return nil // 目录已存在，Mkdir 应该成功返回
	}
	// 确保错误是 DirNotFound，否则可能是其他网络或API问题
	if err != fs.ErrorDirNotFound {
		fs.Errorf(f, "Unexpected error during pre-existence check for Mkdir: %v", err)
		return err
	}

	// 4. 构建请求体
	reqBody := MkdirRequest{
		ParentID: parentID,
		Name:     newDirName,
	}
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal mkdir request body: %w", err)
	}

	// 5. 构建并发送请求
	apiEndpoint := "/upload/v1/file/mkdir"
	req, err := http.NewRequestWithContext(ctx, "POST", f.client.BaseURL+apiEndpoint, bytes.NewReader(reqBytes))
	if err != nil {
		return fmt.Errorf("failed to create mkdir request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.client.Do(ctx, req, reqBytes)
	if err != nil {
		return fmt.Errorf("failed to call mkdir api: %w", err)
	}
	defer resp.Body.Close()

	// 6. 解析响应
	var respData MkdirResponse
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return fmt.Errorf("failed to decode mkdir response: %w", err)
	}

	// 7. 处理业务逻辑错误
	// 根据文档，code=1 表示目录已存在。
	// 尽管我们已经预先检查过，但多一层保护可以处理并发创建的竞态条件。
	if respData.Code == 1 {
		fs.Warningf(f, "Directory '%s' was created by another process concurrently. Treating as success.", dir)
		// 即使API报错，我们也需要尝试获取并缓存这个已存在的目录的ID
		// 我们可以通过再次调用pathToID来强制刷新缓存
		_, _ = f.pathToID(ctx, dir)
		return nil
	}
	if respData.Code != 0 {
		return fmt.Errorf("mkdir api returned an error: code=%d, msg='%s'", respData.Code, respData.Message)
	}

	// 8. 成功后，立即更新缓存
	newDirID := respData.Data.DirID
	f.pathCacheMu.Lock()
	fs.Debugf(f, "[PathCache] Caching newly created directory '%s' -> ID %d", dir, newDirID)
	f.pathCache[dir] = &cacheEntry{
		id:        newDirID,
		expiresAt: time.Now().Add(f.cacheTTL),
	}
	f.pathCacheMu.Unlock()

	return nil
}


// getUploadDomain 获取并缓存上传域名，并设置10分钟的TTL
func (f *Fs) getUploadDomain(ctx context.Context) (string, error) {
	f.uploadDomainMu.Lock()
	defer f.uploadDomainMu.Unlock()

	// 如果已有缓存且未过期，直接返回
	if f.uploadDomain != "" && time.Now().Before(f.uploadDomainExpiresAt) {
		return f.uploadDomain, nil
	}

	fs.Debugf(f, "Upload domain cache is empty or expired, fetching a new one...")

	// 构建请求
	apiEndpoint := "/upload/v2/file/domain"
	req, err := http.NewRequestWithContext(ctx, "GET", f.client.BaseURL+apiEndpoint, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create upload domain request: %w", err)
	}

	// 发送请求
	resp, err := f.client.Do(ctx, req, nil)
	if err != nil {
		return "", fmt.Errorf("failed to call upload domain api: %w", err)
	}
	defer resp.Body.Close()

	// 解析响应
	var respData UploadDomainResponse
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return "", fmt.Errorf("failed to decode upload domain response: %w", err)
	}

	if respData.Code != 0 {
		return "", fmt.Errorf("upload domain api returned an error: code=%d, msg='%s'", respData.Code, respData.Message)
	}

	if len(respData.Data) == 0 {
		return "", errors.New("upload domain api returned no domains")
	}

	// 缓存并返回第一个域名
	uploadDomain := respData.Data[0]
	f.uploadDomain = uploadDomain
	f.uploadDomainExpiresAt = time.Now().Add(10 * time.Minute) // 设置10分钟的过期时间
	fs.Debugf(f, "Fetched and cached new upload domain: %s (expires in 10 minutes)", uploadDomain)

	return f.uploadDomain, nil
}

// putSingle handles the upload of small files in a single request.
func (f *Fs) putSingle(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {

	// 1. 获取父目录ID
	parentPath, fileName := path.Split(src.Remote())
	parentPath = strings.TrimRight(parentPath, "/")
	parentID, err := f.pathToID(ctx, parentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to find parent directory for put: %w", err)
	}

	// 2. 获取上传域名
	uploadDomain, err := f.getUploadDomain(ctx)
	if err != nil {
		return nil, err
	}

	// 3. 计算 MD5 哈希 (rclone 会自动为我们提供)
	hashes := src.Hash(ctx, fs.HashMD5)
	md5sum, ok := hashes[fs.HashMD5]
	if !ok || md5sum == "" {
		return nil, errors.New("MD5 hash is required for upload but was not provided")
	}

	// 4. 构建 multipart/form-data 请求体
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// 添加表单字段
	_ = writer.WriteField("parentFileID", strconv.FormatInt(parentID, 10))
	_ = writer.WriteField("filename", fileName)
	_ = writer.WriteField("etag", md5sum)
	_ = writer.WriteField("size", strconv.FormatInt(size, 10))
	_ = writer.WriteField("duplicate", "1") // 策略1: 保留两者，新文件自动加后缀

	// 添加文件流
	part, err := writer.CreateFormFile("file", fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}
	_, err = io.Copy(part, in)
	if err != nil {
		return nil, fmt.Errorf("failed to copy file stream to form: %w", err)
	}
	err = writer.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	// 5. 构建并发送请求
	apiEndpoint := "/upload/v2/file/single/create"
	fullUploadURL := uploadDomain + apiEndpoint

	req, err := http.NewRequestWithContext(ctx, "POST", fullUploadURL, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create single upload request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// 使用我们自己的 f.client.Do，它会自动添加 Authorization 和 Platform 头
	// 并传递请求体字节以便重试
	reqBodyBytes := body.Bytes()
	resp, err := f.client.Do(ctx, req, reqBodyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to call single upload api: %w", err)
	}
	defer resp.Body.Close()

	// 6. 解析响应
	var respData SingleUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
		return nil, fmt.Errorf("failed to decode single upload response: %w", err)
	}

	if respData.Code != 0 {
		return nil, fmt.Errorf("single upload api returned an error: code=%d, msg='%s'", respData.Code, respData.Message)
	}
	if !respData.Data.Completed {
		return nil, errors.New("single upload api reported upload was not completed")
	}

	// 7. 创建并返回新的 Object
	// API 没有返回完整的 FileInfoV2，所以我们自己构建一个
	newFileInfo := &FileInfoV2{
		FileId:       respData.Data.FileID,
		Filename:     fileName,
		ParentFileId: parentID,
		Type:         0, // 文件类型
		Etag:         md5sum,
		Size:         size,
		UpdateAt:     time.Now().Format("2006-01-02 15:04:05"),
	}

	return newObject(ctx, f, src.Remote(), newFileInfo)
}

// Chunk represents a piece of the file to be uploaded.
type chunk struct {
	data   []byte
	number int
}

// putChunked handles the upload of large files using multipart uploading.
func (f *Fs) putChunked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// --- 步骤 1: 创建文件，获取上传参数或秒传 ---
	parentPath, fileName := path.Split(src.Remote())
	parentPath = strings.TrimRight(parentPath, "/")
	parentID, err := f.pathToID(ctx, parentPath)
	if err != nil {
		return nil, fmt.Errorf("chunked upload: failed to find parent directory: %w", err)
	}

	hashes := src.Hash(ctx, fs.HashMD5)
	md5sum, ok := hashes[fs.HashMD5]
	if !ok || md5sum == "" {
		return nil, errors.New("chunked upload: MD5 hash is required but was not provided")
	}

	createReqBody := ChunkedUploadCreateRequest{
		ParentFileID: parentID,
		Filename:     fileName,
		Etag:         md5sum,
		Size:         src.Size(),
		Duplicate:    1,
	}
	reqBytes, err := json.Marshal(createReqBody)
	if err != nil {
		return nil, fmt.Errorf("chunked upload: failed to marshal create request: %w", err)
	}

	createReq, err := http.NewRequestWithContext(ctx, "POST", f.client.BaseURL+"/upload/v2/file/create", bytes.NewReader(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("chunked upload: failed to create request: %w", err)
	}
	createReq.Header.Set("Content-Type", "application/json")

	createResp, err := f.client.Do(ctx, createReq, reqBytes)
	if err != nil {
		return nil, fmt.Errorf("chunked upload: failed to call create api: %w", err)
	}
	defer createResp.Body.Close()

	var createRespData ChunkedUploadCreateResponse
	if err := json.NewDecoder(createResp.Body).Decode(&createRespData); err != nil {
		return nil, fmt.Errorf("chunked upload: failed to decode create response: %w", err)
	}
	if createRespData.Code != 0 {
		return nil, fmt.Errorf("chunked upload: create api error: code=%d, msg='%s'", createRespData.Code, createRespData.Message)
	}

	// 检查是否秒传成功
	if createRespData.Data.Reuse {
		fs.Debugf(src, "Chunked upload completed via rapid upload (reuse).")
		fileInfo := &FileInfoV2{
			FileId:       createRespData.Data.FileID,
			Filename:     fileName,
			ParentFileId: parentID,
			Type:         0,
			Etag:         md5sum,
			Size:         src.Size(),
			UpdateAt:     time.Now().Format("2006-01-02 15:04:05"),
		}
		return newObject(ctx, f, src.Remote(), fileInfo)
	}

	// --- 步骤 2: 并发上传分片 (线程安全版) ---
	preuploadID := createRespData.Data.PreuploadID
	sliceSize := createRespData.Data.SliceSize
	uploadServer := createRespData.Data.Servers[0]

	var wg sync.WaitGroup
	const numUploaders = 5
	errChan := make(chan error, numUploaders)
	chunkChan := make(chan chunk, numUploaders)

	// 启动 worker goroutines
	for i := 0; i < numUploaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range chunkChan {
				// 计算分片MD5
				hasher := md5.New()
				hasher.Write(c.data)
				sliceMD5 := hex.EncodeToString(hasher.Sum(nil))

				// 构建并发送分片上传请求
				body := &bytes.Buffer{}
				writer := multipart.NewWriter(body)
				_ = writer.WriteField("preuploadID", preuploadID)
				_ = writer.WriteField("sliceNo", strconv.Itoa(c.number))
				_ = writer.WriteField("sliceMD5", sliceMD5)
				part, _ := writer.CreateFormFile("slice", fileName)
				_, _ = part.Write(c.data)
				writer.Close()

				sliceReq, _ := http.NewRequestWithContext(ctx, "POST", uploadServer+"/upload/v2/file/slice", body)
				sliceReq.Header.Set("Content-Type", writer.FormDataContentType())

				sliceResp, err := f.client.Do(ctx, sliceReq, body.Bytes())
				if err != nil {
					errChan <- fmt.Errorf("chunk #%d: request failed: %w", c.number, err)
					return
				}
				
				// 检查HTTP状态码
				if sliceResp.StatusCode >= 400 {
					bodyBytes, _ := io.ReadAll(sliceResp.Body)
					errChan <- fmt.Errorf("chunk #%d: upload failed with status %s, body: %s", c.number, sliceResp.Status, string(bodyBytes))
					sliceResp.Body.Close()
					return
				}
				sliceResp.Body.Close()

				fs.Debugf(src, "Successfully uploaded chunk #%d", c.number)
			}
		}()
	}

	// 主goroutine: 顺序读取文件，并将数据块分发给workers
	chunkNumber := 1
	for {
		buffer := make([]byte, sliceSize)
		n, err := io.ReadFull(in, buffer)

		if n > 0 {
			select {
			case chunkChan <- chunk{data: buffer[:n], number: chunkNumber}:
				chunkNumber++
			case uploadErr := <-errChan:
				// 如果在分发时有worker出错，立即停止
				close(chunkChan)
				return nil, fmt.Errorf("chunked upload stopped due to an error: %w", uploadErr)
			}
		}

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // 正常结束
			}
			// 真实读取错误
			close(chunkChan)
			return nil, fmt.Errorf("failed to read from source file: %w", err)
		}
	}
	close(chunkChan) // 所有块已分发完毕

	wg.Wait() // 等待所有worker完成
	close(errChan)

	// 检查是否有上传错误
	if len(errChan) > 0 {
		return nil, fmt.Errorf("chunked upload: one or more chunks failed to upload: %w", <-errChan)
	}

	// --- 步骤 3: 完成上传并轮询结果 ---
	completeReqBody := ChunkedUploadCompleteRequest{PreuploadID: preuploadID}
	reqBytes, _ = json.Marshal(completeReqBody)
	
	var finalFileID int64 = -1 // 默认为-1，表示未知
	const pollTimeout = 15 * time.Second
	const pollInterval = 1 * time.Second
	ctx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done(): // 轮询超时
			fs.Warningf(src, "Polling for upload completion timed out after %v. Assuming success and returning an optimistic object.", pollTimeout)
			goto end_poll // 使用goto跳出循环，清晰明了
		default:
		}

		completeReq, _ := http.NewRequest("POST", f.client.BaseURL+"/upload/v2/file/upload_complete", bytes.NewReader(reqBytes))
		completeReq.Header.Set("Content-Type", "application/json")
		
		completeResp, err := f.client.Do(ctx, completeReq, reqBytes)
		if err != nil {
			// 如果是上下文超时错误，则正常处理；否则是真实错误
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return nil, fmt.Errorf("chunked upload: failed to call complete api: %w", err)
		}
		
		var completeRespData ChunkedUploadCompleteResponse
		if err := json.NewDecoder(completeResp.Body).Decode(&completeRespData); err != nil {
			completeResp.Body.Close()
			return nil, fmt.Errorf("chunked upload: failed to decode complete response: %w", err)
		}
		completeResp.Body.Close()

		if completeRespData.Data.Completed {
			finalFileID = completeRespData.Data.FileID
			fs.Debugf(src, "Upload completion confirmed. Final file ID: %d", finalFileID)
			goto end_poll
		}

		time.Sleep(pollInterval)
	}

end_poll:
	// 使用最终的fileID（可能是-1）创建并返回对象
	fileInfo := &FileInfoV2{
		FileId:       finalFileID,
		Filename:     fileName,
		ParentFileId: parentID,
		Type:         0,
		Etag:         md5sum,
		Size:         src.Size(),
		UpdateAt:     time.Now().Format("2006-01-02 15:04:05"),
	}
	return newObject(ctx, f, src.Remote(), fileInfo)
}

// Put uploads a file to the remote
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	size := src.Size()

	// 检查文件大小，决定上传策略。未知大小 (-1) 的文件也走分片上传。
	if size < 0 || size > 200*1024*1024 { // 小于0或大于 200 MiB
		fs.Debugf(src, "File size is %d, using chunked upload.", size)
		return f.putChunked(ctx, in, src, options...)
	}

	fs.Debugf(src, "File size is %d, using single part upload.", size)
	return f.putSingle(ctx, in, src, options...)
}

// trashItems a list of file or directory IDs.
// This is the shared helper function for Delete, Rmdir, and Purge.
func (f *Fs) trashItems(ctx context.Context, fileIDs []int64) error {
	if len(fileIDs) == 0 {
		return nil // Nothing to do
	}

	// API 一次最多处理100个，我们需要分块
	const maxIDsPerRequest = 100
	for i := 0; i < len(fileIDs); i += maxIDsPerRequest {
		end := i + maxIDsPerRequest
		if end > len(fileIDs) {
			end = len(fileIDs)
		}
		batch := fileIDs[i:end]

		reqBody := TrashRequest{FileIDs: batch}
		bodyBytes, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("trash failed: failed to marshal request: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, "POST", f.client.BaseURL+"/api/v1/file/trash", bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("trash failed: failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := f.client.Do(ctx, req, bodyBytes)
		if err != nil {
			return fmt.Errorf("trash failed: api call failed for ids %v: %w", batch, err)
		}
		
		// 检查响应，我们可以复用一个通用响应结构体或直接解析
		var trashResp struct { Code int `json:"code"` }
		err = json.NewDecoder(resp.Body).Decode(&trashResp)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("trash failed: failed to decode response for ids %v: %w", batch, err)
		}
		if trashResp.Code != 0 {
			return fmt.Errorf("trash failed: api returned error for ids %v, code: %d", batch, trashResp.Code)
		}
	}

	fs.Debugf(f, "Trashed %d items, path cache cleared.", len(fileIDs))

	return nil
}


// clearPathCacheFor removes cache entries for the given remote path and all its children.
func (f *Fs) clearPathCacheFor(remote string) {
	f.pathCacheMu.Lock()
	defer f.pathCacheMu.Unlock()

	// 确保路径格式统一，以便前缀匹配
	// remote 可能是 "path/to/dir" 或 "path/to/file.txt"
	prefix := remote
	if remote != "" && !strings.HasSuffix(remote, "/") {
		prefix += "/"
	}

	fs.Debugf(f, "Clearing path cache for remote '%s' and its children (prefix: '%s')", remote, prefix)

	// 遍历缓存，删除匹配的条目
	for k := range f.pathCache {
		// 删除自身 ("path/to/dir") 和所有子孙 ("path/to/dir/...")
		if k == remote || strings.HasPrefix(k, prefix) {
			delete(f.pathCache, k)
			fs.Debugf(f, "Cache entry removed: %s", k)
		}
	}
}

// Delete removes a single file.
func (f *Fs) Delete(ctx context.Context, o fs.Object) error {
	obj, ok := o.(*Object)
	if !ok {
		return fmt.Errorf("not a cloud123 object: %T", o)
	}

	fs.Debugf(obj, "Deleting file")
	err := f.trashItems(ctx, []int64{obj.fileInfo.FileId})
	if err != nil {
		return err
	}
	
	// 精确清理该文件的缓存
	f.clearPathCacheFor(obj.Remote())
	return nil
}

// Rmdir removes a directory.
func (f *Fs) Rmdir(ctx context.Context, remote string) error {
	fs.Debugf(f, "Rmdir on %s", remote)
	dirID, err := f.pathToID(ctx, remote)
	if err != nil {
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return nil
		}
		return fmt.Errorf("rmdir: failed to find directory '%s': %w", remote, err)
	}

	if dirID == 0 {
		return errors.New("cannot remove root directory")
	}

	err = f.trashItems(ctx, []int64{dirID})
	if err != nil {
		return err
	}

	// 精确清理该目录及其子孙的缓存
	f.clearPathCacheFor(remote)
	return nil
}

// Purge removes a directory and all its contents.
func (f *Fs) Purge(ctx context.Context, remote string) error {
	fs.Debugf(f, "Purging %s", remote)
	// Purge 和 Rmdir 的逻辑在这里是相同的，因为API支持删除非空目录
	return f.Rmdir(ctx, remote)
}

// Rename renames a file or directory.
func (f *Fs) Rename(ctx context.Context, o fs.Object, newName string) (fs.Object, error) {
	srcObj, ok := o.(*Object)
	if !ok {
		return nil, fmt.Errorf("not a cloud123 object: %T", o)
	}

	fs.Debugf(srcObj, "Renaming to '%s'", newName)

	reqBody := RenameRequest{
		FileID:   srcObj.fileInfo.FileId,
		Filename: newName,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil { return nil, fmt.Errorf("rename: failed to marshal request: %w", err) }

	req, err := http.NewRequestWithContext(ctx, "PUT", f.client.BaseURL+"/api/v1/file/name", bytes.NewReader(bodyBytes))
	if err != nil { return nil, fmt.Errorf("rename: failed to create request: %w", err) }
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.client.Do(ctx, req, bodyBytes)
	if err != nil { return nil, fmt.Errorf("rename: api call failed: %w", err) }
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("rename: api returned error status: %s", resp.Status)
	}
	
	// --- 清理缓存并返回新对象 ---
	// 旧路径需要被清理
	f.clearPathCacheFor(srcObj.Remote())

	// 创建新对象
	_, srcLeaf := path.Split(srcObj.Remote())
	newRemote := strings.TrimSuffix(srcObj.Remote(), srcLeaf) + newName

	newFileInfo := srcObj.fileInfo
	newFileInfo.Filename = newName // 更新文件名
	newFileInfo.UpdateAt = time.Now().Format("2006-01-02 15:04:05")

	return newObject(ctx, f, newRemote, newFileInfo)
}


// Move moves and/or renames a file or directory.
// It intelligently calls the move and rename APIs based on the user's intent.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fmt.Errorf("not a cloud123 object: %T", src)
	}

	srcPath, srcLeaf := path.Split(src.Remote())
	dstPath, dstLeaf := path.Split(remote)
	srcPath = strings.TrimRight(srcPath, "/")
	dstPath = strings.TrimRight(dstPath, "/")

	// 判断操作类型
	isMove := (srcPath != dstPath)
	isRename := (srcLeaf != dstLeaf)

	// --- 情况 D: 无操作 ---
	if !isMove && !isRename {
		fs.Debugf(src, "Move: source and destination are identical, doing nothing.")
		return src, nil
	}

	// --- 情况 A: 只移动 ---
	if isMove && !isRename {
		fs.Debugf(src, "Moving from '%s' to '%s'", srcPath, dstPath)
		dstParentID, err := f.pathToID(ctx, dstPath)
		if err != nil {
			return nil, fmt.Errorf("move: failed to find destination directory '%s': %w", dstPath, err)
		}

		reqBody := MoveRequest{
			FileIDs:        []int64{srcObj.fileInfo.FileId},
			ToParentFileID: dstParentID,
		}
		bodyBytes, err := json.Marshal(reqBody)
		if err != nil {
			return nil, fmt.Errorf("move: failed to marshal request: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, "POST", f.client.BaseURL+"/api/v1/file/move", bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, fmt.Errorf("move: failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := f.client.Do(ctx, req, bodyBytes)
		if err != nil {
			return nil, fmt.Errorf("move: api call failed: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("move: api returned error status: %s", resp.Status)
		}

		// 清理旧路径缓存
		f.clearPathCacheFor(src.Remote())

		// 创建并返回新对象
		newFileInfo := srcObj.fileInfo
		newFileInfo.ParentFileId = dstParentID
		newFileInfo.UpdateAt = time.Now().Format("2006-01-02 15:04:05")
		return newObject(ctx, f, remote, newFileInfo)
	}

	// --- 情况 B: 只重命名 ---
	if !isMove && isRename {
		fs.Debugf(src, "Renaming to '%s' in place", dstLeaf)
		return f.Rename(ctx, src, dstLeaf)
	}

	// --- 情况 C: 既移动又重命名 ---
	if isMove && isRename {
		fs.Debugf(src, "Moving from '%s' to '%s' and renaming to '%s'", srcPath, dstPath, dstLeaf)

		// 1. 移动
		dstParentID, err := f.pathToID(ctx, dstPath)
		if err != nil {
			return nil, fmt.Errorf("move-rename: failed to find destination directory '%s': %w", dstPath, err)
		}

		moveReqBody := MoveRequest{
			FileIDs:        []int64{srcObj.fileInfo.FileId},
			ToParentFileID: dstParentID,
		}
		moveBodyBytes, err := json.Marshal(moveReqBody)
		if err != nil {
			return nil, fmt.Errorf("move-rename: failed to marshal move request: %w", err)
		}

		moveReq, err := http.NewRequestWithContext(ctx, "POST", f.client.BaseURL+"/api/v1/file/move", bytes.NewReader(moveBodyBytes))
		if err != nil {
			return nil, fmt.Errorf("move-rename: failed to create move request: %w", err)
		}
		moveReq.Header.Set("Content-Type", "application/json")

		moveResp, err := f.client.Do(ctx, moveReq, moveBodyBytes)
		if err != nil {
			return nil, fmt.Errorf("move-rename: move api call failed: %w", err)
		}
		defer moveResp.Body.Close()

		if moveResp.StatusCode >= 400 {
			return nil, fmt.Errorf("move-rename: move api returned error status: %s", moveResp.Status)
		}

		// 移动成功后，立即清理旧路径缓存
		f.clearPathCacheFor(src.Remote())

		// 2. 重命名
		// 我们现在对同一个文件对象（它的FileId没变）执行重命名操作。
		// f.Rename 方法会处理自己的缓存清理和新对象创建。
		fs.Debugf(src, "Move successful, now renaming to '%s'", dstLeaf)
		return f.Rename(ctx, src, dstLeaf)
	}

	// 这是一个备用错误，理论上永远不会被触发，因为上面的逻辑覆盖了所有情况。
	return nil, errors.New("internal logic error in Move function: no action taken")
}


// open an object for read. It's a two-step process:
// 1. Get a temporary download URL from the API.
// 2. Make a request to that URL, handling Range headers for seeking.
func (f *Fs) open(ctx context.Context, o *Object, options ...fs.OpenOption) (io.ReadCloser, error) {
	// --- 步骤 1: 获取下载 URL ---
	fs.Debugf(o, "Opening file, requesting download URL")

	// 构建请求
	params := url.Values{}
	params.Add("fileId", strconv.FormatInt(o.fileInfo.FileId, 10))
	
	// 注意：这里的 API 是 GET 请求，我们不需要请求体
	req, err := http.NewRequestWithContext(ctx, "GET", f.client.BaseURL+"/api/v1/file/download_info?"+params.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("open: failed to create download info request: %w", err)
	}

	// 使用 f.client.Do 来自动添加认证头
	resp, err := f.client.Do(ctx, req, nil)
	if err != nil {
		return nil, fmt.Errorf("open: failed to call download info api: %w", err)
	}
	defer resp.Body.Close()

	// 解码响应
	var infoResp DownloadInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&infoResp); err != nil {
		return nil, fmt.Errorf("open: failed to decode download info response: %w", err)
	}
	if infoResp.Code != 0 || infoResp.Data.DownloadURL == "" {
		return nil, fmt.Errorf("open: api returned error for download info: code=%d, msg='%s'", infoResp.Code, infoResp.Message)
	}
	
	downloadURL := infoResp.Data.DownloadURL
	fs.Debugf(o, "Got download URL: %s", downloadURL)

	// --- 步骤 2: 向下载 URL 发起请求，处理 Range ---
	downloadReq, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("open: failed to create download request: %w", err)
	}

	// 处理 Range 请求 (断点续传和seek的关键)
	fs.HandleRangeOption(options, downloadReq.Header)

	// **重要**: 这个请求是发往 CDN 的，不需要我们自己的认证头。
	// 所以我们使用底层的 http.Client，而不是 f.client.Do
	downloadResp, err := f.client.Client.Do(downloadReq)
	if err != nil {
		return nil, fmt.Errorf("open: failed to start download: %w", err)
	}

	// 检查 HTTP 错误状态
	if downloadResp.StatusCode < 200 || downloadResp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(downloadResp.Body)
		downloadResp.Body.Close()
		return nil, fmt.Errorf("open: download failed with status %s: %s", downloadResp.Status, string(bodyBytes))
	}
	
	// 成功！返回响应体，它本身就是一个 io.ReadCloser
	return downloadResp.Body, nil
}

// Check the interfaces are satisfied
var (
	// --- Fs an Fs interface ---
	_ fs.Fs      = (*Fs)(nil)
	_ fs.Lister  = (*Fs)(nil) // For List
	_ fs.Abouter = (*Fs)(nil) // For About
	_ fs.Putter  = (*Fs)(nil) // For Put
	_ fs.Mkdirer = (*Fs)(nil) // For Mkdir
	_ fs.Rmdirer = (*Fs)(nil) // For Rmdir (确保这行存在且未被注释)
	_ fs.Deleter = (*Fs)(nil) // For Delete (确保这行存在且未被注释)
    _ fs.Mover   = (*Fs)(nil) // 当我们实现 Move 时，会添加这一行
	// _ fs.Renamer   = (*Fs)(nil) // 当我们实现 Move 时，会添加这一行
    _ fs.Purger  = (*Fs)(nil) // 当我们实现 Purge 时，会添加这一行

	// --- Object an Object interface ---
	_ fs.Object  = (*Object)(nil)
	_ fs.Opener  = (*Object)(nil) // 检查 Object 是否实现了 Open
)