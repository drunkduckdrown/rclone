package yunpan123

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"
	"encoding/json"
	"net"
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
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/fserrors"
	//"github.com/rclone/rclone/fs/config"
	//"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	//"github.com/rclone/rclone/fs/log"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"

	// 导入你的 tokenmanager 包，路径需要与你的 go.mod 模块路径一致
	"github.com/rclone/rclone/backend/yunpan123/tokenmanager"
)

const (
	singleUploadCutoff = 16 * 1024 * 1024
	duplicatePolicyRename = 1 //  1 代表重命名
	duplicatePolicyOverwrite = 2
	//maxTries      = 10
	minSleep      = 10 * time.Millisecond
	maxSleep      = 2 * time.Second
	decayConstant = 2 // bigger for slower decay, exponential
	dir_cacheTTL  = 15 * time.Second
	connection_timeout = 10 * time.Second
	global_timeout     = 15 * time.Second
	upload_timeout     = 15 * time.Minute
	download_timeout   = 0
	set_transfers          = 5
)

type Object struct {
	fs       *Fs
	remote   string
	id       int64 // 将 id 类型改为 int64
	parentFileId int64 // *** 新增字段 ***
	name     string
	size     int64
	modTime  time.Time
	hash     string // 存储 MD5 哈希值
}

// Check the interfaces are satisfied
//var (
//	_ fs.Object = (*Object)(nil)
//)

// newObject 的签名需要改变，以接收 FileInfoV2
func newObject(ctx context.Context, f *Fs, remote string, info *FileInfoV2) (*Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}

	if info != nil {
		o.id = info.FileId
		o.parentFileId = info.ParentFileId // *** 填充新增字段 ***
		o.name = info.Filename
		o.size = info.Size
		o.hash = info.Etag

		// *** 关键：解析自定义时间格式 ***
		// Go 的时间解析布局必须是 "2006-01-02 15:04:05" 这个固定的字符串
		parsedTime, err := time.Parse("2006-01-02 15:04:05", info.UpdateAt)
		if err != nil {
			fs.Errorf(o, "Failed to parse mod time '%s': %v", info.UpdateAt, err)
			o.modTime = time.Now() // 解析失败时给一个默认值
		} else {
			o.modTime = parsedTime
		}
	} else {
		return nil, fmt.Errorf("internal error: newObject called with nil FileInfoV2 for %s", remote)
	}

	return o, nil
}

// MimeType returns the MIME type of the object
//func (o *Object) MimeType(ctx context.Context) string {
//	return o.mimeType
//}

// Fs returns the parent Fs instance
// func (o *Object) Fs() fs.Fs {
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Remote returns the full path of the file on the remote
func (o *Object) Remote() string {
	return o.remote
}

// Hash 方法现在可以返回 MD5 哈希值了
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	if ty == hash.MD5 {
		return o.hash, nil
	}
	return "", hash.ErrUnsupported
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the file
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// String returns
func (o *Object) String() string {
	return o.remote
}

// SetModTime sets the modification time of the file
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	// TODO: 如果 123 云盘 API 支持设置修改时间，则实现此方法
	// 否则返回 fs.ErrorCantSetModTime
	fs.Debugf(nil, "[Object] SetModTime requested for %s to %s", o.remote, t.Format(time.RFC3339))
	return fs.ErrorCantSetModTime // 假设不支持
}

// Storable returns whether the object can be stored
func (o *Object) Storable() bool {
	return true // 默认所有文件都是可存储的
}

// Open opens the file for reading.
// This delegates to Fs.open, which will handle range requests.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.Debugf(nil, "[Object] Opening file %s for read.", o.remote)
	// 委托给 Fs 上的 open 方法，传入自身 (o) 作为参数
	return o.fs.open(ctx, o, options...)
}

// Fs represents the 123 cloud drive backend
type Fs struct {
	name     string            // rclone remote 的名称 (例如 "my123pan")
	root     string            // 用户配置的根路径 (例如 "/MyFiles")
	apiBaseURL string
	opt      Options           // 配置
	ci       *fs.ConfigInfo // global config
	pacer    *fs.Pacer      // rclone 提供的限速器，用于控制 API 请求频率
	//client   *APIClient      // 你的 123 云盘 API 客户端
	rest     *rest.Client      // *** 新增此行 ***
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
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	// 1. 从 opt 中读取配置参数
	cloudFunctionURL := opt.Cloud_function_url
	cloudFunctionAuthToken := opt.Cloud_function_auth_token // 新名称
	apiBaseURL := opt.Api_base_url

	// 检查必要参数是否已提供
	if cloudFunctionURL == "" {
		return nil, fmt.Errorf("Cloud_function_url is not set in rclone config")
	}
	if cloudFunctionAuthToken == "" { // 新名称
		return nil, fmt.Errorf("Cloud_function_auth_token is not set in rclone config")
	}
	if apiBaseURL == "" {
		return nil, fmt.Errorf("Api_base_url is not set in rclone config")
	}

	// 2. 初始化你的 TokenManager
	fs.Debugf(nil, "[123CloudFs] Initializing TokenManager with URL: %s", cloudFunctionURL)
	tokenMgr := tokenmanager.NewManager(cloudFunctionURL, cloudFunctionAuthToken) // 传递新名称的参数

	// 3. 首次获取 token (使用 /get_token)
	fs.Debugf(nil, "[123CloudFs] Attempting to get initial token from cloud function...")
	err = tokenMgr.GetAndStoreToken("/get_token")
	if err != nil {
		return nil, fmt.Errorf("failed to get initial token from cloud function: %w", err)
	}

	// 4. 启动后台 token 刷新协程
	go tokenMgr.StartAutoRefresh(ctx)
	fs.Debugf(nil, "[123CloudFs] Token auto-refresh goroutine started.")

	// 5. 初始化你的 API 客户端
	//fs.Debugf(nil, "[123CloudFs] Initializing APIClient with BaseURL: %s", apiBaseURL)
	//apiClient := NewAPIClient(apiBaseURL, tokenMgr)

	// 6. 初始化 Fs 结构体
	f := &Fs{
		name:            name,
		root:            root,
		apiBaseURL:      apiBaseURL,
		opt:             *opt,
		ci:              fs.GetConfig(ctx),
		pacer:           fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		//client:          apiClient,
		tokenMgr:        tokenMgr,
		pathCache:       make(map[string]*cacheEntry),
		cacheTTL:        dir_cacheTTL, // 设置目录对应id的缓存时间
	}
	// 根目录的缓存永不过期，或者给一个很长的过期时间
	f.pathCache[""] = &cacheEntry{id: 0, expiresAt: time.Now().AddDate(1, 0, 0)}
	
	// 启动后台缓存清理协程
	go f.startCacheCleaner(ctx)
	
	// 创建基础的 rest.Client
	f.rest = rest.NewClient(&http.Client{
		// 设置基础的 Transport，应用全局超时
		Transport: &http.Transport{
			Proxy: nil, // 设置为 nil 即可禁用所有代理
			DialContext: (&net.Dialer{
				Timeout:   connection_timeout, // 连接超时
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		// 设置默认的请求总超时
		Timeout: global_timeout,
	})
	

	// 7. 定义后端支持的特性 (后续会详细实现)
	f.features = (&fs.Features{
		ReadMimeType:  false,
	}).Fill(ctx, f)

	fs.Debugf(nil, "[123CloudFs] Backend initialized successfully.")
	return f, nil
}


// Register with rclone
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "123cloud", // 后端名称，用户在 rclone config 中会看到
		Description: "123 Cloud Drive (123 Cloud Developer)", // 后端描述
		NewFs:       NewFs, // 后端初始化函数，稍后实现
		Options: []fs.Option{{
		// 云函数URL，用于获取和刷新 token
				Name:     "Cloud_function_url",
				Help:     "URL of your cloud function for token management (e.g., https://***.cn-shenzhen.fcapp.run)",
				Required: true,
				Advanced: false,
		},{
		// 云函数鉴权 (Bearer Token)
				Name:     "Cloud_function_auth_token", // 更改名称以更清晰
				Help:     "Bearer token for authenticating with your cloud function.", // 帮助文本更新
				Required: true,
				Advanced: false,
				Sensitive:  true, // 标记为敏感信息，rclone 会加密存储
		},{
		// 网盘API地址
				Name:     "Api_base_url",
				Help:     "Base URL for 123 Cloud Drive API (e.g., https://open-api.123pan.com)",
				Required: true,
				Advanced: false,
				Default:  "https://open-api.123pan.com", // 假设的默认值，请根据实际情况修改
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	Cloud_function_url              string     `config:"Cloud_function_url"`
	Cloud_function_auth_token       string     `config:"Cloud_function_auth_token"`
	Api_base_url                    string     `config:"Api_base_url"`
}

// ------------------------------------------------------------------------------------
// 以下是 Fs 结构体需要实现的核心 rclone 接口方法

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
	params := url.Values{}
	params.Set("parentFileId", strconv.FormatInt(parentID, 10))
	params.Set("limit", "100") // 对于 int 类型，使用 Itoa 更简洁
	if lastFileID > 0 {
		params.Set("lastFileId", strconv.FormatInt(lastFileID, 10))
	}


	// 发送请求
	var respData FileListV2Response
	
	err := f.pacer.Call(func() (bool, error) {
		opts := f.newMetaOpts(ctx)
		opts.Method = "GET"
		opts.Path = "/api/v2/file/list"
		opts.Parameters = params
		resp, callErr := f.rest.CallJSON(ctx, &opts, nil, &respData)
		return f.shouldRetry(resp, callErr)
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to call list api: %w", err)
	}


	// *** 核心：集中化的缓存写入逻辑 ***
	f.pathCacheMu.Lock()
	defer f.pathCacheMu.Unlock()
	for i := range respData.Data.FileList {
		fileInfo := &respData.Data.FileList[i]
		if fileInfo.Type == 1 && fileInfo.Trashed == 0{ // 只缓存目录,未删除的
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
			if fileInfo.Filename == baseName && fileInfo.Type == 1 && fileInfo.Trashed == 0{
				// 找到了！listDir 已经帮我们把这个 ID 和它的同级目录都缓存了。
				// fileInfo.Trashed == 0非常重要
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
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
}


// About gets quota information from the Fs
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	fs.Debugf(nil, "[123CloudFs] Getting About information...")


	// 发送请求
	var respData UserInfoResponse
	err := f.pacer.Call(func() (bool, error) {
		opts := f.newMetaOpts(ctx)
		opts.Method = "GET"
		opts.Path = "/api/v1/user/info"
		resp, callErr := f.rest.CallJSON(ctx, &opts, nil, &respData)
		return f.shouldRetry(resp, callErr)
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to call about api: %w", err)
	}
	
	

	// 5. 创建并填充 rclone 的 fs.Usage 结构体
	totalSpace := respData.Data.SpacePermanent + respData.Data.SpaceTemp
	usedSpace := respData.Data.SpaceUsed

	usage := &fs.Usage{
		Total: fs.NewUsageValue(totalSpace), // 总空间 = 永久空间 + 临时空间
		Used:  fs.NewUsageValue(usedSpace),  // 已用空间
		Free:  fs.NewUsageValue(totalSpace - usedSpace), // bytes which can be uploaded before reaching the quota
	}

	fs.Debugf(nil, "[123CloudFs] About successful: Total=%s, Used=%s", usage.Total, usage.Used)
	return usage, nil
}

// NewObject finds the Object at remote. It is a mandatory interface method.
// *** 这是考虑到文件和目录可以同名的最终版本 ***
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fs.Debugf(f, "NewObject called for: %s", remote)

	// 1. 分割路径 (不变)
	parentPath, leafName := path.Split(remote)
	parentPath = strings.TrimRight(parentPath, "/")
	if leafName == "" {
		return nil, fs.ErrorIsFile
	}

	// 2. 获取父目录 ID (现在 pathToID 已经很智能了)
	parentID, err := f.pathToID(ctx, parentPath)
	if err != nil {
		if errors.Is(err, fs.ErrorDirNotFound) {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, fmt.Errorf("newobject: failed to find parent directory for '%s': %w", remote, err)
	}
	fs.Debugf(f, "Parent path '%s' resolved to ID: %d", parentPath, parentID)

	// 3. 循环分页列出父目录的内容，直到找到目标文件。
	lastFileID := int64(0)
	for {
		respData, err := f.listDir(ctx, parentID, parentPath, lastFileID)
		if err != nil {
			return nil, err
		}

		// 遍历当前页返回的文件列表
		for i := range respData.Data.FileList {
			fileInfo := &respData.Data.FileList[i]

			// *** 核心逻辑：当名称匹配时，检查类型 ***
			if fileInfo.Filename == leafName {
				// 我们只对文件 (Type != 1) 感兴趣
				if fileInfo.Type != 1 {
					// 找到了文件！立即创建并返回。
					fs.Debugf(f, "Found object (file) '%s' with ID %d", leafName, fileInfo.FileId)
					return newObject(ctx, f, remote, fileInfo)
				}
				// 如果找到了同名目录，我们忽略它，继续寻找可能的同名文件。
				// 因为 API 可能在同一页或不同页返回同名的文件和目录。
			}
		}

		// 检查是否还有更多页面需要查找
		lastFileID = respData.Data.LastFileId
		if lastFileID == -1 {
			// 已经遍历完父目录的所有内容，但没有找到匹配的 *文件*。
			// 即使可能存在同名目录，但对于 NewObject 来说，目标文件就是未找到。
			fs.Debugf(f, "Object (file) not found after listing entire directory: %s", remote)
			return nil, fs.ErrorObjectNotFound
		}
	}
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
	
	// 4.构建请求

	reqBody := MkdirRequest{
		ParentID: parentID,
		Name:     newDirName,
	}
	

	// 发送请求
	var respData MkdirResponse
	
	err = f.pacer.Call(func() (bool, error) {
		opts := f.newMetaOpts(ctx)
		opts.Method = "POST"
		opts.Path = "/upload/v1/file/mkdir"
		resp, callErr := f.rest.CallJSON(ctx, &opts, &reqBody, &respData)
		return f.shouldRetry(resp, callErr)
	})
	
	if err != nil {
		return fmt.Errorf("failed to call mkdir api: %w", err)
	}



	// 7. 处理业务逻辑错误
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
	if f.uploadDomain != "" && time.Now().Before(f.uploadDomainExpiresAt) {
		f.uploadDomainMu.Unlock()
		return f.uploadDomain, nil
	}
	f.uploadDomainMu.Unlock()

	fs.Debugf(f, "Upload domain cache is empty or expired, fetching a new one...")
	


	// 发送请求
	var respData UploadDomainResponse
	
	err := f.pacer.Call(func() (bool, error) {
		opts := f.newMetaOpts(ctx)
		opts.Method = "GET"
		opts.Path = "/upload/v2/file/domain"
		resp, callErr := f.rest.CallJSON(ctx, &opts, nil, &respData)
		return f.shouldRetry(resp, callErr)
	})
	
	if err != nil {
		return "", fmt.Errorf("failed to call upload domain api: %w", err)
	}
	

	if len(respData.Data) == 0 {
		return "", errors.New("upload domain api returned no domains")
	}

	uploadDomain := respData.Data[0]
	f.uploadDomainMu.Lock()
	f.uploadDomain = uploadDomain
	f.uploadDomainExpiresAt = time.Now().Add(10 * time.Minute)
	f.uploadDomainMu.Unlock()
	fs.Debugf(f, "Fetched and cached new upload domain: %s", uploadDomain)
	return uploadDomain, nil
}

// putSingle handles the upload of small files in a single, retry-safe request.
func (f *Fs) putSingle(ctx context.Context, in io.Reader, src fs.ObjectInfo, duplicatePolicy int) (fs.Object, error) {
	// --- 步骤 1: 准备元数据和缓冲文件内容 ---
	fs.Debugf(src, "开始单步上传流程")

	parentPath, fileName := path.Split(src.Remote())
	parentPath = strings.TrimRight(parentPath, "/")
	parentID, err := f.pathToID(ctx, parentPath)
	if err != nil {
		return nil, fmt.Errorf("单步上传：无法解析父目录路径: %w", err)
	}

	//你的代码中没有使用 uploadDomain，如果需要请取消注释
	uploadDomain, err := f.getUploadDomain(ctx)
	if err != nil {
		return nil, err
	}

	md5sum, err := src.Hash(ctx, hash.MD5)
	if err != nil || md5sum == "" {
		return nil, errors.New("单步上传需要文件的完整MD5哈希")
	}

	// 【关键改动】将传入的一次性流 `in` 的全部内容读入内存缓冲区。
	// 这使得文件数据可以被重复读取，从而让pacer重试变得安全。
	fileData, err := io.ReadAll(in)
	if err != nil {
		return nil, fmt.Errorf("单步上传：缓冲文件内容失败: %w", err)
	}
	// 验证读取到的数据大小是否与源信息一致
	if int64(len(fileData)) != src.Size() {
		return nil, fmt.Errorf("单步上传：文件大小不一致，期望 %d，实际读取 %d", src.Size(), len(fileData))
	}

	var finalFileInfo *FileInfoV2 // 用于在pacer循环外接收成功后的结果

	// --- 步骤 2: 将所有请求逻辑放入 pacer 循环 ---
	err = f.pacer.Call(func() (bool, error) {
		// --- 重试会从这里重新开始 ---

		// 1. 在每次循环内，重新获取最新的认证信息
		opts := f.newUploadOpts(ctx) // 使用你为上传创建的辅助函数
		opts.Method = "POST"
		opts.RootURL =uploadDomain
		opts.Path = "/upload/v2/file/single/create"
		opts.NoResponse = true // 使用rest.Do，需要自己处理响应

		// 2. 在每次循环内，重新构建 multipart 请求体
		var bodyBuf bytes.Buffer
		multipartWriter := multipart.NewWriter(&bodyBuf)

		// 写入所有元数据字段
		fields := map[string]string{
			"parentFileID": strconv.FormatInt(parentID, 10),
			"filename":     fileName,
			"etag":         md5sum,
			"size":         strconv.FormatInt(src.Size(), 10),
			"duplicate":    strconv.Itoa(duplicatePolicy),
		}
		for key, val := range fields {
			if err := multipartWriter.WriteField(key, val); err != nil {
				return false, fmt.Errorf("内部错误：写入字段 %s 失败: %w", key, err) // 编程错误，不重试
			}
		}

		// 写入文件数据，从内存缓冲区 fileData 读取
		part, err := multipartWriter.CreateFormFile("file", fileName)
		if err != nil {
			return false, fmt.Errorf("内部错误：创建form file失败: %w", err)
		}
		if _, err := part.Write(fileData); err != nil {
			return false, fmt.Errorf("内部错误：写入文件数据失败: %w", err)
		}
		if err := multipartWriter.Close(); err != nil {
			return false, fmt.Errorf("内部错误：关闭multipart writer失败: %w", err)
		}

		opts.Body = &bodyBuf
		contentLength := int64(bodyBuf.Len())
		opts.ContentLength = &contentLength
		opts.ContentType = multipartWriter.FormDataContentType()

		// 3. 执行请求
		resp, doErr := f.rest.Call(ctx, &opts)
		should, retryErr := f.shouldRetry(resp, doErr)
		if retryErr != nil {
			if resp != nil {
				defer resp.Body.Close()
				//fs.Drain(resp.Body) // 重试前必须清空响应体
			}
			return should, retryErr
		}
		// 如果执行到这里，说明请求成功 (HTTP 2xx)，不再重试
		defer resp.Body.Close()

		// 4. 解码成功的响应体
		var respData SingleUploadResponse
		if err := json.NewDecoder(resp.Body).Decode(&respData); err != nil {
			// 解码失败可能意味着服务器返回了非预期的成功响应，这通常是不可重试的错误
			return false, fmt.Errorf("解码成功响应失败: %w", err)
		}
		if !respData.Data.Completed {
			// API业务逻辑报告未完成，这可能是一个服务器端问题，标记为不可重试错误
			return false, errors.New("API报告上传未完成")
		}

		// 5. 将成功的结果保存到外部变量
		finalFileInfo = &FileInfoV2{
			FileId:       respData.Data.FileID,
			Filename:     fileName,
			ParentFileId: parentID,
			Type:         0, // 假设 0 代表文件
			Etag:         md5sum,
			Size:         src.Size(),
			UpdateAt:     time.Now().Format("2006-01-02 15:04:05"),
		}

		return false, nil // 明确表示成功，停止 pacer 循环
	})

	// --- 步骤 3: 检查结果并返回 ---
	if err != nil {
		return nil, fmt.Errorf("单步上传失败: %w", err)
	}
	if finalFileInfo == nil {
		return nil, errors.New("单步上传完成，但未能获取到文件信息")
	}
	
	fs.Debugf(src, "单步上传成功，文件ID: %s", finalFileInfo.FileId)
	return newObject(ctx, f, src.Remote(), finalFileInfo)
}



// ==============================================================================
// 2. 核心上传逻辑 - putChunked
// ==============================================================================

// putChunked 使用分片上传方式处理大文件
func (f *Fs) putChunked(ctx context.Context, in io.Reader, src fs.ObjectInfo, duplicatePolicy int) (fs.Object, error) {
	// --- 步骤 1: 创建分片上传任务，获取上传参数 ---
	fs.Debugf(src, "开始分片上传流程")
	parentPath, fileName := path.Split(src.Remote())
	parentPath = strings.TrimRight(parentPath, "/")
	parentID, err := f.pathToID(ctx, parentPath)
	if err != nil {
		return nil, fmt.Errorf("分片上传：无法解析父目录路径: %w", err)
	}

	md5sum, err := src.Hash(ctx, hash.MD5)
	if err != nil || md5sum == "" {
		return nil, errors.New("分片上传需要文件的完整MD5哈希")
	}

	createReqBody := ChunkedUploadCreateRequest{
		ParentFileID: parentID,
		Filename:     fileName,
		Etag:         md5sum,
		Size:         src.Size(),
		Duplicate:    duplicatePolicy,
	}

	var createRespData ChunkedUploadCreateResponse

	err = f.pacer.Call(func() (bool, error) {
		opts := f.newMetaOpts(ctx) // 假设你有这个辅助函数来创建元数据请求的opts
		opts.Method = "POST"
		opts.Path = "/upload/v2/file/create"
		resp, err := f.rest.CallJSON(ctx, &opts, &createReqBody, &createRespData)
		return f.shouldRetry(resp, err)
	})
	if err != nil {
		return nil, fmt.Errorf("分片上传“创建”请求失败: %w", err)
	}

	// 检查是否已秒传
	if createRespData.Data.Reuse {
		fs.Debugf(src, "文件已存在，秒传成功")
		return newObject(ctx, f, src.Remote(), &FileInfoV2{
			FileId:         createRespData.Data.FileID,
			Filename:       fileName,
			ParentFileId:   parentID,
			Type:           0,
			Etag:           md5sum,
			Size:           src.Size(),
			UpdateAt:       time.Now().Format("2006-01-02 15:04:05"),
		})
	}

	// --- 步骤 2: 并发上传所有分片 ---
	preuploadID := createRespData.Data.PreuploadID
	sliceSize := createRespData.Data.SliceSize
	if len(createRespData.Data.Servers) == 0 {
		return nil, errors.New("API未返回上传服务器地址")
	}
	uploadServerURL := createRespData.Data.Servers[0]

	err = f.uploadAllChunks(ctx, in, src, uploadServerURL, preuploadID, sliceSize)
	if err != nil {
		// 上传分片过程中出错，返回错误
		return nil, err
	}

	fs.Debugf(src, "所有分片上传完毕，发送合并请求")



	finalFileID := -1
	err = f.pacer.Call(func() (bool, error) {
	// --- 步骤 3: 发送完成请求，确认文件合并 ---
		completeReqBody := ChunkedUploadCompleteRequest{PreuploadID: preuploadID}
		completeOpts := f.newMetaOpts(ctx)
		completeOpts.Method = "POST"
		completeOpts.Path = "/upload/v2/file/upload_complete"
		var completeRespData ChunkedUploadCompleteResponse
		resp, callErr := f.rest.CallJSON(ctx, &completeOpts, &completeReqBody, &completeRespData)
		if callErr != nil {
			return f.shouldRetry(resp, callErr)
		}
		if completeRespData.Data.Completed {
			finalFileID = completeRespData.Data.FileID
			fs.Debugf(src, "服务器确认文件合并完成，文件ID: %d", finalFileID)
			return false, nil // 成功，停止重试
		}
		fs.Debugf(src, "文件仍在合并中，稍后重试...")
		return true, nil // 服务器还在处理，继续轮询（pacer会等待）
	})

	if err != nil {
		return nil, fmt.Errorf("完成分片上传请求失败: %w", err)
	}
	if finalFileID == -1 {
		return nil, errors.New("文件合并完成，但未获取到最终文件ID")
	}
	
	// --- 步骤 4: 创建并返回最终的 fs.Object ---
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

// ==============================================================================
// 3. 并发控制与分片生产者/消费者
// ==============================================================================


// chunkJob 定义了上传任务的数据结构，直接持有分片数据
type chunkJob struct {
	number int    // 分片序号, 从1开始
	data   []byte // 分片数据
}

// uploadAllChunks 负责管理分片的并发上传
func (f *Fs) uploadAllChunks(ctx context.Context, in io.Reader, src fs.ObjectInfo, uploadServerURL, preuploadID string, sliceSize int64) error {
	numChunks := (src.Size() + sliceSize - 1) / sliceSize
	if src.Size() == 0 {
		numChunks = 1
	}

	transfers := set_transfers // 从配置中获取用户设置的并发数
	numWorkers := min(transfers, 5) // 限制最大并发为5，或用户设置的更小值
	if int(numChunks) < numWorkers {
		numWorkers = int(numChunks)
	}
	if numWorkers == 0 {
		numWorkers = 1
	}

	fs.Debugf(src, "准备上传分片。总大小: %s, 分片大小: %s, 分片数: %d, 并发数: %d",
		fs.SizeSuffix(src.Size()), fs.SizeSuffix(sliceSize), numChunks, numWorkers)

	var (
		wg      sync.WaitGroup
		jobs    = make(chan chunkJob, numWorkers)
		errs    = make(chan error, numWorkers)
		errOnce sync.Once
		// 创建一个可取消的上下文，一旦出错，可以通知所有goroutine停止工作
		cancelCtx, cancel = context.WithCancel(ctx)
	)
	defer cancel()

	// 启动消费者 (workers)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			for job := range jobs {
				select {
				case <-cancelCtx.Done():
					return // 上下文被取消，worker退出
				default:
				}
				err := f.uploadChunk(cancelCtx, uploadServerURL, preuploadID, src.Remote(), job)
				if err != nil {
					errOnce.Do(func() {
						errs <- err
						cancel() // 关键：一旦有错误，立即取消其他所有操作
					})
					return
				}
			}
		}(i)
	}

	// 启动生产者 (在单独的goroutine中，防止阻塞主流程)
	go func() {
		defer close(jobs) // 所有任务发送完毕后关闭channel
		chunkBuf := make([]byte, sliceSize)
		for i := 1; i <= int(numChunks); i++ {
			select {
			case <-cancelCtx.Done():
				return // 上下文被取消，生产者退出
			default:
			}

			n, err := io.ReadFull(in, chunkBuf)
			if err == io.EOF {
				break // 正常结束
			}
			if err == io.ErrUnexpectedEOF {
				finalChunkData := make([]byte, n)
				copy(finalChunkData, chunkBuf[:n])
				jobs <- chunkJob{number: i, data: finalChunkData}
				break // 到达文件末尾，这是最后一个分片
			} else if err != nil {
				errOnce.Do(func() {
					errs <- fmt.Errorf("读取源文件失败: %w", err)
					cancel()
				})
				return
			}

			fullChunkData := make([]byte, sliceSize)
			copy(fullChunkData, chunkBuf)
			jobs <- chunkJob{number: i, data: fullChunkData}
		}
	}()

	wg.Wait()
	close(errs)

	// 检查是否有错误发生
	if firstErr := <-errs; firstErr != nil {
		return firstErr // 返回捕获到的第一个错误
	}
	return nil
}

// ==============================================================================
// 4. 单个分片上传逻辑 (消费者)
// ==============================================================================

// uploadChunk 负责上传单个分片，包含完整的重试逻辑
func (f *Fs) uploadChunk(ctx context.Context, url string, preuploadID string, fileName string, job chunkJob) error {
	// 计算分片MD5，这是一个一次性操作，可以放在pacer循环外
	hasher := md5.New()
	hasher.Write(job.data)
	sliceMD5 := hex.EncodeToString(hasher.Sum(nil))

	fs.Debugf(nil, "开始上传分片 #%d, 大小: %s, MD5: %s", job.number, fs.SizeSuffix(int64(len(job.data))), sliceMD5)

	// 将所有请求准备和执行的逻辑都放在pacer循环内，以确保重试的健壮性
	err := f.pacer.Call(func() (bool, error) {
		// --- 重试从这里重新开始 ---

		// 1. 在每次循环内，重新获取最新的认证信息
		opts := f.newUploadOpts(ctx) // 假设你有上传专用的辅助函数
		opts.Method = "POST"
		opts.RootURL = url
		opts.Path = "/upload/v2/file/slice"
		opts.NoResponse = true

		// 2. 在每次循环内，重新构建multipart请求体
		var bodyBuf bytes.Buffer
		multipartWriter := multipart.NewWriter(&bodyBuf)

		_ = multipartWriter.WriteField("preuploadID", preuploadID)
		_ = multipartWriter.WriteField("sliceNo", strconv.Itoa(job.number))
		_ = multipartWriter.WriteField("sliceMD5", sliceMD5)

		part, err := multipartWriter.CreateFormFile("slice", path.Base(fileName))
		if err != nil {
			return false, fmt.Errorf("内部错误：创建form file失败: %w", err) // 编程错误，不重试
		}
		_, err = part.Write(job.data)
		if err != nil {
			return false, fmt.Errorf("内部错误：写入分片数据失败: %w", err) // 编程错误，不重试
		}
		err = multipartWriter.Close()
		if err != nil {
			return false, fmt.Errorf("内部错误：关闭multipart writer失败: %w", err) // 编程错误，不重试
		}

		opts.Body = &bodyBuf
		contentLength := int64(bodyBuf.Len())
		opts.ContentLength = &contentLength
		opts.ContentType = multipartWriter.FormDataContentType()

		// 3. 执行请求
		resp, doErr := f.rest.Call(ctx, &opts)
		should, err := f.shouldRetry(resp, doErr)
		if err != nil && resp != nil {
			defer resp.Body.Close()
			//fs.Drain(resp.Body) // 重试前必须清空响应体
		}
		return should, err
	})

	if err != nil {
		return fmt.Errorf("上传分片 #%d 失败: %w", job.number, err)
	}

	fs.Debugf(nil, "成功上传分片 #%d", job.number)
	return nil
}

 
// Put 是文件上传的入口点，它根据文件大小决定使用单次上传还是分片上传
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// 如果文件大小未知，或者大于阈值，则使用分片上传
	if src.Size() < 0 || src.Size() > singleUploadCutoff {
		fs.Debugf(src, "使用分片上传 (大小: %v)", fs.SizeSuffix(src.Size()))
		return f.putChunked(ctx, in, src, duplicatePolicyRename)
	}
 
	fs.Debugf(src, "使用单文件上传 (大小: %s)", fs.SizeSuffix(src.Size()))
	// putSingle 是你的单文件上传实现
	return f.putSingle(ctx, in, src, duplicatePolicyRename)
}

// Update uploads a file to overwrite. `duplicate=2` (overwrite).
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	// To overwrite, we must first delete the existing object(s) with the same name.
	// This is a more robust approach than relying solely on the 'duplicate' flag.
	err := f.deleteExisting(ctx, src.Remote())
	if err != nil {
		return fmt.Errorf("failed to delete existing object before update: %w", err)
	}

	if src.Size() < 0 || src.Size() > singleUploadCutoff {
		fs.Debugf(src, "Using chunked upload for update (size: %d)", src.Size())
		o, err = f.putChunked(ctx, in, src, duplicatePolicyOverwrite)
		if err != nil {
		return err
		}
		return nil
	}
	fs.Debugf(src, "Using single part upload for update (size: %d)", src.Size())
	o, err = f.putSingle(ctx, in, src, duplicatePolicyOverwrite)
		if err != nil {
		return err
		}
		return nil
}


// deleteExisting is a helper for Update to remove old versions of a file.
func (f *Fs) deleteExisting(ctx context.Context, remote string) error {
    // This is a simplified implementation. A real one would handle multiple files
    // with the same name if the backend supports it.
    obj, err := f.NewObject(ctx, remote)
    if err != nil {
        if errors.Is(err, fs.ErrorObjectNotFound) {
            return nil // Nothing to delete
        }
        return err
    }
    return obj.Remove(ctx)
}

// trashItems a list of file or directory IDs.
// This is the shared helper function for Remove, Rmdir, and Purge.
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
		

		// 发送请求
		var respData CommonResponse
		err := f.pacer.Call(func() (bool, error) {
			opts := f.newMetaOpts(ctx)
			opts.Method = "POST"
			opts.Path = "/api/v1/file/trash"
			resp, callErr := f.rest.CallJSON(ctx, &opts, &reqBody, &respData)
			return f.shouldRetry(resp, callErr)
		})

		if err != nil {
			return nil, fmt.Errorf("failed to call trash api: %w", err)
		}

		if respData.Code != 0 {
			return fmt.Errorf("trash failed: api returned error for ids %v, code: %d, message: %s", batch, respData.Code, respData.Msg)
		}
	}

	fs.Debugf(f, "Trashed %d items.", len(fileIDs))
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

// Remove removes a single file.
func (o *Object) Remove(ctx context.Context) error {
	//obj, ok := o.(*Object)
	//if !ok {
	//	return fmt.Errorf("not a cloud123 object: %T", o)
	//}

	fs.Debugf(o, "Deleting file")
	err := o.fs.trashItems(ctx, []int64{o.id})
	if err != nil {
		return err
	}
	
	// 精确清理该文件的缓存
	o.fs.clearPathCacheFor(o.Remote())
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

 
// internalMove 是一个通用的内部函数，用于将任何项目（文件或目录）移动到新的父目录。
func (f *Fs) internalMove(ctx context.Context, itemID int64, dstParentPath string) error {
	fs.Debugf(nil, "internalMove: moving item ID %d to parent path '%s'", itemID, dstParentPath)
	dstParentID, err := f.pathToID(ctx, dstParentPath)
	if err != nil {
		return fmt.Errorf("move: failed to find destination directory '%s': %w", dstParentPath, err)
	}
	reqBody := MoveRequest{
		FileIDs:        []int64{itemID},
		ToParentFileID: dstParentID,
	}
	
	

	// 发送请求
	var respData CommonResponse
	
	err = f.pacer.Call(func() (bool, error) {
		opts := f.newMetaOpts(ctx)
		opts.Method = "POST"
		opts.Path = "/api/v1/file/move"
		resp, callErr := f.rest.CallJSON(ctx, &opts, &reqBody, &respData)
		return f.shouldRetry(resp, callErr)
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to call move api: %w", err)
	}
	
	if respData.Code != 0 {
		return fmt.Errorf("move failed: api returned error for id, code: %d, message: %s", respData.Code, respData.Msg)
	}
	return nil
}

// internalRename 是一个通用的内部函数，用于重命名任何项目（文件或目录）。
func (f *Fs) internalRename(ctx context.Context, itemID int64, newName string) error {
	fs.Debugf(nil, "internalRename: renaming item ID %d to '%s'", itemID, newName)
	reqBody := RenameRequest{
		FileID:   itemID,
		Filename: newName,
	}
	
	// 构建请求
	opts := f.newMetaOpts(ctx)
	opts.Method = "POST"
	opts.Path = "/api/v1/file/name"

	// 发送请求
	var respData CommonResponse
	
	err := f.pacer.Call(func() (bool, error) {
		resp, callErr := f.rest.CallJSON(ctx, &opts, &reqBody, &respData)
		return f.shouldRetry(resp, callErr)
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to call move api: %w", err)
	}
	
	if respData.Code != 0 {
		return fmt.Errorf("rename failed: api returned error for id %d, code: %d, message: %s", itemID, respData.Code, respData.Msg)
	}
	return nil
}

// Move moves and/or renames a file.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fmt.Errorf("not a cloud123 object: %T", src)
	}
 
	srcPath, srcLeaf := path.Split(src.Remote())
	dstPath, dstLeaf := path.Split(remote)
	srcPath = strings.TrimRight(srcPath, "/")
	dstPath = strings.TrimRight(dstPath, "/")
 
	isMove := (srcPath != dstPath)
	isRename := (srcLeaf != dstLeaf)
 
	if !isMove && !isRename {
		fs.Debugf(src, "Move: source and destination are identical, doing nothing.")
		return src, nil
	}
 
	if isMove {
		fs.Debugf(src, "Moving file from '%s' to '%s'", srcPath, dstPath)
		err := f.internalMove(ctx, srcObj.id, dstPath)
		if err != nil {
			return nil, err
		}
	}
 
	if isRename {
		fs.Debugf(src, "Renaming file to '%s'", dstLeaf)
		err := f.internalRename(ctx, srcObj.id, dstLeaf)
		if err != nil {
			return nil, fmt.Errorf("rename step failed after move: %w", err)
		}
	}
	// *** 新增缓存清理 ***
	f.clearPathCacheFor(src.Remote())

	srcObj.remote = remote
	srcObj.name = dstLeaf
	return srcObj, nil
}

// DirMove moves and/or renames a directory.
// This is the standard implementation for the fs.DirMover interface.
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(src, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}
 
	// 首先，我们需要通过源路径找到源目录的 ID
	srcID, err := srcFs.pathToID(ctx, srcRemote)
	if err != nil {
		// 如果找不到源目录，返回一个标准错误
		if errors.Is(err, fs.ErrorObjectNotFound) {
			return fs.ErrorDirNotFound
		}
		return fmt.Errorf("DirMove: failed to find source directory '%s': %w", srcRemote, err)
	}
 
	srcPath, srcLeaf := path.Split(srcRemote)
	dstPath, dstLeaf := path.Split(dstRemote)
	srcPath = strings.TrimRight(srcPath, "/")
	dstPath = strings.TrimRight(dstPath, "/")
 
	isMove := (srcPath != dstPath)
	isRename := (srcLeaf != dstLeaf)
 
	if !isMove && !isRename {
		fs.Debugf(srcFs, "DirMove: source and destination are identical, doing nothing for '%s'", srcRemote)
		return nil
	}
 
	// --- 移动操作 (如果需要) ---
	if isMove {
		fs.Debugf(srcFs, "Moving directory from '%s' to '%s'", srcPath, dstPath)
		err := f.internalMove(ctx, srcID, dstPath)
		if err != nil {
			return err
		}
	}
 
	// --- 重命名操作 (如果需要) ---
	if isRename {
		fs.Debugf(srcFs, "Renaming directory to '%s'", dstLeaf)
		err := f.internalRename(ctx, srcID, dstLeaf)
		if err != nil {
			return fmt.Errorf("rename step failed after move: %w", err)
		}
	}
 
	// --- 成功处理 ---
	// 清理旧路径的缓存。对于目录移动，这非常重要。
	f.clearPathCacheFor(srcRemote)
	return nil
}


// open an object for read. It's a two-step process:
// 1. Get a temporary download URL from the API.
// 2. Make a request to that URL, handling Range headers for seeking.
func (f *Fs) open(ctx context.Context, o *Object, options ...fs.OpenOption) (io.ReadCloser, error) {
	// --- 步骤 1: 获取下载 URL ---
	fs.Debugf(o, "Opening file, requesting download URL")

	// 构建请求
	params := url.Values{}
	params.Set("fileId", strconv.FormatInt(o.id, 10))
	


	// 发送请求
	var infoResp DownloadInfoResponse
	
	err := f.pacer.Call(func() (bool, error) {
		opts := f.newMetaOpts(ctx)
		opts.Method = "GET"
		opts.Path = "/api/v1/file/download_info"
		opts.Parameters = params
		resp, callErr := f.rest.CallJSON(ctx, &opts, nil, &infoResp)
		return f.shouldRetry(resp, callErr)
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to call download_info api: %w", err)
	}

	if infoResp.Code != 0 || infoResp.Data.DownloadURL == "" {
		return nil, fmt.Errorf("open: api returned error for download info: code=%d, msg='%s'", infoResp.Code, infoResp.Message)
	}
	
	downloadURL := infoResp.Data.DownloadURL
	fs.Debugf(o, "Got download URL: %s", downloadURL)

	// --- 步骤 2: 向下载 URL 发起请求，处理 Range ---
	
	downloadOpts := f.newDownloadOpts(options...)
	parsedURL, err := url.Parse(downloadURL)
	downloadOpts.RootURL = fmt.Sprintf("%s://%s", parsedURL.Scheme, parsedURL.Host)
	downloadOpts.Path = parsedURL.RequestURI()
	downloadOpts.Method = "GET"
	
	var downloadResp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		var doErr error
		downloadResp, doErr = f.rest.Call(ctx, &downloadOpts)
		should, err := f.shouldRetry(downloadResp, doErr)
		if err != nil && downloadResp != nil {
			defer downloadResp.Body.Close()
			//fs.Drain(downloadResp.Body) // 重试前清空响应体
		}
		return should, err
	})
	
	if err != nil {
		return nil, fmt.Errorf("下载失败: %w", err)
	}
	
	return downloadResp.Body, nil
}


// 返回元数据请求的 rest.Opts
func (f *Fs) newMetaOpts(ctx context.Context) rest.Opts {
	return rest.Opts{
		Timeout:      global_timeout,
		RootURL:         f.apiBaseURL
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + f.tokenMgr.GetAndStoreToken("/get_token"),
			"Platform":      "open_platform",
		},
	}
}

// 返回上传数据请求的 rest.Opts
func (f *Fs) newUploadOpts(ctx context.Context) rest.Opts {
	return rest.Opts{
		Timeout: upload_timeout,
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer " + f.tokenMgr.GetAndStoreToken("/get_token"),
			"Platform":      "open_platform",
		},
	}
}

// newDownloadOpts 专门为不需要认证的下载请求创建 rest.Opts
// 它使用 0 超时（永不超时）并传递 range options
func (f *Fs) newDownloadOpts(options ...fs.OpenOption) rest.Opts {
	return rest.Opts{
		Method:     "GET",
		Options:    options,
		NoResponse: true,
		Timeout:    download_timeout,
	}
}


// Check the interfaces are satisfied
var (
	_ fs.Fs             = (*Fs)(nil)
	_ fs.Abouter        = (*Fs)(nil)
	_ fs.Purger         = (*Fs)(nil)
	_ fs.Mover          = (*Fs)(nil)
	_ fs.DirMover       = (*Fs)(nil)
	_ fs.Object         = (*Object)(nil)
)

// shouldRetry 是一个健壮的重试决策函数，它封装了所有重试逻辑。
// 这个版本严格遵循“优先检查响应（resp），其次检查错误（err）”的原则。
func (f *Fs) shouldRetry(resp *http.Response, err error) (bool, error) {

	// ----------------------------------------------------------------
	// 步骤 1: 优先处理 `resp` (如果存在)
	// ----------------------------------------------------------------
	if resp != nil {
		// 如果我们收到了响应，那么HTTP状态码是首要的判断依据。

		// 情况 A: HTTP状态码是 200 OK，需要检查响应体内的业务码。
		if resp.StatusCode == http.StatusOK {
			// 【关键操作】读取响应体，并立即用可重复读的副本替换它。
			// 这样做可以让我们检查业务码，同时不影响外层代码后续对响应体的解码。
			bodyBytes, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				// 如果连一个200 OK的响应体都无法读取，这是一个严重的IO错误，不应重试。
				return false, fmt.Errorf("读取200 OK响应体失败: %w", readErr)
			}
			// 【关键操作】用一个新的、可重复读的Reader替换掉原始的、已被消耗的Body。
			resp.Body = io.NopCloser(bytes.NewReader(bodyBytes))

			// 解码JSON来检查业务 `code`。
			var result CommonResponse
			if err := json.Unmarshal(bodyBytes, &result); err != nil {
				// 200 OK的响应体不是我们预期的JSON格式，这是服务器合同错误，不重试。
				// 此时原始的 err 是 nil，我们必须返回一个新的错误。
				return false, fmt.Errorf("解码200 OK响应失败: %w, 响应体: %q", err, string(bodyBytes))
			}

			// 根据业务 `code` 进行决策。
			switch {
			case result.Code == 0:
				// 业务码为0，表示完全成功，不需要重试。
				// 我们返回 (false, nil)，这里的nil错误至关重要，它告诉调用者一切正常。
				return false, nil

			case result.Code == 401:
				// 业务码为401，表示Token失效。
				fs.Debugf(nil, "API业务码401：Token可能已过期，尝试续期...")
				// 注意：这里我们无法访问 `ctx`，所以假设 GetAndStoreToken 内部能处理。
				// 如果它需要ctx，您必须修改 shouldRetry 的函数签名。
				if tokenErr := f.tokenMgr.GetAndStoreToken("/renew_token"); tokenErr != nil {
					// Token续期失败，这是一个致命错误，终止重试。
					return false, fmt.Errorf("token续期失败，终止操作: %w", tokenErr)
				}
				fs.Debugf(nil, "Token续期成功，将重试请求。")
				// 返回 (true, nil) 来触发一次“干净”的重试。
				// pacer看到true会重试，看到nil错误则不会包装它。
				return true, nil

			case result.Code == 429:
				// 业务码为429，表示API层面的限流。
				fs.Debugf(nil, "API业务码429：触发业务层限流。")
				// 返回这个rclone特定的错误，pacer会识别它并自动降速。
				return true, fserrors.ErrTooManyRequests

			case result.Code >= 5000:
				// API定义的、不可重试的服务器端错误。
				fs.Debugf(nil, "API业务码 %d：服务器定义的不可重试错误。", result.Code)
				return false, fmt.Errorf("API错误 (code=%d): %s", result.Code, result.Message)

			default:
				// 其他所有未知的非零业务码，默认为不可重试的错误。
				return false, fmt.Errorf("未知的API错误 (code=%d): %s", result.Code, result.Message)
			}
		}

		// 情况 B: HTTP状态码不是 200 OK。
		// 使用rclone的标准HTTP错误分类器来判断。
		// 它能正确处理 429 (Too Many Requests), 5xx (Server Errors) 等。
		if fserrors.ShouldRetryHTTP(resp, fserrors.Retries) {
			fs.Debugf(nil, "HTTP状态码 %d，将进行重试。", resp.StatusCode)
			// 必须返回由 fserrors 包装后的错误，以便 pacer 正确处理。
			return true, fserrors.NewErrorFromResponse(resp)
		}
		
		// 对于其他不可重试的HTTP状态码（如404 Not Found），不进行重试。
		fs.Debugf(nil, "不可重试的HTTP状态码：%d", resp.StatusCode)
		return false, fserrors.NewErrorFromResponse(resp)
	}

	// ----------------------------------------------------------------
	// 步骤 2: 只有在 `resp` 为 `nil` 时，才分析 `err`
	// ----------------------------------------------------------------
	// 如果代码执行到这里，说明请求连响应都没收到，`resp`是`nil`。
	// `err` 此时代表了网络层或请求准备阶段的错误。
	if err != nil {
		// 使用rclone的标准错误分类器来判断这个错误是否值得重试。
		if fserrors.ShouldRetry(err) {
			fs.Debugf(nil, "发生网络层错误，将进行重试：%v", err)
			return true, err
		}
		
		fs.Debugf(nil, "发生不可重试的错误：%v", err)
		return false, err
	}

	// ----------------------------------------------------------------
	// 步骤 3: 兜底情况
	// ----------------------------------------------------------------
	// 如果 `resp` 和 `err` 都为 `nil`，理论上不应发生。
	// 这表示调用成功，但为了安全，我们明确地返回不重试。
	return false, nil
}
