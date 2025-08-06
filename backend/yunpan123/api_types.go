package yunpan123

import (
	//"time"
	"io"
)

// --- 123 云盘 API 响应的通用结构 ---
// 假设 123 云盘的 API 响应通常包含一个状态码和数据
// 你需要根据 123 云盘的实际 API 文档来调整这些结构体

type CommonResponse struct {
	Code int    `json:"code"` // 假设是 API 返回的状态码，0表示成功
	Message  string `json:"message"`  // 假设是 API 返回的消息
	Data interface{} `json:"data"` // 具体数据部分，通常是嵌套的
}

// baseResponse 是所有API响应共有的基础结构，用于解析code和message
type baseResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}


// --- 存储空间信息 (About) 相关的 API 类型 ---
// UserInfoResponse 对应 /api/v1/user/info API 的响应
type UserInfoResponse struct {
	Code int    `json:"code"`
	Message  string `json:"message"`
	Data struct {
		SpaceUsed      int64 `json:"spaceUsed"`      // 已用空间，单位字节
		SpacePermanent int64 `json:"spacePermanent"` // 永久空间，单位字节
		SpaceTemp      int64 `json:"spaceTemp"`      // 临时空间，单位字节
	} `json:"data"`
}


// --- 文件/目录列表 (List) 相关的 API 类型 ---

// --- 文件/目录列表 (List) 相关的 API 类型 ---
// FileListV2Response 对应 /api/v2/file/list API 的响应
type FileListV2Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		LastFileId int64        `json:"lastFileId"`
		FileList   []FileInfoV2 `json:"fileList"`
	} `json:"data"`
}

// FileInfoV2 代表 123 云盘上的一个文件或目录的元数据
type FileInfoV2 struct {
	FileId       int64  `json:"fileId"`
	Filename     string `json:"filename"`
	ParentFileId int64  `json:"parentFileId"`
	Type         int    `json:"type"` // 0: 文件, 1: 文件夹
	Etag         string `json:"etag"` // MD5
	Size         int64  `json:"size"`
	Trashed      int    `json:"trashed"` // 0: 否, 1: 是
	UpdateAt     string `json:"updateAt"` // 格式: "2025-02-24 17:56:45"
	// 我们也包含其他字段，以备将来使用
	Category    int    `json:"category"`
	Status      int    `json:"status"`
	CreateAt    string `json:"createAt"`
}

// --- 创建目录 (Mkdir) 相关的 API 类型 ---
// MkdirRequest 对应 /upload/v1/file/mkdir API 的请求体
type MkdirRequest struct {
	ParentID int64  `json:"parentID"`
	Name     string `json:"name"`
}

// MkdirResponse 对应 /upload/v1/file/mkdir API 的响应体
type MkdirResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		DirID int64 `json:"dirID"`
	} `json:"data"`
}

// --- 文件上传 (Put) 相关的 API 类型 ---

// UploadDomainResponse 对应 /upload/v2/file/domain API 的响应
type UploadDomainResponse struct {
	Code    int      `json:"code"`
	Message string   `json:"message"`
	Data    []string `json:"data"`
}

// SingleUploadResponse 对应 /upload/v2/file/single/create API 的响应
type SingleUploadResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		FileID    int64 `json:"fileID"`
		Completed bool  `json:"completed"`
	} `json:"data"`
}


// --- 分片上传相关的 API 类型 ---

// ChunkedUploadCreateRequest 对应 /upload/v2/file/create 的请求体
type ChunkedUploadCreateRequest struct {
	ParentFileID int64  `json:"parentFileID"`
	Filename     string `json:"filename"`
	Etag         string `json:"etag"`
	Size         int64  `json:"size"`
	Duplicate    int    `json:"duplicate"`
}

// ChunkedUploadCreateResponse 对应 /upload/v2/file/create 的响应
type ChunkedUploadCreateResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		FileID      int64    `json:"fileID"`
		Reuse       bool     `json:"reuse"`
		PreuploadID string   `json:"preuploadID"`
		SliceSize   int64    `json:"sliceSize"`
		Servers     []string `json:"servers"`
	} `json:"data"`
}

// ChunkedUploadCompleteRequest 对应 /upload/v2/file/upload_complete 的请求体
type ChunkedUploadCompleteRequest struct {
	PreuploadID string `json:"preuploadID"`
}

// ChunkedUploadCompleteResponse 对应 /upload/v2/file/upload_complete 的响应
type ChunkedUploadCompleteResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Completed bool  `json:"completed"`
		FileID    int64 `json:"fileID"`
	} `json:"data"`
}

// TrashRequest 对应 /api/v1/file/trash 的请求体
type TrashRequest struct {
	FileIDs []int64 `json:"fileIDs"`
}

// MoveRequest 定义了调用移动 API 时发送的 JSON body
type MoveRequest struct {
	FileIDs        []int64 `json:"fileIds"` // API 对文件和文件夹一视同仁
	ToParentFileID int64   `json:"toParentFileId"`
}
 
// RenameRequest 定义了调用重命名 API 时发送的 JSON body
type RenameRequest struct {
	FileID   int64  `json:"fileId"`
	Filename string `json:"filename"`
}

// --- 文件下载 (Open) 相关的 API 类型 ---
// 假设下载 API 返回一个临时的下载链接
type DownloadURLResponse struct {
	Code int    `json:"code"`
	Message  string `json:"message"`
	Data struct {
		URL string `json:"url"` // 文件的下载链接
		// Other fields like "expireTime" for the URL
	} `json:"data"`
}

// --- 文件上传 (Put) 相关的 API 类型 ---
// 假设上传 API 需要一些预签名 URL 或分块信息
type UploadInitResponse struct {
	Code int    `json:"code"`
	Message  string `json:"message"`
	Data struct {
		UploadID string `json:"uploadId"` // 分块上传ID
		Parts    []struct {
			PartNumber int    `json:"partNumber"`
			UploadURL  string `json:"uploadUrl"` // 每个分块的上传URL
		} `json:"parts"`
		// Other fields...
	} `json:"data"`
}

// --- 创建目录 (Mkdir) 相关的 API 类型 ---
type CreateFolderResponse struct {
	Code int    `json:"code"`
	Message  string `json:"message"`
	Data struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		ParentID string `json:"parentId"`
		// ... other folder info ...
	} `json:"data"`
}


// --- 下载相关的 API 类型 ---

// DownloadInfoResponse 对应 /api/v1/file/download_info 的响应
type DownloadInfoResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		DownloadURL string `json:"downloadUrl"`
	} `json:"data"`
}