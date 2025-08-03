package cloud123

import (
	"context"
	"fmt"
	"io"
	"time"
	"path"
	"mime"
	"path/filepath"

	"github.com/rclone/rclone/fs"
	//"github.com/rclone/rclone/fs/log"
)

// Object represents a 123 Cloud Drive file
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
var (
	_ fs.Object = (*Object)(nil)
)

// newObject 的签名需要改变，以接收 FileInfoV2
func newObject(ctx context.Context, f *Fs, remote string, info *FileInfoV2) (*Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}

	if info != nil {
		o.id = info.FileId
		o.parentFileId = info.parentFileId // *** 填充新增字段 ***
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
func (o *Object) Fs() fs.Fs {
	return o.fs
}

// Remote returns the full path of the file on the remote
func (o *Object) Remote() string {
	return o.remote
}

// Hash 方法现在可以返回 MD5 哈希值了
func (o *Object) Hash(ctx context.Context, ty fs.HashType) (string, error) {
	if ty == fs.HashMD5 {
		return o.hash, nil
	}
	return "", fs.ErrHashUnsupported
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the file
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
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


// Update updates the object with new data and metadata.
//func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	// TODO: 如果 123 云盘 API 支持文件覆盖更新，则实现此方法
	// 否则返回 fs.ErrorNotImplemented
	//fs.Debugf(nil, "[Object] Update requested for %s, new size: %d", o.remote, src.Size())
	//return fs.ErrorNotImplemented // 暂时不支持
//}