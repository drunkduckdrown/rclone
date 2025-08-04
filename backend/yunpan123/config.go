package yunpan123

import (
	"context"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
)

// Register with rclone
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "123cloud", // 后端名称，用户在 rclone config 中会看到
		Description: "123 Cloud Drive (Baidu Cloud Family)", // 后端描述
		NewFs:       NewFs, // 后端初始化函数，稍后实现
		Options: []fs.Option{{
		// 云函数URL，用于获取和刷新 token
				Name:     "cloud_function_url",
				Help:     "URL of your cloud function for token management (e.g., https://***.cn-shenzhen.fcapp.run)",
				Required: true,
				Advanced: false,
		},{
		// 云函数URL，用于获取和刷新 token
				Name:     "api_base_url",
				Help:     "Base URL for 123 Cloud Drive API (e.g., https://open-api.123pan.com)",
				Required: true,
				Advanced: false,
				Default:  "https://open-api.123pan.com", // 假设的默认值，请根据实际情况修改
		},{
		// 云函数鉴权 (Bearer Token)
				Name:     "cloud_function_auth_token", // 更改名称以更清晰
				Help:     "Bearer token for authenticating with your cloud function.", // 帮助文本更新
				Required: true,
				Advanced: false,
				Sensitive:  true, // 标记为敏感信息，rclone 会加密存储
		}},
	})
}