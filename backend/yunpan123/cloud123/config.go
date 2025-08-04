package cloud123

import (
	"context"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/obscure"
)

// Register with rclone
func init() {
	fs.RegisterFs(&fs.RegInfo{
		Name:        "123cloud", // 后端名称，用户在 rclone config 中会看到
		Description: "123 Cloud Drive (Baidu Cloud Family)", // 后端描述
		NewFs:       NewFs, // 后端初始化函数，稍后实现
		Config: func(ctx config.Configurator) { // 配置向导
			// 云函数URL，用于获取和刷新 token
			_ = ctx.SetValueAndHint(config.ConfigItem{
				Name:     "cloud_function_url",
				Help:     "URL of your cloud function for token management (e.g., https://***.cn-shenzhen.fcapp.run)",
				Type:     config.TypeString,
				Required: true,
				Advanced: false,
			})
			// 123 云盘 API 的基础 URL
			_ = ctx.SetValueAndHint(config.ConfigItem{
				Name:     "api_base_url",
				Help:     "Base URL for 123 Cloud Drive API (e.g., https://open-api.123pan.com)",
				Type:     config.TypeString,
				Required: true,
				Advanced: false,
				Default:  "https://open-api.123pan.com", // 假设的默认值，请根据实际情况修改
			})
			// 云函数鉴权 Key (现在是 Bearer Token)
			_ = ctx.SetValueAndHint(config.ConfigItem{
				Name:     "cloud_function_auth_token", // 更改名称以更清晰
				Help:     "Bearer token for authenticating with your cloud function.", // 帮助文本更新
				Type:     config.TypeString,
				Required: true,
				Advanced: false,
				Obscure:  true, // 标记为敏感信息，rclone 会加密存储
			})
		},
	})
}