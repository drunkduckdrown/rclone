package yunpan123

import (
	"fmt"
	"os"

	// 导入你的 123 云盘后端包。
	// 这里的路径需要与你在 go.mod 中定义的模块路径一致，
	// 加上你后端代码所在的目录名 (cloud123)。
	_ "github.com/rclone/rclone/backend/yunpan123/cloud123"

	"github.com/rclone/rclone/cmd"
	_ "github.com/rclone/rclone/fs/config/configfile" // 确保 rclone 能加载配置文件
)

func main() {
	// rclone 的主命令入口，它会查找所有已注册的后端并执行命令
	cmd.Main()
}
