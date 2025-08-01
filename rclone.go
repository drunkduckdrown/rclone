// Sync files and directories to and from local and remote object stores
//
// Nick Craig-Wood <nick@craig-wood.com>
package main

import (
	_ "github.com/drunkduckdrown/rclone/backend/all" // import all backends
	"github.com/drunkduckdrown/rclone/cmd"
	_ "github.com/drunkduckdrown/rclone/cmd/all"    // import all commands
	_ "github.com/drunkduckdrown/rclone/lib/plugin" // import plugins

	// 新增：在这里强制导入 openlist 后端
    	_ "github.com/drunkduckdrown/rclone/backend/openlist"
)

func main() {
	cmd.Main()
}
