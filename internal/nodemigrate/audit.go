package nodemigrate

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// OpenLog appends only to a private regular file in the already-locked state dir.
func OpenLog(statePath string) (*os.File, error) {
	f, err := os.OpenFile(statePath+".log", os.O_WRONLY|os.O_CREATE|os.O_APPEND|unix.O_NOFOLLOW|unix.O_NONBLOCK, privateFileMode)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		closeReadOnly(f)
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		closeReadOnly(f)
		return nil, fmt.Errorf("日志必须是权限为 0600 的普通文件")
	}
	var stat unix.Stat_t
	if err = unix.Fstat(int(f.Fd()), &stat); err != nil || stat.Nlink != 1 {
		closeReadOnly(f)
		return nil, fmt.Errorf("日志存在硬链接或无法检查文件身份")
	}
	return f, nil
}
