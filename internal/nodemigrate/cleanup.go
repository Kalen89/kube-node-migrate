package nodemigrate

import (
	"fmt"
	"io"
	"log/slog"
	"os"
)

type debugLogger struct{}

func (debugLogger) Debugf(format string, args ...any) { slog.Debug(fmt.Sprintf(format, args...)) }

var logger debugLogger

func closeReadOnly(closer io.Closer) {
	if err := closer.Close(); err != nil {
		logger.Debugf("close resource: %v", err)
	}
}
func removeTemporary(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logger.Debugf("remove temporary: %v", err)
	}
}
