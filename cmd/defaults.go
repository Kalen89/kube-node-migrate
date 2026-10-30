package cmd

import (
	"fmt"
	"os"
	"path/filepath"
)

// Prefer the operator's existing credentials over the node's admin config.
// A multi-file KUBECONFIG must not silently select only part of a merged context.
func detectKubeconfig(env, userPath, k3sPath string) (string, error) {
	var paths []string
	for _, path := range filepath.SplitList(env) {
		if path != "" {
			paths = append(paths, path)
		}
	}
	if len(paths) > 1 {
		return "", fmt.Errorf("KUBECONFIG 含多个文件，请通过 --kubeconfig 显式指定一个完整配置文件")
	}
	if len(paths) == 1 {
		return paths[0], nil
	}
	for _, path := range []string{userPath, k3sPath} {
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("kubeconfig 不是普通文件: %s", path)
		}
		return path, nil
	}
	return "", fmt.Errorf("未找到 kubeconfig，请通过 --kubeconfig 指定配置文件")
}
