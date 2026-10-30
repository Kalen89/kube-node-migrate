package nodemigrate

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/Kalen89/kube-node-migrate/internal/filesystem"
)

// Host is the SSH endpoint selected from the Node's InternalIP at planning time.
type Host struct {
	Name         string `json:"name"`
	IP           string `json:"ip"`
	IPV6         string `json:"ipv6,omitempty"`
	User         string `json:"user"`
	Port         int    `json:"port"`
	Key          string `json:"key,omitempty"`
	KnownHosts   string `json:"knownHosts,omitempty"`
	Architecture string `json:"architecture"`
}

func (h Host) Addr() string {
	if h.IP != "" {
		return h.IP
	}
	return h.IPV6
}
func nodeHost(n *corev1.Node, o Options) (Host, error) {
	h := Host{Name: n.Name, User: o.SSHUser, Port: o.SSHPort, Key: o.SSHKey, KnownHosts: o.SSHKnownHosts, Architecture: n.Status.NodeInfo.Architecture}
	for _, a := range n.Status.Addresses {
		if a.Type == corev1.NodeInternalIP && net.ParseIP(a.Address) != nil {
			if strings.Contains(a.Address, ":") {
				h.IPV6 = a.Address
			} else {
				h.IP = a.Address
			}
		}
	}
	if h.Addr() == "" {
		return h, fmt.Errorf("节点 %s 缺少 InternalIP", n.Name)
	}
	return h, nil
}

type SSHHosts struct{}

func shellQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }

// Run never bypasses known_hosts and always uses noninteractive authentication.
func (SSHHosts) Run(ctx context.Context, h Host, command string) (string, error) {
	return (SSHHosts{}).runInput(ctx, h, command, nil)
}

func (SSHHosts) runInput(ctx context.Context, h Host, command string, input io.Reader) (string, error) {
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultWaitTimeout)
		defer cancel()
	}
	if net.ParseIP(h.Addr()) == nil || h.User == "" || strings.ContainsAny(h.User, "@ \t\n\r") || h.Port < 1 || h.Port > 65535 {
		return "", errors.New("SSH 端点无效")
	}
	if h.User != "root" {
		command = "sudo -n -- sh -c " + shellQuote(command)
	}
	address := h.Addr()
	if strings.Contains(address, ":") {
		address = "[" + address + "]"
	}
	args := []string{"-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=10", "-o", "ServerAliveCountMax=3", "-p", strconv.Itoa(h.Port)}
	if h.Key != "" {
		args = append(args, "-i", h.Key)
	}
	if h.KnownHosts != "" {
		args = append(args, "-o", "UserKnownHostsFile="+strconv.Quote(h.KnownHosts))
	}
	args = append(args, "--", h.User+"@"+address, command)
	cmd := exec.CommandContext(ctx, "ssh", args...)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	cmd.Stdin = input
	err := cmd.Run()
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if err != nil {
		return "", fmt.Errorf("节点 %s SSH 命令失败: %w: %s", h.Name, err, strings.TrimSpace(stderr.String()))
	}
	return out.String(), nil
}

type diskSummary = filesystem.Summary

func (e *Engine) filesystem(ctx context.Context, node Host, path, mode string) (*diskSummary, error) {
	if e.Hosts == nil {
		return nil, fmt.Errorf("SSH 执行器未初始化")
	}
	limit := e.State.Options.CopyTimeout
	if limit <= 0 {
		limit = 12 * time.Hour
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	runner, ok := e.Hosts.(filesystemHost)
	if !ok {
		return nil, errors.New("主机执行器不支持 Go 文件系统助手")
	}
	result, err := runner.Filesystem(ctx, node, e.State.Options.FilesystemHelper, path, mode)
	if err != nil {
		return nil, err
	}
	var summary diskSummary
	if err := json.Unmarshal([]byte(result), &summary); err != nil {
		return nil, fmt.Errorf("节点 %s 文件系统摘要格式错误: %w", node.Addr(), err)
	}
	if summary.Version != filesystem.Version {
		return nil, errors.New("文件系统助手协议版本不匹配")
	}
	if mode != "free" && summary.Entries == 0 {
		return nil, errors.New("文件系统摘要缺少根目录条目")
	}
	if mode == "digest" && !validDigest(&summary.Digest) {
		return nil, errors.New("文件系统摘要 SHA-256 无效")
	}
	if mode == "inventory" {
		if err := filesystem.ValidateInventory(&summary); err != nil {
			return nil, fmt.Errorf("节点 %s 文件系统 inventory 无效: %w", node.Addr(), err)
		}
	}
	return &summary, nil
}

func validDigest(d *Digest) bool {
	if d == nil || d.Entries == 0 || len(d.SHA256) != sha256HexLength {
		return false
	}
	_, err := hex.DecodeString(d.SHA256)
	return err == nil
}

// Preflight inspects hosts and may cache the Go helper; it never changes workloads.
func (e *Engine) Preflight(ctx context.Context) error {
	if err := CheckExecutionState(e.State); err != nil {
		return err
	}
	return e.preflightInstances(ctx)
}

func (e *Engine) checkSpace(ctx context.Context) error {
	type usage struct {
		needs                 filesystem.SpaceRequirements
		freeBytes, freeInodes uint64
		blockSize             uint64
	}
	filesystems := make(map[uint64]*usage)
	for _, w := range volumeViews(e.State) {
		if w.Volume == nil || !oneOf(w.Volume.Phase, phasePrepared, phasePrecopying, phasePrecopyReady, phaseStopping, phaseStopped) {
			continue
		}
		if w.Volume.TargetPV == nil {
			return fmt.Errorf("须先准备全部目标卷，再验证实际文件系统容量")
		}
		path, err := volumePath(&w.Volume.PV)
		if err != nil {
			return err
		}
		result, err := e.filesystem(ctx, e.State.SourceConfig, path, "inventory")
		if err != nil {
			return err
		}
		targetPath, err := volumePath(w.Volume.TargetPV)
		if err != nil {
			return err
		}
		available, err := e.filesystem(ctx, e.State.TargetConfig, targetPath, "inventory")
		if err != nil {
			return err
		}
		needs, err := filesystem.RequiredSpace(result, available)
		if err != nil {
			return fmt.Errorf("计算 PVC %s/%s 剩余复制空间: %w", w.Namespace, w.Volume.PVC.Name, err)
		}
		group := filesystems[available.Device]
		if group == nil {
			group = &usage{freeBytes: available.FreeBytes, freeInodes: available.FreeInodes, blockSize: available.BlockSize}
			filesystems[available.Device] = group
		}
		if group.blockSize != available.BlockSize {
			return errors.New("相同目标文件系统的 allocation block size 改变")
		}
		group.freeBytes = min(group.freeBytes, available.FreeBytes)
		group.freeInodes = min(group.freeInodes, available.FreeInodes)
		if err := group.needs.Add(needs); err != nil {
			return err
		}
		e.logf("剩余复制空间 pvc=%s/%s growthBytes=%d scratchBytes=%d growthInodes=%d scratchInodes=%d", w.Namespace, w.Volume.PVC.Name, needs.GrowthBytes, needs.ScratchBytes, needs.GrowthInodes, needs.ScratchInodes)
	}
	for _, group := range filesystems {
		bytes, inodes, err := group.needs.Total()
		if err != nil {
			return err
		}
		if group.freeBytes < bytes || group.freeInodes < inodes {
			return fmt.Errorf("目标卷文件系统空间/inode 不足: 需要 %d 字节/%d 条目（含串行临时副本），可用 %d 字节/%d inode", bytes, inodes, group.freeBytes, group.freeInodes)
		}
	}
	return nil
}
