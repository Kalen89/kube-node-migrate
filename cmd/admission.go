package cmd

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Each API server has its own admission informer cache. One successful dry-run
// through a VIP cannot prove that the controller's API server observes the hold.
// Refresh endpoint membership for every preflight/hold, reusing existing clients.
func newAdmissionProbeProvider(config *rest.Config, cluster, configured client.Client) func(context.Context) ([]client.Client, error) {
	clients := map[string]client.Client{config.Host: configured}
	return func(ctx context.Context) ([]client.Client, error) {
		var endpoints corev1.Endpoints
		if err := cluster.Get(ctx, types.NamespacedName{Namespace: "default", Name: "kubernetes"}, &endpoints); err != nil {
			return nil, fmt.Errorf("读取 default/kubernetes API Server 端点以验证 HA 准入保护: %w", err)
		}
		addresses, err := admissionAPIAddresses(&endpoints)
		if err != nil {
			return nil, err
		}
		result := []client.Client{configured}
		for _, address := range addresses {
			if address == config.Host {
				continue
			}
			if clients[address] == nil {
				individual := rest.CopyConfig(config)
				individual.Host = address
				probe, err := client.New(individual, client.Options{Scheme: scheme.Scheme})
				if err != nil {
					return nil, fmt.Errorf("创建 API Server 准入探测客户端: %w", err)
				}
				clients[address] = probe
			}
			result = append(result, clients[address])
		}
		return result, nil
	}
}

func admissionAPIAddresses(endpoints *corev1.Endpoints) ([]string, error) {
	addresses := map[string]bool{}
	for _, subset := range endpoints.Subsets {
		for _, port := range subset.Ports {
			if port.Name != "https" || port.Protocol != "" && port.Protocol != corev1.ProtocolTCP {
				continue
			}
			if port.Port <= 0 || port.Port > 65535 {
				return nil, fmt.Errorf("API Server HTTPS 端口无效")
			}
			for _, address := range subset.Addresses {
				if net.ParseIP(address.IP) == nil {
					return nil, fmt.Errorf("API Server 端点 IP 无效")
				}
				addresses["https://"+net.JoinHostPort(address.IP, strconv.Itoa(int(port.Port)))] = true
			}
		}
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("default/kubernetes 缺少 Ready 的 HTTPS API Server 端点")
	}
	result := make([]string, 0, len(addresses))
	for address := range addresses {
		result = append(result, address)
	}
	sort.Strings(result)
	return result, nil
}
