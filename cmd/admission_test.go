package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestAdmissionAPIAddressesValidateAndDeduplicate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		addresses []corev1.EndpointAddress
		port      corev1.EndpointPort
		want      []string
	}{
		{"IPv4 and IPv6", []corev1.EndpointAddress{{IP: "10.0.0.1"}, {IP: "2001:db8::1"}, {IP: "10.0.0.1"}}, corev1.EndpointPort{Name: "https", Port: 6443}, []string{"https://10.0.0.1:6443", "https://[2001:db8::1]:6443"}},
		{"empty", nil, corev1.EndpointPort{Name: "https", Port: 6443}, nil},
		{"invalid address", []corev1.EndpointAddress{{IP: "not-an-ip"}}, corev1.EndpointPort{Name: "https", Port: 6443}, nil},
		{"invalid port", []corev1.EndpointAddress{{IP: "10.0.0.1"}}, corev1.EndpointPort{Name: "https", Port: 70000}, nil},
		{"wrong protocol", []corev1.EndpointAddress{{IP: "10.0.0.1"}}, corev1.EndpointPort{Name: "https", Port: 6443, Protocol: corev1.ProtocolUDP}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ep := &corev1.Endpoints{Subsets: []corev1.EndpointSubset{{Addresses: tc.addresses, Ports: []corev1.EndpointPort{tc.port}}}}
			got, err := admissionAPIAddresses(ep)
			if tc.want == nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
}

func TestAdmissionProbeProviderRefreshesMembershipAndKeepsConfiguredEntry(t *testing.T) {
	endpoints := &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "kubernetes"}, Subsets: []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: "10.0.0.1"}}, Ports: []corev1.EndpointPort{{Name: "https", Port: 6443}}}}}
	cluster := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(endpoints).Build()
	configured := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	config := &rest.Config{Host: "https://configured.invalid:6443"}
	provider := newAdmissionProbeProvider(config, cluster, configured)
	first, err := provider(t.Context())
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Same(t, configured, first[0])
	second, err := provider(t.Context())
	require.NoError(t, err)
	require.Same(t, first[1], second[1])
	endpoints.Subsets[0].Addresses = append(endpoints.Subsets[0].Addresses, corev1.EndpointAddress{IP: "10.0.0.2"})
	require.NoError(t, cluster.Update(t.Context(), endpoints))
	third, err := provider(t.Context())
	require.NoError(t, err)
	require.Len(t, third, 3)
	require.Same(t, first[1], third[1])
	require.Equal(t, "https://configured.invalid:6443", config.Host)
	endpoints.Subsets = nil
	require.NoError(t, cluster.Update(t.Context(), endpoints))
	_, err = provider(t.Context())
	require.ErrorContains(t, err, "缺少 Ready")
}
