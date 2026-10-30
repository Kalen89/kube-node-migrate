package nodemigrate

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// claimNodes protects suspended operations after their execution Lease expires.
// Both markers remain until workload migration completes; another plan cannot adopt its nodes.
func (e *Engine) claimNodes(ctx context.Context) error {
	var nodes []*corev1.Node
	for _, identity := range []NodeIdentity{e.State.Source, e.State.Target} {
		var n corev1.Node
		if err := e.Client.Get(ctx, types.NamespacedName{Name: identity.Name}, &n); err != nil {
			return err
		}
		if n.UID != identity.UID {
			return errors.New("节点 UID 发生变化")
		}
		if owner := n.Annotations[labelKey]; owner != "" && owner != e.State.ID {
			return fmt.Errorf("节点 %s 属于未完成的迁移 %s", n.Name, owner)
		}
		nodes = append(nodes, &n)
	}
	for _, n := range nodes {
		if n.Annotations[labelKey] == e.State.ID {
			continue
		}
		if n.Annotations == nil {
			n.Annotations = make(map[string]string)
		}
		n.Annotations[labelKey] = e.State.ID
		if err := e.Client.Update(ctx, n); err != nil {
			return err
		}
	}
	return nil
}
