package nodemigrate

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	leaseDurationSeconds int32 = 60
	leaseRenewInterval         = 15 * time.Second
	leaseRequestTimeout        = 10 * time.Second
)

func (e *Engine) acquireLease(ctx context.Context) (*coordinationv1.Lease, error) {
	key := types.NamespacedName{Namespace: systemNamespace, Name: leaseName}
	var lease coordinationv1.Lease
	err := e.Client.Get(ctx, key, &lease)
	now := metav1.NewMicroTime(time.Now())
	holder := e.State.ID + "/" + rand.Text()
	if apierrors.IsNotFound(err) {
		lease = coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace,
			Labels: map[string]string{labelKey: e.State.ID}}, Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holder, LeaseDurationSeconds: pointer(leaseDurationSeconds), AcquireTime: &now, RenewTime: &now}}
		if err := e.Client.Create(ctx, &lease); err != nil {
			return nil, err
		}
		return &lease, nil
	}
	if err != nil {
		return nil, err
	}
	if lease.Labels[labelKey] == "" {
		return nil, errors.New("迁移锁名称被其他资源占用")
	}
	if lease.Spec.HolderIdentity != nil {
		renewed := lease.CreationTimestamp.Time
		if lease.Spec.RenewTime != nil {
			renewed = lease.Spec.RenewTime.Time
		}
		duration := leaseDurationSeconds
		if lease.Spec.LeaseDurationSeconds != nil {
			duration = *lease.Spec.LeaseDurationSeconds
		}
		if time.Since(renewed) < time.Duration(duration)*time.Second {
			return nil, errors.New("另一个节点迁移任务正在执行")
		}
	}
	lease.Spec.HolderIdentity = &holder
	lease.Spec.AcquireTime = &now
	lease.Spec.LeaseDurationSeconds = pointer(leaseDurationSeconds)
	lease.Spec.RenewTime = &now
	lease.Labels[labelKey] = e.State.ID
	if err := e.Client.Update(ctx, &lease); err != nil {
		return nil, err
	}
	return &lease, nil
}

func pointer[T any](value T) *T { return &value }

func (e *Engine) withLease(ctx context.Context, run func(context.Context) error) error {
	lease, err := e.acquireLease(ctx)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	finished := make(chan struct{})
	lost := make(chan error, 1) // One renewal failure; the operation owns and joins this goroutine.
	go func() {
		defer close(finished)
		ticker := time.NewTicker(leaseRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				requestCtx, stop := context.WithTimeout(runCtx, leaseRequestTimeout)
				err := e.renewLease(requestCtx, lease.UID, *lease.Spec.HolderIdentity)
				stop()
				if err != nil {
					lost <- err
					cancel()
					return
				}
			}
		}
	}()
	result := run(runCtx)
	cancel()
	<-finished
	select {
	case err := <-lost:
		result = errors.Join(result, fmt.Errorf("迁移锁续租失败: %w", err))
	default:
	}
	// Release must still run after Ctrl-C, but gets its own short bounded deadline.
	releaseCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), leaseRequestTimeout)
	defer stop()
	return errors.Join(result, e.releaseLease(releaseCtx, lease.UID, *lease.Spec.HolderIdentity))
}

func (e *Engine) renewLease(ctx context.Context, uid types.UID, holder string) error {
	var current coordinationv1.Lease
	if err := e.Client.Get(ctx, types.NamespacedName{Namespace: systemNamespace, Name: leaseName}, &current); err != nil {
		return err
	}
	if current.UID != uid || current.Spec.HolderIdentity == nil || *current.Spec.HolderIdentity != holder {
		return errors.New("迁移锁所有权已改变")
	}
	now := metav1.NewMicroTime(time.Now())
	current.Spec.RenewTime = &now
	return e.Client.Update(ctx, &current)
}

func (e *Engine) releaseLease(ctx context.Context, uid types.UID, holder string) error {
	var current coordinationv1.Lease
	err := e.Client.Get(ctx, types.NamespacedName{Namespace: systemNamespace, Name: leaseName}, &current)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.UID != uid || current.Spec.HolderIdentity == nil || *current.Spec.HolderIdentity != holder {
		return nil
	}
	return e.Client.Delete(ctx, &current, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &current.UID, ResourceVersion: &current.ResourceVersion}})
}
