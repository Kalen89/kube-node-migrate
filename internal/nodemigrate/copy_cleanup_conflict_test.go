package nodemigrate

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestCopyCleanupRetriesConcurrentDeploymentUpdate(t *testing.T) {
	e, w, _ := fixture(t)
	job := copyJob(w, w.Volume.CopyID)
	require.NoError(t, e.Client.Create(t.Context(), job))
	deployment := &appsv1.Deployment{ObjectMeta: *job.ObjectMeta.DeepCopy()}
	deployment.Name, deployment.UID, deployment.ResourceVersion = copyRelease(w)+"-sshd", "sshd-uid", ""
	require.NoError(t, e.Client.Create(t.Context(), deployment))
	w.Volume.CopyJobName, w.Volume.CopyJobUID = job.Name, job.UID
	deletes := 0
	e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if _, ok := obj.(*appsv1.Deployment); ok {
			deletes++
			var current appsv1.Deployment
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), &current))
			if deletes == 1 {
				current.Annotations["controller-update"] = "changed"
				require.NoError(t, c.Update(ctx, &current))
				return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, obj.GetName(), errors.New("status changed after checkpoint"))
			}
			options := &client.DeleteOptions{}
			for _, option := range opts {
				option.ApplyToDelete(options)
			}
			require.NotNil(t, options.Preconditions)
			require.Equal(t, current.UID, *options.Preconditions.UID)
			require.Equal(t, current.ResourceVersion, *options.Preconditions.ResourceVersion)
			var recorded *CopyCleanupResource
			for i := range w.Volume.CopyCleanup.Resources {
				if w.Volume.CopyCleanup.Resources[i].Kind == "Deployment" {
					recorded = &w.Volume.CopyCleanup.Resources[i]
				}
			}
			require.NotNil(t, recorded)
			require.Equal(t, current.ResourceVersion, recorded.ResourceVersion)
		}
		return c.Delete(ctx, obj, opts...)
	}})
	require.NoError(t, (&processCopier{engine: e}).cleanup(t.Context(), e.State, w))
	require.Equal(t, 2, deletes)
	require.Nil(t, w.Volume.CopyCleanup)
}

func TestCopyCleanupConflictNeverAdoptsChangedIdentityOrOwner(t *testing.T) {
	for _, change := range []string{"replacement", "owner"} {
		t.Run(change, func(t *testing.T) {
			e, w, _ := fixture(t)
			job := copyJob(w, w.Volume.CopyID)
			require.NoError(t, e.Client.Create(t.Context(), job))
			deployment := &appsv1.Deployment{ObjectMeta: *job.ObjectMeta.DeepCopy()}
			deployment.Name, deployment.UID, deployment.ResourceVersion = copyRelease(w)+"-sshd", "sshd-uid", ""
			require.NoError(t, e.Client.Create(t.Context(), deployment))
			w.Volume.CopyJobName, w.Volume.CopyJobUID = job.Name, job.UID
			deletes := 0
			e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*appsv1.Deployment); ok {
					deletes++
					var current appsv1.Deployment
					require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(obj), &current))
					if change == "replacement" {
						require.NoError(t, c.Delete(ctx, &current))
						current.UID, current.ResourceVersion = "replacement-uid", ""
						require.NoError(t, c.Create(ctx, &current))
					} else {
						current.Labels["app.kubernetes.io/managed-by"] = "Other"
						require.NoError(t, c.Update(ctx, &current))
					}
					return apierrors.NewConflict(schema.GroupResource{Resource: "deployments"}, obj.GetName(), errors.New("concurrent replacement"))
				}
				return c.Delete(ctx, obj, opts...)
			}})
			require.ErrorContains(t, (&processCopier{engine: e}).cleanup(t.Context(), e.State, w), "UID 或 Helm 归属改变")
			require.Equal(t, 1, deletes)
			require.NotNil(t, w.Volume.CopyCleanup)
			require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(deployment), &appsv1.Deployment{}))
		})
	}
}

func TestCopyCleanupConflictRetriesAreBoundedAndRequireCheckpoint(t *testing.T) {
	for _, failure := range []string{"persistent-conflict", "checkpoint"} {
		t.Run(failure, func(t *testing.T) {
			e, w, _ := fixture(t)
			job := copyJob(w, w.Volume.CopyID)
			require.NoError(t, e.Client.Create(t.Context(), job))
			w.Volume.CopyJobName, w.Volume.CopyJobUID = job.Name, job.UID
			deletes := 0
			e.Save = func(*State) error {
				if failure == "checkpoint" && deletes > 0 {
					return errors.New("checkpoint unavailable")
				}
				return nil
			}
			e.Client = interceptor.NewClient(e.Client.(client.WithWatch), interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*batchv1.Job); ok {
					deletes++
					return apierrors.NewConflict(schema.GroupResource{Resource: "jobs"}, obj.GetName(), errors.New("concurrent update"))
				}
				return c.Delete(ctx, obj, opts...)
			}})
			err := (&processCopier{engine: e}).cleanup(t.Context(), e.State, w)
			if failure == "checkpoint" {
				require.ErrorContains(t, err, "checkpoint unavailable")
				require.Equal(t, 1, deletes)
			} else {
				require.True(t, apierrors.IsConflict(err), "%v", err)
				require.Equal(t, retry.DefaultRetry.Steps, deletes)
			}
			require.NotNil(t, w.Volume.CopyCleanup)
			require.NoError(t, e.Client.Get(t.Context(), client.ObjectKeyFromObject(job), &batchv1.Job{}))
		})
	}
}
