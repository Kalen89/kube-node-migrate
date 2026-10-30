# Workload Pod and PVC migration

English | [简体中文](workloads.zh-CN.md)

The tool's default and only execution workflow migrates selected workload instances and their data. The user separately decides when to retire the source machine, how to replace nodes, and how to change control-plane membership.

## Usage

`--mode workloads` and `--source-policy keep-running` are no longer needed. There are no `agent` or `isolate` entry points.

```sh
./kube-node-migrate-linux-amd64 plan \
  --namespace prod \
  --source worker-old \
  --target worker-new \
  --kubeconfig /etc/rancher/k3s/k3s.yaml \
  --config examples/workloads.yaml \
  --state /opt/node-migration/business/state.json

./kube-node-migrate-linux-amd64 apply --state /opt/node-migration/business/state.json --allow-downtime
./kube-node-migrate-linux-amd64 verify --state /opt/node-migration/business/state.json
```

Namespace allowlists and exclusions are supported. With `--namespace`, only the allowlist is used. Otherwise exclusions apply, with `kube-system` excluded by default and more exclusions available through `--exclude-namespace`. DaemonSets and static Pods are not included in the workload inventory. Either node may have a control-plane role label; the target must still be schedulable and satisfy the workload's nodeSelector, affinity, and tolerations.

## Execution order

1. Inventory source instances, controllers, and PVC identities. Check resources, admission permissions, filesystems, and actual target free space. Before stopping writes, prewarm sshd on the source and workload/helper/rsync images on the target. An already-cordoned source is supported; these checks remain when online pre-copy is disabled.
2. Temporarily cordon the source to prevent new Pods from scheduling there. Use a Lease and node-ownership markers to prevent conflicting tasks.
3. Pre-copy every volume of the current instance while the source remains running, and recheck peer replica health.
4. Add temporary CREATE admission protection scoped to the current instance. Verify that Pod and individual PVC server-side dry-runs are rejected by this operation's rules at the configured API entry point and every Ready HTTPS API Server published in `default/kubernetes`. An RBAC rejection does not count as proof that protection works.
5. Before eviction, recheck target nodeSelector, taints, required NodeAffinity, and hostPort conflicts. Evict the current instance normally, respecting PDBs, and confirm through CRI that containers for the original Pod UID have exited. Check only this instance; do not stop the runtime or other Pods.
6. Run final checksum sync, remove extra target files, and compare complete digests. All PVCs belonging to an instance must pass before any are rebound. By default, source digest, final sync, and target digest are serial. With `parallelDigest`, final checksum sync runs first, followed by concurrent full digests; matching digests are still required.
7. Start the target instance only after all original-name PVCs bind to target PVs. StatefulSets retain ordinals and controllers; Deployment replacement Pods remain owned by the original ReplicaSet. After StatefulSet protection is removed, if no Pod has appeared, a temporary top-level annotation update requeues reconciliation. If the Pod is still absent, notifications are repeated no more often than every 15 seconds to avoid simply waiting through retry backoff.
8. Proceed only after the current instance and peer replicas are Ready. At completion, remove this task's protection and ownership markers and restore the source's original scheduling state.

The tool does not delete Nodes, add NoExecute taints that evict unrelated Pods, stop K3s/Agent/kubelet services, or change role membership. It does not add permanent target-pinning rules to controller templates. Future volumeless instances follow the original scheduling configuration; migrated local PVs remain bound to the target node.

Filesystem preflight reads path and allocation information. Using the target copy already present, it estimates remaining space/inode growth and serial rsync temporary-file requirements. On a shared target filesystem, volume growth is summed and the largest single temporary peak is retained. Future deletions are not credited, and reusable sparse-file space is based on actual allocation. This checks current free space; it does not replace final stopped-source digests or guarantee capacity for future application writes.

Node capacity uses confirmed instance identities and serial-stage peaks, including init containers, restartable init sidecars, and overhead. Task replacement and helper Pods already on the target are deduplicated by identity. Namespace quota checks use the actual peak additional requests/limits/Pods needed by helpers and workload replacement, plus all temporary PVCs. Node reserve is not repeated for each namespace. Current LimitRange defaults and resource/storage bounds are included.

Reconciliation annotation keys include the operation and instance IDs. Only top-level StatefulSet metadata changes; replicas and Pod templates remain unchanged. The annotation is removed after Pod creation. Resume can recover this task's leftover annotation but stops if another client owns it or the UID/Spec changes. Optimistic resourceVersion locking refreshes objects after conflicts and preserves unrelated annotations.

Creation, scheduling, container startup, and Ready each have separate logs and durations but share one `waitTimeout` deadline. Source digest, final sync/helper release, and target verification are timed separately. At stage start, redacted `ActiveStage` is atomically refreshed; on completion, an `Events` record is appended and the parent stage restored. `.report.json` is available while the command runs. Transaction changes still synchronously persist the private checkpoint and fsync the file/directory before reporting and advancing. Observation-only stage/event/timestamp changes replace the rebuildable report without rewriting full private state or using asynchronous background persistence. `status` uses the latest private transaction checkpoint; live observations come from the report. After abnormal exit, the report can retain a previously running stage or lose recent observation-only records; consult timestamps and logs.

## Resuming after failure

```sh
./kube-node-migrate-linux-amd64 status --state /opt/node-migration/business/state.json --json
./kube-node-migrate-linux-amd64 resume --state /opt/node-migration/business/state.json --allow-downtime
```

When the original instance is still Ready and not terminating, failure handling attempts to remove this task's known protection. After termination/write shutdown begins, a failure retains current protection and the checkpoint and does not process the next instance. Rebound volumes are not rolled back, and the instance is not started until all its volumes complete. After possible target writes, the tool does not repeat offline copying or automatically revert to old data.

With online pre-copy enabled, resuming preparation/stopping checkpoints repeats pre-copy for every volume if the original Pod still has the same UID, is Ready, and has no deletion timestamp. Pre-stop checks run again to avoid stale threshold results. A Pod already terminating or exited does not return to online pre-copy.

If final sync completes but digests differ, the tool records failure and stops. After fixing the cause, use `resume` with the original checkpoint. A source rescan, final sync, and verification repeat only if the source remains stopped, original PVC binding identities and every volume's state are unchanged, there are no extra consumers, and rebinding/startup has not begun. Once any volume enters cutover or the target may have accepted writes, resync must not overwrite target data.

Before copy-helper cleanup, the tool saves resource UIDs, names, kinds, and controller ownership. If deletion is interrupted, `resume` can clean up remaining confirmed resources from this task. Normal resourceVersion updates are revalidated and retried. Replaced names, unknown objects, or changed ownership still stop cleanup without expanding its scope. Do not manually delete copy Jobs, Helm Secrets, or checkpoints to bypass identity checks.

Retiring the old machine is a separate user operation, not a success condition. Post-migration `verify` checks target instances, controllers, target PVCs/PVs, and verification records. It does not require the source to remain online or force the user to keep its Node object or old PVs.

State format 8 stores helper resources and copy-recovery records. Workload checkpoint formats 6 and 7 can be read, with format 6 using original resource defaults, subject to the standalone marker compatibility boundary in the [README](../README.md#version-compatibility). Earlier node-replacement checkpoints are view-only and are never executed automatically. CLI/configuration reject legacy modes, node-disposition policies, and node-label modification flags.

## Permissions and verification boundaries

The tool needs the v1 ValidatingAdmissionPolicy API, permission to manage selected resources and temporary protections, StatefulSet metadata patch permission, and impersonation for independent-identity dry-runs. A server-side dry-run of the reconciliation annotation patch runs before migration; insufficient permissions prevent stopping the source. CLI probes use a controller identity and the impersonated `system:masters` group only for authorization queries and dry-runs. They do not create real probe Pods or grant RBAC permissions.

Admission protection is verified at the configured API entry point and every Ready HTTPS API Server published in `default/kubernetes`. The execution host must be able to read these Endpoints and connect directly with kubeconfig TLS verification retained. Endpoint discovery, connection, or protection failures prevent eviction.

Scheduling preflight covers current target constraints and explicit hostPort conflicts. Required Pod affinity/anti-affinity and hard topology spread are rejected; the full scheduler is not simulated. Independent-identity dry-runs confirm protection effectiveness, not whether future workload Pods will pass every webhook, scoped quota, or other admission rule. Kubernetes API and scheduler decisions remain authoritative.

See [workloads.yaml](../examples/workloads.yaml) for configuration. SSH fingerprints must be verified in advance; the tool does not automatically trust unknown hosts. Host image cache and CRI checks still use K3s/containerd and are read-only, with no node lifecycle management.

Pod Ready does not replace checks for middleware replication, quorum, or real application requests. Optional HTTP `healthChecks` supplement these checks. The user must assess whether the source can be retired based on the cluster and deployment environment.
