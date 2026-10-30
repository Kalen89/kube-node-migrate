# kube-node-migrate

English | [简体中文](README.zh-CN.md)

Migrate workloads and local persistent data between Kubernetes nodes, with online pre-copy, full data verification, and checkpoint-based recovery. Volume migration currently supports local-path PVCs. Version **0.1.0**. Original controllers, other replicas, and old data are retained. The operator decides when to retire the old machine and how to handle control-plane membership.

## Development status and AI assistance

**Development currently relies heavily on AI assistance, which is used to produce most of the code and documentation.** AI contributes to implementation, test writing, and documentation. Existing functionality is continually checked through automated tests and practical migration exercises.

The project is still in early development. Code structure, edge-case handling, failure recovery, and compatibility will be improved incrementally, alongside additional verified migration scenarios, tests, and usage documentation. Code reviews, reproducible bug reports, and contributions are welcome. The support matrix and limitations in this README describe the current scope.

## Why this project exists

### Why Local PV?

This project is intended for small and medium-sized deployments, from a few servers to a few hundred. In these environments, Local PV can be a practical way to use the disks already attached to application servers while keeping the storage architecture manageable. Server count is a starting point for this decision; workload requirements, hardware, recovery objectives, and operational capacity also matter.

- **Infrastructure cost:** a separate shared or distributed storage layer can require additional servers, disks, network capacity, spare capacity, and redundancy. For smaller deployments, this extra investment can account for a large share of the infrastructure budget.
- **Operational cost:** an additional storage system brings its own deployment, monitoring, upgrades, capacity planning, recovery procedures, and specialist knowledge. These costs remain even when the application cluster is small.
- **I/O path:** local disks avoid the extra remote block-storage path. Network storage can add latency, consume network bandwidth, and introduce further replication work; the effect can be substantial for I/O-sensitive workloads on constrained networks. Actual performance depends on the hardware, storage implementation, and workload, so this project does not assume a fixed performance gain.
- **Application-level availability:** components such as MySQL, PostgreSQL, Redis, and MinIO offer replication, failover, clustering, or distributed data-protection mechanisms. When these are correctly configured across failure domains, local disks can support an architecture where the application manages its own availability. Adding storage-level replication as well can increase storage amplification, network traffic, and write cost; that additional layer should meet a concrete requirement.

For deployments that already have suitable application-level redundancy, using local disks can reduce infrastructure overhead and keep the data path easier to reason about. This does not make shared storage unnecessary in every environment, and Local PV itself does not provide replication or automatic cross-node recovery. Replicas, quorum, persistence, backups, and recovery procedures still need to match each component's requirements. See the [Kubernetes Local volume documentation](https://kubernetes.io/docs/concepts/storage/volumes/#local), [PostgreSQL replication documentation](https://www.postgresql.org/docs/current/warm-standby.html), and [MySQL Group Replication documentation](https://dev.mysql.com/doc/refman/8.4/en/group-replication.html).

### Why migrate an entire server's workloads?

Local data belongs to a particular node. Over time, that node may need to be replaced because of aging hardware, disk or controller fault warnings, recurring server faults, insufficient capacity, hardware upgrades, operating-system maintenance, or changes to the virtualization platform. After a recoverable fault, operators may also want to move readable data off the repaired machine before retiring it.

Moving a stateless Pod is usually straightforward. Moving a node's stateful workloads also requires copying their data and preserving the relationship between Pods, PVCs, and PVs. Evicting or rescheduling a Pod alone does not move its local data. A single server can host several components and many PVCs across namespaces, making manual copy, shutdown, rebinding, verification, and recovery difficult to coordinate.

`kube-node-migrate` provides a repeatable workflow for this job: inventory the selected workloads, pre-copy while they are running, stop one instance at a time, perform the final sync and complete verification, switch its volumes, and wait for the replacement to become Ready. Checkpoints preserve progress after a failure, and old PVs and data remain available for operator inspection. Pre-copy aims to reduce the amount of data left to transfer during downtime; it does not promise zero downtime or a fixed time saving.

Migration requires the source node to be accessible and its data readable. If the server is unreachable or its disks have lost data, recovery must come from application replicas or backups; this tool cannot reconstruct missing data. The tool handles workload migration. Node retirement and control-plane or etcd membership changes remain the operator's responsibility.

## Version compatibility

Runtime markers use the `kube-node-migrate.io` prefix: `operation` identifies task ownership, `migration` marks node migration state, and `reconcile-<instance-id>` is a temporary annotation used to requeue StatefulSet reconciliation. This standalone version changes the command, state directory, and marker names. Complete any tasks already in progress under the previous version with that version's executable and checkpoint.

## Quick start

Extract the complete release bundle and enter its directory. The execution host needs cluster administration credentials and SSH key access to both nodes. The nodes must already belong to the same cluster, be Ready, and have matching architectures. The target must be schedulable.

List node names, then generate a plan:

```bash
kubectl get nodes -o wide
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new
```

Review the namespaces, instances, PVCs, and target node in the output, then run:

```bash
./kube-node-migrate-linux-amd64 apply --allow-downtime
```

`apply` handles image prewarming, pre-copy, instance-by-instance cutover, and final verification. A successful run does not require another manual `verify`.

Normal use does not require YAML or explicit image references, tool paths, state directories, or a dedicated known_hosts file:

- State defaults to `.kube-node-migrate/state.json` in the current directory; a private directory is created automatically.
- Credentials are detected from a single-file `KUBECONFIG`, the user's default kubeconfig, then K3s's `/etc/rancher/k3s/k3s.yaml`. Use `--kubeconfig` to override detection.
- The filesystem helper and the pinned pv-migrate dependency are discovered in the release bundle.
- SSH defaults to root, port 22, the existing identity or agent, and standard known_hosts. Host key verification remains enabled.
- Online pre-copy defaults to three successful rounds. Instances are processed serially; final sync and full verification are always required.

Use the **Node names** shown by `kubectl get nodes` for node arguments. If kubectl credentials are not configured, add `--kubeconfig /etc/rancher/k3s/k3s.yaml` where appropriate.

## Generate default configuration

To customize a migration, print the default YAML, edit it, and use it when generating the plan:

```bash
./kube-node-migrate-linux-amd64 config print defaults > migration.yaml
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new \
  --config migration.yaml
```

`config print defaults` writes static defaults to standard output. It does not connect to Kubernetes or SSH, read deployment credentials, or create migration state. Omit redirection to view the configuration in the terminal. The output groups `image` and `resources` under `helper`, `sshd`, and `rsync`. The namespace allowlist is empty and the exclusion list contains `kube-system`. These defaults match `plan`.

## Select namespaces

**An explicit allowlist takes precedence. The exclusion list is used only when no allowlist is set.**

```bash
# Select namespaces outside the exclusion list; kube-system is excluded by default
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new

# Select only prod, pre, and test; exclusions do not apply
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new \
  --namespace prod,pre,test

# Without an allowlist, also exclude monitoring and logging
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new \
  --exclude-namespace monitoring,logging
```

These are three independent ways to select scope. Choose one `plan` invocation for each task. Existing state files are never overwritten. Use separate working directories for separate tasks, or specify a distinct path such as `--state /opt/migration/task-002/state.json`.

You can also use a configuration file:

```yaml
# Use namespaces as the allowlist; when omitted or empty, use exclusions.
namespaces: [prod, pre, test]
excludeNamespaces: [kube-system, monitoring, logging]
```


```bash
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new \
  --config migration.yaml
```

DaemonSets and static Pods are always skipped. Unsupported controllers, shared volumes, rolling updates, and other unsupported conditions prevent planning. The complete inventory is displayed and saved before execution; resume uses that saved inventory.

## Where helper Pods run

Helper Pods must run in the same namespace as the PVCs they mount. They cannot all be moved into a separate shared namespace. A volume in `prod` uses helpers in `prod`; a volume in `pre` uses helpers in `pre`.

| Configuration | Purpose | Node |
| --- | --- | --- |
| `helper` | Bind temporary target PVCs; the same resource settings are used for image prewarming Pods | Binding on the target; prewarming on the source or target as needed |
| `sshd` | Mount the source PVC and provide the copy channel | Source |
| `rsync` | Mount the temporary target PVC and receive data | Target |

All namespaces share one serial queue. Binding helpers are removed after use. Each volume uses one sshd/rsync pair, which is cleaned up before processing the next volume. A permanent helper set is not kept in every namespace. Deployments without PVCs do not create volume-copy Pods.

Before stopping a workload, the tool prewarms sshd on the source, and workload images, binding helper, and rsync on the target. Prewarming also works when the source is already cordoned. Source-side copy images are prepared even with `--precopy=false`, so an image pull problem is discovered before workload downtime begins.

Logs include the PVC namespace and copy Job name to help locate the helper Pods.

## View progress and resume after failure

Run from the same working directory:

```bash
./kube-node-migrate-linux-amd64 status
./kube-node-migrate-linux-amd64 resume --allow-downtime
```

`status --json` prints a redacted report. Logs are written to `.kube-node-migrate/state.json.log`. After fixing a failure, use `resume`; do not delete or hand-edit the checkpoint. Use `verify` for an independent check. If a task used an explicit `--state`, use the same path for subsequent commands.

## Offline environments

The bundle includes the main executable, filesystem helpers, and the original pv-migrate archive. **It does not currently include the three helper container images**, and does not automatically import image archives into nodes.

- With an internal registry: mirror the images in advance and override the references in `examples/offline.yaml`.
- In an isolated environment without a registry: import the images into K3s/containerd on both nodes in advance. Names, tags, and architectures must match the configuration.

The helper images bind target volumes, provide the source-side SSH channel, and run rsync. Keep the defaults for normal use. See the [examples](#examples) for offline preparation and overrides.

## Build and further reading

```bash
make test-integration check
make release VERSION=0.1.0
# After placing original pv-migrate v3.7.0 archives for both architectures in /opt/offline-tools:
make bundle VERSION=0.1.0 PV_MIGRATE_DIR=/opt/offline-tools
```

The root `main.go` is the entry point; `cmd/` defines subcommands. When running from source, first use `make build`, then run `./bin/kube-node-migrate plan ...`. Helpers are discovered in the same `bin/` directory. The temporary executable created by `go run .` has no adjacent helper or copy tool, so PVC migration requires explicit `--filesystem-helper` and `--pv-migrate` or `--pv-migrate-archive` paths. The standalone filesystem helper is in `tools/kube-node-fs/`.

- [11 examples](#examples): multiple replicas, multiple PVCs, stateless workloads, multiple namespaces, bandwidth limits, thresholds, offline use, non-root SSH, and more.
- [Workload migration and recovery boundaries](docs/workloads.md).
- [Pre-copy completion and final cutover conditions](docs/precopy.md).

Detailed preparation, configuration, examples, and troubleshooting follow below.

## Support matrix

| Object or condition | Support |
| --- | --- |
| Deployment | Instances without PVCs on the source node; replacement Pods remain owned by the original ReplicaSet |
| StatefulSet | Multiple replicas and one or more PVCs per instance; original Pod ordinals are retained |
| Volumes | Bound RWO Filesystem PVCs using `rancher.io/local-path` and WaitForFirstConsumer |
| Nodes | Already in the same cluster, Ready, matching architectures; the target is schedulable and satisfies existing scheduling constraints |
| Namespaces | Allowlist first; otherwise exclusions, including `kube-system` by default; DaemonSets and static Pods are always skipped |
| Execution environment | Linux, kubectl, OpenSSH; host image cache and CRI checks currently use the K3s/containerd toolchain |
| APIs and permissions | v1 ValidatingAdmissionPolicy API, workload resource management, Leases, admission protection, StatefulSet metadata patching, reads of `default/kubernetes` Endpoints, and impersonation |

Shared PVCs, Deployments with PVCs, direct hostPath mounts, active Jobs, HPAs, and controllers undergoing rolling updates prevent planning. Source and target nodes may have control-plane role labels, but toleration, affinity, and capacity checks must still pass.

Target nodeSelector, tolerations, and required NodeAffinity are checked during planning, execution, and before eviction. Explicit hostPort conflicts are checked against existing target Pods and incoming instances. Required Pod affinity/anti-affinity and hard topology spread constraints are rejected. Admission protection is tested through the configured API entry point and every Ready HTTPS API Server in `default/kubernetes`. Eviction is allowed only after every entry point rejects a probe using the controller identity. An unreadable endpoint list, failed direct connection, or policy that has not taken effect stops progress; the check does not fall back to testing only the load balancer. The execution host must be able to connect directly to those API Servers while retaining kubeconfig TLS verification.

Preflight does not simulate every scheduler plugin. Kubernetes makes the final admission decision for scoped ResourceQuotas, webhooks, and other admission rules. A successful independent-identity admission dry-run does not guarantee that the replacement workload will pass every admission rule.

Volumes may contain regular files, directories, and symbolic links. Extended attributes (including ACLs stored as xattrs), hard-linked regular files, FIFOs, sockets, and device files are rejected and require a different copy strategy. Planning scans for these conditions, and final digest calculation checks them again. Do not remove attributes or special files merely to bypass data requirements.

Migration handles only selected workload instances. The source is temporarily cordoned and its original scheduling state is restored afterward. The tool does not stop node services, delete Nodes, or change master/etcd membership.

## 1. Prepare tools and dependencies

### Use a complete offline bundle

Choose a bundle matching the execution host architecture, such as `kube-node-migrate-0.1.0-linux-amd64.tar.gz`. It contains the main executable, filesystem helpers for both architectures, and the original pv-migrate archive for the execution host.

```bash
mkdir -p /opt/kube-node-migrate
tar -xzf kube-node-migrate-0.1.0-linux-amd64.tar.gz -C /opt/kube-node-migrate
cd /opt/kube-node-migrate
./kube-node-migrate-linux-amd64 --version
```

Version output begins with `kube-node-migrate version 0.1.0` and includes the source `commit`, UTC build time `built`, and checkpoint format `state`. The current format is 8. Direct `go run` builds report unknown commit/build time. Save the full version output when troubleshooting. On arm64 execution hosts, use the corresponding bundle and `kube-node-migrate-linux-arm64`; examples below use amd64.

### Build from source

Go 1.26.2 or a compatible version and make are required. Fetch the source, then run from the repository root:

```bash
git clone https://github.com/Kalen89/kube-node-migrate.git
cd kube-node-migrate
```


```bash
make test-integration check
make release VERSION=0.1.0
```

`make release` writes the main executables and filesystem helpers for both architectures into `bin/`. Real rsync integration tests also require rsync on the build host.

GitHub Actions runs formatting and static checks, real rsync integration tests, dependency verification, and Linux amd64/arm64 builds on pushes, pull requests, and manual dispatch. CI uses public Go dependencies and does not need internal infrastructure credentials.

Default helper images are hosted on Docker Hub. Use `helper.image`, `sshd.image`, and `rsync.image` to select another registry for restricted or offline environments; see example 8. Changing defaults does not change image references saved in an existing checkpoint. Resume continues to use the saved task configuration.

To build and run only on the current execution host, use the commands below. Actual migration on Linux still requires the pv-migrate dependency described next:

```bash
make build VERSION=0.1.0
./bin/kube-node-migrate --version
./bin/kube-node-migrate plan --source worker-old --target worker-new
```

To build complete offline bundles, place these two **original pv-migrate v3.7.0 release archives** in `/opt/offline-tools`:

```text
pv-migrate_v3.7.0_linux_x86_64.tar.gz
pv-migrate_v3.7.0_linux_arm64.tar.gz
```


```bash
make bundle VERSION=0.1.0 PV_MIGRATE_DIR=/opt/offline-tools
```

Before producing either bundle, `make bundle` checks both original archives and their regular-file `pv-migrate` entries against the same pinned SHA-256 digests used at runtime. It also verifies Linux executable architectures and the main program's version and build metadata. After packaging, it checks the file list, permissions, sizes, and digests, then writes two `.tar.gz` archives and their `.tar.gz.sha256` files. Packaging uses Go's standard library rather than platform tar/checksum commands and can run on macOS or Linux. To fix build metadata, pass `COMMIT` and `BUILD_TIME`, for example `make bundle VERSION=0.1.0 COMMIT=unknown BUILD_TIME=2026-10-30T00:00:00Z`.

Bring each archive and its checksum file to the deployment environment and verify before extraction:

```bash
sha256sum -c kube-node-migrate-0.1.0-linux-amd64.tar.gz.sha256
# On a macOS preparation host:
shasum -a 256 -c kube-node-migrate-0.1.0-linux-amd64.tar.gz.sha256
```

The migration does not download these tools at runtime. Alternatively, use `--pv-migrate /opt/tools/pv-migrate` during `plan` to select an existing v3.7.0 executable. Archive-based preparation checks the embedded SHA-256 digest, so retain the original archive.

## 2. Check the execution environment

Run on a host with cluster administration credentials. It can be the source, target, or a separate management host, and must have SSH access to both nodes' InternalIP addresses. If credentials, images, and dependencies are already prepared, the two quick-start commands are sufficient.

```bash
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml get nodes -o wide
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n prod get pods -o wide
```

`--source` and `--target` take the **Node names** in the first column, not IP addresses. `worker-old`, `worker-new`, and `192.0.2.11/12` below are examples; replace them with actual values.

```bash
ssh root@192.0.2.11 true
ssh root@192.0.2.12 true
```

On first connection, verify SSH fingerprints through a trusted channel. The default uses the execution user's standard known_hosts, usually `~/.ssh/known_hosts`. A dedicated file and `sshKnownHosts` setting are not required.

SSH is non-interactive, so prepare a working key or agent in advance. Non-root SSH users need `sudo -n`. The execution user must also be able to read kubeconfig, the SSH private key, and tool files. K3s and kubelet do not need to be stopped for migration.

## 3. Generate a plan

Use a separate task directory for each migration. Do not reuse another migration's state file.

```bash
install -d -m 0700 /opt/node-migration/task-001
cd /opt/kube-node-migrate

./kube-node-migrate-linux-amd64 plan \
  --source worker-old \
  --target worker-new \
  --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml \
  --config examples/migration.yaml \
  --state /opt/node-migration/task-001/state.json
```

`plan` saves the inventory and checks controllers, Pod/PVC identities, permissions, SSH, filesystems, and other prerequisites. It may cache the filesystem helper, but does not evict workload Pods or switch PVCs.

Confirm that the source, target, namespaces, instances, and volumes match your intent:

```bash
./kube-node-migrate-linux-amd64 status --state /opt/node-migration/task-001/state.json
./kube-node-migrate-linux-amd64 status --state /opt/node-migration/task-001/state.json --json
```

All instances on the source node in the selected namespaces are inventoried. There is currently no Pod-name or label filter. Review the complete list before execution.

## 4. Migrate and verify

Run after accepting the per-instance downtime required for cutover:

```bash
./kube-node-migrate-linux-amd64 apply \
  --state /opt/node-migration/task-001/state.json \
  --allow-downtime

./kube-node-migrate-linux-amd64 verify \
  --state /opt/node-migration/task-001/state.json
```

The tool prewarms images, prepares target volumes, and checks capacity. It then processes one instance at a time: online pre-copy → protect the instance → normal Eviction and confirmation that the original containers have exited → final sync and full digest verification → PVC cutover → create the target instance and wait for Ready.

Every PVC belonging to an instance must pass verification before any of its volumes are rebound. The instance starts only after all its volumes are rebound. The next instance is processed only after the current instance and its peer replicas have recovered.

Execution and resume disk checks read source/target inventories and actual target allocation. They require remaining growth in space/inodes plus the temporary peak needed for serial rsync file rewrites, rather than a second full copy of data already present. For volumes sharing a target filesystem, growth is summed and the largest single temporary-copy requirement is retained. Future deletions are not credited in advance, and sparse-file logical size is not treated as reusable allocated space. Logs show growth and temporary space separately; the underlying disk still needs enough headroom. After a long pre-copy, capacity, quota, and remaining copy space are checked again before evicting a still-running source instance.

Check workload instances and PVCs through Kubernetes as well:

```bash
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n prod get pods -o wide
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n prod get pvc
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml get nodes
```

A successful `verify` means the tool's instance, controller, target binding, and recorded verification checks passed. Database replication, quorum, and real application requests still need the relevant middleware and application checks.

## 5. Monitor progress and resume

```bash
tail -f /opt/node-migration/task-001/state.json.log

./kube-node-migrate-linux-amd64 status \
  --state /opt/node-migration/task-001/state.json --json
```

The task directory contains:

| File | Purpose |
| --- | --- |
| `state.json` | Private transaction checkpoint containing original application configuration; required for resume and must not be published |
| `state.json.log` | Append-only execution log |
| `state.json.report.json` | Atomically refreshed, redacted live progress report; rebuildable from the private checkpoint |
| `state.json.lock` | Prevents concurrent execution of the same task |

`ActiveStage` identifies the stage at the last save, and `Events` records completed stages and durations. Source digest, final sync/helper release, and target verification are timed separately. Pod creation, scheduling, container startup, and Ready are shown separately but share one startup deadline. A stopped process may leave its last stage in the report; consult `UpdatedAt` and logs.

Transaction changes are still synchronously written to the private checkpoint, with file and directory fsync completed before the report is refreshed and execution advances. Observation-only changes to `ActiveStage`, `Events`, or `UpdatedAt` atomically replace the redacted report and coalesce repeated full private-checkpoint writes. The report is rebuildable, is not individually fsynced, and is not authoritative for resume. `status` and `status --json` use the latest private transaction checkpoint; consult `.report.json` for live stage timing. An abnormal exit can lose recent observation-only records without weakening transaction durability.

Find and fix the logged cause, then use the **original state file**:

```bash
./kube-node-migrate-linux-amd64 resume \
  --state /opt/node-migration/task-001/state.json \
  --allow-downtime

./kube-node-migrate-linux-amd64 verify \
  --state /opt/node-migration/task-001/state.json
```

`apply` and `resume` use saved checkpoint parameters. Editing the original YAML does not change an active task. Do not delete state, manually change bindings, or start a separate migration for an instance whose cutover has already begun. If execution fails after source writes have stopped, the current protection is retained. Once the target has accepted new writes, the tool does not automatically revert to old data.

When pre-copy is enabled and resume finds the original Pod still Ready, with the same UID and not terminating, a task interrupted during preparation or stopping repeats the pre-copy policy. Target constraints and hostPort conflicts are checked again before eviction. An instance already terminating continues through the stopped-source workflow without requiring the source to become online again.

After a final digest mismatch, fix the cause and `resume` with the original checkpoint. A fresh source scan, final sync, and full verification are allowed only after confirming that source writes have stopped, every volume retains its original binding, no extra consumers exist, and cutover/startup has not begun. If a normal copy-helper controller update causes a resource-version conflict, the tool rechecks the UID and Helm ownership, saves the latest version, and retries within a bounded limit. Persistent conflicts preserve the current state. Interrupted cleanup resumes only for resources whose UIDs and ownership were checkpointed. Replaced names or unknown objects stop cleanup without expanding its scope. See [workload migration and recovery boundaries](docs/workloads.md#resuming-after-failure).

## Scope and defaults

- Setting `--namespace` or YAML `namespaces` selects only the allowlist; exclusions do not apply.
- Without an allowlist, namespaces outside the exclusion list are selected. `kube-system` is excluded by default; `--exclude-namespace` or `excludeNamespaces` can add exclusions.
- Plans save the resolved namespace and instance inventory. Resume does not rediscover or expand the scope.
- Without `--state`, state is stored at `.kube-node-migrate/state.json` in the current directory. Run one task per directory, or use explicit, separate state paths.
- Without `--kubeconfig`, detection checks a single-file `KUBECONFIG`, the user's default kubeconfig, then the K3s default. If the environment variable contains a file list, explicitly select one complete configuration file.

## Common flags and configuration

Explicit CLI flags override YAML, which overrides defaults. Relative file paths in YAML are resolved against its directory. Namespaces, dependencies, SSH, bandwidth, and pre-copy settings are fixed during `plan`; subsequent commands primarily use `--state`.

| `plan` flag | Purpose and default |
| --- | --- |
| `--namespace` / `-n` | Optional allowlist; repeatable or comma-separated; takes precedence over exclusions |
| `--exclude-namespace` | Add exclusions when there is no allowlist; `kube-system` is excluded by default |
| `--state` | Defaults to `.kube-node-migrate/state.json`; creates a private directory |
| `--kubeconfig` / `--context` | Select cluster credentials and context; an explicit kubeconfig is recommended |
| `--ssh-user` / `--ssh-port` / `--ssh-key` | Defaults to root, 22, and the existing SSH identity/agent |
| `--ssh-known-hosts` | Optional separate trusted-host file; otherwise uses standard SSH known_hosts |
| `--pv-migrate` / `--pv-migrate-archive` | Existing tool or original offline archive; otherwise discovered next to the main executable |
| `--filesystem-helper` | Defaults to the adjacent filesystem helper for the node architecture |
| `--precopy` | Defaults to true; false copies only after source writes stop |
| `--precopy-rounds` | Defaults to 3; allowed range 1–20 |
| `--precopy-threshold` | Defaults to 0, using successful-round completion; an explicit value checks rsync container duration |
| `--parallel-digest` | Defaults to false; when enabled, reads complete source and target SHA-256 digests concurrently after final checksum sync |
| `--rsync-bandwidth-limit` | KiB/s; defaults to 0, unlimited |
| `--pv-concurrency` | Currently fixed at 1; instances are processed serially |
| `--reserve-cpu` / `--reserve-memory` / `--reserve-pods` | Migration headroom; base defaults are `200m`, `256Mi`, and `2`, raised automatically when required |
| `--wait-timeout` | Defaults to 10m per Kubernetes transition; four startup stages share one deadline |
| `--copy-timeout` | Defaults to 12h per copy or filesystem operation |

All fields in this editable example are supported by the current CLI:

```yaml
namespaces: [prod, pre, test]
excludeNamespaces: [kube-system, monitoring, logging]
pvConcurrency: 1
precopy: true
precopyRounds: 3
precopyThreshold: 0s
parallelDigest: false
rsyncBandwidthLimit: 51200
reserve:
  cpu: 300m
  memory: 1Gi
  pods: 3
helper:
  image: docker.io/rancher/mirrored-library-busybox:1.36.1
  resources:
    requests: {cpu: 100m, memory: 128Mi}
    limits: {cpu: "1", memory: 512Mi}
sshd:
  image: docker.io/utkuozdemir/pv-migrate-sshd:v3.7.0
  resources:
    requests: {cpu: 100m, memory: 128Mi}
    limits: {cpu: "1", memory: 512Mi}
rsync:
  image: docker.io/utkuozdemir/pv-migrate-rsync:v3.7.0
  resources:
    requests: {cpu: 100m, memory: 128Mi}
    limits: {cpu: "1", memory: 512Mi}
healthChecks: []
```

Helper services are grouped under `helper`, `sshd`, and `rsync`, each with `image` and `resources`. Image precedence is: explicit CLI flag > nonempty service `image` > legacy `images` group > original top-level image field > default. Both older configuration forms remain supported.

`resources` accepts CPU/memory `requests` and `limits`. Quantities must be positive, with requests no greater than limits. Partial overrides inherit unspecified defaults. Settings apply to actual Pods, source/target capacity checks, and namespace quota checks. `reserve.cpu` and `reserve.memory` describe total migration headroom and are raised if helpers need more; they do not rewrite Pod requests or limits. Editing YAML for an active task does not change its saved plan.

`reserve` groups scheduling headroom: `cpu`, `memory`, and extra `pods` slots. Preflight checks whether existing target workloads + incoming workloads + headroom exceed allocatable capacity. These values do not lock resources or create placeholder Pods. Normal use can omit `reserve`; base defaults are raised according to helper and cutover requirements. The default configuration command prints this group.

Target capacity checks use the checkpoint to count confirmed instances not yet on the target. Replacement Pods are deduplicated by identity, and running task helpers count toward total headroom. Source checks consider peak demand at each serial online/offline copy stage. Disabling pre-copy can use capacity freed when the current instance exits, but missing source-side images still require room for an online prewarming helper. Workload accounting includes sequential init-container peaks, restartable init sidecars, and Pod overhead.

Namespace ResourceQuota is calculated separately from node `reserve`. Checks use the peak additional requests, limits, and Pods across prewarming, binding, online copying, offline copying, and workload replacement, plus all temporary PVCs and storage quotas. The full node reserve is not repeated for each namespace. Helpers incorporate current LimitRange defaults, and checks cover Container/Pod resource bounds and PVC storage bounds. Scoped quotas, webhooks, and other final admission rules remain subject to the API.

Reserve precedence is: explicit `--reserve-*` flag > nonempty field in `reserve` > original top-level field > default. Legacy `reserveCPU`, `reserveMemory`, and `reservePods` remain supported; individual grouped fields can be overridden. Checkpoint and capacity-check semantics remain unchanged.

`parallelDigest` / `--parallel-digest` changes only how final full digests are read. The default false keeps serial scanning. With independent source/target storage, true can overlap reads after source writes stop and final rsync `--checksum` completes. Keep the default for shared HDDs or disks on the same physical host to avoid competing I/O. It does not skip checksums, digests, stopped-source checks, or binding checks, and does not guarantee a measured speedup.

The grouped service format requires a program version that supports it; older programs must use their supported format. New checkpoints use format 8 to prevent older programs from ignoring newer recovery protections. Formats 6 and 7 can be read by the new program; format 6 resumes with the original resource defaults. Old whole-node migration checkpoints remain view-only. This format support does not override the standalone-version marker compatibility boundary described above.

Dependencies can also be selected through YAML `pvMigrate`, `pvMigrateArchive`, and `filesystemHelper`. SSH user, port, key, kubeconfig, source/target, and timeouts use CLI flags.

## Examples

Run these commands from `/opt/kube-node-migrate`. Each example uses a different state directory. Check nodes, credentials, and dependencies first. For examples showing only `plan`, review the plan, then use the same state file with `apply` and `verify` as in section 4.

### Example 1: Verify data with a test StatefulSet first

`examples/demo.yaml` creates a dedicated `migration-demo` namespace, one Pod, and one local-path PVC. Its container entry point does not create test data, preventing an empty volume from recreating matching content after restart. Before migration, write a unique token once and save its SHA-256:

```bash
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml apply -f examples/demo.yaml
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n migration-demo \
  wait --for=condition=Ready pod/app-0 --timeout=180s
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n migration-demo get pod app-0 -o wide
MIGRATION_TEST_TOKEN="migration-demo-$(date -u +%Y%m%dT%H%M%SZ)-$$"
printf '%s\n' "$MIGRATION_TEST_TOKEN" | kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml \
  -n migration-demo exec -i app-0 -- sh -c 'cat > /data/migration-token'
MIGRATION_TEST_SHA256=$(kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml \
  -n migration-demo exec app-0 -- sha256sum /data/migration-token)
printf '%s\n' "$MIGRATION_TEST_SHA256"
```

Use its actual node as the source, and choose another Ready, schedulable node as the target:

```bash
MIGRATION_SOURCE_NODE=$(kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml \
  -n migration-demo get pod app-0 -o jsonpath='{.spec.nodeName}')
MIGRATION_TARGET_NODE=worker-new

./kube-node-migrate-linux-amd64 plan \
  --source "$MIGRATION_SOURCE_NODE" --target "$MIGRATION_TARGET_NODE" \
  --namespace migration-demo --kubeconfig /etc/rancher/k3s/k3s.yaml \
  --config examples/migration.yaml --state /opt/node-migration/demo/state.json
./kube-node-migrate-linux-amd64 apply --state /opt/node-migration/demo/state.json --allow-downtime
./kube-node-migrate-linux-amd64 verify --state /opt/node-migration/demo/state.json

kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n migration-demo get pod app-0 -o wide
MIGRATION_ACTUAL_SHA256=$(kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml \
  -n migration-demo exec app-0 -- sha256sum /data/migration-token)
test -n "$MIGRATION_TEST_SHA256" && test "$MIGRATION_ACTUAL_SHA256" = "$MIGRATION_TEST_SHA256"
```

The Pod should still be named `app-0` and run on the selected target, and the final `test` must return 0. Missing data or a different digest fails acceptance. Run the steps in the same terminal and retain the pre-migration digest; do not regenerate the token or overwrite the expected digest afterward. Confirm that source and target differ. This example creates test resources and does not automatically remove their data.

### Example 2: Replicated StatefulSet with two PVCs per instance

Assume `middleware/db-0` runs on `worker-old` and mounts `data-db-0` and `logs-db-0`, while other replicas run on other nodes:

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace middleware \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/migration.yaml \
  --state /opt/node-migration/db/state.json
./kube-node-migrate-linux-amd64 status --state /opt/node-migration/db/state.json
./kube-node-migrate-linux-amd64 apply --state /opt/node-migration/db/state.json --allow-downtime
./kube-node-migrate-linux-amd64 verify --state /opt/node-migration/db/state.json
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n middleware get pod db-0 -o wide
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n middleware get pvc data-db-0 logs-db-0
```

Both volumes are discovered from the instance's mounts. Rebinding begins only after both pass verification. Replicas on other nodes keep running. Other workload instances on the source in the selected namespaces are also inventoried, so review the saved plan first.

### Example 3: Migrate only stateless Deployment instances

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace frontend \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/migration.yaml \
  --state /opt/node-migration/frontend/state.json
./kube-node-migrate-linux-amd64 apply --state /opt/node-migration/frontend/state.json --allow-downtime
./kube-node-migrate-linux-amd64 verify --state /opt/node-migration/frontend/state.json
```

Replacement Pod names may change. The original ReplicaSet and Deployment template remain unchanged. Deployments with PVCs are not supported. If the controller later recreates a volumeless instance, it schedules according to the original template; the operator decides whether the old node should remain cordoned.

### Example 4: Migrate multiple namespaces in one task

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new \
  --namespace middleware --namespace platform \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/migration.yaml \
  --state /opt/node-migration/multi-ns/state.json
```

You can also write `--namespace middleware,platform`. CLI namespaces replace the entire YAML `namespaces` list. Without an allowlist, use `--exclude-namespace monitoring,logging` to add exclusions. After reviewing the plan, run the same `apply` and `verify` commands against this state file.

### Example 5: Limit bandwidth on HDDs or shared virtualization storage

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/low-impact.yaml \
  --state /opt/node-migration/low-impact/state.json
```

This configuration limits each rsync stream to 20 MiB/s and keeps three online pre-copy rounds. The limit controls transfer speed, not the disk reads required for scanning, checksums, and full digests. Total duration may increase.

Keep the default `parallelDigest: false` for this scenario. Consider adding `--parallel-digest` at planning time only when source and target have independent storage and disk pressure has been assessed. Measure the actual benefit.

### Example 6: Require an explicit pre-copy duration condition

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/threshold.yaml \
  --state /opt/node-migration/threshold/state.json
```

The configuration attempts up to five rounds. One round with a trusted rsync container duration of at most 45 seconds satisfies the pre-copy policy. If none qualifies, the source keeps running and progress stops. This duration includes scanning and connection setup and is not a bound on final downtime. Once conditions improve, `resume` with the original checkpoint; active tasks do not automatically read edited YAML.

### Example 7: Skip online pre-copy for small volumes

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/migration.yaml \
  --precopy=false --state /opt/node-migration/no-precopy/state.json
```

All copying then happens while source writes are stopped. Final sync and digest verification still run. Use this for small datasets where the resulting downtime is acceptable.

### Example 8: Use an internal registry and offline dependencies

Replace the three image references in `examples/offline.yaml` with the actual internal registry, and prepare the pinned original archive and filesystem helpers:

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/offline.yaml \
  --state /opt/node-migration/offline/state.json
```

The full bundle includes tools and dependency archives, but not container images. Both nodes must be able to pull the images or have usable cached copies, with authentication configured as needed. The example archive name targets an amd64 execution host; change it for arm64. To use an existing executable instead, pass `--pv-migrate /opt/tools/pv-migrate`.

#### Prepare image archives without an internal registry

On a preparation host with image-source access, for amd64 nodes:

```bash
docker pull --platform linux/amd64 docker.io/rancher/mirrored-library-busybox:1.36.1
docker pull --platform linux/amd64 docker.io/utkuozdemir/pv-migrate-sshd:v3.7.0
docker pull --platform linux/amd64 docker.io/utkuozdemir/pv-migrate-rsync:v3.7.0
docker save -o migration-images-amd64.tar \
  docker.io/rancher/mirrored-library-busybox:1.36.1 \
  docker.io/utkuozdemir/pv-migrate-sshd:v3.7.0 \
  docker.io/utkuozdemir/pv-migrate-rsync:v3.7.0
```

Transfer the archive into the isolated environment, copy it to both nodes, and run on each node:

```bash
sudo k3s ctr -n k8s.io images import /opt/migration-images-amd64.tar
sudo k3s ctr -n k8s.io images ls
```

These are the tool's default image references. Names, tags, and node architectures must match; prepare an arm64 archive for arm64 nodes. When using cached copies under these default references, do not also apply an offline.yaml that points to different repositories. If references change, save and import the exact configured names. See the [K3s image import documentation](https://docs.k3s.io/import-images/).

### Example 9: Non-root SSH, a dedicated key, and a custom port

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/migration.yaml \
  --ssh-user ops --ssh-port 2222 --ssh-key /home/ops/.ssh/id_ed25519 \
  --state /opt/node-migration/ssh-custom/state.json
```

The same SSH user, port, and identity settings apply to both nodes. `ops` needs working `sudo -n`. Trusted-host records must match the actual InternalIP and port.

The commands use the execution user's standard known_hosts. Use `--ssh-known-hosts /path/to/known_hosts` or YAML `sshKnownHosts` only when a separate trusted-host file is needed. Omitting these options does not disable host key verification.

### Example 10: Add an HTTP application health check

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/health-check.yaml \
  --state /opt/node-migration/with-health/state.json
```

Replace the example URL with a stable Service, VIP, or application health endpoint. Checks run before and after the complete migration. Instances are still verified individually using Pod Ready. Checks use HTTP GET, do not automatically follow redirects, and reject URLs with credentials, a query, or a fragment. Skipping verification for a self-signed certificate requires explicit configuration appropriate to the trusted environment.

### Example 11: The source is already cordoned

An already-cordoned source can still be planned and its existing workload instances migrated. The target must be schedulable. The tool records the source's original state and preserves its cordoned state after completion.

If the source was schedulable before migration, it becomes schedulable again afterward. To keep new Pods off the old node, the operator can run `kubectl cordon worker-old`. Retirement, removal, and control-plane replacement are separate follow-up operations.

## Troubleshooting

| Message or symptom | Action |
| --- | --- |
| State file already exists | Inspect it with `status`, use `resume` for the original task, or choose a new directory for a new task |
| Source or target is not a Node name | Check names and InternalIP addresses with `kubectl get nodes -o wide` |
| Target capacity or scheduling checks fail | Check requests, Pod limits, taints, affinity, nodeSelector, and available target storage |
| Permission preflight fails | Provide the required API, admission management, metadata patching, or impersonation permissions |
| SSH host key verification fails | Verify the actual node and fingerprint through a trusted channel, then update the correct record |
| A PDB blocks Eviction | Check peer health and the application's allowed disruption policy, then resume |
| Explicit pre-copy threshold is not met | The source stays running; check disk pressure and ongoing writes, then resume when conditions improve |
| Copy or digest verification fails | Retain the checkpoint and current protection, fix the cause, then `resume` without early cutover or startup |
| Pod is not created, is Pending, or fails startup | Use stage logs to identify creation, scheduling, or container problems; check controllers, resources, images, and events |
| Editing YAML does not affect resume | An active task uses its saved plan; YAML is read only by `plan` |

See [workload migration](docs/workloads.md) and [pre-copy completion conditions](docs/precopy.md) for detailed cutover and recovery rules.

## License

Copyright 2026 kube-node-migrate contributors.

Licensed under the [Apache License, Version 2.0](LICENSE). Third-party dependencies and bundled upstream tools retain their respective licenses.

## Acknowledgments

This project uses [pv-migrate](https://github.com/utkuozdemir/pv-migrate) as its volume-copy tool. Thank you to its author [Utku Özdemir](https://github.com/utkuozdemir) and all contributors for their work on Kubernetes PVC migration.

Thanks also to the [rsync](https://rsync.samba.org/) contributors for the data-transfer foundation, and the [Kubernetes](https://kubernetes.io/) and [Rancher local-path-provisioner](https://github.com/rancher/local-path-provisioner) communities for the orchestration and local-volume infrastructure.
