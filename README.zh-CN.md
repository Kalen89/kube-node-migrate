# kube-node-migrate

[English](README.md) | 简体中文

在 Kubernetes 节点之间迁移工作负载及本地持久化数据，支持在线预同步、完整数据校验和断点续跑。当前卷迁移范围为 local-path PVC。版本 **0.1.0**。保留原控制器、其他副本及旧数据，旧机器下线和控制面成员处置由操作者决定。

## 开发现状与 AI 使用说明

**本项目目前以 AI 辅助开发为主，大部分代码和文档借助 AI 完成。** AI 参与代码实现、测试编写和文档整理，现有功能通过自动化测试和实际迁移场景持续验证。

项目仍处于早期迭代阶段。后续会逐步完善代码结构、边界处理、异常恢复和兼容性，扩展经过验证的迁移场景，并持续补充测试与使用文档。欢迎大家参与代码审查、反馈可复现的问题，以及贡献改进。当前支持范围和使用限制以本文说明为准。

## 为什么做这个项目

### 为什么选择 Local PV

本项目主要面向几台到几百台服务器的中小规模部署。在这类环境中，直接使用应用服务器已有的本地磁盘，是控制基础设施成本和运维复杂度的一种选择。服务器数量只是判断起点，是否适合 Local PV，还取决于业务负载、硬件、恢复目标和团队运维能力。

- **建设成本**：独立的共享存储或分布式存储通常需要额外的服务器、磁盘、网络带宽、冗余和预留容量。对于较小的部署，这部分额外投入可能占据较高的基础设施预算比例。
- **运维成本**：额外维护一套存储系统，也意味着增加部署、监控、升级、容量规划、故障恢复和专业知识要求。这些成本不会因为应用集群较小而消失。
- **I/O 路径**：本地磁盘可以减少远程块存储访问环节。网络存储可能引入额外延迟、占用网络带宽，并增加存储层复制开销；对于网络受限、对 I/O 敏感的业务，影响可能较明显。实际性能取决于硬件、存储实现和负载，不能统一承诺固定比例的提升。
- **组件自身的高可用能力**：MySQL、PostgreSQL、Redis、MinIO 等主流组件提供复制、故障切换、集群或分布式数据保护机制。按故障域正确配置后，可以采用“组件负责自身高可用、数据落在本地磁盘”的架构。再叠加一层存储复制，可能增加容量放大、网络流量和写入成本，应根据实际需求评估收益。

对于已经具备适当应用层冗余的部署，本地磁盘有助于减少额外基础设施投入，让数据路径更容易理解和维护。这不意味着所有环境都不需要共享存储；Local PV 本身也不提供复制或跨节点自动恢复。副本、仲裁、持久化、备份和恢复流程仍需按各组件的要求配置。可参考 [Kubernetes Local 卷说明](https://kubernetes.io/docs/concepts/storage/volumes/#local)、[PostgreSQL 复制说明](https://www.postgresql.org/docs/current/warm-standby.html)和 [MySQL Group Replication 说明](https://dev.mysql.com/doc/refman/8.4/en/group-replication.html)。

### 为什么需要服务器级别的迁移

本地数据与所在节点绑定，但服务器在使用过程中会遇到硬件老化、磁盘或 RAID 控制器故障预警、反复发生的主机故障、容量不足、硬件升级、操作系统维护，以及虚拟化平台调整等情况。对于可恢复的故障，也可能需要在服务器修复、数据仍可读取后，将业务迁到新机器，再退出旧机器。

无状态 Pod 通常可以重新调度，有状态业务还需要迁走数据并维持 Pod、PVC 和 PV 之间的关系。单纯驱逐或重建 Pod，并不会把本地数据搬到另一台服务器。一台机器可能承载多个命名空间、多个组件和多块 PVC，手工协调复制、停写、改绑、校验和失败恢复，容易漏步骤，也难以重复执行。

`kube-node-migrate` 将这件事整理成可重复的流程：盘点选定业务，在业务运行时预同步，逐实例停写，完成最终同步和完整校验，再切换卷并等待目标实例 Ready。失败后通过检查点继续，旧 PV 和旧数据保留供操作者核查。预同步的目的是减少停机期间剩余的数据传输量，不承诺零停机或固定的时间收益。

迁移要求源节点仍可访问、源数据仍可读取。如果服务器已经无法访问或磁盘数据丢失，应依赖组件副本或备份恢复，本工具无法重建丢失的数据。工具负责工作负载迁移；旧节点何时下线，以及控制面、etcd 成员如何调整，由操作者决定。

## 版本兼容说明

运行时标记统一使用 `kube-node-migrate.io` 前缀：`operation` 标记任务归属，`migration` 标记节点迁移状态，`reconcile-<实例标识>` 是恢复 StatefulSet 协调的临时注解。该独立版本调整了命令、状态目录及标记名称；原版本的在途任务请使用原版本程序及原检查点完成。

## 快速使用

解压完整发布包并进入目录。执行机需有集群管理凭据，能够通过 SSH 密钥访问源、目标节点；两个节点已加入同一集群并 Ready，目标可调度，架构一致。

先查看节点名称，再生成计划：

```bash
kubectl get nodes -o wide
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new
```

检查输出的命名空间、实例、PVC 和目标节点，然后执行：

```bash
./kube-node-migrate-linux-amd64 apply --allow-downtime
```

`apply` 自动完成镜像预热、预同步、逐实例切换及最终验证，成功后无需再手动执行一次 `verify`。

常规使用不需要 YAML，也不需要填写镜像、工具路径、状态目录或专用 known_hosts：

- 状态默认保存在当前目录的 `.kube-node-migrate/state.json`，自动创建私有目录。
- 集群凭据依次使用单文件 `KUBECONFIG`、用户默认 kubeconfig、K3s 的 `/etc/rancher/k3s/k3s.yaml`；可用 `--kubeconfig` 覆盖。
- 文件系统助手和固定版本 pv-migrate 从发布包中自动发现。
- SSH 默认使用 root、22 端口、现有 identity／agent 和标准 known_hosts，仍校验主机指纹。
- 默认在线预同步 3 轮，逐实例串行执行，最终同步和完整摘要校验始终保留。

节点参数使用 `kubectl get nodes` 中的 **Node 名称**。常规 `kubectl` 命令若未设置凭据，可加 `--kubeconfig /etc/rancher/k3s/k3s.yaml`。

## 生成默认配置

需要自定义配置时，先输出默认 YAML，按现场需要修改后用于生成计划：

```bash
./kube-node-migrate-linux-amd64 config print defaults > migration.yaml
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new \
  --config migration.yaml
```

`config print defaults` 只向标准输出写入静态默认配置，不连接 Kubernetes 或 SSH，不读取现场凭据，也不创建迁移状态。省略重定向时直接在终端查看。生成结果按 `helper`、`sshd`、`rsync` 分组，每组包含 `image` 和 `resources`，白名单为空，黑名单默认包含 `kube-system`；默认值与 `plan` 一致。

## 迁移哪些命名空间

**有白名单就用白名单；没有白名单，才使用黑名单。**

```bash
# 默认迁移黑名单以外的所有命名空间，黑名单默认包含 kube-system
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new

# 只迁移 prod、pre 和 test，黑名单不参与筛选
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new \
  --namespace prod,pre,test

# 没有白名单时，追加排除 monitoring、logging
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new \
  --exclude-namespace monitoring,logging
```

上面是三种独立选择方式，每次任务只选一种 `plan`。已有状态文件时不会覆盖；多个任务使用不同工作目录，或用 `--state /opt/migration/task-002/state.json` 指定各自状态文件。

也可以显式使用配置文件：

```yaml
# 有 namespaces 时只按白名单选择；省略或为空时，按黑名单过滤。
namespaces: [prod, pre, test]
excludeNamespaces: [kube-system, monitoring, logging]
```

```bash
./kube-node-migrate-linux-amd64 plan --source worker-old --target worker-new \
  --config migration.yaml
```

DaemonSet 和静态 Pod 始终跳过。未支持的控制器、共享卷、正在滚动更新等情况会阻止计划，完整清单会在执行前展示并保存；续跑沿用原清单。

## 辅助 Pod 在哪里运行

辅助 Pod 与所挂载的 PVC 位于同一 namespace，不能统一放到另一个专用 namespace。迁移 `prod` 的卷时在 `prod` 创建，迁移 `pre` 的卷时在 `pre` 创建。

| 配置主体 | 用途 | 节点 |
| --- | --- | --- |
| `helper` | 绑定目标临时 PVC；同一份资源配置也用于镜像预热 Pod | 绑定在目标节点；预热按需在源或目标节点 |
| `sshd` | 挂载源 PVC，提供复制通道 | 源节点 |
| `rsync` | 挂载目标临时 PVC，接收数据 | 目标节点 |

多个 namespace 共用同一个全局串行队列。绑定 helper 用完清理；复制时运行当前卷对应的一组 sshd、rsync，清理完成后处理下一块卷，不会在每个 namespace 常驻一套。无 PVC 的 Deployment 不创建卷复制 Pod。

停写前按节点预热实际所需镜像：源节点预热 sshd，目标预热业务镜像、绑定 helper 和 rsync；已 cordon 的源节点也可执行预热。即使设置 `--precopy=false`，也先完成源端复制镜像准备，避免业务停机后才发现源节点无法拉取镜像。

日志会输出所处理 PVC 的 namespace 和复制 Job 名，可据此定位辅助 Pod。

## 查看进度和失败续跑

在同一工作目录中执行：

```bash
./kube-node-migrate-linux-amd64 status
./kube-node-migrate-linux-amd64 resume --allow-downtime
```

`status --json` 输出脱敏报告；运行日志保存在 `.kube-node-migrate/state.json.log`。修复失败原因后使用 `resume`，不要删除或手改检查点。需要独立复查时可以执行 `verify`。显式指定过 `--state` 的任务，后续命令使用同一路径。

## 离线环境

发布包包含主程序、文件系统助手和 pv-migrate 原始归档；**目前不包含三个辅助容器镜像**，也不会自动将镜像归档导入节点。

- 有内网仓库：提前同步镜像，在 `examples/offline.yaml` 中覆盖镜像地址。
- 无仓库的隔离环境：提前将镜像归档导入源、目标的 K3s/containerd，镜像名称、标签及架构应与配置一致。

辅助镜像分别用于绑定目标卷、提供源数据的 SSH 通道、运行 rsync。日常使用保留默认值即可。离线准备和覆盖方式见下文的 [操作案例](#操作案例)。

## 构建和专题说明

```bash
make test-integration check
make release VERSION=0.1.0
# 在 /opt/offline-tools 放入两个架构的 pv-migrate v3.7.0 原始归档后：
make bundle VERSION=0.1.0 PV_MIGRATE_DIR=/opt/offline-tools
```

源码入口为根目录 `main.go`，`cmd/` 定义各子命令。源码运行先执行 `make build`，再使用 `./bin/kube-node-migrate plan ...`；助手会从同一 `bin/` 目录自动发现。`go run .` 的临时可执行文件旁没有助手或复制工具，处理 PVC 时必须显式指定 `--filesystem-helper` 和 `--pv-migrate`／`--pv-migrate-archive`。独立文件系统助手位于 `tools/kube-node-fs/`。

- [11 个操作案例](#操作案例)：多副本、多 PVC、无状态、多个命名空间、限速、阈值、离线、非 root SSH 等。
- [业务迁移与恢复边界](docs/workloads.zh-CN.md)。
- [预同步成功与最终切换条件](docs/precopy.zh-CN.md)。

完整的准备步骤、参数说明、操作案例和故障处理均在下文。

## 支持范围

| 对象或条件 | 说明 |
| --- | --- |
| Deployment | 支持迁移源节点上的无 PVC 实例，替换 Pod 仍归属原 ReplicaSet |
| StatefulSet | 支持多副本、单实例一个或多个 PVC，保留原 Pod 序号 |
| 数据卷 | `rancher.io/local-path`、WaitForFirstConsumer、已绑定的 RWO Filesystem PVC |
| 节点 | 源、目标已加入同一集群，均 Ready、架构一致，目标可调度并满足原有调度约束 |
| 命名空间 | 白名单优先；未指定白名单时使用黑名单，默认排除 `kube-system`；始终跳过 DaemonSet 和静态 Pod |
| 执行环境 | Linux、kubectl、OpenSSH，当前主机镜像缓存及 CRI 检查使用 K3s/containerd 工具链 |
| API 和权限 | v1 ValidatingAdmissionPolicy API、业务资源管理、Lease、准入保护、StatefulSet metadata patch、读取 `default/kubernetes` Endpoints 和 impersonation 权限 |

共享 PVC、挂载 PVC 的 Deployment、直接 hostPath、活动 Job、HPA、正在滚动更新的控制器等情况会阻止计划。源、目标可以带控制面角色标签，实际污点容忍、亲和性及容量检查仍必须通过。

目标 Node 的 nodeSelector、污点容忍及必需 NodeAffinity 会在计划、执行和驱逐前重新检查；显式 hostPort 会检查目标已有 Pod 和待迁入实例间的冲突。必需 Pod 亲和／反亲和以及硬 topology spread 约束会直接拒绝。准入保护会在当前连接入口及 `default/kubernetes` 中每个 Ready 的 HTTPS API Server 上分别验证；只有各入口都实际拒绝控制器身份的探测，才允许驱逐源实例。端点无法读取、直连失败或任一入口尚未生效时停止，不会退回只验证负载均衡入口。该检查需要执行机能直连这些 API Server，并保留 kubeconfig 的 TLS 校验。

预检不模拟完整 scheduler 插件；ResourceQuota 的 scope、webhook 等最终准入仍由 Kubernetes API 决定，准入保护的独立身份 dry-run 也不代表业务重建一定通过全部准入。

卷内支持普通文件、目录和符号链接；存在扩展属性（xattr，包括依赖扩展属性保存的 ACL）、普通文件硬链接、FIFO、socket 或设备文件时会拒绝迁移，需要另行制定复制策略。计划中的文件系统扫描会提前检查这些限制，最终摘要再次检查；不要通过删除属性或特殊文件来绕过数据要求。

迁移只处理选定范围内的业务实例。工具会临时 cordon 源节点，完成后恢复原调度状态；不会停止节点服务、删除 Node 或调整 master／etcd 成员。

## 1. 准备工具和依赖

### 使用完整离线包

发布包按执行机架构选择，例如 `kube-node-migrate-0.1.0-linux-amd64.tar.gz`。包内包含主程序、两种架构的文件系统助手和执行端所需的 pv-migrate 原始归档。

```bash
mkdir -p /opt/kube-node-migrate
tar -xzf kube-node-migrate-0.1.0-linux-amd64.tar.gz -C /opt/kube-node-migrate
cd /opt/kube-node-migrate
./kube-node-migrate-linux-amd64 --version
```

版本输出以 `kube-node-migrate version 0.1.0` 开头，同时显示 `commit`、UTC 构建时间 `built` 和检查点格式 `state`。当前格式为 8；直接 `go run` 时 commit／构建时间为 `unknown`。排查现场问题时请保存完整版本输出。arm64 执行机使用对应发布包和 `kube-node-migrate-linux-arm64`，下文以 amd64 为例。

### 从源码构建

需要 Go 1.26.2 或兼容版本及 make。先获取源码，再在仓库根目录执行：

```bash
git clone https://github.com/Kalen89/kube-node-migrate.git
cd kube-node-migrate
```

```bash
make test-integration check
make release VERSION=0.1.0
```

`make release` 在 `bin/` 下生成两种架构的主程序及文件系统助手；运行真实 rsync 集成用例还需要本机安装 rsync。

GitHub Actions 在 push、Pull Request 和手动触发时执行格式与静态检查、真实 rsync 集成测试、依赖校验及 Linux amd64／arm64 构建。CI 使用公网 Go 依赖，不需要公司内网凭据。

默认辅助镜像来自 Docker Hub；访问受限或离线环境可以通过 `helper.image`、`sshd.image`、`rsync.image` 指定镜像仓库，详见案例八。修改默认值不会改变已有检查点中保存的镜像地址，续跑仍使用原任务配置。

只在当前执行机运行时，可用以下方式构建并调用主程序；Linux 现场迁移仍需准备下述 pv-migrate 依赖：

```bash
make build VERSION=0.1.0
./bin/kube-node-migrate --version
./bin/kube-node-migrate plan --source worker-old --target worker-new
```

生成完整离线包时，先在 `/opt/offline-tools` 准备以下两个 **pv-migrate v3.7.0 原始发布归档**：

```text
pv-migrate_v3.7.0_linux_x86_64.tar.gz
pv-migrate_v3.7.0_linux_arm64.tar.gz
```

```bash
make bundle VERSION=0.1.0 PV_MIGRATE_DIR=/opt/offline-tools
```

`make bundle` 在生成任一发布包前，使用运行时相同的固定 SHA-256 校验两个原始归档及其中的 `pv-migrate` 普通文件条目，并检查 Linux 主程序与助手的架构、主程序版本和构建信息。打包后逐项复核文件清单、权限、大小和摘要，输出两个 `.tar.gz` 及各自的 `.tar.gz.sha256`。归档和校验使用 Go 标准库，不依赖平台的 tar 或摘要命令，可在 macOS 或 Linux 构建机运行。需要固定构建信息时可传入 `COMMIT` 和 `BUILD_TIME`，例如 `make bundle VERSION=0.1.0 COMMIT=unknown BUILD_TIME=2026-10-30T00:00:00Z`。

将归档和对应校验文件一起带入现场，解压前在所在目录校验：

```bash
sha256sum -c kube-node-migrate-0.1.0-linux-amd64.tar.gz.sha256
# macOS 准备机使用：
shasum -a 256 -c kube-node-migrate-0.1.0-linux-amd64.tar.gz.sha256
```

迁移时不联网下载这些工具。也可在 `plan` 时用 `--pv-migrate /opt/tools/pv-migrate` 指定已有的 v3.7.0 可执行文件；归档方式会校验内置 SHA-256，请保留原始归档。

## 2. 检查执行环境

在有集群管理凭据的执行机上操作。执行机可以是源、目标或第三台管理机，需要能 SSH 访问两节点的 InternalIP。已准备好凭据、镜像及依赖时，可直接使用本文开头的两条常规命令。

```bash
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml get nodes -o wide
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n prod get pods -o wide
```

`--source`、`--target` 使用第一列的 **Node 名称**，不是 IP。下文的 `worker-old`、`worker-new` 和 `192.0.2.11/12` 都是示例，需替换为实际值。

```bash
ssh root@192.0.2.11 true
ssh root@192.0.2.12 true
```

首次连接时通过可信渠道核对 SSH 指纹。默认复用执行用户的标准 known_hosts（通常为 `~/.ssh/known_hosts`），不需要创建专用文件，也不需要填写 `sshKnownHosts`。

SSH 使用非交互模式，应提前配置可用的密钥或 agent。非 root SSH 用户需能够使用 `sudo -n`。执行用户还需能读取 kubeconfig、SSH 私钥和工具文件。无需为迁移停止 K3s 或 kubelet。

## 3. 生成计划

每次迁移使用独立任务目录。不要复用另一轮迁移的状态文件。

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

`plan` 保存计划，并检查控制器、Pod/PVC 身份、权限、SSH、文件系统等条件；可能缓存文件系统助手，但不会驱逐业务 Pod 或切换 PVC。

检查计划中的源、目标、命名空间、实例和卷是否符合预期：

```bash
./kube-node-migrate-linux-amd64 status --state /opt/node-migration/task-001/state.json
./kube-node-migrate-linux-amd64 status --state /opt/node-migration/task-001/state.json --json
```

同一源节点上处于所选命名空间的实例都会进入盘点，当前没有按 Pod 名或 label 过滤的参数。请在执行前检查完整清单。

## 4. 执行迁移并验证

确认可以接受逐实例切换时的短暂停机后执行：

```bash
./kube-node-migrate-linux-amd64 apply \
  --state /opt/node-migration/task-001/state.json \
  --allow-downtime

./kube-node-migrate-linux-amd64 verify \
  --state /opt/node-migration/task-001/state.json
```

程序先预热镜像、准备目标卷并检查容量，再逐实例处理：在线预同步 → 保护当前实例 → 正常 Eviction 并确认旧容器退出 → 最终同步与完整摘要校验 → 切换 PVC → 创建目标实例并等待 Ready。

同一实例的全部 PVC 校验通过后才开始任何一块卷的改绑，全部改绑完成后才启动实例。当前实例及同组副本恢复后，才处理下一实例。

执行与续跑的磁盘预检读取源、目标文件清单和目标实际分配块，按剩余新增空间／inode 加上串行 rsync 重写文件所需的临时峰值计算，不要求目标在已有副本之外再空出一整份全部数据。多个卷共用目标文件系统时累加增长量，并保留最大的单次临时副本需求；不会预先抵扣未来删除的目标多余文件，也不会将稀疏文件的逻辑大小当作可复用的已占空间。日志会分别列出增长量和临时空间，仍需确认底层磁盘有相应余量。长时间预同步结束后，驱逐仍在线的源实例前，会重新检查当前容量、配额和剩余复制空间。

再从 Kubernetes 核对业务实例和 PVC：

```bash
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n prod get pods -o wide
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n prod get pvc
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml get nodes
```

`verify` 成功表示工具的实例、控制器、目标绑定及校验记录检查通过。数据库复制、仲裁和实际业务请求仍应按对应中间件和应用的检查方式确认。

## 5. 查看进度与失败续跑

```bash
tail -f /opt/node-migration/task-001/state.json.log

./kube-node-migrate-linux-amd64 status \
  --state /opt/node-migration/task-001/state.json --json
```

任务目录中包含：

| 文件 | 用途 |
| --- | --- |
| `state.json` | 私有事务检查点，包含原应用配置，用于续跑，不应公开 |
| `state.json.log` | 追加写入的运行日志 |
| `state.json.report.json` | 原子刷新的脱敏实时进度报告，可从私有检查点重建 |
| `state.json.lock` | 防止多个进程同时执行同一任务 |

`ActiveStage` 表示上次保存时正在执行的阶段，`Events` 记录已结束阶段及耗时。源摘要、最终同步及助手释放、目标校验分别计时；Pod 创建、调度、容器运行、Ready 分开显示，共用一个启动等待总期限。进程退出后报告可能保留上次阶段，请结合 `UpdatedAt` 和日志判断。

事务字段改变时，程序仍同步写入私有检查点，并完成文件和目录的 fsync 后才刷新报告、进入下一步；只改变 `ActiveStage`、`Events` 或 `UpdatedAt` 的纯观测更新，仅原子替换脱敏报告，合并重复的全量私有写盘。报告可重建，不单独 fsync，也不作为续跑依据。`status`／`status --json` 基于最近的私有事务检查点，实时阶段和耗时请查看 `.report.json`；异常退出可能丢失最近的纯观测记录，切换事务的持久化要求不变。

失败后先定位日志中的原因，处理后使用 **原状态文件**：

```bash
./kube-node-migrate-linux-amd64 resume \
  --state /opt/node-migration/task-001/state.json \
  --allow-downtime

./kube-node-migrate-linux-amd64 verify \
  --state /opt/node-migration/task-001/state.json
```

`apply` 和 `resume` 使用检查点保存的参数，修改原 YAML 不会改变在途任务。不要删除状态文件、手工改写绑定或另起一轮任务处理已经切换一半的实例。源停写后失败时会保留当前保护；目标产生新写入后，不会自动回切旧数据。

启用在线预同步时，若上次停在准备或停写阶段，但原 Pod 仍保持相同 UID、Ready 且未开始终止，`resume` 会重新完成在线预同步策略，再重新检查目标节点约束和 hostPort 冲突后驱逐。已开始终止的实例继续停写后的流程，不重新要求源在线。

最终摘要不一致时，修复原因后仍使用原检查点 `resume`。程序重新确认源已停写、全部卷仍保持原绑定且没有额外消费者，且尚未开始切换或启动后，才重新扫描源、执行最终同步和完整校验。复制助手控制器被正常更新而产生版本冲突时，先重新核验原 UID 和 Helm 归属，再保存最新版本并有限重试；持续冲突仍保留现场。复制助手清理中断时，按检查点记录的 UID 和归属继续清理已确认的本次资源；同名对象被替换或出现未知资源时停止，不扩大清理范围。具体条件见 [业务迁移与恢复边界](docs/workloads.zh-CN.md#失败续跑)。

## 迁移范围与默认值

- 设置 `--namespace` 或 YAML `namespaces` 时，只使用白名单，黑名单不再参与筛选。
- 未设置白名单时，迁移黑名单以外的命名空间；`kube-system` 默认在黑名单中，`--exclude-namespace` 或 `excludeNamespaces` 可以追加排除项。
- 计划保存实际命名空间和实例清单，续跑不重新发现或扩大范围。
- 未指定 `--state` 时使用当前目录的 `.kube-node-migrate/state.json`；同一目录同一时间处理一个任务，多个任务使用各自工作目录或显式状态路径。
- 未指定 `--kubeconfig` 时依次识别单文件 `KUBECONFIG`、用户默认 kubeconfig、K3s 默认配置。环境变量为多文件列表时，显式指定一个完整配置文件。

## 常用参数和配置

命令行显式参数优先于 YAML，YAML 优先于默认值。YAML 中的相对文件路径按配置文件所在目录解析。命名空间、依赖、SSH、带宽和预同步等参数在 `plan` 时确定；后续命令主要使用 `--state`。

| `plan` 参数 | 用途及默认值 |
| --- | --- |
| `--namespace`／`-n` | 可选白名单，可重复或逗号分隔；优先于黑名单 |
| `--exclude-namespace` | 无白名单时追加黑名单，默认包含 `kube-system` |
| `--state` | 默认 `.kube-node-migrate/state.json`，自动创建私有目录 |
| `--kubeconfig`／`--context` | 指定集群凭据和 context；建议显式传 kubeconfig |
| `--ssh-user`／`--ssh-port`／`--ssh-key` | 默认 root、22、使用现有 SSH identity／agent |
| `--ssh-known-hosts` | 可选，仅用于单独管理可信主机记录；默认省略，复用 SSH 标准 known_hosts |
| `--pv-migrate`／`--pv-migrate-archive` | 已有工具或原始离线归档，不指定时从主程序同目录发现 |
| `--filesystem-helper` | 默认按节点架构选择主程序同目录的文件系统助手 |
| `--precopy` | 默认 true；false 时直接进入停写后的复制 |
| `--precopy-rounds` | 默认 3，范围 1～20 |
| `--precopy-threshold` | 默认 0，按成功轮数完成；显式设置时按 rsync 容器耗时判断 |
| `--parallel-digest` | 默认 false；显式开启时，在最终 checksum 同步后并发读取源、目标完整 SHA-256 摘要 |
| `--rsync-bandwidth-limit` | KiB/s，默认 0，不限速 |
| `--pv-concurrency` | 当前流程固定为 1，逐实例串行 |
| `--reserve-cpu`／`--reserve-memory`／`--reserve-pods` | 迁移容量预留；基础默认值为 `200m`、`256Mi`、`2`，按实际需要自动提高 |
| `--wait-timeout` | 默认 10m，每个 Kubernetes 转换的等待上限；四段启动等待共享一份期限 |
| `--copy-timeout` | 默认 12h，单次复制或文件系统操作的执行期限 |

以下是一份可直接修改的配置，各字段都被当前 CLI 支持：

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

辅助服务按 `helper`、`sshd`、`rsync` 分组，每组的 `image` 和 `resources` 一起配置。镜像优先级为：显式命令行参数 > 服务的非空 `image` > 旧 `images` 分组 > 原顶层镜像字段 > 默认值。原来的两种写法仍兼容。

`resources` 支持 CPU 和内存的 `requests`、`limits`，数量必须为正数，且 requests 不得超过 limits。允许只配置部分数量，未配置的项继承默认值。配置用于实际 Pod、源目标容量预检及各命名空间的配额检查；`reserve.cpu`、`reserve.memory` 是迁移容量预留总量，低于辅助 Pod 所需容量时会自动提高，不会改写 Pod 的 requests 或 limits。修改在途任务的 YAML 不会改变已保存的计划。

`reserve` 统一配置迁移容量预留：`cpu` 为 CPU 调度容量，`memory` 为内存调度容量，`pods` 为额外 Pod 名额。预检计算“目标已有业务 + 待迁入业务 + 预留”是否超过节点可分配容量；这些值不会锁定节点资源或创建占位 Pod。常规使用可省略 `reserve`，程序使用基础默认值并根据辅助 Pod 和切换需求自动提高预留量。默认配置生成命令会输出该分组。

目标容量按当前检查点计算尚未在目标出现的确认实例，已创建的本次替换 Pod 按身份去重，仍在运行的本次辅助 Pod 计入总预留，避免重复计算。源节点按串行在线复制、停写后复制等阶段检查实际峰值；关闭预同步可利用当前实例退出后释放的容量，但源端镜像缺失时仍需先容纳在线预热 helper。业务资源统计包含顺序 init 容器峰值、restartable init sidecar 和 Pod overhead。

命名空间 ResourceQuota 与节点 `reserve` 分开计算：按预热、绑定、在线复制、停写后复制和业务替换各阶段的实际新增 requests、limits、Pod 数取峰值，并检查待创建的全部临时 PVC 及存储配额。不会为每个 namespace 重复预留整个节点的 reserve。辅助 Pod 纳入当前 LimitRange 默认值，检查 Container／Pod 资源范围及 PVC storage 范围；带 scope 的配额、webhook 和其他最终准入仍以 API 为准。

预留配置优先级为：显式 `--reserve-*` 参数 > `reserve` 分组中的非空字段 > 原顶层字段 > 默认值。原 `reserveCPU`、`reserveMemory`、`reservePods` 仍兼容；可以只覆盖分组内的一项。检查点和容量预检行为保持不变。

`parallelDigest`／`--parallel-digest` 仅改变最终完整摘要的读取顺序，默认 false 保留串行行为。源、目标位于独立存储且希望重叠读取时可显式设为 true；源停止写入并完成最终 rsync `--checksum` 后，并发扫描两侧完整内容再比较。共享机械盘或同一宿主机磁盘时建议保留默认值，避免争抢 I/O。它不跳过 checksum、任何摘要、停写或绑定检查，也不承诺实测加速。

新的服务分组写法需要使用包含此功能的程序；旧程序应使用其支持的配置格式。新检查点使用格式 8，防止旧程序忽略更新后的恢复保护后继续执行；原格式 6、7 的业务迁移检查点可由新程序读取，格式 6 按原资源默认值续跑。旧版整机迁移检查点仍只允许查看。

依赖也可用 YAML 的 `pvMigrate`、`pvMigrateArchive`、`filesystemHelper` 指定。SSH 用户、端口、私钥、kubeconfig、源目标和超时使用命令行参数。

## 操作案例

以下命令均在 `/opt/kube-node-migrate` 下执行。各案例使用不同状态目录，请先按前文核对节点、凭据及依赖。仅列出 `plan` 的案例，检查计划后按第 4 节对相同状态文件执行 `apply`、`verify`。

### 案例一：先用测试 StatefulSet 验证数据

`examples/demo.yaml` 创建独立的 `migration-demo` 命名空间、一个 Pod 和一块 local-path PVC。容器入口不生成验证数据，避免空卷重启后重建相同内容造成误判。迁移前仅写入一次本次唯一 token，并保存 SHA-256：

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

将实际所在节点作为源，选择另一台 Ready、可调度节点作为目标：

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

预期 Pod 名仍为 `app-0`，位于所选目标节点，最后的 `test` 返回 0；文件缺失或摘要不同均为验收失败。以上步骤在同一终端完成，并保存迁移前输出的摘要；迁移后不要重新生成 token 或覆盖期望摘要。确认源、目标不同后再执行；这个例子会创建测试资源，不会自动清理测试数据。

### 案例二：多副本 StatefulSet，单实例两块 PVC

假设 `middleware` 中 `db-0` 位于 `worker-old`，挂载 `data-db-0` 和 `logs-db-0`，其余副本分布在其他节点：

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

两块卷自动从实例挂载关系发现，全部校验通过后才开始改绑。其他节点的副本保持运行；所选命名空间中源节点上的其他业务实例也会被盘点，应先核对计划清单。

### 案例三：只迁移无状态 Deployment 实例

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace frontend \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/migration.yaml \
  --state /opt/node-migration/frontend/state.json
./kube-node-migrate-linux-amd64 apply --state /opt/node-migration/frontend/state.json --allow-downtime
./kube-node-migrate-linux-amd64 verify --state /opt/node-migration/frontend/state.json
```

替换 Pod 名可能变化，原 ReplicaSet 和 Deployment 模板保持不变。当前不支持挂载 PVC 的 Deployment。后续控制器再次重建无卷实例时，仍按原模板调度；旧节点是否继续 cordon，由操作者决定。

### 案例四：一次迁移多个业务命名空间

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new \
  --namespace middleware --namespace platform \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/migration.yaml \
  --state /opt/node-migration/multi-ns/state.json
```

也可写为 `--namespace middleware,platform`。命令行命名空间会覆盖 YAML 中的整个 `namespaces` 列表。未提供白名单时可以用 `--exclude-namespace monitoring,logging` 追加排除项。检查计划后，对此状态文件执行同样的 `apply` 和 `verify`。

### 案例五：机械盘或多台虚拟机共用宿主机磁盘时限速

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/low-impact.yaml \
  --state /opt/node-migration/low-impact/state.json
```

该配置为每路 rsync 限制 20 MiB/s，保留三轮在线预同步。带宽限制控制传输速度，不限制扫描、checksum 和完整摘要所需的磁盘读取；总耗时可能增加。

该场景保留 `parallelDigest: false` 的默认串行摘要。只有源、目标采用独立存储且已评估磁盘压力时，才考虑在 `plan` 时显式增加 `--parallel-digest`，并以现场测量判断是否受益。

### 案例六：要求预同步耗时达到显式条件

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/threshold.yaml \
  --state /opt/node-migration/threshold/state.json
```

配置为最多尝试 5 轮，一轮可信的 rsync 容器运行时间不超过 45 秒即可通过预同步策略；未达标时保留源服务运行并停止本次推进。这个时间包含扫描和连接，也不代表最终停机上限。条件改善后使用原检查点 `resume`；在途任务不会自动读取修改后的 YAML。

### 案例七：小数据卷跳过在线预同步

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/migration.yaml \
  --precopy=false --state /opt/node-migration/no-precopy/state.json
```

此时数据复制全部计入停写窗口，最终同步和摘要校验仍然执行。适用于数据较少且可以接受相应停机时间的任务。

### 案例八：使用内网镜像仓库和离线依赖

先将 `examples/offline.yaml` 的三个镜像地址改为实际内网仓库，准备固定版本原始归档和文件系统助手：

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/offline.yaml \
  --state /opt/node-migration/offline/state.json
```

完整发布包当前包含工具及依赖归档，不包含容器镜像。源、目标需能拉取这些镜像，或已有可用缓存，并已配置相应认证。示例归档名按 amd64 执行机填写，arm64 执行机需改为对应归档。指定现有可执行文件时可改用 `--pv-migrate /opt/tools/pv-migrate`。

#### 没有内网仓库时准备镜像归档

在能够访问镜像源的准备机上，以 amd64 节点为例：

```bash
docker pull --platform linux/amd64 docker.io/rancher/mirrored-library-busybox:1.36.1
docker pull --platform linux/amd64 docker.io/utkuozdemir/pv-migrate-sshd:v3.7.0
docker pull --platform linux/amd64 docker.io/utkuozdemir/pv-migrate-rsync:v3.7.0
docker save -o migration-images-amd64.tar \
  docker.io/rancher/mirrored-library-busybox:1.36.1 \
  docker.io/utkuozdemir/pv-migrate-sshd:v3.7.0 \
  docker.io/utkuozdemir/pv-migrate-rsync:v3.7.0
```

将归档带入隔离环境，复制到源、目标节点后，在两节点分别执行：

```bash
sudo k3s ctr -n k8s.io images import /opt/migration-images-amd64.tar
sudo k3s ctr -n k8s.io images ls
```

以上对应工具的默认镜像地址。镜像名称、标签和节点架构应一致，arm64 节点准备对应架构的归档。使用这些默认地址的本地缓存时，不要再套用包含其他仓库地址的 offline.yaml；若修改了仓库地址，应保存、导入与配置相同的引用。镜像管理方式另见 [K3s 镜像导入文档](https://docs.k3s.io/import-images/)。

### 案例九：非 root SSH、专用私钥及自定义端口

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/migration.yaml \
  --ssh-user ops --ssh-port 2222 --ssh-key /home/ops/.ssh/id_ed25519 \
  --state /opt/node-migration/ssh-custom/state.json
```

两个节点使用同一组 SSH 用户、端口和 identity 参数；`ops` 需要可用的 `sudo -n`。主机记录应匹配实际 InternalIP 及端口。

上述命令复用执行用户的标准 known_hosts。只有需要单独管理可信记录时，才使用可选的 `--ssh-known-hosts /path/to/known_hosts`，或在 YAML 中填写 `sshKnownHosts`。省略这些选项仍会校验主机指纹。

### 案例十：增加平台 HTTP 健康检查

```bash
./kube-node-migrate-linux-amd64 plan \
  --source worker-old --target worker-new --namespace prod \
  --kubeconfig /etc/rancher/k3s/k3s.yaml --config examples/health-check.yaml \
  --state /opt/node-migration/with-health/state.json
```

将示例 URL 改为稳定的 Service、VIP 或平台健康地址。检查在迁移前和全部迁移后执行，实例仍逐个按 Pod Ready 验证。检查为 HTTP GET，不自动跟随重定向，URL 不支持凭据、query 或 fragment；自签证书的跳过校验需按实际可信环境显式配置。

### 案例十一：源节点原本已经 cordon

已经 cordon 的源节点仍可生成计划并迁移其现有业务实例，目标必须可调度。工具记录源节点原状态，完成后保持其原先的 cordon 状态。

如果迁移前源节点可调度，完成后会恢复可调度。需要继续阻止新 Pod 回到旧节点时，由操作者另行执行 `kubectl cordon worker-old`；节点下线、移除或控制面替换属于后续独立操作。

## 常见问题

| 提示或现象 | 处理方式 |
| --- | --- |
| 状态文件已存在 | 用 `status` 查看，原任务用 `resume`；新任务使用新的目录 |
| 源或目标不是 Node 名 | 用 `kubectl get nodes -o wide` 核对名称及 InternalIP |
| 目标容量或调度检查失败 | 检查 requests、Pod 数上限、污点、亲和性、nodeSelector 及目标可用空间 |
| 权限预检失败 | 补齐 API、准入管理、metadata patch 或 impersonation 权限后再执行 |
| SSH 主机密钥校验失败 | 通过可信渠道核对实际节点与指纹，更新正确记录后再继续 |
| Eviction 被 PDB 阻止 | 检查其他副本健康及业务允许的中断策略，处理后续跑 |
| 显式预同步阈值未满足 | 当前源实例保持运行，检查磁盘及持续写入情况，条件改善后续跑 |
| 复制或摘要校验失败 | 保留检查点和当前保护，修复原因后 `resume`，不提前切换或启动 |
| Pod 未创建、Pending 或启动失败 | 按分段日志查看创建、调度或容器原因；检查控制器、资源、镜像和事件 |
| 调整 YAML 后续跑没有变化 | 在途任务使用状态中保存的原计划，YAML 只在 `plan` 读取 |

更详细的切换与恢复条件见 [业务迁移流程](docs/workloads.zh-CN.md) 和 [预同步完成条件](docs/precopy.zh-CN.md)。

## 许可证

Copyright 2026 kube-node-migrate contributors.

本项目采用 [Apache License 2.0](LICENSE)。第三方依赖及随包分发的上游工具继续遵循各自的许可证。

## 致谢

本项目使用 [pv-migrate](https://github.com/utkuozdemir/pv-migrate) 作为卷复制工具。感谢原作者 [Utku Özdemir](https://github.com/utkuozdemir)及所有贡献者，为 Kubernetes PVC 迁移提供基础能力。

同时感谢 [rsync](https://rsync.samba.org/) 贡献者提供数据传输基础，以及 [Kubernetes](https://kubernetes.io/) 和 [Rancher local-path-provisioner](https://github.com/rancher/local-path-provisioner) 社区提供编排和本地卷基础设施。
