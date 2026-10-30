# 业务 Pod 和 PVC 迁移

[English](workloads.md) | 简体中文

本工具的默认、唯一执行流程是迁移选定业务实例及其数据。源机器何时下线、如何替换节点、如何调整控制面或成员关系，由用户另行决定。

## 使用

不再需要 `--mode workloads` 和 `--source-policy keep-running`，也不提供 `agent` 或 `isolate` 入口。

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

命名空间支持白名单和黑名单。指定 `--namespace` 时只按白名单选择；未指定时按黑名单过滤，默认排除 `kube-system`，可通过 `--exclude-namespace` 追加。DaemonSet 和静态 Pod 不进入业务实例清单。源、目标均可带控制面角色标签，目标仍需可调度并满足业务的 nodeSelector、亲和性和污点容忍。

## 执行顺序

1. 盘点源实例、控制器和 PVC 身份，检查资源、准入权限、文件系统与目标实际空间。停写前在源节点预热 sshd，在目标预热业务、helper 和 rsync 镜像，兼容源节点原本已 cordon 的情况；关闭在线预同步也保留这些检查。
2. 对源节点临时 cordon，避免其他新 Pod 调度到源节点；以 Lease 和节点占用标记阻止冲突任务。
3. 为当前实例全部卷在线预复制，保持源实例运行，重新检查同组其他副本健康。
4. 添加仅针对当前实例的临时 CREATE 保护，在当前连接入口及 `default/kubernetes` 发布的每个 Ready HTTPS API Server 上，分别验证 Pod 和每块 PVC 的服务端 dry-run 被本次规则拒绝。RBAC 拒绝不能冒充保护生效。
5. 驱逐前重新检查目标 Node 的 nodeSelector、污点、必需 NodeAffinity 和 hostPort 冲突，再正常驱逐当前实例，遵守 PDB，通过 CRI 确认原 Pod UID 的容器退出。只检查当前实例，不停止整个运行时或其他 Pod。
6. 最终 checksum 同步、清理目标多余文件并完整比较摘要。同一实例全部 PVC 通过后才允许任何一块改绑。默认源摘要、最终同步、目标摘要串行；显式开启 `parallelDigest` 时，先完成最终 checksum 同步，再并发读取两侧完整摘要，仍须一致才能切换。
7. 原名称 PVC 全部绑定到目标 PV 后启动目标实例。StatefulSet 保持原序号和控制器，Deployment 替换 Pod 归属原 ReplicaSet。StatefulSet 保护解除后，如果 Pod 尚未创建，以临时顶层注解更新触发原控制器协调；Pod 仍不存在时，至少间隔 15 秒补发一次通知，避免一直等待失败重试退避。
8. 当前实例及同组副本 Ready 后再处理下一个，最终释放本次临时保护和占用标记，恢复源节点原调度状态。

工具不会删除 Node、添加驱逐其他 Pod 的 NoExecute 污点、停止 K3s／Agent／kubelet 服务或调整角色成员。控制器模板也不增加永久目标固定策略；后续无卷实例重建仍遵循原配置，迁移后的本地 PV 绑定目标节点。

文件系统预检只读取路径及分配信息，按目标已有副本估算剩余增长和串行 rsync 临时文件所需空间／inode。同一目标文件系统累加各卷增长量并取最大的单次临时峰值，不预支未来删除空间；稀疏文件按实际已分配块计算可复用量。它用于检查当前可用空间，不代替停写后的完整摘要或保证后续业务写入空间。

节点容量按当前确认实例及串行阶段计算剩余峰值，包含 init 容器、restartable init sidecar 和 overhead；已在目标创建的本次替换 Pod 与辅助 Pod 按身份去重。namespace 配额使用实际辅助 Pod 与业务替换所需 requests／limits／Pod 数峰值及全部临时 PVC，不将节点 reserve 重复施加到每个 namespace。当前 LimitRange 的默认值和资源／存储范围也参与检查。

协调通知的注解键包含本次操作与实例标识，仅修改 StatefulSet 顶层 metadata，不修改 replicas 或 Pod 模板。Pod 创建后清理本次注解；中断续跑可恢复本次遗留注解，遇到其他客户端占用、UID 或 Spec 改变时停止。使用带 resourceVersion 的乐观锁，冲突后重新读取，保留其他注解。

创建、调度、容器运行和 Ready 各有独立日志及耗时，但共享一个 `waitTimeout` 总期限；源摘要、最终同步及助手释放、目标摘要校验分别计时。阶段开始时原子刷新脱敏 `ActiveStage`，结束后追加 `Events` 并恢复上层阶段；命令运行期间即可查看 `.report.json`，无需等待退出。事务字段变化仍先同步持久化私有检查点并 fsync 文件和目录，再刷新报告和推进；仅阶段、事件或更新时间改变时只替换可重建报告，不重复写入完整私有状态，也不后台异步保存。`status` 显示最近私有事务检查点，实时观测以报告为准。进程异常退出时，报告可能保留上次的运行中阶段或丢失最近纯观测记录，应结合时间戳及日志判断。

## 失败续跑

```sh
./kube-node-migrate-linux-amd64 status --state /opt/node-migration/business/state.json --json
./kube-node-migrate-linux-amd64 resume --state /opt/node-migration/business/state.json --allow-downtime
```

原实例仍 Ready、尚未终止时，失败会尝试解除已知的本次保护。开始终止或停写后失败，保留当前保护和检查点，不处理下一实例。已经改绑的卷不会被回退；全部卷完成前不会启动实例。目标已可能写入后，不重新离线复制或自动回切旧数据。

启用在线预同步且原 Pod 仍同 UID、Ready、没有终止时间戳时，准备／停写检查点的续跑会刷新全部卷的在线预同步策略，然后重新执行停写前检查，避免使用过期的阈值结果。原 Pod 已开始终止或已经退出时，不回到在线预同步。

最终同步完成但摘要不一致时，工具保留失败记录并停止。排除原因后，对原检查点执行 `resume`：只有源实例仍已停写、原 PVC 绑定身份及同实例全部卷状态未改变、没有额外消费者，且尚未开始改绑或启动时，才重新扫描源摘要、执行最终同步并再次校验。任何卷已经进入切换或目标可能写入后，禁止用重同步覆盖目标数据。

复制助手清理前会保存本次资源的 UID、名称、类型和控制器归属。若删除中断，`resume` 可继续清理已确认属于本次操作的剩余资源，正常 resourceVersion 更新会重新核验后重试；同名资源被替换、出现未知对象或归属改变时仍停止，不扩大清理范围。不要手工删除复制 Job、Helm Secret 或检查点来绕过身份校验。

旧机器下线是独立的用户操作，不是成功条件。迁移完成后的 `verify` 检查目标实例、控制器、目标 PVC/PV 和校验记录，不要求源机器仍在线，也不强制用户保留源节点对象或旧 PV。

状态格式 8 保存辅助服务资源及复制恢复记录；格式 6、7 的业务迁移检查点可读取，格式 6 按原资源默认值续跑，但仍须遵守 [README 的独立版本标记兼容边界](../README.zh-CN.md#版本兼容说明)。更早的节点替换检查点只读查看，不自动执行；CLI 和配置拒绝旧模式、节点处置策略及节点标签修改参数。

## 权限和验证边界

需要 v1 ValidatingAdmissionPolicy API、所选资源和临时保护的管理权限、StatefulSet metadata 的 patch 权限，以及独立身份 dry-run 所需 impersonation 权限。迁移前执行协调注解 patch 的服务端 dry-run，权限不满足时不会先停止源实例。CLI 探测使用控制器身份及 impersonated `system:masters` 组，仅做授权查询和 dry-run；不创建真实探测 Pod、不授予 RBAC 权限。

保护策略会在当前连接入口及 `default/kubernetes` 发布的每个 Ready HTTPS API Server 上分别验证。执行机需要读取这些 Endpoints，并在保留 kubeconfig TLS 校验的前提下直连各入口；端点发现、连接或保护验证失败时，不会驱逐源实例。

调度预检覆盖最新目标 Node 约束及显式 hostPort 冲突；必需 Pod 亲和／反亲和、硬 topology spread 会拒绝，并不模拟完整 scheduler。保护策略的独立身份 dry-run 用于确认保护规则生效，不保证未来业务 Pod 通过所有 webhook、带 scope 的配额或其他准入；最终结果仍由 Kubernetes API 和 scheduler 决定。

配置示例见 [workloads.yaml](../examples/workloads.yaml)。SSH 主机指纹需预先可信核验；工具不会自动信任未知主机。当前主机镜像缓存和 CRI 检查仍使用 K3s/containerd 工具链，属于只读检查，不包含节点生命周期管理。

Pod Ready 不能代替中间件复制、仲裁或业务请求可用性验证。可选 HTTP `healthChecks` 用于补充；源节点能下线与否，仍由用户按集群和现场环境评估。
