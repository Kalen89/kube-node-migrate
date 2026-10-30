# Changelog

English | [简体中文](CHANGELOG.zh-CN.md)

## 0.1.0

- Use generic `prod`, `pre`, and `test` namespace examples and neutral `km-` temporary-resource prefixes.

- Provide English and Chinese topic guides for usage, pre-copy, and workload migration, with language-matched cross-links and both versions included in offline bundles.

- License the project under Apache License 2.0 and include the license in offline bundles.

- Provide complete English and Chinese README and changelog documents, covering project motivation, Local PV trade-offs, development status, and upstream acknowledgments. Offline bundles include both languages.
- Use `kube-node-migrate` for the project and command, `github.com/Kalen89/kube-node-migrate` as the Go module, and `kube-node-migrate.io` for runtime markers.
- Use public Docker Hub references for default copy images and examples, with configurable internal registries. Existing tasks retain checkpointed image references.
- Add GitHub Actions checks using public dependencies, real rsync integration tests, and builds for both Linux architectures.
- Provide `plan`, `apply`, `resume`, `status`, and `verify`. The normal flow is `plan` followed by `apply`, which verifies automatically on completion.
- Provide `config print defaults` to print static YAML matching plan defaults for inspection or editing.
- Support namespace allowlists and exclusions: an explicit allowlist takes precedence; otherwise exclusions apply, with `kube-system` excluded by default.
- Detect cluster credentials and release-bundle dependencies, create a private task directory by default, and reuse standard SSH known_hosts.
- Use root `main.go` as the entry point, `cmd/` for subcommands, and `tools/` for the standalone filesystem helper.
- Support Deployments without PVCs and replicated StatefulSets with one or more PVCs per instance, retaining original controllers and peer replicas.
- Support online pre-copy, successful-round or explicit-duration completion conditions, and per-stream rsync bandwidth limits.
- Run final checksum synchronization and full digest verification after source writes stop. Rebind original-name PVCs only after all volumes pass, retaining old PVs and data.
- Use PDB-aware Eviction, CREATE admission protection, source-container exit confirmation, UID checks, and durable checkpoints for cutover and recovery.
- Requeue StatefulSet reconciliation after protection is removed, then clean up temporary annotations without changing the original Spec, Pod template, or replica count.
- Report progress and durations separately for copy verification, Pod creation, scheduling, container startup, and Ready, with redacted progress reports refreshed alongside checkpoints.
- Provide image prewarming, capacity and filesystem preflight checks, optional HTTP health checks, and offline release bundles.
- Group helper configuration under `helper`, `sshd`, and `rsync`, each with `image` and `resources`, retaining legacy image configuration support.
- Group migration headroom under `reserve.cpu`, `reserve.memory`, and `reserve.pods`, retaining legacy top-level fields.
- Apply custom CPU/memory requests and limits to actual helper Pods, capacity checks, and namespace quota checks; save them in checkpoints for resume.
- Restore the source's original scheduling state after migration. Node retirement and control-plane membership changes remain operator decisions.
- Include source commit, UTC build time, and checkpoint format in `--version`. New checkpoints use format 8, with format 6/7 workload-checkpoint reading subject to standalone marker compatibility.
- Verify offline dependencies against pinned runtime digests, check both architecture archives and executable entries, validate build metadata and bundle contents, and generate `.sha256` files.
- Document source execution through `make build` and the `bin/` executable; direct `go run` requires explicit helper and copy-tool paths.
- Verify examples with a unique token written once before migration and a saved SHA-256 digest. Container startup does not recreate test data; document xattr, hard-link, and special-file limitations.
- Resume final digest mismatches through the original checkpoint. Rescan and resync only when the source remains stopped, original bindings and no-consumer conditions hold, and cutover/startup has not begun. Never overwrite a target already switched or started.
- Persist copy-resource UIDs and ownership before cleanup. Resume cleanup only for confirmed task resources, reject replacements and unknown objects, and revalidate/checkpoint/retry boundedly after controller resource-version conflicts.
- Repeat pre-copy on preparation/stopping resume when the original instance is still online and pre-copy is enabled. Recheck target constraints and hostPort conflicts before eviction; allow normal source-volume consumers when pre-copy is disabled.
- Prewarm images on their actual execution nodes: sshd on the source; workload, helper, and rsync on the target. Prepare copy images even when the source is cordoned or pre-copy is disabled.
- Add `parallelDigest` / `--parallel-digest`, default false. When enabled, read full source and target digests concurrently after final checksum sync without reducing verification requirements.
- Coalesce full private-checkpoint writes for observation-only stage/event/timestamp changes by atomically refreshing a rebuildable report. Transaction changes still fsync the private checkpoint and directory before reporting success.
- On resume, calculate remaining target growth and serial rsync temporary peaks using existing allocation, rather than requiring a second full dataset's free space. Do not credit future deletions or sparse unallocated space.
- Account for the current migration stage and actual Pod identities in node capacity checks, including init containers, sidecars, and overhead. Check namespace quotas using actual helper peaks and temporary PVCs independently of node reserve, including LimitRange defaults and bounds.
- Refresh target constraints and explicit hostPort conflict checks during planning, execution, and before eviction. Reject required Pod affinity/anti-affinity and hard topology spread constraints; document final API admission and scheduler boundaries.
- Rediscover API Servers during preflight and before stopping writes, and verify admission protection at every entry point to prevent controller replacement races in HA clusters.
