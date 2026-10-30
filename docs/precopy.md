# Pre-copy completion and cutover conditions

English | [简体中文](precopy.zh-CN.md)

This document defines successful pre-copy rounds, policy completion, and the conditions for cutover. It applies to the local-path volumes supported by this tool. All PVCs belonging to an instance must pass verification before cutover. Online copies reduce the data left to transfer during downtime; they are not database replicas that can be started directly.

## Three different completion conditions

| State | Required conditions | Allowed next action |
| --- | --- | --- |
| Successful round | The copy Job succeeds and rsync exits normally; helpers exit and release the volumes; relevant checks and checkpoint persistence succeed | Run another round or evaluate pre-copy policy completion |
| Pre-copy policy complete (`ready=true`) | The configured successful-round count is reached; with an explicit threshold, at least one round has a trusted actual sync duration within the threshold | Recheck the source and volumes, then proceed to stop source writes |
| Final copy verified | Source writers have exited; final sync after stopping writes succeeds, including content checks and deletion of extra target files; complete source/target digests match | Rebind PVCs/PVs and start the service on the target |

`ready=true` does not mean the source and target are identical and does not authorize immediate volume cutover. Migration execution requires `--allow-downtime`. Disabling pre-copy does not skip the final stopped-source sync and verification.

## When the source PV is continuously written

A file may change again after it has been scanned but before a round finishes. A successful rsync round means the round completed normally. It does not require online dirty data to reach zero and does not establish database consistency.

After several pre-copy rounds, stop the source service and use the final sync to capture remaining additions, modifications, deletions, renames, and truncations. With source writes stopped, compare file content to repair files left with the same size and modification time but different content. Then compare complete digests. Final sync or verification failure prevents cutover.

If continuous writes keep the remaining changes large, the successful-round policy can still proceed once downtime is allowed, but downtime may be longer. An explicit duration threshold provides a stricter pre-copy condition; no threshold is a bound on downtime. Application exit, final scanning, sync, full digest verification, mounting, startup, and recovery all contribute to downtime.

## Default policy: successful rounds

```yaml
precopy: true
precopyRounds: 3
precopyThreshold: 0s
```

Each `apply` or `resume` invocation completes the configured number of successful rounds for volumes whose source has not stopped, then proceeds to the checks before stopping writes. The default three rounds are a configurable warm-up count, not a consistency standard. Failed rounds are not counted, and enough historical rounds do not cause a current failure to be ignored.

## Optional policy: an explicit duration threshold

```yaml
precopy: true
precopyRounds: 5
precopyThreshold: 45s
```

This example attempts up to five rounds per invocation. One successful round with an actual sync duration of at most 45 seconds completes the policy. The 45-second threshold is explicitly configured, not a universal requirement.

Logs and reports distinguish:

- `elapsed` / `elapsedMillis`: the complete round, including creation, scheduling, sync, cleanup, and waiting for volume release.
- `sync` / `syncMillis`: the successful rsync container's runtime for the current copy Job, including scanning, connection setup, and transfer; not just network transfer time.
- `setupCleanup`: complete round time minus actual sync time, including scheduling, cleanup, and orchestration waits.
- `ready`: whether the current pre-copy policy has completed.
- `thresholdMet` (legacy report field `converged`): only whether the explicit duration threshold was met, not whether data has converged or the copies are identical.

Without trusted container timing, for example after Pod cleanup, multiple attempts, or missing status, duration is unknown. It cannot be treated as zero or as satisfying the threshold. If no round meets the explicit condition, retain the running source and checkpoint, stop further batches, and wait for `resume`.

## Rechecks before stopping writes and before cutover

After the pre-copy policy completes, revalidate the original source Pod's UID, Ready state, node, peer replica health, and unchanged controller. Also confirm temporary target volume ownership, helper release, and the absence of workload consumers on target volumes. A failed check prevents stopping the source.

After stopping writes, prevent the controller from recreating the source instance and confirm that the actual writers have exited. Deleting a Kubernetes Pod object alone does not confirm process exit. If the source is unreachable or exit cannot be established, do not bypass the checks and switch volumes. Retain the checkpoint and investigate final sync or digest failures before starting the target service.

A final digest mismatch is recorded as a recoverable failure. Once the cause is fixed, `resume` using the original checkpoint rescans source digests and repeats final sync only if the source remains stopped, every volume retains its original binding with no extra consumers, and rebinding/startup has not begun. After cutover or possible target writes, the tool does not recopy over the target or automatically revert to old data.

### Optional parallel full digests

The default `parallelDigest: false` keeps the serial order: complete source digest → final checksum sync → complete target digest. Setting `parallelDigest: true` or `plan --parallel-digest` changes the order to: final checksum sync → concurrent complete source/target digests → comparison. Both modes require confirmed source write shutdown. Parallel mode does not reduce verification scope or skip consistency checks.

Independent storage may benefit from overlapping reads. Shared HDDs or disks on the same physical host can become slower due to competing I/O, so serial mode remains the default. No speedup ratio is guaranteed; choose based on measurements in the actual environment.

## Interruption, recovery, and concurrency

- A failed or canceled round, or a change in source identity, is not recorded as successful. The volume remains in pre-copy.
- If execution stops after pre-copy but before stopping writes, `resume` repeats pre-copy whenever the original Pod has the same UID, remains Ready, and has no deletion timestamp, even if the instance checkpoint has entered preparation/stopping. Potentially stale duration results are not reused. This also applies when resuming after PDB-denied Eviction.
- Volumes whose source is already stopping continue final sync through the original checkpoint without requiring the stopped Pod to become online again.
- Instances are processed serially. A volume failure preserves the current instance's state and protection and prevents the next instance from starting. Every volume in an instance must pass verification before any rebinding.
- New plans default to a zero threshold. Legacy node-replacement checkpoints are view-only and must be handled by their original version; they are not silently converted to the new workflow.
- Once the target accepts new writes, old source data is stale and automatic direct rollback to that copy is prohibited.
