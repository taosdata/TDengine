---
sidebar_label: Heartbeat and Leader Election
title: Heartbeat and Leader Election Analysis
toc_max_heading_level: 4
---

TDengine maintains liveness awareness among cluster members through heartbeats. When a heartbeat times out, a new leader is elected by the Raft protocol (multi-replica) or by the Arbitrator (dual-replica). When a cluster experiences frequent leader switches, fails to elect a leader, or becomes briefly unavailable for writing, you typically need to analyze and tune the heartbeat and election parameters. This document describes the heartbeat and election mechanism, lists all related parameters, provides tuning guidance, and explains what information to collect when a problem occurs.

## Overview of Heartbeat and Election

Three kinds of interaction in a TDengine cluster are directly related to heartbeats and leader switches:

1. **vnode (Raft group) heartbeat**: The leader of each vgroup periodically sends `sync-heartbeat` messages to its followers, and followers reply with `sync-heartbeat-reply`. If a follower does not receive a heartbeat from the leader within the election timeout, it starts a new election.
2. **mnode (Raft group) heartbeat**: The mnodes also form a Raft group. Their heartbeat and election parameters are independent of those of vnodes, so mnode leader switches and vnode leader switches must be analyzed separately.
3. **dnode status report**: Each dnode periodically reports its status to the mnode. If the mnode does not receive a status report from a dnode within `statusTimeoutMs`, it marks that dnode as offline, which may in turn trigger a leader switch for every vgroup on that dnode.

For dual-replica (Arbitrator) deployments, leader election within a vgroup is not decided by a Raft majority. Instead, the mnode acts as the Arbitrator, and vnodes exchange dedicated arbitration heartbeats (`arb-hb`) with it. For related parameters, see [Dual-Replica (Arbitrator) Parameters](#dual-replica-arbitrator-parameters).

### Actual Value of the Election Timeout

The election timeout is not a fixed value. Each time the election timer is reset, the system picks a random value in the range `[election baseline, 2 × election baseline]` (logged as `reset elect timer, min:%d, max:%d, ms:%d`). With the default configuration, for example, the actual vnode election timeout is randomly distributed between 4000 ms and 8000 ms. This randomization reduces the chance that multiple nodes start an election at the same time.

Therefore, when you evaluate whether heartbeat and election parameters are reasonable, make sure that **election baseline > heartbeat interval × 2**. Otherwise, normal heartbeat jitter can trigger unnecessary elections.

## Heartbeat and Election Parameters

Most of the following parameters are global configuration parameters and must be modified with `ALTER ALL DNODES`. Parameters marked as local can be modified individually with `ALTER DNODE <dnode_id>`.

### vnode/mnode (Raft Group) Heartbeat and Election Parameters

| Parameter | Default | Unit | Range | Scope | Dynamically Modifiable | Description |
| --------- | ------- | ---- | ----- | ----- | ---------------------- | ----------- |
| `syncVnodeElectIntervalMs` | 4000 | ms | 10 - 172800000 | Global | Yes, effective immediately | Election timeout baseline for vnode Raft groups. The actual timeout is randomly chosen in `[baseline, 2 × baseline]`. Introduced in `v3.3.6.23` |
| `syncVnodeHeartbeatIntervalMs` | 1000 | ms | 10 - 172800000 | Global | Yes, effective immediately | Interval at which the leader of a vnode Raft group sends heartbeats. Introduced in `v3.3.6.23` |
| `syncMnodeElectIntervalMs` | 3000 | ms | 10 - 172800000 | Global | Yes, effective immediately | Election timeout baseline for mnode Raft groups. Introduced in `v3.3.6.23` |
| `syncMnodeHeartbeatIntervalMs` | 500 | ms | 10 - 172800000 | Global | Yes, effective immediately | Interval at which the leader of the mnode Raft group sends heartbeats. Introduced in `v3.3.6.23` |
| `syncElectInterval` | 4000 | ms | 10 - 172800000 | Global | No | Legacy heartbeat/election parameter. It has been replaced by `syncVnodeElectIntervalMs` and `syncMnodeElectIntervalMs`, has no effect in current versions, and should not be modified |
| `syncHeartbeatInterval` | 1000 | ms | 10 - 172800000 | Global | No | Legacy heartbeat/election parameter. It has been replaced by `syncVnodeHeartbeatIntervalMs` and `syncMnodeHeartbeatIntervalMs`, has no effect in current versions, and should not be modified |
| `syncHeartbeatTimeout` | 20000 | ms | 10 - 172800000 | Global | No | Threshold used by the leader to decide whether heartbeat replies from replicas have timed out. If no heartbeat reply has been received from a quorum of replicas within this period, the leader rejects new write requests (propose returns a heartbeat timeout error) |
| `syncSnapReplMaxWaitN` | 128 | entries | 16 - 256 | Global | Yes, effective immediately | Maximum number of log entries that can be waited on during snapshot replication. If a replica falls far behind and must be restored by a snapshot, a long-running restore keeps that replica from participating in elections |
| `snapshotRateLimit` | 0 | MB/s | 0 - 10240 | Global | Yes, effective immediately | Total snapshot sending bandwidth limit per dnode. 0 means no limit. When multiple vgroups replicate snapshots at the same time, disk I/O can be saturated, indirectly causing heartbeat timeouts. Introduced in `v3.4.2.0` |

### dnode Status Report Parameters

| Parameter | Default | Unit | Range | Scope | Dynamically Modifiable | Description |
| --------- | ------- | ---- | ----- | ----- | ---------------------- | ----------- |
| `statusInterval` | 1 | s | 1 - 30 | Global | Yes, effective immediately | Interval at which a dnode reports its status to the mnode. Setting this parameter automatically converts the value to `statusIntervalMs`. Introduced in `v3.3.0.0` |
| `statusIntervalMs` | 1000 | ms | 50 - 30000 | Global | Yes, effective immediately | Same as above, in milliseconds. Introduced in `v3.3.6.23` |
| `statusTimeoutMs` | 5000 | ms | 50 - 30000 | Global | Yes, effective immediately | Timeout after which the mnode considers a dnode offline. A dnode that fails to report its status within this period is marked offline, triggering a leader switch for the vgroups on it. Introduced in `v3.3.6.23` |
| `statusSRTimeoutMs` | 5000 | ms | 50 - 30000 | Global | Yes, effective immediately | Send-receive timeout for status requests (status, key sync, config, and so on). Introduced in `v3.3.6.23` |

### Dual-Replica (Arbitrator) Parameters

The following parameters take effect only in dual-replica (Arbitrator) deployments. Versions with the `Sec` suffix are in seconds and versions with the `Ms` suffix are in milliseconds; the two are equivalent, and setting a `Sec` version automatically converts the value to the corresponding `Ms` version.

| Parameter | Default | Unit | Range | Scope | Dynamically Modifiable | Description |
| --------- | ------- | ---- | ----- | ----- | ---------------------- | ----------- |
| `arbHeartBeatIntervalMs` | 2000 | ms | 100 - 172800000 | Global | Yes, effective immediately | Interval at which a dnode sends arbitration heartbeats to the Arbitrator (mnode) and at which the Arbitrator checks heartbeats. Introduced in `v3.3.6.23` |
| `arbHeartBeatIntervalSec` | 2 | s | 1 - 172800 | Global | Yes, effective immediately | Same as above, in seconds |
| `arbCheckSyncIntervalMs` | 3000 | ms | 100 - 172800000 | Global | Yes, effective immediately | Interval at which the Arbitrator checks the data synchronization status of replicas in a vgroup. Introduced in `v3.3.6.23` |
| `arbCheckSyncIntervalSec` | 3 | s | 1 - 172800 | Global | Yes, effective immediately | Same as above, in seconds |
| `arbSetAssignedTimeoutMs` | 14000 | ms | 100 - 172800000 | Global | Yes, effective immediately | Timeout after which the Arbitrator considers a member's heartbeat lost. If no heartbeat is received from a member within this period, the member is considered unavailable and another member can be designated as the assigned leader. Introduced in `v3.3.6.23` |
| `arbSetAssignedTimeoutSec` | 14 | s | 1 - 172800 | Global | Yes, effective immediately | Same as above, in seconds |
| `syncAssignedCheckAppliedGap` | 20 | entries | 0 - 10000 | Global | Yes, effective immediately | In dual-replica mode, the threshold for the gap between the peer's applied index and commit index before an assigned leader steps down. The assigned leader steps down only when the gap is within this value, preventing the peer from entering the restoring state. 0 means no gap check (immediate step down). Introduced in `v3.4.1.0` |

### Unified Timeout Parameter: syncTimeout

`syncTimeout` is a shortcut parameter for adjusting heartbeat and election timeouts together. Its default value is 0, which means it is disabled. After you modify `syncTimeout`, the system automatically derives and applies the other parameters according to the following formulas, so **you do not need to modify them one by one**:

| Derived Parameter | Formula |
| ----------------- | ------- |
| `arbSetAssignedTimeoutMs` | `syncTimeout` |
| `arbHeartBeatIntervalMs` | `syncTimeout / 4` |
| `arbCheckSyncIntervalMs` | `syncTimeout / 4` |
| `syncVnodeElectIntervalMs` | `(syncTimeout - syncTimeout / 4) / 2` |
| `syncMnodeElectIntervalMs` | `(syncTimeout - syncTimeout / 4) / 2` |
| `statusTimeoutMs` | `(syncTimeout - syncTimeout / 4) / 2` |
| `statusSRTimeoutMs` | `(syncTimeout - syncTimeout / 4) / 4` |
| `syncVnodeHeartbeatIntervalMs` | `(syncTimeout - syncTimeout / 4) / 8` |
| `syncMnodeHeartbeatIntervalMs` | `(syncTimeout - syncTimeout / 4) / 8` |
| `statusIntervalMs` | `(syncTimeout - syncTimeout / 4) / 8` |

For example, if you set `syncTimeout` to 20000 (20 seconds), the derived values are: `arbSetAssignedTimeoutMs = 20000`, `arbHeartBeatIntervalMs = arbCheckSyncIntervalMs = 5000`, `syncVnodeElectIntervalMs = syncMnodeElectIntervalMs = statusTimeoutMs = 7500`, `statusSRTimeoutMs = 3750`, and `syncVnodeHeartbeatIntervalMs = syncMnodeHeartbeatIntervalMs = statusIntervalMs = 1875`.

> Note: `syncTimeout` is a global parameter and must be modified with `ALTER ALL DNODES`. After it is modified, the values listed above are automatically pushed to all dnodes and their current values are overwritten. If you later modify one of those parameters individually, modifying `syncTimeout` again overwrites it once more.

### Debugging Parameters

When a problem occurs, you can temporarily enable the following parameters to obtain more detailed logs. Restore the default values after the issue is located, because these settings generate a large volume of logs and reduce performance.

| Parameter | Default | Range | Scope | Description |
| --------- | ------- | ----- | ----- | ----------- |
| `sDebugFlag` | 131/135 | 131 / 135 / 143 | Global | Log level of the sync (replication/election) module. 131 outputs error and warning logs, 135 adds debug logs, and 143 adds trace logs |
| `mDebugFlag` | 131/135 | 131 / 135 / 143 | Global | Log level of the mnode module. Useful for analyzing mnode leader switches and dnode status determination |
| `vDebugFlag` | 131/135 | 131 / 135 / 143 | Global | Log level of the vnode module |
| `dDebugFlag` | 131/135 | 131 / 135 / 143 | Global | Log level of the dnode module. Useful for analyzing dnode status reporting |
| `debugFlag` | 131/135 | 131 / 135 / 143 | Global | Global log level switch |
| `syncLogHeartbeat` | false | true / false | Local | Whether to raise heartbeat logs from the trace level to the info level. When enabled, every heartbeat sent and received is logged, which helps analyze heartbeat latency and packet loss |
| `syncRoutineReportInterval` | 300 | 5 - 600 (seconds) | Local | Interval for the routine sync node status log (`timer routines`). This log contains the term, commit index, match index, election count, and other complete state information, and is key to analyzing leader switches |

## Tuning Parameters for Leader Switch Issues

### Troubleshooting Approach

1. **Identify the object of the leader switch first**: Is it an mnode leader switch, a leader switch in one or more vgroups, or a batch of leader switches caused by a dnode being marked offline?
2. **Then identify the trigger**: network jitter or packet loss, excessive node load (CPU, memory, disk I/O), a full disk or WAL, a process restart or OOM kill, or a system clock jump.
3. **Tune parameters last**: Tuning parameters only alleviates the problem of heartbeats and elections being overly sensitive to jitter. If the root cause is a network or hardware fault, tuning parameters only extends the failover time and does not eliminate the problem.

### Tuning Recommendations

| Symptom | Parameters to Tune First | Direction |
| ------- | ------------------------ | --------- |
| Frequent leader switches in vgroups, with many heartbeat `slow` records in the log | `syncVnodeElectIntervalMs`, `syncVnodeHeartbeatIntervalMs` | Increase both, keeping "election baseline ≥ heartbeat interval × 4", for example a heartbeat of 2000 ms and an election baseline of 8000 ms |
| Frequent mnode leader switches | `syncMnodeElectIntervalMs`, `syncMnodeHeartbeatIntervalMs` | Increase both, for example a heartbeat of 1000 ms and an election baseline of 6000 ms |
| Poor network quality or cross-data-center deployment that requires relaxing all timeouts | `syncTimeout` | Derive all heartbeat and election parameters at once, for example set it to 20000 - 30000 |
| A dnode is incorrectly marked offline, causing batch leader switches of its vgroups | `statusInterval`, `statusTimeoutMs`, `statusSRTimeoutMs` | Increase `statusTimeoutMs` (for example to 10000 - 15000) so that it is several times larger than `statusIntervalMs` |
| Frequent assigned leader switches in dual-replica mode | `arbHeartBeatIntervalMs`, `arbSetAssignedTimeoutMs`, `arbCheckSyncIntervalMs` | Increase the heartbeat interval and timeout thresholds, keeping `arbSetAssignedTimeoutMs` at least five times `arbHeartBeatIntervalMs` |
| A replica falls far behind and heartbeats time out during snapshot replication | `snapshotRateLimit`, `syncSnapReplMaxWaitN` | Limit the snapshot replication bandwidth so that snapshot replication does not saturate disk I/O and cause heartbeat timeouts |
| After an assigned leader steps down, the peer enters the restoring state in dual-replica mode | `syncAssignedCheckAppliedGap` | Increase the threshold appropriately so that the leader waits for the peer to catch up before stepping down. Set it to 0 to disable the check |

### Tuning Procedure and Examples

1. Record the current values before modification so that you can compare and roll back:

   ```sql
   SHOW CLUSTER VARIABLES LIKE '%elect%';
   SHOW CLUSTER VARIABLES LIKE '%heartbeat%';
   SHOW CLUSTER VARIABLES LIKE '%status%';
   ```

2. Use `ALTER ALL DNODES` to modify global parameters. Heartbeat and election parameters are global, so they must be modified on all dnodes at the same time; otherwise, inconsistent timeouts across nodes make leader switches worse.

   ```sql
   -- Adjust the vnode heartbeat and election parameters
   ALTER ALL DNODES 'syncVnodeHeartbeatIntervalMs' '2000';
   ALTER ALL DNODES 'syncVnodeElectIntervalMs' '8000';

   -- Adjust the mnode heartbeat and election parameters
   ALTER ALL DNODES 'syncMnodeHeartbeatIntervalMs' '1000';
   ALTER ALL DNODES 'syncMnodeElectIntervalMs' '6000';

   -- Adjust the dnode status report timeout
   ALTER ALL DNODES 'statusTimeoutMs' '10000';
   ```

3. Alternatively, derive all related parameters at once using `syncTimeout`:

   ```sql
   ALTER ALL DNODES 'syncTimeout' '20000';
   ```

4. Confirm that the values are consistent across all dnodes after the modification:

   ```sql
   SHOW DNODE 1 VARIABLES LIKE '%elect%';
   SHOW DNODE 2 VARIABLES LIKE '%elect%';
   ```

5. Observe the cluster for a period of time covering at least one full business cycle to see whether leader switches still occur. If they do, continue collecting information and investigate root causes such as network and disk I/O.

### Notes

- Increasing election and heartbeat parameters **extends the failover time**. With an election baseline of 8000 ms, the theoretical failover time is on the order of 8 - 16 seconds, which must be acceptable to the business.
- Do not set the heartbeat interval or election timeout too small. Otherwise, even minor jitter triggers elections and the cluster becomes less stable.
- Do not modify too many parameters at once. Adjust one group at a time (vnode, mnode, status, or arb), observe the result, and then decide whether to continue.
- Global parameters must be modified with `ALTER ALL DNODES`. Using `ALTER DNODE` on a global parameter is rejected or results in inconsistent configuration across nodes.

## Information to Collect

When a leader switch problem occurs and needs to be submitted for analysis, collect the following information. Logs and status information must cover the period **from 5 to 10 minutes before to 5 to 10 minutes after** the leader switch.

### 1. Cluster and Configuration Status

```sql
-- Basic information about the cluster and its nodes
SHOW DNODES;
SHOW MNODES;
SHOW VGROUPS;

-- Current effective values of heartbeat and election parameters
SHOW CLUSTER VARIABLES LIKE '%elect%';
SHOW CLUSTER VARIABLES LIKE '%heartbeat%';
SHOW CLUSTER VARIABLES LIKE '%status%';
SHOW CLUSTER VARIABLES LIKE '%arb%';
SHOW CLUSTER VARIABLES LIKE '%sync%';
```

### 2. Roles and Election Time

```sql
-- Roles of vnodes and the time of the most recent election
-- Pay attention to whether role_time changes frequently
SELECT * FROM information_schema.ins_vnodes;

-- Roles of mnodes and the time each became its current role
SELECT * FROM information_schema.ins_mnodes;

-- For dual-replica deployments, collect the arbitration group status
SHOW ARBGROUPS;
SELECT * FROM information_schema.ins_arbgroups;
```

Record the `vgroup_id` and `dnode_id` where the leader switch occurred, the `role_time`, and the role before and after the switch.

### 3. taosd Logs

On every dnode, collect the `taosdlog.*` files in the `/var/log/taos/` directory (the default path; use the actual path if `logDir` has been changed). It is recommended to temporarily raise the log level of the sync module and enable heartbeat logging, reproduce the problem, and then restore the settings:

```sql
-- Enable debug/trace logs for the sync module
ALTER ALL DNODES 'sDebugFlag' '143';

-- Raise heartbeat logs to the info level (local parameter, can be enabled only on the affected dnodes)
ALTER DNODE 1 'syncLogHeartbeat' 'true';
ALTER DNODE 2 'syncLogHeartbeat' 'true';

-- Reduce the interval of routine sync status logs to locate the full state at the moment of the switch
ALTER DNODE 1 'syncRoutineReportInterval' '30';

-- Restore the default values after the problem is reproduced
ALTER ALL DNODES 'sDebugFlag' '131';
ALTER DNODE 1 'syncLogHeartbeat' 'false';
ALTER DNODE 1 'syncRoutineReportInterval' '300';
```

Keywords to look for in the logs:

| Keyword | Meaning |
| ------- | ------- |
| `reset elect timer` | The election timer was reset. Shows the min/max and the actual value of each election timeout |
| `become leader` / `become follower` | Role change, which is direct evidence of a leader switch |
| `sync-heartbeat` / `sync-heartbeat-reply` | Heartbeat sending and receiving. With `syncLogHeartbeat` enabled, every heartbeat is logged |
| `slow(` | Printed when a heartbeat or heartbeat reply takes longer than 1500 ms. An important clue for network or load problems |
| `timer routines` | Routine sync node status report, containing the term, commit index, match index, election count, and more. This is the key information for analyzing leader switches |
| `heartbeat timeout` | The leader has detected a heartbeat reply timeout and will reject write requests |
| `arb-hb` | Arbitrator heartbeat sending and receiving in dual-replica mode |
| `offline` / `status msg timeout` | A dnode was considered offline by the mnode |

### 4. Operating System and Network Information

Collect the following on the server hosting each dnode:

- Inter-node network latency and packet loss: `ping` between nodes and latency statistics to the node hosting the mnode. Sampling for at least 10 minutes is recommended.
- Network and disk load: continuous sampling output of `sar -n DEV 1`, `iostat -x 1`, and `top`/`vmstat 1`.
- System logs: `dmesg` or `/var/log/messages`, checking in particular for OOM events, disk errors, and NIC anomalies.
- System time synchronization status: `chronyc sources` or `ntpq -p`, and the clock offset between nodes. A clock jump between nodes causes heartbeat timeouts and abnormal status reporting.
- Free disk space: verify that the disks holding the data directory and the WAL directory are not full.

### 5. Other Information

- Version information: `SELECT server_version();` or `taosd -V`
- Deployment topology: number of replicas (dual-replica or three-replica), number of dnodes, and whether the deployment spans data centers
- The time when the problem occurred (to the second, with the time zone), the frequency, and the scope (a single vgroup or all of them)
- The behavior and error codes observed on the application side when the problem occurred, for example `Sync timeout`, `Sync leader is unreachable`, or `Sync leader is restoring`
- If the process crashed, also collect the core file and the `taosdlog` covering the crash time
