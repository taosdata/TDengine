# [6492510964] 虚拟超级表窗口查询性能优化测试 (Interval window)

### 1. 背景

1. 需求文档：[虚拟表窗口计算性能优化 RS](https://taosdata.feishu.cn/wiki/FKzWwNibkic1SRkLPmNcD1kXnEf)

### 2. 环境准备

1. CPU 信息：
  ```yaml
  $ lscpu
  Architecture:                x86_64
    CPU op-mode(s):            32-bit, 64-bit
    Address sizes:             46 bits physical, 48 bits virtual
    Byte Order:                Little Endian
  CPU(s):                      72
    On-line CPU(s) list:       0-71
  Vendor ID:                   GenuineIntel
    Model name:                Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
      CPU family:              6
      Model:                   79
      Thread(s) per core:      2
      Core(s) per socket:      18
      Socket(s):               2
      Stepping:                1
      CPU(s) scaling MHz:      40%
      CPU max MHz:             3000.0000
      CPU min MHz:             1200.0000
      BogoMIPS:                4588.97
      Flags:                   fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush dts acpi mmx fxsr sse sse2 ss ht tm pbe syscall nx pdpe1gb rdtscp lm constant_tsc arch_perfmon pebs bts rep_good nopl xtopology nonstop_tsc cpuid aperfmperf pni pc
                               lmulqdq dtes64 monitor ds_cpl vmx smx est tm2 ssse3 sdbg fma cx16 xtpr pdcm pcid dca sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand lahf_lm abm 3dnowprefetch cpuid_fault epb cat_l3 cdp_l3 pti intel_ppin ssbd ibr
                               s ibpb stibp tpr_shadow flexpriority ept vpid ept_ad fsgsbase tsc_adjust bmi1 hle avx2 smep bmi2 erms invpcid rtm cqm rdt_a rdseed adx smap intel_pt xsaveopt cqm_llc cqm_occup_llc cqm_mbm_total cqm_mbm_local dtherm ida arat pln pts vnmi md_cle
                               ar flush_l1d
  Virtualization features:
    Virtualization:            VT-x
  Caches (sum of all):
    L1d:                       1.1 MiB (36 instances)
    L1i:                       1.1 MiB (36 instances)
    L2:                        9 MiB (36 instances)
    L3:                        90 MiB (2 instances)
  NUMA:
    NUMA node(s):              1
    NUMA node0 CPU(s):         0-71
  Vulnerabilities:
    Gather data sampling:      Not affected
    Ghostwrite:                Not affected
    Indirect target selection: Not affected
    Itlb multihit:             KVM: Mitigation: Split huge pages
    L1tf:                      Mitigation; PTE Inversion; VMX conditional cache flushes, SMT vulnerable
    Mds:                       Mitigation; Clear CPU buffers; SMT vulnerable
    Meltdown:                  Mitigation; PTI
    Mmio stale data:           Mitigation; Clear CPU buffers; SMT vulnerable
    Reg file data sampling:    Not affected
    Retbleed:                  Not affected
    Spec rstack overflow:      Not affected
    Spec store bypass:         Mitigation; Speculative Store Bypass disabled via prctl
    Spectre v1:                Mitigation; usercopy/swapgs barriers and __user pointer sanitization
    Spectre v2:                Mitigation; Retpolines; IBPB conditional; IBRS_FW; STIBP conditional; RSB filling; PBRSB-eIBRS Not affected; BHI Not affected
    Srbds:                     Not affected
    Tsx async abort:           Mitigation; Clear CPU buffers; SMT vulnerable
  ```

1. 内存信息：
  ```sql
  free -h
                 total        used        free      shared  buff/cache   available
  Mem:            62Gi       2.1Gi        50Gi        14Mi        10Gi        60Gi
  Swap:          8.0Gi          0B       8.0Gi
  ```

1. 磁盘信息：
  ```bash
  lsblk
  
  NAME        MAJ:MIN RM   SIZE RO TYPE MOUNTPOINTS
  loop0         7:0    0  63.8M  1 loop /snap/core20/2669
  loop1         7:1    0     4K  1 loop /snap/bare/5
  loop2         7:2    0  73.9M  1 loop /snap/core22/2133
  loop3         7:3    0  73.9M  1 loop /snap/core22/2045
  loop4         7:4    0  11.1M  1 loop /snap/firmware-updater/167
  loop6         7:6    0   516M  1 loop /snap/gnome-42-2204/202
  loop7         7:7    0   1.7M  1 loop /snap/jump/81
  loop8         7:8    0  91.7M  1 loop /snap/gtk-common-themes/1535
  loop9         7:9    0  10.8M  1 loop /snap/snap-store/1270
  loop10        7:10   0  49.3M  1 loop /snap/snapd/24792
  loop11        7:11   0  50.8M  1 loop /snap/snapd/25202
  loop12        7:12   0   576K  1 loop /snap/snapd-desktop-integration/315
  loop13        7:13   0 516.2M  1 loop /snap/gnome-42-2204/226
  loop14        7:14   0 247.6M  1 loop /snap/firefox/6966
  loop15        7:15   0  18.5M  1 loop /snap/firmware-updater/210
  loop16        7:16   0 248.8M  1 loop /snap/firefox/7024
  nvme0n1     259:0    0 476.9G  0 disk
  ├─nvme0n1p1 259:1    0     1G  0 part /boot/efi
  └─nvme0n1p2 259:2    0 475.9G  0 part /
  ```

1. 操作系统信息：
  ```sql
  uname -a
  Linux simon-X99 6.14.0-35-generic #35~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Tue Oct 14 13:55:17 UTC 2 x86_64 x86_64 x86_64 GNU/Linux
  ```

### 3. 数据准备

1. 数据说明：
  这批测试数据的原始表由 **5 张表**组成（`test_table_session_d0` ~ `test_table_session_d4`）都属于同一张超级表 `test_stable_session`，每张表结构相同：
  - `ts TIMESTAMP`
  - `val0` ~ `val4` 五个整型列
  **数据分布为：每张表 100 万行**，时间戳按“分层间隔”生成：相邻点默认间隔 **2ms**，但在特定行边界插入更大的间隔（例如每 10/100/1000/… 行分别插入 12ms、120ms、1.2s、12s、120s）。
 这样同一份数据在做 `SESSION(ts, tol)` 时会呈现预期的窗口数：`1ms→100万窗口、10ms→10万窗口、…、1000s→1窗口`。
  虚拟超级表的结构和超级表的结构相同，每张虚拟子表都来源于一张原始表，比如 `test_vtable_session_d0` 的每一列都和 `test_table_session_d0` 对应
1. 原始表数据准备：
   - interval_insert.json
  ```python
  {
    "filetype": "insert",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "thread_count": 8,
    "confirm_parameter_prompt": "no",
    "result_file": "./insert_res.txt",
    "databases": [
      {
        "dbinfo": {
          "name": "test",
          "drop": "yes"
        },
        "super_tables": [
          {
            "name": "test_stable_interval",
            "child_table_exists": "no",
            "childtable_count": 5,
            "childtable_prefix": "test_table_interval_d",
  
            "auto_create_table": "no",
            "batch_create_tbl_num": 5,
  
            "data_source": "rand",
            "insert_mode": "taosc",
  
            "start_timestamp":1500000000000,
            "insert_rows": 1000000,
            "interlace_rows": 0,
            "insert_interval": 0,
  
            "disorder_ratio": 0,
            "disorder_range": 0,
  
            "timestamp_step": 1,
  
            "columns": [
              { "type": "INT", "name": "val0" },
              { "type": "INT", "name": "val1" },
              { "type": "INT", "name": "val2" },
              { "type": "INT", "name": "val3" },
              { "type": "INT", "name": "val4" }
            ],
            "tags": [
              { "type": "INT", "name": "tint", "min": 0, "max": 4 }
            ]
          }
        ]
      }
    ]
  }
  ```

1. 虚拟超级表数据准备：
  ```sql
  create stable test_vtable_interval(ts timestamp, val0 int, val1 int, val2 int, val3 int, val4 int) tags(tint int) virtual 1;
  
  create vtable test_vtable_interval_d0 (test_table_interval_d0.val0, test_table_interval_d0.val1, test_table_interval_d0.val2, test_table_interval_d0.val3, test_table_interval_d0.val4) using test_vtable_interval tags(0);
  
  create vtable test_vtable_interval_d1 (test_table_interval_d1.val0, test_table_interval_d1.val1, test_table_interval_d1.val2, test_table_interval_d1.val3, test_table_interval_d1.val4) using test_vtable_interval tags(1);
  
  create vtable test_vtable_interval_d2 (test_table_interval_d2.val0, test_table_interval_d2.val1, test_table_interval_d2.val2, test_table_interval_d2.val3, test_table_interval_d2.val4) using test_vtable_interval tags(2);
  
  create vtable test_vtable_interval_d3 (test_table_interval_d3.val0, test_table_interval_d3.val1, test_table_interval_d3.val2, test_table_interval_d3.val3, test_table_interval_d3.val4) using test_vtable_interval tags(3);
  
  create vtable test_vtable_interval_d4 (test_table_interval_d4.val0, test_table_interval_d4.val1, test_table_interval_d4.val2, test_table_interval_d4.val3, test_table_interval_d4.val4) using test_vtable_interval tags(4);
  ```

1. 性能测试脚本：
   - bench_interval_explain_analyze.py
  ```python
  #!/usr/bin/env python3
  import argparse
  import csv
  import os
  import re
  import subprocess
  import time
  from datetime import datetime
  
  PLANNING_RE = re.compile(r"Planning\s*Time\s*:\s*([0-9.]+)\s*(ms|s)", re.IGNORECASE)
  EXEC_RE     = re.compile(r"Execution\s*Time\s*:\s*([0-9.]+)\s*(ms|s)", re.IGNORECASE)
  INSET_RE    = re.compile(r"in set\s*\(\s*([0-9.]+)\s*s\s*\)", re.IGNORECASE)
  
  def to_ms(val: float, unit: str) -> float:
      unit = unit.lower()
      return val * 1000.0 if unit == "s" else val
  
  def run_one(taos_bin: str, db: str, table: str, tol: str, keep_dir: str | None):
      # INTERVAL 窗口
      sql = (
          "explain analyze "
          f"select count(val0), sum(val1), min(val2), max(val3), avg(val4) "
          f"from {table} interval({tol});"
      )
  
      t0 = time.perf_counter()
      proc = subprocess.run(
          [taos_bin, "-d", db, "-s", sql],
          stdout=subprocess.PIPE,
          stderr=subprocess.STDOUT,
          text=True,
          check=False,
      )
      t1 = time.perf_counter()
  
      out = proc.stdout or ""
      wall_ms = (t1 - t0) * 1000.0
  
      planning_ms = None
      execution_ms = None
      inset_s = None
  
      m = PLANNING_RE.search(out)
      if m:
          planning_ms = to_ms(float(m.group(1)), m.group(2))
  
      m = EXEC_RE.search(out)
      if m:
          execution_ms = to_ms(float(m.group(1)), m.group(2))
  
      m = INSET_RE.search(out)
      if m:
          inset_s = float(m.group(1))
  
      # 兜底：抓不到 Execution Time 就用 in set
      if execution_ms is None and inset_s is not None:
          execution_ms = inset_s * 1000.0
  
      saved_path = None
      if keep_dir:
          os.makedirs(keep_dir, exist_ok=True)
          saved_path = os.path.join(keep_dir, f"{table}__{tol}.out.txt")
          with open(saved_path, "w", encoding="utf-8") as f:
              f.write(out)
  
      row = {
          "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
          "table": table,
          "tol": tol,
          "rc": proc.returncode,
          "wall_ms": f"{wall_ms:.3f}",
          "planning_ms": "" if planning_ms is None else f"{planning_ms:.3f}",
          "execution_ms": "" if execution_ms is None else f"{execution_ms:.3f}",
          "in_set_s": "" if inset_s is None else f"{inset_s:.6f}",
          "output_file": "" if saved_path is None else saved_path,
      }
      return row
  
  def build_tols(start: int, end: int, factor: int):
      tols = []
      v = start
      while v <= end:
          tols.append(f"{v}a")
          if v == end:
              break
          v *= factor
      return tols
  
  def main():
      ap = argparse.ArgumentParser()
      ap.add_argument("--taos", default="taos")
      ap.add_argument("--db", default="test")
      ap.add_argument("--out", default="bench_interval_results.csv")
      ap.add_argument("--keep-dir", default="bench_interval_outputs")
      ap.add_argument("--no-keep", action="store_true")
      ap.add_argument("--tables", default="test_stable_interval,test_vtable_interval",
                      help="Comma-separated table names (default: test_stable_interval,test_vtable_interval)")
      ap.add_argument("--start", type=int, default=1, help="start multiplier for 'a' (default: 1)")
      ap.add_argument("--end", type=int, default=10_000_000, help="end multiplier for 'a' (default: 10000000)")
      ap.add_argument("--factor", type=int, default=10, help="multiply step factor (default: 10)")
      args = ap.parse_args()
  
      tables = [t.strip() for t in args.tables.split(",") if t.strip()]
      tols = build_tols(args.start, args.end, args.factor)
  
      keep_dir = None if args.no_keep else args.keep_dir
  
      with open(args.out, "w", newline="", encoding="utf-8") as f:
          w = csv.DictWriter(
              f,
              fieldnames=["ts", "table", "tol", "rc", "wall_ms", "planning_ms", "execution_ms", "in_set_s", "output_file"],
          )
          w.writeheader()
  
          for table in tables:
              for tol in tols:
                  row = run_one(args.taos, args.db, table, tol, keep_dir)
                  w.writerow(row)
                  print(f"[{row['ts']}] {row['table']} tol={row['tol']} rc={row['rc']} "
                        f"wall_ms={row['wall_ms']} planning_ms={row['planning_ms']} "
                        f"execution_ms={row['execution_ms']} in_set_s={row['in_set_s']}")
  
      print(f"\nDone. CSV saved to: {args.out}")
      if keep_dir:
          print(f"Full outputs saved under: {keep_dir}/")
  
  if __name__ == "__main__":
      main()
  ```

### 4. 测试步骤

1. 使用 3.1 中提供的 json, 执行 `taosBenchmark -f interval_insert.json`
2. 在 taosc 执行 3.3 中的 sql
3. 执行 3.4 中的性能测试脚本，得到测试结果

### 5. 测试结果

1. 运行结果
   - （优化后）
    ```yaml
    [2026-03-04 15:25:23] test_stable_interval tol=1a rc=0 wall_ms=9241.287 planning_ms=6.980 execution_ms=9056.403 in_set_s=9.063993
    [2026-03-04 15:25:24] test_stable_interval tol=10a rc=0 wall_ms=1302.023 planning_ms=6.292 execution_ms=1107.940 in_set_s=1.114787
    [2026-03-04 15:25:24] test_stable_interval tol=100a rc=0 wall_ms=495.911 planning_ms=5.827 execution_ms=316.657 in_set_s=0.323036
    [2026-03-04 15:25:25] test_stable_interval tol=1000a rc=0 wall_ms=444.024 planning_ms=7.137 execution_ms=241.117 in_set_s=0.248788
    [2026-03-04 15:25:25] test_stable_interval tol=10000a rc=0 wall_ms=421.624 planning_ms=6.305 execution_ms=246.764 in_set_s=0.253597
    [2026-03-04 15:25:26] test_stable_interval tol=100000a rc=0 wall_ms=426.774 planning_ms=6.567 execution_ms=198.405 in_set_s=0.205502
    [2026-03-04 15:25:26] test_stable_interval tol=1000000a rc=0 wall_ms=321.333 planning_ms=6.738 execution_ms=163.373 in_set_s=0.170432
    [2026-03-04 15:25:26] test_stable_interval tol=10000000a rc=0 wall_ms=392.359 planning_ms=6.290 execution_ms=192.074 in_set_s=0.198880
    [2026-03-04 15:25:40] test_vtable_interval tol=1a rc=0 wall_ms=13233.457 planning_ms=10.727 execution_ms=13010.302 in_set_s=13.022219
    [2026-03-04 15:25:42] test_vtable_interval tol=10a rc=0 wall_ms=2329.757 planning_ms=187.844 execution_ms=1995.467 in_set_s=2.184523
    [2026-03-04 15:25:43] test_vtable_interval tol=100a rc=0 wall_ms=922.486 planning_ms=9.812 execution_ms=730.936 in_set_s=0.742021
    [2026-03-04 15:25:44] test_vtable_interval tol=1000a rc=0 wall_ms=724.592 planning_ms=11.517 execution_ms=576.512 in_set_s=0.589149
    [2026-03-04 15:25:44] test_vtable_interval tol=10000a rc=0 wall_ms=622.746 planning_ms=10.706 execution_ms=397.462 in_set_s=0.409247
    [2026-03-04 15:25:45] test_vtable_interval tol=100000a rc=0 wall_ms=420.206 planning_ms=11.801 execution_ms=250.189 in_set_s=0.263240
    [2026-03-04 15:25:45] test_vtable_interval tol=1000000a rc=0 wall_ms=448.150 planning_ms=11.306 execution_ms=228.543 in_set_s=0.241026
    [2026-03-04 15:25:46] test_vtable_interval tol=10000000a rc=0 wall_ms=420.554 planning_ms=10.865 execution_ms=227.143 in_set_s=0.239246
    ```

   - 优化前
    ```yaml
    [2026-03-04 15:24:55] test_vtable_interval tol=1a rc=0 wall_ms=13410.025 planning_ms=10.471 execution_ms=13241.656 in_set_s=13.253273
    [2026-03-04 15:24:58] test_vtable_interval tol=10a rc=0 wall_ms=2403.627 planning_ms=206.237 execution_ms=2009.927 in_set_s=2.217281
    [2026-03-04 15:24:59] test_vtable_interval tol=100a rc=0 wall_ms=1102.039 planning_ms=9.334 execution_ms=897.959 in_set_s=0.908498
    [2026-03-04 15:25:00] test_vtable_interval tol=1000a rc=0 wall_ms=900.077 planning_ms=10.753 execution_ms=706.523 in_set_s=0.718425
    [2026-03-04 15:25:00] test_vtable_interval tol=10000a rc=0 wall_ms=619.180 planning_ms=11.502 execution_ms=406.653 in_set_s=0.419409
    [2026-03-04 15:25:01] test_vtable_interval tol=100000a rc=0 wall_ms=292.063 planning_ms=11.004 execution_ms=179.745 in_set_s=0.191789
    [2026-03-04 15:25:01] test_vtable_interval tol=1000000a rc=0 wall_ms=394.891 planning_ms=8.828 execution_ms=227.194 in_set_s=0.237235
    [2026-03-04 15:25:01] test_vtable_interval tol=10000000a rc=0 wall_ms=418.140 planning_ms=11.235 execution_ms=198.410 in_set_s=0.210719
    ```

1. 结果汇总

  | interval | stable | vtable(优化前) | vtable(优化后) | 优化倍率(前/后) | 优化倍率(后/stable) |
| --- | --- | --- | --- | --- | --- |
| 1a | 10.268001 | 22.716700 | 13.022219 | 1.743× | 0.787× |
| 10a | 1.114787 | 17.440916 | 2.184523 | 7.985× | 0.511× |
| 100a | 0.323036 | 16.941701 | 0.742021 | 22.86× | 0.437× |
| 1000a | 0.248788 | 18.022920 | 0.589149 | 30.56× | 0.422× |
| 10000a | 0.253597 | 18.280976 | 0.409247 | 44.72× | 0.617× |
| 100000a | 0.205502 | 17.132654 | 0.263240 | 65.12× | 0.781× |
| 1000000a | 0.170432 | 18.723603 | 0.241026 | 77.61× | 0.707× |
| 10000000a | 0.198880 | 18.184261 | 0.239246 | 76.03× | 0.832× |

### 6. 测试总结

1. **优化后 vtable 在性能全面优于优化前的 vtable 性能**：相对优化前提升 **1.7×** ~ **77.61×**，窗口内数据越多优化效果越明显。
2. **优化后 vtable 性能普遍差于 stable 性能：** 性能约为 stable 的 **0.42x~0.83×**， 窗口内数据越多性能差距越小。
