# [6492510964] 虚拟超级表窗口查询性能优化测试 (Session window)

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
   - gen_session_data.py
  ```python
  from datetime import datetime, timedelta
  
  N_PER_TABLE = 1_000_000
  TABLES = 5
  START = datetime(2026, 1, 1, 0, 0, 0)
  
  BATCH = 2000  # 每条 INSERT 拼多少行，可按性能调
  
  def gap_ms_for_index(i: int) -> int:
      """
      i 表示相邻 gap 的位置（1..N-1）
      分层 gap 规则：同一份数据满足 1ms->100w, 10ms->10w ... 1000s->1 窗口
      """
      gap = 2  # 默认 2ms：保证 >1ms
  
      if i % 100_000 == 0:
          gap = 120_000      # 120s
      elif i % 10_000 == 0:
          gap = 12_000       # 12s
      elif i % 1_000 == 0:
          gap = 1_200        # 1.2s
      elif i % 100 == 0:
          gap = 120          # 120ms
      elif i % 10 == 0:
          gap = 12           # 12ms
      return gap
  
  def fmt_ts_ms(dt: datetime) -> str:
      # TDengine ms 精度：YYYY-MM-DD HH:MM:SS.mmm
      return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{dt.microsecond // 1000:03d}"
  
  # 1) 先计算每张表的总跨度（ms），保证“完全不重叠”
  total_span_ms = 0
  for i in range(1, N_PER_TABLE):  # 1..N-1
      total_span_ms += gap_ms_for_index(i)
  
  # 给一点 buffer，保证前表最后一条 < 后表第一条（毫秒级就够）
  BUFFER_MS = 1
  TABLE_OFFSET = timedelta(milliseconds=total_span_ms + BUFFER_MS)
  
  print(f"Per-table span: {total_span_ms} ms = {total_span_ms/1000:.0f} s")
  
  out = "origin_table_5tables.sql"
  with open(out, "w", encoding="utf-8") as f:
      # 建表
      f.write(f"DROP STABLE IF EXISTS test_stable_session;\n")
      f.write(
          f"CREATE STABLE test_stable_session("
          f"ts TIMESTAMP, val0 INT, val1 INT, val2 INT, val3 INT, val4 INT"
          f") TAGS (tint int);\n"
      )
      for t in range(TABLES):
          f.write(
              f"CREATE TABLE test_table_session_d{t} USING test_stable_session TAGS (1);\n"
          )
      f.write("\n")
  
      # 2) 插入数据：每张表起点 = START + t * TABLE_OFFSET（保证时间范围完全不重叠）
      for t in range(TABLES):
          cur = START + t * TABLE_OFFSET
  
          idx = 0
          while idx < N_PER_TABLE:
              end = min(idx + BATCH, N_PER_TABLE)
              rows = []
              for k in range(idx, end):
                  # val0..val4 给一个确定性的模式（方便复现/比对）
                  v0 = k
                  v1 = k + 1
                  v2 = k + 2
                  v3 = k + 3
                  v4 = k + 4
  
                  rows.append(f"('{fmt_ts_ms(cur)}',{v0},{v1},{v2},{v3},{v4})")
  
                  if k < N_PER_TABLE - 1:
                      cur += timedelta(milliseconds=gap_ms_for_index(k + 1))
  
              f.write(f"INSERT INTO test_table_session_d{t} VALUES " + ",".join(rows) + ";\n")
              idx = end
  
  print(f"generated {out}")
  ```

1. 虚拟超级表数据准备：
  ```sql
  create stable test_vtable_session(ts timestamp, val0 int, val1 int, val2 int, val3 int, val4 int) tags(tint int) virtual 1;
  
  create vtable test_vtable_session_d0 (test_table_session_d0.val0, test_table_session_d0.val1, test_table_session_d0.val2, test_table_session_d0.val3, test_table_session_d0.val4) using test_vtable_session tags(0);
  
  create vtable test_vtable_session_d1 (test_table_session_d1.val0, test_table_session_d1.val1, test_table_session_d1.val2, test_table_session_d1.val3, test_table_session_d1.val4) using test_vtable_session tags(1);
  
  create vtable test_vtable_session_d2 (test_table_session_d2.val0, test_table_session_d2.val1, test_table_session_d2.val2, test_table_session_d2.val3, test_table_session_d2.val4) using test_vtable_session tags(2);
  
  create vtable test_vtable_session_d3 (test_table_session_d3.val0, test_table_session_d3.val1, test_table_session_d3.val2, test_table_session_d3.val3, test_table_session_d3.val4) using test_vtable_session tags(3);
  
  create vtable test_vtable_session_d4 (test_table_session_d4.val0, test_table_session_d4.val1, test_table_session_d4.val2, test_table_session_d4.val3, test_table_session_d4.val4) using test_vtable_session tags(4);
  ```

1. 性能测试脚本：
   - bench_session_explain_analyze.py
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
      sql = (
          "explain analyze "
          f"select count(val0), sum(val1), min(val2), max(val3), avg(val4) "
          f"from {table} session(ts, {tol});"
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
  
      # 兜底：Execution Time 没抓到就用 in set
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
  
  def main():
      ap = argparse.ArgumentParser()
      ap.add_argument("--taos", default="taos")
      ap.add_argument("--db", default="test")
      ap.add_argument("--out", default="bench_session_results.csv")
      ap.add_argument("--keep-dir", default="bench_outputs")
      ap.add_argument("--no-keep", action="store_true")
      ap.add_argument("--tables", default="test_stable_session,test_vtable_session")
      ap.add_argument("--start", type=int, default=1)
      ap.add_argument("--end", type=int, default=1_000_000)
      ap.add_argument("--factor", type=int, default=10)
      args = ap.parse_args()
  
      tables = [t.strip() for t in args.tables.split(",") if t.strip()]
      tols = []
      v = args.start
      while v <= args.end:
          tols.append(f"{v}a")
          if v == args.end:
              break
          v *= args.factor
  
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

1. 使用 3.1 中提供的原始表数据准备脚本生成 `origin_table_5tables.sql`
2. 在 taosc 执行 `source '/path/to/sql/origin_table_5tables.sql';`
3. 在 taosc 执行 3.2 中的 sql
4. 执行 3.3 中的性能测试脚本，得到测试结果

### 5. 测试结果 {folded="true"}

1. 运行结果
   - （优化后）
    ```yaml
    [2026-03-03 17:15:58] test_stable_session tol=1a rc=0 wall_ms=31146.914 planning_ms=5.803 execution_ms=31003.474 in_set_s=31.009831
    [2026-03-03 17:16:06] test_stable_session tol=10a rc=0 wall_ms=8034.341 planning_ms=5.778 execution_ms=7808.107 in_set_s=7.814396
    [2026-03-03 17:16:13] test_stable_session tol=100a rc=0 wall_ms=6627.703 planning_ms=6.446 execution_ms=6421.856 in_set_s=6.428851
    [2026-03-03 17:16:19] test_stable_session tol=1000a rc=0 wall_ms=6525.802 planning_ms=6.472 execution_ms=6311.665 in_set_s=6.318646
    [2026-03-03 17:16:26] test_stable_session tol=10000a rc=0 wall_ms=6323.509 planning_ms=6.278 execution_ms=6129.805 in_set_s=6.136609
    [2026-03-03 17:16:32] test_stable_session tol=100000a rc=0 wall_ms=6400.261 planning_ms=6.001 execution_ms=6232.195 in_set_s=6.238692
    [2026-03-03 17:16:38] test_stable_session tol=1000000a rc=0 wall_ms=6324.428 planning_ms=5.717 execution_ms=6121.780 in_set_s=6.127836
    [2026-03-03 17:17:22] test_vtable_session tol=1a rc=0 wall_ms=43579.489 planning_ms=10.943 execution_ms=43380.806 in_set_s=43.393144
    [2026-03-03 17:17:29] test_vtable_session tol=10a rc=0 wall_ms=7535.359 planning_ms=10.220 execution_ms=7320.259 in_set_s=7.331850
    [2026-03-03 17:17:34] test_vtable_session tol=100a rc=0 wall_ms=4142.579 planning_ms=11.014 execution_ms=3935.874 in_set_s=3.948347
    [2026-03-03 17:17:38] test_vtable_session tol=1000a rc=0 wall_ms=3939.683 planning_ms=11.812 execution_ms=3768.023 in_set_s=3.781351
    [2026-03-03 17:17:41] test_vtable_session tol=10000a rc=0 wall_ms=3711.781 planning_ms=10.213 execution_ms=3524.035 in_set_s=3.535914
    [2026-03-03 17:17:45] test_vtable_session tol=100000a rc=0 wall_ms=3512.141 planning_ms=11.906 execution_ms=3361.685 in_set_s=3.375188
    [2026-03-03 17:17:49] test_vtable_session tol=1000000a rc=0 wall_ms=3815.358 planning_ms=11.784 execution_ms=3622.527 in_set_s=3.63570
    ```

   - 优化前
    ```yaml
    [2026-03-03 17:30:50] test_vtable_session tol=1a rc=0 wall_ms=40564.628 planning_ms=11.066 execution_ms=40383.113 in_set_s=40.395439
    [2026-03-03 17:31:07] test_vtable_session tol=10a rc=0 wall_ms=16235.249 planning_ms=10.430 execution_ms=16091.996 in_set_s=16.103621
    [2026-03-03 17:31:23] test_vtable_session tol=100a rc=0 wall_ms=16447.632 planning_ms=9.892 execution_ms=16273.456 in_set_s=16.284503
    [2026-03-03 17:31:39] test_vtable_session tol=1000a rc=0 wall_ms=16048.551 planning_ms=10.889 execution_ms=15815.199 in_set_s=15.827274
    [2026-03-03 17:31:54] test_vtable_session tol=10000a rc=0 wall_ms=14839.306 planning_ms=11.570 execution_ms=14618.666 in_set_s=14.631443
    [2026-03-03 17:32:09] test_vtable_session tol=100000a rc=0 wall_ms=15420.792 planning_ms=11.267 execution_ms=15237.566 in_set_s=15.250017
    [2026-03-03 17:32:25] test_vtable_session tol=1000000a rc=0 wall_ms=15240.454 planning_ms=10.373 execution_ms=15061.863 in_set_s=15.073464
    ```

1. 结果汇总

  | tol | stable | vtable(优化前) | vtable(优化后) | 优化倍率(前/后) | 优化倍率(后/stable) |
| --- | --- | --- | --- | --- | --- |
| 1a | 31.009831 | 40.395439 | 43.393144 | 0.931× | 0.715× |
| 10a | 7.814396 | 16.103621 | 7.331850 | 2.196× | 1.066× |
| 100a | 6.428851 | 16.284503 | 3.948347 | 4.124× | 1.628× |
| 1000a | 6.318646 | 15.827274 | 3.781351 | 4.186× | 1.671× |
| 10000a | 6.136609 | 14.631443 | 3.535914 | 4.138× | 1.736× |
| 100000a | 6.238692 | 15.250017 | 3.375188 | 4.518× | 1.848× |
| 1000000a | 6.127836 | 15.073464 | 3.635700 | 4.146× | 1.685× |

### 6. 测试总结

1. **优化后 vtable 在 tol ≥ 10a 时性能全面优于优化前的 vtable 性能**：相对优化前提升 **2.2×~4.5×**，
2. **优化后 vtable 在 tol ≥ 10a 时性能全面优于 stable 性能：**相对 stable 提升 **1.1×~1.85×**；
3. **tol = 1a 时性能有一定的回退：**因为窗口数量过多且每个窗口内只有一条数据，导致优化后的计划并不能很好的处理这种场景。但是对比优化前的性能下降不超过 10%。
