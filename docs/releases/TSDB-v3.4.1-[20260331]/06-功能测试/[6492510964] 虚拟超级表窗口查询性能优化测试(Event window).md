# [6492510964] 虚拟超级表窗口查询性能优化测试(Event window)

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

1. 测试数据说明：
  虚拟表共 6 列，其中前五列来自同一个原始表，第六列来自另一张原始表。前五列通过 taosBenchmark 生成，每列 1,000,000 条数据。第六列为 event 列，为手工构造的数据集。
  对 event 列构造了如下的数据集：
  使用 event window 条件为 `START WITH (c_event = 1 OR c_event = 3) END WITH (c_event = 2 OR c_event = 3)` 来构造该列的数据，窗口数从 1 到 1,000,000 不等，每个窗口的数据也从 1 到 1,000,000 不等，窗口数 * 每个窗口内数据 <= 1,000,000。
  测试结果中的 win 和 row 含义如下：
**win**：事件窗口（EVENT_WINDOW）数量。
**row**：总共落入这些窗口的数据行数。
1. 原始表数据准备：
   - insert.json
  ```json
  {
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "connection_pool_size": 8,
    "thread_count": 60,
    "create_table_thread_count": 4,
    "result_file": "./insert_res.txt",
    "confirm_parameter_prompt": "no",
    "num_of_records_per_req": 1000000,
    "prepared_rand": 10000,
    "chinese": "no",
    "escape_character": "yes",
    "continue_if_fail": "no",
    "databases": [
      {
        "dbinfo": {
          "name": "test_vtable_perf",
          "drop": "yes",
          "vgroups": 8,
          "precision": "ms"
        },
        "super_tables": [
          {
            "name": "stb_bool",
            "child_table_exists": "no",
            "childtable_count": 1,
            "childtable_prefix": "ctb_bool",
            "auto_create_table": "no",
            "batch_create_tbl_num": 5,
            "data_source": "rand",
            "insert_mode": "taosc",
            "non_stop_mode": "no",
            "line_protocol": "line",
            "insert_rows": 1000000,
            "childtable_limit": 0,
            "childtable_offset": 0,
            "interlace_rows": 0,
            "insert_interval": 0,
            "partial_col_num": 0,
            "timestamp_step": 1,
            "start_timestamp": "2020-10-01 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "use_sample_ts": "no",
            "tags_file": "",
            "columns": [
              {"type": "bool", "name": "bool_col", "count": 1, "max": 1, "min": 0 }
            ],
            "tags": [
              {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1}
            ]
          },
          {
            "name": "stb_int",
            "child_table_exists": "no",
            "childtable_count": 1,
            "childtable_prefix": "ctb_int",
            "auto_create_table": "no",
            "batch_create_tbl_num": 5,
            "data_source": "rand",
            "insert_mode": "taosc",
            "non_stop_mode": "no",
            "line_protocol": "line",
            "insert_rows": 1000000,
            "childtable_limit": 0,
            "childtable_offset": 0,
            "interlace_rows": 0,
            "insert_interval": 0,
            "partial_col_num": 0,
            "timestamp_step": 1,
            "start_timestamp": "2020-10-01 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "use_sample_ts": "no",
            "tags_file": "",
            "columns": [
              {"type": "int", "name": "int_col", "count": 1, "max": 2147483647, "min": -2147483648 }
            ],
            "tags": [
              {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1}
            ]
          },
          {
            "name": "stb_float",
            "child_table_exists": "no",
            "childtable_count": 1,
            "childtable_prefix": "ctb_float",
            "auto_create_table": "no",
            "batch_create_tbl_num": 5,
            "data_source": "rand",
            "insert_mode": "taosc",
            "non_stop_mode": "no",
            "line_protocol": "line",
            "insert_rows": 1000000,
            "childtable_limit": 0,
            "childtable_offset": 0,
            "interlace_rows": 0,
            "insert_interval": 0,
            "partial_col_num": 0,
            "timestamp_step": 1,
            "start_timestamp": "2020-10-01 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "use_sample_ts": "no",
            "tags_file": "",
            "columns": [
              {"type": "float", "name": "float_col", "count": 1, "max": 100000, "min": -100000 }
            ],
            "tags": [
              {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1}
            ]
          },
          {
            "name": "stb_double",
            "child_table_exists": "no",
            "childtable_count": 1,
            "childtable_prefix": "ctb_double",
            "auto_create_table": "no",
            "batch_create_tbl_num": 5,
            "data_source": "rand",
            "insert_mode": "taosc",
            "non_stop_mode": "no",
            "line_protocol": "line",
            "insert_rows": 1000000,
            "childtable_limit": 0,
            "childtable_offset": 0,
            "interlace_rows": 0,
            "insert_interval": 0,
            "partial_col_num": 0,
            "timestamp_step": 1,
            "start_timestamp": "2020-10-01 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "use_sample_ts": "no",
            "tags_file": "",
            "columns": [
              {"type": "double", "name": "double_col", "count": 1, "max": 100000000, "min": -100000000 }
            ],
            "tags": [
              {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1}
            ]
          },
          {
            "name": "stb_ubigint",
            "child_table_exists": "no",
            "childtable_count": 1,
            "childtable_prefix": "ctb_ubigint",
            "auto_create_table": "no",
            "batch_create_tbl_num": 5,
            "data_source": "rand",
            "insert_mode": "taosc",
            "non_stop_mode": "no",
            "line_protocol": "line",
            "insert_rows": 1000000,
            "childtable_limit": 0,
            "childtable_offset": 0,
            "interlace_rows": 0,
            "insert_interval": 0,
            "partial_col_num": 0,
            "timestamp_step": 1,
            "start_timestamp": "2020-10-01 00:00:00.000",
            "sample_format": "csv",
            "sample_file": "./sample.csv",
            "use_sample_ts": "no",
            "tags_file": "",
            "columns": [
              {"type": "ubigint", "name": "u_bigint_col", "count": 1, "max": 18446744073709551615, "min": 0 }
            ],
            "tags": [
              {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1}
            ]
          }
        ]
      }
    ]
  }
  
  ```

1. 测试脚本准备：
  ```python
  #!/usr/bin/env python3
  import os
  import csv
  import subprocess
  import re
  
  start_ts = 1601481600000
  
  win_options = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
  row_options = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
  
  # “点数/条数”语义：
  # row = 总点数（总条数）
  # win = 窗口个数
  # 每个窗口分到 L 个点：窗口跨度应为 L-1（ms），count 应为 L
  ROW_SCALE = max(row_options)  # 1_000_000 点
  
  # 让时间轴上恰好有 ROW_SCALE 个 1ms 点：索引 0..ROW_SCALE-1
  end_ts = start_ts + (ROW_SCALE - 1)
  time_range = end_ts - start_ts
  
  output_dir = "output_event"
  csv_dir = os.path.join(output_dir, "csv")
  os.makedirs(csv_dir, exist_ok=True)
  abs_csv_dir = os.path.abspath(csv_dir)
  os.makedirs(abs_csv_dir, exist_ok=True)
  
  # 不再区分 dense/sparse：每个 (win,row) 只生成一套表
  table_defs = []
  for win in win_options:
      for row in row_options:
          if row >= win:
              table_defs.append((f"t_event_{win}_win_{row}_row", win, row))
  
  def run_sql(sql):
      try:
          res = subprocess.run(
              ["taos", "-s", sql],
              stdout=subprocess.PIPE,
              stderr=subprocess.PIPE,
              text=True,
              timeout=1200
          )
          out = res.stdout
          m = re.search(r'in set\s*\(([\d\.]+)s\)', out)
          if m:
              return float(m.group(1))
          m2 = re.search(r'\(([\d\.]+)s\)', out)
          if m2:
              return float(m2.group(1))
          return None
      except subprocess.TimeoutExpired:
          return None
  
  def execute_sql_file(sql_file_path):
      abs_path = os.path.abspath(sql_file_path)
      print(f"\n📌 Executing SQL file: {abs_path}")
      try:
          res = subprocess.run(
              ["taos", "-s", f"source {abs_path}"],
              stdout=subprocess.PIPE,
              stderr=subprocess.PIPE,
              text=True,
              timeout=3600
          )
          print(res.stdout)
          if res.stderr:
              print("⚠️  Errors/Warnings:\n", res.stderr)
      except subprocess.TimeoutExpired:
          print(f"❌ Timeout executing {abs_path}")
  
  def generate_event_csv_load_sql():
      load_sql_path = os.path.join(output_dir, "load_all_event.sql")
      with open(load_sql_path, "w") as fsql:
          fsql.write("create database if not exists test_vtable_perf_event;\n")
          fsql.write("use test_vtable_perf_event;\n\n")
  
          for table, win, row in table_defs:
              fsql.write(f"create table if not exists {table} (ts timestamp, c_event int);\n")
          fsql.write("\n")
  
          for table, win, row in table_defs:
              csv_path = os.path.join(csv_dir, f"{table}.csv")
              abs_path = os.path.join(abs_csv_dir, f"{table}.csv")
  
              # row 个点均分到 win 个窗口：sum(win_lens) == row
              base = row // win
              rem = row % win
              win_lens = [(base + 1) if i < rem else base for i in range(win)]
  
              # 点索引 k 映射到 ts：1 点 = 1ms
              def ts_at_point(k: int) -> int:
                  return start_ts + k
  
              with open(csv_path, "w", newline="") as f:
                  writer = csv.writer(f)
                  writer.writerow(["ts", "c_event"])
  
                  cursor = 0  # 当前窗口起始点索引（0..row-1）
                  for L in win_lens:
                      if L <= 0:
                          continue
  
                      if L == 1:
                          # 单点窗口：用 3 表示 start+end
                          writer.writerow([ts_at_point(cursor), 3])
                          cursor += 1
                      else:
                          start_p = cursor
                          end_p = cursor + L - 1  # L 个点 => 跨度 L-1
                          writer.writerow([ts_at_point(start_p), 1])
                          writer.writerow([ts_at_point(end_p), 2])
                          cursor += L
  
                  assert cursor == row, f"[BUG] cursor({cursor}) != row({row}) for {table}"
  
              fsql.write(f"insert into {table} file '{abs_path}';\n")
  
      print("✔ load_all_event.sql generated.")
  
  def generate_vstable_vtable_event_sql():
      sql_path = os.path.join(output_dir, "vstable_vtable_event.sql")
      with open(sql_path, "w") as fsql:
          fsql.write("create database if not exists test_vtable_perf;\n")
          fsql.write("use test_vtable_perf;\n\n")
  
          for table, win, row in table_defs:
              short = table.replace("t_event_", "")
              vstable = f"vst_event_{short}"
  
              fsql.write(
                  f"create stable {vstable} ("
                  "ts timestamp,"
                  "c_bool bool,"
                  "c_int int,"
                  "c_float float,"
                  "c_double double,"
                  "c_ubigint bigint unsigned,"
                  "c_event int"
                  ") tags(tint int,tchar varchar(20)) virtual 1;\n"
              )
  
              vtable = f"vct_event_{short}_d0"
              fsql.write(
                  f"create vtable {vtable} ("
                  "c_bool from ctb_bool0.bool_col,"
                  "c_int from ctb_int0.int_col,"
                  "c_float from ctb_float0.float_col,"
                  "c_double from ctb_double0.double_col,"
                  "c_ubigint from ctb_ubigint0.u_bigint_col,"
                  f"c_event from test_vtable_perf_event.{table}.c_event"
                  f") using {vstable} tags(0,'0');\n\n"
              )
  
      print("✔ vstable_vtable_event.sql generated.")
  
  def generate_stable_event_sql():
      sql_path = os.path.join(output_dir, "stable_event.sql")
      with open(sql_path, "w") as fsql:
          fsql.write("create database if not exists test_vtable_perf;\n")
          fsql.write("use test_vtable_perf;\n\n")
  
          for table, win, row in table_defs:
              short = table.replace("t_event_", "")
              stable = f"st_event_{short}"
              ctable = f"ct_event_{short}_d0"
              vtable = f"vct_event_{short}_d0"
  
              fsql.write(
                  f"create stable {stable} ("
                  "ts timestamp,"
                  "c_bool bool,"
                  "c_int int,"
                  "c_float float,"
                  "c_double double,"
                  "c_ubigint bigint unsigned,"
                  "c_event int"
                  ") tags(tint int,tchar varchar(20));\n"
              )
              fsql.write(f"create table {ctable} using {stable} tags(0,'0');\n")
              fsql.write(f"insert into {ctable} select * from {vtable};\n\n")
  
      print("✔ stable_event.sql generated.")
  
  def performance_test_event():
      csv_result = os.path.join(output_dir, "benchmark_event_result.csv")
      with open(csv_result, "w", newline="") as f:
          writer = csv.writer(f)
          writer.writerow(["table", "type", "elapsed_s"])
  
          event_clause = (
              "EVENT_WINDOW "
              "START WITH (c_event = 1 OR c_event = 3) "
              "END WITH   (c_event = 2 OR c_event = 3)"
          )
  
          select_cols = (
              "_wstart,_wend,_wduration,"
              "count(c_bool),count(c_int),max(c_float),min(c_double),avg(c_ubigint)"
          )
  
          for table, win, row in table_defs:
              short = table.replace("t_event_", "")
  
              vst = f"vst_event_{short}"
              sql = f"select {select_cols} from test_vtable_perf.{vst} {event_clause};"
              elapsed = run_sql(sql)
              writer.writerow([vst, "vstable", elapsed])
              print(f"[VST-EVENT] {vst} default -> {elapsed}s")
  
       #       st = f"st_event_{short}"
       #       sql = f"select {select_cols} from test_vtable_perf.{st} {event_clause};"
       #       elapsed = run_sql(sql)
       #       writer.writerow([st, "stable", elapsed])
       #       print(f"[ST-EVENT] {st} default -> {elapsed}s")
  
      print(f"\n✔ benchmark_event_result.csv saved to {csv_result}")
  
  def main():
      os.makedirs(output_dir, exist_ok=True)
  
      print(f"Using start_ts={start_ts}, end_ts={end_ts}, time_range={time_range}ms (points={ROW_SCALE})")
      print("🏁 Generating EVENT CSV and SQL files...")
      generate_event_csv_load_sql()
      generate_vstable_vtable_event_sql()
      generate_stable_event_sql()
  
      print("\n🏁 Executing SQL files...")
      execute_sql_file(os.path.join(output_dir, "load_all_event.sql"))
      execute_sql_file(os.path.join(output_dir, "vstable_vtable_event.sql"))
      execute_sql_file(os.path.join(output_dir, "stable_event.sql"))
  
      print("\n🏁 Start EVENT performance test (stable + virtual stable only, default only)...")
      performance_test_event()
  
      print("\n🎉 All done. Check output_event/ for SQL, CSV, and benchmark_event_result.csv")
  
  if __name__ == "__main__":
      main()
  ```

### 4. 测试步骤

1. 使用 taosBenchmark + 2.1 中提供的 insert.json 生成原始表数据
2. 运行 2.2 中提供的测试脚本 test_performance.py, 得到测试结果 benchmark_result.csv

### 5. 测试结果

1. 输出结果：
   - 优化后:
    ```sql {wrap}
    [VST-EVENT] vst_event_1_win_1_row default -> 0.047679s 
    [ST-EVENT] st_event_1_win_1_row default -> 0.834085s 
    [VST-EVENT] vst_event_1_win_10_row default -> 0.049368s 
    [ST-EVENT] st_event_1_win_10_row default -> 0.839462s 
    [VST-EVENT] vst_event_1_win_100_row default -> 0.05179s 
    [ST-EVENT] st_event_1_win_100_row default -> 0.86594s 
    [VST-EVENT] vst_event_1_win_1000_row default -> 0.059491s 
    [ST-EVENT] st_event_1_win_1000_row default -> 0.932147s 
    [VST-EVENT] vst_event_1_win_10000_row default -> 0.053674s 
    [ST-EVENT] st_event_1_win_10000_row default -> 1.006032s 
    [VST-EVENT] vst_event_1_win_100000_row default -> 0.092736s 
    [ST-EVENT] st_event_1_win_100000_row default -> 0.87807s 
    [VST-EVENT] vst_event_1_win_1000000_row default -> 0.477207s 
    [ST-EVENT] st_event_1_win_1000000_row default -> 0.91886s 
    [VST-EVENT] vst_event_10_win_10_row default -> 0.051655s 
    [ST-EVENT] st_event_10_win_10_row default -> 0.8476s 
    [VST-EVENT] vst_event_10_win_100_row default -> 0.050397s 
    [ST-EVENT] st_event_10_win_100_row default -> 0.874148s 
    [VST-EVENT] vst_event_10_win_1000_row default -> 0.060699s 
    [ST-EVENT] st_event_10_win_1000_row default -> 0.920588s 
    [VST-EVENT] vst_event_10_win_10000_row default -> 0.058728s 
    [ST-EVENT] st_event_10_win_10000_row default -> 0.877699s 
    [VST-EVENT] vst_event_10_win_100000_row default -> 0.095427s 
    [ST-EVENT] st_event_10_win_100000_row default -> 1.6166s 
    [VST-EVENT] vst_event_10_win_1000000_row default -> 0.466623s 
    [ST-EVENT] st_event_10_win_1000000_row default -> 0.886554s 
    [VST-EVENT] vst_event_100_win_100_row default -> 0.049479s 
    [ST-EVENT] st_event_100_win_100_row default -> 1.067916s 
    [VST-EVENT] vst_event_100_win_1000_row default -> 0.053708s 
    [ST-EVENT] st_event_100_win_1000_row default -> 0.878217s 
    [VST-EVENT] vst_event_100_win_10000_row default -> 0.068601s 
    [ST-EVENT] st_event_100_win_10000_row default -> 1.471339s 
    [VST-EVENT] vst_event_100_win_100000_row default -> 0.112216s 
    [ST-EVENT] st_event_100_win_100000_row default -> 0.862597s 
    [VST-EVENT] vst_event_100_win_1000000_row default -> 0.472743s 
    [ST-EVENT] st_event_100_win_1000000_row default -> 0.887622s 
    [VST-EVENT] vst_event_1000_win_1000_row default -> 0.082205s 
    [ST-EVENT] st_event_1000_win_1000_row default -> 0.902774s 
    [VST-EVENT] vst_event_1000_win_10000_row default -> 0.083714s 
    [ST-EVENT] st_event_1000_win_10000_row default -> 0.861153s 
    [VST-EVENT] vst_event_1000_win_100000_row default -> 0.141632s 
    [ST-EVENT] st_event_1000_win_100000_row default -> 0.782261s 
    [VST-EVENT] vst_event_1000_win_1000000_row default -> 0.485174s 
    [ST-EVENT] st_event_1000_win_1000000_row default -> 0.891515s 
    [VST-EVENT] vst_event_10000_win_10000_row default -> 0.329112s 
    [ST-EVENT] st_event_10000_win_10000_row default -> 1.174367s 
    [VST-EVENT] vst_event_10000_win_100000_row default -> 0.38863s 
    [ST-EVENT] st_event_10000_win_100000_row default -> 2.137272s 
    [VST-EVENT] vst_event_10000_win_1000000_row default -> 0.713231s 
    [ST-EVENT] st_event_10000_win_1000000_row default -> 1.949182s 
    [VST-EVENT] vst_event_100000_win_100000_row default -> 1.77976s 
    [ST-EVENT] st_event_100000_win_100000_row default -> 1.05945s 
    [VST-EVENT] vst_event_100000_win_1000000_row default -> 2.186189s 
    [ST-EVENT] st_event_100000_win_1000000_row default -> 1.136232s 
    [VST-EVENT] vst_event_1000000_win_1000000_row default -> 17.527427s 
    [ST-EVENT] st_event_1000000_win_1000000_row default -> 11.155836s
    ```

   - 优化前:
    ```sql {wrap}
    [VST-EVENT] vst_event_1_win_1_row default -> 6.763891s 
    [VST-EVENT] vst_event_1_win_10_row default -> 7.631449s 
    [VST-EVENT] vst_event_1_win_100_row default -> 7.574875s 
    [VST-EVENT] vst_event_1_win_1000_row default -> 7.577721s
    [VST-EVENT] vst_event_1_win_10000_row default -> 7.751756s 
    [VST-EVENT] vst_event_1_win_100000_row default -> 7.638375s 
    [VST-EVENT] vst_event_1_win_1000000_row default -> 7.968963s 
    [VST-EVENT] vst_event_10_win_10_row default -> 7.834109s 
    [VST-EVENT] vst_event_10_win_100_row default -> 7.650583s 
    [VST-EVENT] vst_event_10_win_1000_row default -> 7.851172s 
    [VST-EVENT] vst_event_10_win_10000_row default -> 7.655838s 
    [VST-EVENT] vst_event_10_win_100000_row default -> 7.655719s 
    [VST-EVENT] vst_event_10_win_1000000_row default -> 7.820531s 
    [VST-EVENT] vst_event_100_win_100_row default -> 7.873696s 
    [VST-EVENT] vst_event_100_win_1000_row default -> 7.90608s 
    [VST-EVENT] vst_event_100_win_10000_row default -> 7.754127s 
    [VST-EVENT] vst_event_100_win_100000_row default -> 7.80273s 
    [VST-EVENT] vst_event_100_win_1000000_row default -> 7.840618s 
    [VST-EVENT] vst_event_1000_win_1000_row default -> 7.824808s 
    [VST-EVENT] vst_event_1000_win_10000_row default -> 7.808912s 
    [VST-EVENT] vst_event_1000_win_100000_row default -> 7.948702s 
    [VST-EVENT] vst_event_1000_win_1000000_row default -> 8.016321s 
    [VST-EVENT] vst_event_10000_win_10000_row default -> 8.023509s 
    [VST-EVENT] vst_event_10000_win_100000_row default -> 8.005123s 
    [VST-EVENT] vst_event_10000_win_1000000_row default -> 8.000532s 
    [VST-EVENT] vst_event_100000_win_100000_row default -> 8.179312s 
    [VST-EVENT] vst_event_100000_win_1000000_row default -> 8.662608s 
    [VST-EVENT] vst_event_1000000_win_1000000_row default -> 21.870949s
    ```

1. 结果汇总：
  单位: 秒

  | win | row | 超级表(st) | 虚拟超级表(优化前) | 虚拟超级表(优化后) | 性能差距（优化后/优化前） | 性能差距（优化后/非虚拟表） |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 0.834 | 6.764 | 0.048 | 141.863 | 17.494 |
| 10 | 0.839 | 7.631 | 0.049 | 154.583 | 17.004 |
| 100 | 0.866 | 7.575 | 0.052 | 146.261 | 16.72 |
| 1000 | 0.932 | 7.578 | 0.059 | 127.376 | 15.669 |
| 10000 | 1.006 | 7.752 | 0.054 | 144.423 | 18.743 |
| 100000 | 0.878 | 7.638 | 0.093 | 82.367 | 9.469 |
| 1000000 | 0.919 | 7.969 | 0.477 | 16.699 | 1.925 |
| 10 | 0.848 | 7.834 | 0.052 | 151.662 | 16.407 |
| 100 | 0.874 | 7.651 | 0.05 | 151.806 | 17.344 |
| 1000 | 0.921 | 7.851 | 0.061 | 129.346 | 15.166 |
| 10000 | 0.878 | 7.656 | 0.059 | 130.351 | 14.945 |
| 100000 | 1.617 | 7.656 | 0.095 | 80.227 | 16.941 |
| 1000000 | 0.887 | 7.821 | 0.467 | 16.762 | 1.9 |
| 100 | 1.068 | 7.874 | 0.049 | 159.132 | 21.584 |
| 1000 | 0.878 | 7.906 | 0.054 | 147.203 | 16.353 |
| 10000 | 1.471 | 7.754 | 0.069 | 113.031 | 21.449 |
| 100000 | 0.863 | 7.803 | 0.112 | 69.529 | 7.687 |
| 1000000 | 0.888 | 7.841 | 0.473 | 16.583 | 1.878 |
| 1000 | 0.903 | 7.825 | 0.082 | 95.187 | 10.981 |
| 10000 | 0.861 | 7.809 | 0.084 | 93.282 | 10.286 |
| 100000 | 0.782 | 7.949 | 0.142 | 56.125 | 5.523 |
| 1000000 | 0.892 | 8.016 | 0.485 | 16.521 | 1.837 |
| 10000 | 1.174 | 8.024 | 0.329 | 24.379 | 3.569 |
| 100000 | 2.137 | 8.005 | 0.389 | 20.603 | 5.5 |
| 1000000 | 1.949 | 8.001 | 0.713 | 11.215 | 2.732 |
| 100000 | 1.059 | 8.179 | 1.78 | 4.596 | 0.595 |
| 1000000 | 1.136 | 8.663 | 2.186 | 3.962 | 0.52 |
| 1000000 | 1000000 | 11.156 | 21.871 | 17.527 | 1.248 | 0.636 |


### 6. 测试总结

- 优化**提升的倍数**与 **窗口数 **以及 **窗口内的数据量** 成 **负相关** 的关系。
- 优化后虚拟超级表的性能 **普遍优于优化前**：提升倍数范围约 **1.25x ~ 159.13x**
- 优化后虚拟超级表的性能在**大部分场景下优于超级表查询，性能差距在 1.8x ~ 21.5x， **当 win 数量特别多，且每个窗口内数据很少时，虚拟超级表的性能劣于超级表查询。
