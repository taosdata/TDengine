# [6492510964] 虚拟超级表窗口查询性能优化测试(State window)

### 1. 背景

1. 需求文档：[虚拟表窗口计算性能优化 RS](https://taosdata.feishu.cn/wiki/FKzWwNibkic1SRkLPmNcD1kXnEf)
2. 第一次优化性能对比测试：[TS-7132 虚拟表窗口计算性能优化测试](https://taosdata.feishu.cn/wiki/FxAnwvXrniaiOmkvyNkcvr2snmf)

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
  虚拟表共 6 列，其中每一列都来自不同的原始表。前五列通过 taosBenchmark 生成，每列 1,000,000 条数据。第六列为 state 列，为手工构造的数据集。
  对 state 列构造了如下几种数据集：
   - 窗口数据分布密集，窗口数从 1 到 1,000,000 不等，每个窗口的数据也从 1 到 1,000,000 不等，窗口数 * 每个窗口内数据 <= 1,000,000。
    - 此数据集窗口的数据分布集中，所有窗口的数据都是连在一起的。
    - 此数据集用来模拟窗口的分布比较集中的情况，并测试此情况下不同数据量的性能影响。
   - 窗口数据分布均匀，窗口数从 1 到 1,000,000 不等，每个窗口的数据也从 1 到 1,000,000 不等，窗口数 * 每个窗口内数据 <= 1,000,000。
    - 此数据集窗口的数据分布比较均匀，第一个窗口的第一条数据到最后一个窗口的最后一条数据的时间范围基本等同于虚拟表的时间范围。
    - 比如生成 10000 条 state 列数据，他们的时间戳的差值是一样的，并且差值为表的 (end_ts - start_ts) / 10000。
    - 此数据集用来模拟窗口的分布比较均匀的情况，并测试此情况下不同数据量的性能影响。
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
            "insert_rows": 10000000,
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
            "insert_rows": 10000000,
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
            "insert_rows": 10000000,
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
            "insert_rows": 10000000,
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
            "insert_rows": 10000000,
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
  
  # ============================================
  # 时间范围
  # ============================================
  start_ts = 1601481600000
  end_ts = 1601482600000
  time_range = end_ts - start_ts
  
  # ============================================
  # 输出目录
  # ============================================
  output_dir = "output"
  csv_dir = os.path.join(output_dir, "csv")
  os.makedirs(csv_dir, exist_ok=True)
  abs_csv_dir = os.path.abspath(csv_dir)
  os.makedirs(abs_csv_dir, exist_ok=True)
  
  # ============================================
  # win 和 row 组合生成表定义（带下划线）
  # ============================================
  win_options = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
  row_options = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]
  
  table_defs = []
  for win in win_options:
      for row in row_options:
          if row >= win:
              table_defs.append((f"t_state_{win}_win_{row}_row_dense", win, row, True))
              table_defs.append((f"t_state_{win}_win_{row}_row_sparse", win, row, False))
  
  # ============================================
  # 执行 taos SQL
  # ============================================
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
          m = re.search(r'\(([\d\.]+)s\)', out)
          if m:
              return float(m.group(1))
          return None
      except subprocess.TimeoutExpired:
          return None
  
  # ============================================
  # 执行 SQL 文件
  # ============================================
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
  
  # ============================================
  # 生成 CSV + load_all.sql
  # ============================================
  def generate_csv_load_sql():
      load_sql_path = os.path.join(output_dir, "load_all.sql")
      with open(load_sql_path, "w") as fsql:
          fsql.write("create database if not exists test_vtable_perf_state;\n")
          fsql.write("use test_vtable_perf_state;\n\n")
          for table, win, row, dense in table_defs:
              fsql.write(f"create table if not exists {table} (ts timestamp, c_state bool);\n\n")
  
          for table, win, row, dense in table_defs:
              csv_path = os.path.join(csv_dir, f"{table}.csv")
              abs_path = os.path.join(abs_csv_dir, f"{table}.csv")
              with open(csv_path, "w", newline="") as f:
                  writer = csv.writer(f)
                  writer.writerow(["ts", "c_state"])
  
                  rows_per_window = row // win
                  extra_rows = row % win
  
                  for i in range(row):
                      window_idx = i // rows_per_window
                      # 对最后一段余数处理
                      if window_idx >= win:
                          window_idx = win - 1
                      c_state = "true" if (window_idx % 2 == 0) else "false"
  
                      if dense:
                          ts = start_ts + i
                      else:
                          step = time_range / (row - 1) if row > 1 else 0
                          ts = int(start_ts + step * i)
  
                      writer.writerow([ts, c_state])
  
              fsql.write(f"insert into {table} file '{abs_path}';\n")
      print("✔ load_all.sql generated.")
  
  # ============================================
  # 生成 stable + vtable SQL
  # ============================================
  def generate_stable_vtable_sql():
      sql_path = os.path.join(output_dir, "stable_vtable.sql")
      with open(sql_path, "w") as fsql:
          fsql.write("create database if not exists test_vtable_perf;\n")
          fsql.write("use test_vtable_perf;\n\n")
          for table, win, row, dense in table_defs:
              short = table.replace("t_state_", "")
              stable = f"vst_table_{short}"
              fsql.write(
                  f"create stable {stable} (ts timestamp,c_bool bool,c_int int,c_float float,"
                  "c_double double,c_ubigint bigint unsigned,c_state bool)"
                  " tags(tint int,tchar varchar(20)) virtual 1;\n"
              )
              vtable = f"vct_{short}_d0"
              fsql.write(
                  f"create vtable {vtable} (c_bool from ctb_bool0.bool_col,"
                  "c_int from ctb_int0.int_col,c_float from ctb_float0.float_col,"
                  "c_double from ctb_double0.double_col,c_ubigint from ctb_ubigint0.u_bigint_col,"
                  f"c_state from test_vtable_perf_state.{table}.c_state) using {stable} tags(0,'0');\n"
              )
      print("✔ stable_vtable.sql generated.")
  
  # ============================================
  # 生成 st_table + ct_table SQL
  # ============================================
  def generate_st_ct_sql():
      sql_path = os.path.join(output_dir, "st_ct.sql")
      with open(sql_path, "w") as fsql:
          fsql.write("create database if not exists test_vtable_perf;\n")
          fsql.write("use test_vtable_perf;\n\n")
          for table, win, row, dense in table_defs:
              short = table.replace("t_state_", "")
              stable = f"st_table_{short}"
              fsql.write(
                  f"create stable {stable} (ts timestamp,c_bool bool,c_int int,c_float float,"
                  "c_double double,c_ubigint bigint unsigned,c_state bool)"
                  " tags(tint int,tchar varchar(20));\n"
              )
              ctable = f"ct_{short}_d0"
              vtable = f"vct_{short}_d0"
              fsql.write(f"create table {ctable} using {stable} tags(0,'0');\n")
              fsql.write(f"insert into {ctable} select * from {vtable};\n")
      print("✔ st_ct.sql generated.")
  
  # ============================================
  # 性能测试
  # ============================================
  def performance_test():
      csv_result = os.path.join(output_dir, "benchmark_result.csv")
      with open(csv_result, "w", newline="") as f:
          writer = csv.writer(f)
          writer.writerow(["table", "type", "hint", "elapsed_s"])
          for table, win, row, dense in table_defs:
              short = table.replace("t_state_", "")
  
              # vst
              vst = f"vst_table_{short}"
              for hint in ["", "/*+ WIN_OPTIMIZE_BATCH() */"]:
                  sql = f"select _wstart,_wend,_wduration,c_state,count(c_bool),count(c_int),max(c_float),min(c_double),avg(c_ubigint) from test_vtable_perf.{vst} state_window(c_state);" if hint=="" else f"select {hint} _wstart,_wend,_wduration,c_state,count(c_bool),count(c_int),max(c_float),min(c_double),avg(c_ubigint) from test_vtable_perf.{vst} state_window(c_state);"
                  elapsed = run_sql(sql)
                  writer.writerow([vst,"stable", "optimize" if hint else "default", elapsed])
                  print(f"[VST] {vst} {hint or 'default'} -> {elapsed}s")
  
              # st
              st = f"st_table_{short}"
              sql = f"select _wstart,_wend,_wduration,c_state,count(c_bool),count(c_int),max(c_float),min(c_double),avg(c_ubigint) from test_vtable_perf.{st} state_window(c_state);"
              elapsed = run_sql(sql)
              writer.writerow([st,"st", "default", elapsed])
              print(f"[ST] {st} default -> {elapsed}s")
  
      print(f"\n✔ benchmark_result.csv saved to {csv_result}")
  
  # ============================================
  # 主入口
  # ============================================
  def main():
      os.makedirs(output_dir, exist_ok=True)
      print("🏁 Generating CSV and SQL files...")
      generate_csv_load_sql()
      generate_stable_vtable_sql()
      generate_st_ct_sql()
  
      # 自动执行 SQL 文件
      print("\n🏁 Executing SQL files...")
      execute_sql_file(os.path.join(output_dir, "load_all.sql"))
      execute_sql_file(os.path.join(output_dir, "stable_vtable.sql"))
      execute_sql_file(os.path.join(output_dir, "st_ct.sql"))
  
      print("\n🏁 Start performance test (vst/vct/st/ct)...")
      performance_test()
      print("\n🎉 All done. Check output/ directory for SQL, CSV, and benchmark_result.csv")
  
  if __name__ == "__main__":
      main()
  ```

### 4. 测试步骤

1. 使用 taosBenchmark + 2.1 中提供的 insert.json 生成原始表数据
2. 运行 2.2 中提供的测试脚本 test_performance.py, 得到测试结果 benchmark_result.csv

### 5. 测试结果

1. benchmark_result.csv 结果：
  <view type="2">

    > ⚠ 嵌入文件，需在飞书中查看 (token: PawWb733wooDJxxnQ8KcWwwrn3g)

  </view>

1. 结果汇总：
   - 数据分布均匀时：

    | win | row | 超级表 | 虚拟超级表（优化前） | 虚拟超级表（优化后） | 性能差距（优化后/优化前） | 性能差距（优化后/非虚拟表） |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 0.930218 | 10.197857 | 0.053678 | 189.982 | 17.33 |
| 10 | 0.726527 | 9.84918 | 0.489613 | 20.116 | 1.484 |
| 100 | 0.788488 | 10.06031 | 0.462328 | 21.76 | 1.705 |
| 1000 | 0.807012 | 10.107859 | 0.47927 | 21.09 | 1.684 |
| 10000 | 0.804032 | 9.997322 | 0.514354 | 19.437 | 1.563 |
| 100000 | 0.771174 | 10.28136 | 0.754682 | 13.623 | 1.022 |
| 1000000 | 0.790944 | 11.528146 | 3.432115 | 3.359 | 0.23 |
| 10 | 0.741187 | 10.065768 | 0.429529 | 23.434 | 1.726 |
| 100 | 0.749632 | 10.104404 | 0.483962 | 20.879 | 1.549 |
| 1000 | 0.801143 | 10.104917 | 0.501104 | 20.165 | 1.599 |
| 10000 | 0.770953 | 10.092223 | 0.492651 | 20.486 | 1.565 |
| 100000 | 0.763588 | 10.203319 | 0.702203 | 14.53 | 1.087 |
| 1000000 | 0.789283 | 11.49174 | 3.435733 | 3.345 | 0.23 |
| 100 | 0.774062 | 10.118185 | 0.441682 | 22.908 | 1.753 |
| 1000 | 0.806981 | 10.182604 | 0.48767 | 20.88 | 1.655 |
| 10000 | 0.768661 | 10.154181 | 0.485091 | 20.933 | 1.585 |
| 100000 | 0.762873 | 10.319932 | 0.729424 | 14.148 | 1.046 |
| 1000000 | 0.808644 | 11.51907 | 3.479254 | 3.311 | 0.232 |
| 1000 | 0.774004 | 10.225869 | 0.44904 | 22.773 | 1.724 |
| 10000 | 0.766379 | 10.157416 | 0.519784 | 19.542 | 1.474 |
| 100000 | 0.802125 | 10.272472 | 0.730633 | 14.06 | 1.098 |
| 1000000 | 0.816673 | 11.600371 | 3.4398 | 3.372 | 0.237 |
| 10000 | 0.883125 | 10.364749 | 0.686392 | 15.1 | 1.287 |
| 100000 | 0.894345 | 10.61481 | 1.046768 | 10.141 | 0.854 |
| 1000000 | 0.953849 | 12.469682 | 4.430778 | 2.814 | 0.215 |
| 100000 | 1.85218 | 11.642581 | 1.797683 | 6.476 | 1.03 |
| 1000000 | 1.982494 | 13.981909 | 5.272688 | 2.652 | 0.376 |
| 1000000 | 1000000 | 15.355558 | 27.728312 | 19.24844 | 1.441 | 0.798 |

   - 数据分布集中时：

    | win | row | 超级表 | 虚拟超级表（优化前） | 虚拟超级表（优化后） | 性能差距（优化后/优化前） | 性能差距（优化后/非虚拟表） |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 0.880001 | 10.283624 | 0.058597 | 175.497 | 15.018 |
| 10 | 0.851837 | 10.23126 | 0.049924 | 204.937 | 17.063 |
| 100 | 0.928012 | 10.148866 | 0.057327 | 177.035 | 16.188 |
| 1000 | 0.926427 | 10.222311 | 0.054198 | 188.61 | 17.093 |
| 10000 | 0.853158 | 10.163886 | 0.088633 | 114.674 | 9.626 |
| 100000 | 0.872569 | 10.291809 | 0.383728 | 26.821 | 2.274 |
| 1000000 | 0.785992 | 11.515425 | 3.45242 | 3.335 | 0.228 |
| 10 | 0.840865 | 10.214894 | 0.048797 | 209.334 | 17.232 |
| 100 | 0.914951 | 10.309222 | 0.05609 | 183.798 | 16.312 |
| 1000 | 1.462108 | 10.077271 | 0.043535 | 231.475 | 33.585 |
| 10000 | 0.923319 | 10.014316 | 0.096507 | 103.768 | 9.567 |
| 100000 | 0.892957 | 10.41376 | 0.373696 | 27.867 | 2.39 |
| 1000000 | 0.744478 | 11.53542 | 3.426369 | 3.367 | 0.217 |
| 100 | 1.204907 | 10.10774 | 0.047863 | 211.181 | 25.174 |
| 1000 | 0.888459 | 10.067688 | 0.053736 | 187.355 | 16.534 |
| 10000 | 0.901077 | 10.2139 | 0.093829 | 108.857 | 9.603 |
| 100000 | 0.830862 | 10.214807 | 0.386783 | 26.41 | 2.148 |
| 1000000 | 0.776217 | 11.559977 | 3.465454 | 3.336 | 0.224 |
| 1000 | 0.946248 | 10.109453 | 0.088539 | 114.181 | 10.687 |
| 10000 | 0.857591 | 10.227969 | 0.128701 | 79.471 | 6.663 |
| 100000 | 1.170322 | 10.378777 | 0.384857 | 26.968 | 3.041 |
| 1000000 | 0.881676 | 11.586747 | 3.487726 | 3.322 | 0.253 |
| 10000 | 1.183844 | 10.267924 | 0.335791 | 30.578 | 3.526 |
| 100000 | 1.046259 | 10.380954 | 0.568019 | 18.276 | 1.842 |
| 1000000 | 1.008183 | 12.41794 | 4.52747 | 2.743 | 0.223 |
| 100000 | 2.22338 | 11.644958 | 1.939977 | 6.003 | 1.146 |
| 1000000 | 2.051193 | 13.731957 | 5.417884 | 2.535 | 0.379 |
| 1000000 | 1000000 | 14.934684 | 27.997903 | 19.426133 | 1.441 | 0.769 |


### 6. 测试总结

1. 通用结论：
  - 优化**提升的倍数**与 **窗口数 **以及 **窗口内的数据量** 成 **负相关** 的关系。
  - 优化后的性能均好于优化前的性能。
  - 数据分布密集时优化的效果要好于数据分布均匀的场景。
1. 数据分布均匀时：
  - 优化后的查询性能对比优化前有** 1.4x - 189.9x **的提升。
  - 优化后的查询性能基本和非虚拟超级表的查询性能**持平**，只有在 **窗口内数据量** 占 **总数据量 **的比值超过 1/10 时，性能才开始**差于**非虚拟超级表的查询性能。
1. 数据分布密集时：
  - 优化后的查询性能对比优化前有** 1.4x - 231.4x **的提升。
  - 优化后的查询性能基本比非虚拟超级表的查询性能**更好**，只有在 **窗口内数据量** 占 **总数据量** 的比值超过 1/10 时，性能才开始**接近**非虚拟超级表的查询性能，并逐渐变差。在 **窗口内数据量 **等于 **总数据量 **时，优化后的查询性能最差。
