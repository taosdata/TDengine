# TS-5485 开启 TSMA 时写入性能验证

#### 1. 环境准备 {folded="true"}

1. CPU 信息：
  ```yaml
  lscpu
  
  Architecture:             x86_64
    CPU op-mode(s):         32-bit, 64-bit
    Address sizes:          40 bits physical, 48 bits virtual
    Byte Order:             Little Endian
  CPU(s):                   8
    On-line CPU(s) list:    0-7
  Vendor ID:                GenuineIntel
    Model name:             QEMU Virtual CPU version 2.5+
      CPU family:           15
      Model:                107
      Thread(s) per core:   1
      Core(s) per socket:   8
      Socket(s):            1
      Stepping:             1
      BogoMIPS:             4599.99
      Flags:                fpu de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx l
                            m constant_tsc nopl xtopology cpuid tsc_known_freq pni ssse3 cx16 sse4_1 sse4_2 x2apic popcnt aes hypervisor
                            lahf_lm cpuid_fault pti
  Virtualization features:
    Hypervisor vendor:      KVM
    Virtualization type:    full
  Caches (sum of all):
    L1d:                    256 KiB (8 instances)
    L1i:                    256 KiB (8 instances)
    L2:                     32 MiB (8 instances)
    L3:                     16 MiB (1 instance)
  NUMA:
    NUMA node(s):           1
    NUMA node0 CPU(s):      0-7
  ```

1. 内存信息：
  ```sql
  free -h
                 total        used        free      shared  buff/cache   available
  Mem:            31Gi       1.7Gi       6.3Gi       1.0Mi        23Gi        29Gi
  Swap:          3.8Gi        28Mi       3.8Gi
  ```

1. 磁盘信息：
  ```bash
  lsblk
  
  NAME                      MAJ:MIN RM   SIZE RO TYPE MOUNTPOINTS
  loop0                       7:0    0  63.8M  1 loop /snap/core20/2599
  loop1                       7:1    0  63.8M  1 loop /snap/core20/2669
  loop2                       7:2    0   1.7M  1 loop /snap/jump/81
  loop3                       7:3    0    87M  1 loop /snap/lxd/29351
  loop4                       7:4    0  89.4M  1 loop /snap/lxd/31333
  loop5                       7:5    0  49.3M  1 loop /snap/snapd/24792
  loop6                       7:6    0  50.8M  1 loop /snap/snapd/25202
  sda                         8:0    0   300G  0 disk
  ├─sda1                      8:1    0     1M  0 part
  ├─sda2                      8:2    0   1.8G  0 part /boot
  └─sda3                      8:3    0 298.2G  0 part
    └─ubuntu--vg-ubuntu--lv 253:0    0 298.2G  0 lvm  /
  sr0                        11:0    1     4M  0 rom
  sr1                        11:1    1     2G  0 rom
  ```

1. 操作系统信息：
  ```sql
  uname -a
  Linux smj-test 5.15.0-141-generic #151-Ubuntu SMP Sun May 18 21:35:19 UTC 2025 x86_64 x86_64 x86_64 GNU/Linux
  ```

#### 2. 数据准备 {folded="true"}

1. insert.json
```json
{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "thread_count": 4,
    "create_table_thread_count": 4,
    "result_file": "./insert_res.txt",
    "confirm_parameter_prompt": "no",
    "num_of_records_per_req": 10000,
    "prepared_rand": 10000,
    "chinese": "no",
    "escape_character": "yes",
    "continue_if_fail": "no",
    "databases": [
        {
            "dbinfo": {
                "name": "test",
                "drop": "yes",
                "vgroups": 4,
                "precision": "ms"
            },
            "super_tables": [
                {
                    "name": "meters",
                    "child_table_exists": "no",
                    "childtable_count": 10000,
                    "childtable_prefix": "d",
                    "auto_create_table": "no",
                    "batch_create_tbl_num": 5,
                    "data_source": "rand",
                    "insert_mode": "taosc",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 10000,
                    "childtable_limit": 0,
                    "childtable_offset": 0,
                    "interlace_rows": 0,
                    "insert_interval": 0,
                    "partial_col_num": 0,
                    "timestamp_step": 1000,
                    "start_timestamp": "2020-10-01 00:00:00.000",
                    "sample_format": "csv",
                    "sample_file": "./sample.csv",
                    "use_sample_ts": "no",
                    "tags_file": "",
                    "columns": [
                        {"type": "FLOAT", "name": "current", "count": 1, "max": 12, "min": 8 },
                        { "type": "INT", "name": "voltage", "max": 225, "min": 215 },
                        { "type": "FLOAT", "name": "phase", "max": 1, "min": 0 }
                    ],
                    "tags": [
                        {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1},
                        {"type": "BINARY",  "name": "location", "len": 16,
                            "values": ["San Francisco", "Los Angles", "San Diego",
                                "San Jose", "Palo Alto", "Campbell", "Mountain View",
                                "Sunnyvale", "Santa Clara", "Cupertino"]
                        }
                    ]
                }
            ]
        }
    ]
}

```

1. insert2.json
```json
{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "thread_count": 4,
    "create_table_thread_count": 4,
    "result_file": "./insert_res.txt",
    "confirm_parameter_prompt": "no",
    "num_of_records_per_req": 10000,
    "prepared_rand": 10000,
    "chinese": "no",
    "escape_character": "yes",
    "continue_if_fail": "no",
    "databases": [
        {
            "dbinfo": {
                "name": "test",
                "drop": "no",
                "vgroups": 4,
                "precision": "ms"
            },
            "super_tables": [
                {
                    "name": "meters",
                    "child_table_exists": "no",
                    "childtable_count": 10000,
                    "childtable_prefix": "d",
                    "auto_create_table": "no",
                    "batch_create_tbl_num": 5,
                    "data_source": "rand",
                    "insert_mode": "taosc",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 10000,
                    "childtable_limit": 0,
                    "childtable_offset": 0,
                    "interlace_rows": 0,
                    "insert_interval": 0,
                    "partial_col_num": 0,
                    "timestamp_step": 1000,
                    "start_timestamp": "2021-10-01 00:00:00.000",
                    "sample_format": "csv",
                    "sample_file": "./sample.csv",
                    "use_sample_ts": "no",
                    "tags_file": "",
                    "columns": [
                        {"type": "FLOAT", "name": "current", "count": 1, "max": 12, "min": 8 },
                        { "type": "INT", "name": "voltage", "max": 225, "min": 215 },
                        { "type": "FLOAT", "name": "phase", "max": 1, "min": 0 }
                    ],
                    "tags": [
                        {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1},
                        {"type": "BINARY",  "name": "location", "len": 16,
                            "values": ["San Francisco", "Los Angles", "San Diego",
                                "San Jose", "Palo Alto", "Campbell", "Mountain View",
                                "Sunnyvale", "Santa Clara", "Cupertino"]
                        }
                    ]
                }
            ]
        }
    ]
}
```

#### 3. 测试步骤

1. 使用步骤 1 中提供的 insert.json 文件，执行命令，并记录写入速率：
```sql
taosBenchmark -f insert.json
```

1. 创建完毕后，在 taosc 中执行：
其中，{tsma_name} 可以根据不同的测试使用不同的值
```sql
create tsma tsma1 on test.meters function(avg(current), avg(voltage), avg(phase)) interval(5m);
```

1. Tsma 创建成功后，使用步骤 1 中提供的 insert2.json, 执行命令，并记录写入速率：
```shell
taosBenchmark -f insert2.json
```

1. 使用如下 python 脚本监控运行中 cpu / 内存用量
  ```shell
  python3 thread_monitor.py <PID> -i <frequency> -d <duration>
  ```

  ```python
  import argparse
  import time
  import psutil
  from collections import defaultdict
  
  def get_thread_name(pid, tid):
      """获取线程名，如果线程已退出返回 None"""
      try:
          with open(f"/proc/{pid}/task/{tid}/status") as f:
              for line in f:
                  if line.startswith("Name:"):
                      return line.split()[1].strip()
      except (FileNotFoundError, ProcessLookupError):
          return None
      return None
  
  def sample_process(pid):
      """采样线程 CPU/内存，如果线程已退出则忽略"""
      try:
          p = psutil.Process(pid)
      except psutil.NoSuchProcess:
          return None
  
      stats = {}
      for t in p.threads():
          tid = t.id
          tname = get_thread_name(pid, tid) or str(tid)
          stats[tid] = {
              "name": tname,
              "cpu_time": t.user_time + t.system_time,
              "rss": 0,
          }
          # 内存 VmRSS
          try:
              with open(f"/proc/{pid}/task/{tid}/status") as f:
                  for line in f:
                      if line.startswith("VmRSS:"):
                          stats[tid]["rss"] = int(line.split()[1]) * 1024  # 转为字节
                          break
          except (FileNotFoundError, ProcessLookupError):
              stats[tid]["rss"] = 0
      return stats
  
  def monitor_average(pid, interval, duration):
      try:
          before = sample_process(pid)
          if before is None:
              print(f"进程 {pid} 不存在或已结束")
              return
      except psutil.NoSuchProcess:
          print(f"进程 {pid} 不存在或已结束")
          return
  
      samples = int(duration / interval)
      rss_accumulator = defaultdict(list)
  
      # 周期性采样内存
      for _ in range(samples):
          time.sleep(interval)
          after_sample = sample_process(pid)
          if after_sample is None:
              print(f"进程 {pid} 已结束")
              break
          for tid, s in after_sample.items():
              rss_accumulator[s["name"]].append(s["rss"])
  
      # 采样结束后计算 CPU 差值
      after = sample_process(pid)
      if after is None:
          print(f"进程 {pid} 已结束")
          return
  
      agg = defaultdict(lambda: {"cpu": 0.0, "rss": 0.0})
      for tid, a in after.items():
          if tid not in before:
              continue
          b = before[tid]
          tname = a["name"]
          cpu_diff = a["cpu_time"] - b["cpu_time"]
          cpu_pct = (cpu_diff / duration) * 100.0  # 单核百分比
          agg[tname]["cpu"] += cpu_pct
  
      # 平均内存
      for tname, rss_list in rss_accumulator.items():
          if rss_list:
              agg[tname]["rss"] = sum(rss_list) / len(rss_list)
  
      # 输出
      print(f"\n[{time.strftime('%H:%M:%S')}] 平均结果 (总时长 {duration}s):")
      print(f"{'线程名':20} {'AvgCPU%':>10} {'AvgRSS(MB)':>12}")
      for name, data in agg.items():
          print(f"{name:20} {data['cpu']:10.2f} {data['rss']/1024/1024:12.2f}")
  
  if __name__ == "__main__":
      parser = argparse.ArgumentParser(description="按线程名统计平均CPU/内存")
      parser.add_argument("pid", type=int, help="进程 PID")
      parser.add_argument("-i", "--interval", type=float, default=1.0, help="采样间隔 (秒)")
      parser.add_argument("-d", "--duration", type=float, default=10.0, help="总时长 (秒)")
      args = parser.parse_args()
  
      monitor_average(args.pid, args.interval, args.duration)
  
  ```

#### 4. 测试结果

1. 第一次调用 insert.json 时的写入速率：

  | 写入速率(record/s) |  |
| --- | --- |
| 1049958 | ![](./images/img_R3EFbvDaaoUwMLxphgmcCkbjnFh.png) |
| 870406 | ![](./images/img_QYlUbgnPGo0NPTxaiJecdWZtn4b.png) |
| 840172 | ![](./images/img_XrMsbYWv8oJg5nxgs6RciRc2nif.png) |
| 平均：920178 |  |

1. 不创建 tsma，直接使用 insert2.json 时的写入速率：

  | 写入速率(record/s) |  |
| --- | --- |
| 796459 | ![](./images/img_XJD9bzP7Ho6qsNxNwvCcube1n5e.png) |
| 766673 | ![](./images/img_G85ib2XsooGlqUxzmjyc2Pfdnaf.png) |
| 770722 | ![](./images/img_TpQobIog2ozFHpxH9hQcYvpLnjc.png) |
| 平均：777951 |  |

   - 流计算资源消耗情况：
  无
   - 写入线程资源消耗：

    | 线程名 | AvgCPU% | AvgRSS（MB） |
| --- | --- | --- |
| vnode-write | 22.90 | 1234.44 |
| vnode-commit | 55.84 | 1234.49 |
| vnode-merge | 81.23 | 1234.49 |

1. 创建 1 个 tsma， 使用 insert2.json 时：
   - 写入速率

    | 写入速率(record/s) |  |
| --- | --- |
| 697576 | ![](./images/img_Tm3TbmG9jo0s4AxehiZcoQsJnzS.png) |
| 655394 | ![](./images/img_UE5kb2u9zojYl7xxCPAcNgJpnAc.png) |
| 602239 | ![](./images/img_VV99bzX6hog3y3x4dxAcgu9InBf.png) |
| 平均：651736 |  |

   - 流计算资源消耗情况：

    | 线程名 | AvgCPU% | AvgRSS(MB) |
| --- | --- | --- |
| vnode-st-reader | 250.85 | 2315.29 |
| snode-stream-ru | 55.15 | 2333.68 |
| snode-stream-tr | 3.71 | 2315.54 |

   - 写入线程资源消耗：

    | 线程名 | AvgCPU% |
| --- | --- |
| vnode-write | 17.17 |
| vnode-commit | 38.77 |
| vnode-merge | 59.43 |

1. 创建 3 个 tsma， 使用 insert2.json 时：
   - 写入速率

    | 写入速率(record/s) |  |
| --- | --- |
| 655396 | ![](./images/img_VwkYbUyvjoYFhdxsw6Vcbox4nwc.png) |
| 652013 | ![](./images/img_KAKhbupi7o2jxbxlAREcgNUin8f.png) |
| 596883 | ![](./images/img_LHIfbKbdhoMpS8xM5wQcUJLSntc.png) |
| 平均：634764 |  |

   - 流计算资源消耗情况：

    | 线程名 | AvgCPU% | AvgRSS(MB) |
| --- | --- | --- |
| vnode-st-reader | 242.31 | 3276.57 |
| snode-stream-ru | 19.29 | 3506.11 |
| snode-stream-tr | 13.56 | 3276.02 |

   - 写入线程资源消耗：

    | 线程名 | AvgCPU% |
| --- | --- |
| vnode-write | 16.82 |
| vnode-commit | 38.47 |
| vnode-merge | 55.80 |

1. 创建 5 个 tsma， 使用 insert2.json 时：
   - 写入速率：

    | 写入速率(record/s) |  |
| --- | --- |
| 591776 | ![](./images/img_QgIebUbStofrsGxG0GTcunFBnZf.png) |
| 589223 | ![](./images/img_MmLVbojOAoQQXHxJEfPc9LK1nJd.png) |
| 635367 | ![](./images/img_CKU9biP8HogbELxMag3c9r58nDb.png) |
| 平均：605455 |  |

   - 流计算资源消耗情况：

    | 线程名 | AvgCPU% | AvgRSS(MB) |
| --- | --- | --- |
| vnode-st-reader | 229.42 | 4036.55 |
| snode-stream-ru | 5.41 | 4504.42 |
| snode-stream-tr | 21.87 | 4036.55 |

   - 写入线程资源消耗：

    | 线程名 | AvgCPU% |
| --- | --- |
| vnode-write | 14.44 |
| vnode-commit | 28.83 |
| vnode-merge | 52.79 |

1. 不创建 tsma，且使用 insert2.json 写入非 tsma 所在库时：
   - 写入速率：

    | 写入速率(record/s) |  |
| --- | --- |
| 719990 | ![](./images/img_GXn4bpRKtoL12DxpQ2Xcuuoznpb.png) |
| 730887 | ![](./images/img_NRrgbd680oCG9bxshLgcA4vWnfc.png) |
| 704895 | ![](./images/img_WByabOxqooRpEIxrTBmc8fmlnnc.png) |
| 平均：718590 |  |

   - 写入线程资源消耗：

    | 线程名 | AvgCPU% |
| --- | --- |
| vnode-write | 17.88 |
| vnode-commit | 42.61 |
| vnode-merge | 60.16 |

1. 创建 1 个 tsma，且使用 insert2.json 写入非 tsma 所在库时：
   - 写入速率：

    | 写入速率(record/s) |  |
| --- | --- |
| 655663 | ![](./images/img_E9pnbSOOTons9cxpI54cQMGVnVb.png) |
| 662185 | ![](./images/img_FoqTbEVGhoR0Kvx9jxUcmNpInMg.png) |
| 616370 | ![](./images/img_BzaGbcLQzogeQxxiHLXcROsonic.png) |
| 平均：644739 |  |

   - 流计算资源消耗情况：

    | 线程名 | AvgCPU% |
| --- | --- |
| vnode-st-reader | 299.82 |
| snode-stream-ru | 12.02 |
| snode-stream-tr | 0.15 |

   - 写入线程资源消耗：

    | 线程名 | AvgCPU% |
| --- | --- |
| vnode-write | 14.65 |
| vnode-commit | 36.40 |
| vnode-merge | 53.30 |

#### 5. 结果分析及总结

1. 写入速率数据（record/s）
   - TSMA 所在库和写入数据是同一个库

    | **TSMA数量** | **写入速率 1** | **写入速率 2** | **写入速率 3** | **平均速率** | **相比无 TSMA 的下降幅度** |
| --- | --- | --- | --- | --- | --- |
| 0 | 796459 | 766673 | 770722 | 777951 | -（基准） |
| 1 | 697576 | 655394 | 602239 | 651736 | 16.6% |
| 3 | 655396 | 652013 | 596883 | 634764 | 18.8% |
| 5 | 591776 | 589223 | 635367 | 605455 | 22.5% |

   - TSMA 所在库和写入数据是不同的库

    | **TSMA数量** | **写入速率 1** | **写入速率 2** | **写入速率 3** | **平均速率** | **相比无 TSMA 的下降幅度** |
| --- | --- | --- | --- | --- | --- |
| 0 | 719990 | 730887 | 704895 | 718590 | -（基准） |
| 1 | 655663 | 662185 | 616370 | 644739 | 11.3% |

1. CPU 使用率

  | **TSMA数量** | 流计算线程 CPU% (reader + runner + trigger) | 写入线程 CPU% |
| --- | --- | --- |
| 0 | - | 159.97 |
| 1 | 309.71 | 115.37 |
| 3 | 275.16 | 111.09 |
| 5 | 256.70 | 96.06 |

1. taosd 进程内存占用(MB)

  | **TSMA数量** | 内存占用(MB) |
| --- | --- |
| 0 | 1234 |
| 1 | 2315 |
| 3 | 3276 |
| 5 | 4036 |

1. 总结
   - 开启 TSMA 会降低写入的性能，第一个 TSMA 带来的性能损失最大，后续 TSMA 平均每个会带来 1% - 2% 的写入性能下降。但是下降幅度符合预期（写入性能下降在一倍以内）。如果写入的库不是 TSMA 所在的库，性能也会受到影响，但是性能影响幅度略小。
   - 每个 TSMA 约消耗 500-600 MB 内存，增长接近线性。
