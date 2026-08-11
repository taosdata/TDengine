# 硬件故障排查手册：内存、硬盘、控制器

适用场景：数据库报出数据校验失败（如 `block checksum mismatch`、
`Data file corrupted`）、进程异常退出、或怀疑机器有静默数据损坏时，
用来定位到具体的硬件部件。

本文按**排查顺序**组织，每一节都给出可直接执行的命令和判读标准。

---

## 目录

- [0. 5 分钟快速筛查](#0-5-分钟快速筛查)
- [1. 内存排查](#1-内存排查)
- [2. 硬盘排查](#2-硬盘排查)
- [3. RAID 卡与控制器排查](#3-raid-卡与控制器排查)
- [4. 文件系统排查](#4-文件系统排查)
- [5. CPU 与主板排查](#5-cpu-与主板排查)
- [6. 用数据库文件做压力复现](#6-用数据库文件做压力复现)
- [7. 综合判读矩阵](#7-综合判读矩阵)
- [8. 命令速查表](#8-命令速查表)

---

## 0. 5 分钟快速筛查

先跑这一组，大部分情况能直接指向方向。全部是只读操作。

```bash
# ---- 1. 内核硬件错误（最高信息密度，优先看）----
dmesg -T | grep -iE "error|fail|mce|edac|hardware|medium|reset|timeout" | tail -50

# ---- 2. 跨重启的历史记录（进程反复重启时 dmesg 会被冲掉）----
journalctl -k --since "7 days ago" --no-pager \
    | grep -iE "i/o error|blk_update_request|medium error|edac|mce|hardware error"

# ---- 3. 内存 ECC 错误计数 ----
ras-mc-ctl --error-count 2>/dev/null || echo "未安装 rasdaemon"
edac-util -v 2>/dev/null || echo "未安装 edac-utils"

# ---- 4. 硬盘健康 ----
lsblk -o NAME,SIZE,TYPE,MOUNTPOINT,MODEL
for d in /dev/sd? /dev/nvme?n?; do
    [ -b "$d" ] && echo "=== $d ===" && smartctl -H "$d" 2>/dev/null | tail -3
done

# ---- 5. 硬件事件日志（BMC/IPMI，能看到 CPU/内存/电源的硬件告警）----
ipmitool sel elist 2>/dev/null | tail -30

# ---- 6. 块设备累计 I/O 错误 ----
grep -H . /sys/block/*/device/ioerr_cnt 2>/dev/null
```

**快速判读**：

| 看到什么 | 指向 |
|---|---|
| `blk_update_request: I/O error` / `medium error` | 硬盘或链路，转 §2 |
| `EDAC ... CE` / `mce: Hardware Error` | 内存或 CPU，转 §1 / §5 |
| `UDMA_CRC_Error` 增长 | 线缆或接口，转 §2.4 |
| 什么都没有 | 不代表没问题（尤其非 ECC 内存），继续逐项排查 |

---

## 1. 内存排查

内存故障是静默数据损坏最常见的来源，也是最容易被漏掉的 —— 因为**非 ECC 内存
没有任何检测能力**，所有计数器都会返回空。

### 1.1 先确认是不是 ECC 内存

这一步决定后面所有内存检查是否有意义：

```bash
dmidecode -t memory | grep -iE "Error Correction Type|Total Width|Data Width|Size|Type:|Manufacturer|Part Number"
```

关键看 `Error Correction Type`：

| 输出 | 含义 |
|---|---|
| `Single-bit ECC` / `Multi-bit ECC` | 有 ECC，后面的计数器有意义 |
| `None` | **无 ECC**，所有计数器恒为空，必须用 memtest |

另一个判断方法：`Total Width` 比 `Data Width` 大 8 位（如 72 vs 64）说明有 ECC。

```bash
# 逐条内存的详细信息
dmidecode -t 17 | grep -A16 "Memory Device" | grep -iE "Size|Locator|Speed|Manufacturer|Serial|Part"
```

### 1.2 读取 ECC 错误计数

**方法一：rasdaemon（推荐）**

```bash
# 安装
yum install -y rasdaemon    # RHEL/CentOS
apt-get install -y rasdaemon # Debian/Ubuntu

systemctl enable --now rasdaemon

# 查看
ras-mc-ctl --error-count     # 各 DIMM 的 CE/UE 计数
ras-mc-ctl --summary         # 汇总
ras-mc-ctl --errors          # 详细错误记录（含时间、地址、DIMM 位置）
```

**方法二：edac-utils**

```bash
yum install -y edac-utils
edac-util -v                 # 每个 csrow 的错误计数
edac-util -s                 # EDAC 驱动状态
edac-util --report=full
```

**方法三：直接读 sysfs（无需装包）**

```bash
# 可纠正错误
grep -H . /sys/devices/system/edac/mc/mc*/csrow*/ce_count 2>/dev/null
grep -H . /sys/devices/system/edac/mc/mc*/ce_count 2>/dev/null
# 不可纠正错误
grep -H . /sys/devices/system/edac/mc/mc*/csrow*/ue_count 2>/dev/null
grep -H . /sys/devices/system/edac/mc/mc*/ue_count 2>/dev/null
# 每个 DIMM 的位置标签，用于定位物理插槽
grep -H . /sys/devices/system/edac/mc/mc*/dimm*/dimm_label 2>/dev/null
```

**方法四：内核日志**

```bash
dmesg -T | grep -iE "edac|EDAC|corrected error|uncorrected|mce"
journalctl -k --since "30 days ago" | grep -iE "edac|mce"
```

### 1.3 判读标准

| 指标 | 含义 | 判断 |
|---|---|---|
| **CE**（Correctable Error） | 单 bit 错误，已被 ECC 纠正 | 偶发少量可接受；**持续增长要换条** |
| **UE**（Uncorrectable Error） | 多 bit 错误，无法纠正 | **非零就是硬故障**，立即换 |

关键在**趋势**，不是绝对值。测一下增长速度：

```bash
ras-mc-ctl --error-count > /tmp/ce1.txt
sleep 3600
ras-mc-ctl --error-count > /tmp/ce2.txt
diff /tmp/ce1.txt /tmp/ce2.txt
```

一小时内 CE 增长几十上百 → 那条内存在退化。

**大量 CE 通常预示 UE**，而 UE 落在 page cache 里就表现为静默数据损坏。

### 1.4 memtest：非 ECC 内存的唯一手段

非 ECC 内存必须靠 memtest。需要停机。

```bash
# 安装（会加进 GRUB 启动项）
yum install -y memtest86+
grub2-mkconfig -o /boot/grub2/grub.cfg
# 重启后在 GRUB 菜单选 memtest86+
```

至少跑**一轮完整 pass**（大内存机器可能要几小时）。有错误会直接标红显示，
并给出物理地址。

**在线替代方案**（不停机，但覆盖不完整 —— 只能测到空闲内存）：

```bash
yum install -y memtester
# 测 4GB，跑 3 轮。注意不要超过可用内存，否则会触发 OOM
memtester 4G 3
```

```bash
# 用 stressapptest 施加更真实的混合压力（Google 开发，比 memtester 更接近实际负载）
yum install -y stressapptest
stressapptest -s 300 -M 4096 -m 8 -W    # 300秒，4GB，8线程，含内存拷贝校验
```

### 1.5 定位到具体的物理插槽

有 ECC 时可以精确定位：

```bash
ras-mc-ctl --error-count          # 显示 mc#/csrow#/channel#
ras-mc-ctl --layout               # 物理布局
dmidecode -t 17 | grep -iE "Locator|Size"   # 插槽标识如 DIMM_A1
```

对照主板手册把 `mc0/csrow2/ch1` 换算成物理插槽位置。

### 1.6 内存排查的补充手段

```bash
# 内存压力和 OOM 历史 —— 内存吃紧会放大问题
dmesg -T | grep -iE "oom|killed process|page allocation failure"
free -h
cat /proc/meminfo | grep -iE "MemAvailable|Committed_AS|HardwareCorrupted"

# HardwareCorrupted 非零 = 内核已标记出坏页，强证据
grep HardwareCorrupted /proc/meminfo

# 内核标记的坏页列表
ls /sys/devices/system/memory/hard_offline_page 2>/dev/null
```

`HardwareCorrupted` 非零是**内存故障的直接证据**，内核已经把那些页隔离了。

---

## 2. 硬盘排查

### 2.1 安装 smartmontools

```bash
yum install -y smartmontools     # RHEL/CentOS/Rocky/Alma/Anolis/openEuler
dnf install -y smartmontools
apt-get install -y smartmontools # Debian/Ubuntu
zypper install -y smartmontools  # SUSE

which smartctl && smartctl --version
```

**离线环境**：

```bash
# 从本地 ISO 装
mount -o loop /path/to/os.iso /mnt/iso
yum install -y --disablerepo=* --enablerepo=c7-media smartmontools

# 或在同版本同架构机器上下载 rpm
yum install --downloadonly --downloaddir=/tmp/pkg smartmontools
rpm -ivh /tmp/pkg/smartmontools-*.rpm
```

注意核对架构：`uname -m`。

**容器内无效** —— 看不到宿主物理盘。要在宿主机装，或给容器加
`--privileged --device=/dev/sda`。

### 2.2 先确定数据在哪块盘上

```bash
df -h /data                                  # 换成实际数据目录
lsblk -o NAME,SIZE,TYPE,MOUNTPOINT,MODEL,SERIAL,ROTA
# ROTA=1 机械盘，ROTA=0 SSD

# LVM 环境要往下追一层
pvs; vgs; lvs
dmsetup deps -o devname

# 软 RAID
cat /proc/mdstat
```

### 2.3 SMART 整体健康与全量属性

```bash
smartctl -H /dev/sda          # 一句话结论
smartctl -a /dev/sda          # 全部属性
smartctl -x /dev/sda          # 更详细，含错误日志和温度历史

# SAS 盘 / RAID 后面的盘可能需要指定类型
smartctl -a -d sat /dev/sda           # SATA 盘接在 SAS 控制器上
smartctl -a -d megaraid,0 /dev/sda    # MegaRAID 后面的第 0 块盘
smartctl -a -d cciss,0 /dev/sda       # HP Smart Array
smartctl --scan                       # 让 smartctl 自己探测正确的 -d 参数
```

### 2.4 关键 SMART 属性判读

**机械盘（SATA/SAS）**：

| 属性 | ID | 含义 | 判断 |
|---|---|---|---|
| `Reallocated_Sector_Ct` | 5 | 已重映射的坏扇区 | 非零且增长 = 介质退化 |
| `Current_Pending_Sector` | 197 | 读不稳定、待重映射 | **非零就要警惕**：典型的"有时读对有时读错" |
| `Offline_Uncorrectable` | 198 | 不可纠正扇区 | 非零 = 已有数据损坏 |
| `UDMA_CRC_Error_Count` | 199 | 接口传输误码 | **指向线缆/接口，不是盘体** |
| `Reported_Uncorrect` | 187 | 报告给主机的不可纠正错误 | 非零 = 有读失败 |
| `Spin_Retry_Count` | 10 | 启动重试 | 非零 = 机械问题 |
| `Command_Timeout` | 188 | 命令超时 | 大量增长 = 链路或盘故障 |

**SSD/NVMe**：

```bash
smartctl -a /dev/nvme0n1
# 或用 nvme-cli（信息更全）
yum install -y nvme-cli
nvme smart-log /dev/nvme0n1
nvme error-log /dev/nvme0n1 | head -40
nvme id-ctrl /dev/nvme0n1 | grep -iE "fr |mn |sn "   # 固件版本
```

| 字段 | 含义 | 判断 |
|---|---|---|
| `media_errors` / `Media_and_Data_Integrity_Errors` | 盘自己检测到的完整性错误 | **非零基本可定案** |
| `percentage_used` | 寿命消耗 | 接近或超过 100% 会开始出静默错误 |
| `critical_warning` | 严重告警位 | 非零立即处理 |
| `num_err_log_entries` | 错误日志条数 | 持续增长要查 error-log |
| `Available_Spare` | 剩余备用块 | 低于阈值 = 接近寿命终点 |

**一个重点提醒**：`UDMA_CRC_Error_Count` 值得单独盯 —— 它计的是接口传输误码，
也就是**盘上数据完好、但传输途中出错**。这正好解释"文件离线校验是好的、
但线上读取报损坏"这种现象。对应的处置是换线缆、重插背板，不是换盘。

### 2.5 主动自检

```bash
smartctl -t short /dev/sda        # 短测，约 2 分钟
sleep 150
smartctl -l selftest /dev/sda     # 看结果

smartctl -t long /dev/sda         # 完整表面扫描，几小时（不影响业务但会占 I/O）
smartctl -l selftest /dev/sda
smartctl -l error /dev/sda        # 盘自己记录的错误日志
```

自检结果里 `Completed without error` 之外的任何状态都要跟进。

### 2.6 只读表面扫描

不改数据，纯读一遍全盘找读不出来的扇区：

```bash
# badblocks 只读模式（-n/-w 会写数据，生产环境绝对不要用）
badblocks -sv -c 4096 /dev/sda

# 或用 dd 全盘读一遍，观察是否有 I/O 错误
dd if=/dev/sda of=/dev/null bs=1M status=progress
dmesg -T | tail -20     # 读完立即看有没有新的 I/O 错误
```

### 2.7 I/O 层统计

```bash
# 累计错误计数
grep -H . /sys/block/*/device/ioerr_cnt 2>/dev/null

# 实时 I/O 状况，看 await 和 %util 是否异常
iostat -xmz 2 10

# SCSI 层错误
dmesg -T | grep -iE "sd [0-9]|scsi|ata[0-9]|nvme.*error|Sense Key|Add. Sense"
```

`Sense Key: Medium Error` 是介质错误的明确信号。
`Sense Key: Aborted Command` 更多指向链路。

---

## 3. RAID 卡与控制器排查

这一层最容易产生"盘是好的但数据是坏的"，尤其是**写缓存开着而电池坏了**的情况。

### 3.1 识别控制器

```bash
lspci | grep -iE "raid|storage|sas|scsi|nvme"
lsscsi
dmesg -T | grep -iE "megaraid|mpt|smartpqi|hpsa|aacraid|3ware"
```

### 3.2 各厂商工具

**LSI / Broadcom / MegaRAID（最常见）**：

```bash
storcli /c0 show all                  # 整体状态
storcli /c0 show                      # 简要
storcli /c0/eall/sall show all        # 所有物理盘详情
storcli /c0/vall show all             # 所有逻辑卷
storcli /c0/bbu show all              # 电池状态（关键）
storcli /c0 show events               # 事件日志
storcli /c0 show termlog              # 详细固件日志

# 老版本命令
MegaCli -AdpAllInfo -aALL
MegaCli -LDInfo -Lall -aAll
MegaCli -PDList -aALL
MegaCli -AdpBbuCmd -aALL              # 电池
MegaCli -AdpEventLog -GetEvents -f /tmp/events.log -aALL
```

**Dell PERC**：

```bash
perccli /c0 show all
perccli /c0/bbu show all
omreport storage controller
omreport storage pdisk controller=0
```

**HP / HPE Smart Array**：

```bash
ssacli ctrl all show config detail
ssacli ctrl slot=0 show status
ssacli ctrl slot=0 pd all show detail
hpssacli ctrl all show config detail   # 老版本
```

**Adaptec**：

```bash
arcconf getconfig 1
arcconf getlogs 1 device
```

**软 RAID (mdadm)**：

```bash
cat /proc/mdstat
mdadm --detail /dev/md0
mdadm --examine /dev/sd[a-z]1

# Mismatch_Cnt 非零 = 镜像/校验不一致，重要信号
cat /sys/block/md0/md/mismatch_cnt

# 主动一致性校验
echo check > /sys/block/md0/md/sync_action
cat /proc/mdstat                       # 看进度
cat /sys/block/md0/md/mismatch_cnt     # 校验后再看
```

### 3.3 判读要点

| 检查项 | 为什么重要 |
|---|---|
| **BBU / 电池状态** | 写缓存开启但电池失效时，掉电会丢失缓存中的数据，是静默损坏的经典成因 |
| **Cache Policy** | `WriteBack` 且电池坏 = 高风险；应临时切 `WriteThrough` |
| **Consistency Check 结果** | 不一致说明 RAID 内部数据已经有问题 |
| **Media Error Count** | 控制器统计的介质错误 |
| **Other Error Count** | 非介质错误，通常是链路或控制器自身 |
| **Predictive Failure** | 盘即将失效的预警 |
| **固件版本** | 老固件的已知 bug 可能导致数据损坏，查厂商 release notes |

电池坏了的应急处置：

```bash
# 临时改为 WriteThrough，牺牲性能换安全
storcli /c0/v0 set wrcache=wt
```

---

## 4. 文件系统排查

```bash
# 内核报告的文件系统错误
dmesg -T | grep -iE "ext4-fs error|xfs.*(corrupt|error)|btrfs.*error|remount-ro"

# ext4
tune2fs -l /dev/sda1 | grep -iE "state|error|mount count|Last checked"
dumpe2fs -h /dev/sda1 2>/dev/null | grep -iE "state|error"
# 只读检查（-n 不修改任何东西）
e2fsck -fn /dev/sda1

# XFS
xfs_info /dev/sda1
xfs_db -r -c "check" /dev/sda1        # 只读
xfs_repair -n /dev/sda1               # -n = 只报告不修复

# 挂载参数（是否被内核强制改成只读）
mount | grep /data
```

`Filesystem state: clean with errors` 或 `remount-ro` 是明确信号。

---

## 5. CPU 与主板排查

CPU cache 故障和内存故障症状相似，都会造成数据在计算过程中被改。

```bash
# MCE (Machine Check Exception) —— CPU 级硬件错误
dmesg -T | grep -iE "mce|machine check|hardware error"
journalctl -k --since "30 days ago" | grep -iE "mce|machine check"

# mcelog（部分发行版）
yum install -y mcelog
mcelog --client 2>/dev/null
cat /var/log/mcelog 2>/dev/null

# BMC 硬件事件日志 —— 能看到 CPU/内存/电源/风扇的硬件告警
ipmitool sel elist
ipmitool sel list
ipmitool sdr list | grep -iE "temp|volt|fan"     # 传感器读数

# 温度（过热会导致各种随机错误）
sensors 2>/dev/null
ipmitool sdr type temperature

# CPU 信息与已知问题
lscpu
dmidecode -t processor | grep -iE "version|status|core count"

# 主板/BIOS 版本 —— 老 BIOS 的内存时序 bug 会导致随机损坏
dmidecode -t bios | grep -iE "vendor|version|release"
dmidecode -t baseboard | grep -iE "manufacturer|product"
```

CPU 压力测试（同时会测内存和散热）：

```bash
yum install -y stress-ng
stress-ng --cpu 0 --cpu-method matrixprod --timeout 600 --metrics
stress-ng --cpu 0 --vm 4 --vm-bytes 2G --timeout 600    # CPU + 内存混合
```

跑测试时另开一个窗口监控：

```bash
watch -n2 'sensors; grep -H . /sys/devices/system/edac/mc/mc*/ce_count'
```

---

## 6. 用数据库文件做压力复现

前面的计数器都是**被动**的 —— 硬件自己检测到才会记录。间歇性故障可能一直不被记录。
所以还需要**主动施压复现**。

这里用 TDengine 的诊断工具，因为它们的校验粒度比通用工具细，
而且直接对应生产上出问题的数据路径。

### 6.1 区分内存侧 vs 存储链路

关键在于**是否绕过 page cache**：

- 走 page cache：文件第一遍读完就留在内存里，后续主要在测 **DRAM 和 CPU 路径**
- 绕过 page cache（O_DIRECT / 先清缓存）：每次真的走设备，把 **HBA、线缆、控制器**纳入

`run.sh` 的 `--drop-cache` 就是这个用途。两种模式各跑一轮对比：

```bash
cd <repo>/source/taos-community/tools/rdb-selftest

# A. 不清缓存
./run.sh -i 0 -j 2 -n 500000 -r 3

# B. 每轮读前清页缓存，强制读落到设备
./run.sh -i 0 -j 2 -n 500000 -r 3 --drop-cache
```

判读：

| 结果 | 指向 |
|---|---|
| 只有 B 复现 | **存储链路**：盘、HBA、线缆、RAID 卡缓存 |
| A 和 B 都复现 | **内存侧**：DRAM、CPU cache、内存控制器 |
| 都不复现 | 故障间歇性很强，延长时间或加大并发 |

> `--drop-cache` 是**全局**清缓存（`/proc/sys/vm/drop_caches`），会一起丢掉 taosd 的
> page cache，导致查询变慢、磁盘 I/O 突增。生产环境上正常服务时不要用；
> 如果 taosd 已经因为损坏在反复重启，那本来也没什么缓存可保留。

### 6.2 写读循环，复现完整链路

```bash
cd <repo>/source/taos-community/tools/rdb-selftest

# 默认 20 轮
./run.sh

# 加压：无限轮、4 并发、大数据库、3 轮读、每轮清页缓存
./run.sh -i 0 -j 4 -n 500000 -r 3 --drop-cache

# 只测读路径：写一次，之后反复重读
./run.sh --keep-db -i 0 -r 5
```

失败时它会保留数据库并自动复查，直接给出"盘上坏了"还是"读的时候坏了"的判定。

### 6.3 关于压力条件

**一次跑干净只是与运行时长和负载成正比的弱证据。** 间歇性故障大部分时候是安静的。

如果故障原本是在高负载下出现的，就要在相当的压力下测：

```bash
# 一边跑数据库压力，一边施加内存压力
stress-ng --vm 4 --vm-bytes 4G --timeout 3600 &
./run.sh -i 0 -j 4 -n 500000 --drop-cache
```

同时监控计数器变化：

```bash
watch -n5 'ras-mc-ctl --error-count; smartctl -A /dev/sda | grep -iE "Reallocated|Pending|CRC"'
```

---

## 7. 综合判读矩阵

### 按现象定位

| 现象 | 最可能的部件 | 优先查 |
|---|---|---|
| 文件离线校验**也是坏的**（坏字节持久化在盘上） | 介质、控制器写缓存、文件系统 | §2 §3 §4 |
| 文件离线校验**是好的**（读的时候才出错） | 内存、CPU cache、HBA、线缆、控制器缓存 | §1 §3 §5 |
| 只有 `-D`（O_DIRECT）能复现 | 存储链路：盘、HBA、线缆 | §2 §3 |
| 加不加 `-D` 都复现 | 内存侧 | §1 §5 |
| 多个盘、多个文件同时中招 | 内存或控制器（单盘故障不会跨盘） | §1 §3 |
| 集中在一块盘 | 该盘介质 | §2 |
| 有 `I/O error` / `medium error` 内核日志 | 盘或链路 | §2 |
| 零 I/O 错误但有数据损坏 | 内存（静默位翻转不会产生 I/O 错误） | §1 |
| `HardwareCorrupted` 非零 | 内存，已确诊 | §1.6 |
| `UDMA_CRC_Error` 增长 | 线缆、接口、背板 | §2.4 |
| CE 大量增长 | 内存条退化 | §1.3 |
| UE 非零 | 内存硬故障 | §1.3 |
| `Mismatch_Cnt` 非零 | RAID 一致性问题 | §3.2 |
| 温度异常 + 随机错误 | 散热 | §5 |

### 重要提醒

**计数器为空不等于健康。** 特别是：

- 非 ECC 内存：`ras-mc-ctl`/`edac-util` 恒为空，必须 memtest
- 容器内：拿不到 `dmesg`、EDAC、SMART，要到宿主机上查
- 没装 rasdaemon：ECC 错误可能发生过但没被记录

**零 I/O 错误反而是内存故障的旁证。** 盘或链路出问题时内核通常会留下 I/O 错误记录；
如果数据确实损坏了但内核一片安静，更符合静默的内存位翻转。

---

## 8. 命令速查表

```bash
# ============ 内存 ============
dmidecode -t memory | grep -i "error correction"    # 是否 ECC（先看这个）
ras-mc-ctl --error-count                            # ECC 错误计数
ras-mc-ctl --summary
edac-util -v
grep -H . /sys/devices/system/edac/mc/mc*/csrow*/{ce,ue}_count
grep HardwareCorrupted /proc/meminfo                # 非零 = 已确诊坏页
dmesg -T | grep -iE "edac|mce|hardware error"
memtester 4G 3                                      # 在线测（覆盖不全）
# memtest86+ 需重启，非 ECC 内存唯一可靠手段

# ============ 硬盘 ============
lsblk -o NAME,SIZE,TYPE,MOUNTPOINT,MODEL,SERIAL
smartctl -H /dev/sda                                # 健康结论
smartctl -a /dev/sda                                # 全部属性
smartctl -a /dev/sda | grep -iE "Reallocated|Pending|Uncorrectable|UDMA_CRC|Command_Timeout"
nvme smart-log /dev/nvme0n1                         # NVMe
nvme smart-log /dev/nvme0n1 | grep -iE "media_errors|percentage_used|critical"
smartctl -t short /dev/sda && sleep 150 && smartctl -l selftest /dev/sda
smartctl -l error /dev/sda
badblocks -sv /dev/sda                              # 只读扫描
grep -H . /sys/block/*/device/ioerr_cnt

# ============ RAID / 控制器 ============
lspci | grep -iE "raid|sas"
storcli /c0 show all
storcli /c0/bbu show all                            # 电池，关键
storcli /c0 show events
MegaCli -AdpAllInfo -aALL
perccli /c0 show all                                # Dell
ssacli ctrl all show config detail                  # HP
cat /proc/mdstat                                    # 软 RAID
cat /sys/block/md0/md/mismatch_cnt

# ============ 文件系统 ============
dmesg -T | grep -iE "ext4-fs error|xfs.*corrupt|remount-ro"
tune2fs -l /dev/sda1 | grep -i state
e2fsck -fn /dev/sda1                                # 只读检查
xfs_repair -n /dev/sda1                             # 只读检查

# ============ CPU / 主板 ============
dmesg -T | grep -iE "mce|machine check"
ipmitool sel elist                                  # BMC 事件日志
ipmitool sdr type temperature
sensors
dmidecode -t bios | grep -i version

# ============ 内核历史（跨重启）============
journalctl -k --since "7 days ago" | grep -iE "i/o error|blk_update_request|edac|mce"

# ============ 数据库层复现 ============
./run.sh -i 0 -j 2 -n 500000 -r 3                   # 测 DRAM/CPU
./run.sh -i 0 -j 2 -n 500000 -r 3 --drop-cache      # 测设备链路（全局清缓存，慎用）
./run.sh --keep-db -i 0 -r 5                        # 只测读路径
```

---

## 附：排查记录模板

排查时逐项填写，避免遗漏和重复劳动：

```
主机：                     数据目录：              物理盘：
故障时间：                 首次报错时间：

【0. 快速筛查】
dmesg 硬件错误：           □无 □有：
journalctl 历史：          □无 □有：
ipmitool sel：             □无 □有：

【1. 内存】
是否 ECC：                 □是 □否（否则以下全部无效，需 memtest）
CE 计数：            增长速度：         /小时
UE 计数：
HardwareCorrupted：
memtest 结果：             □未做 □通过 □失败，地址：

【2. 硬盘】
型号/序列号：
SMART 整体：               □PASSED □FAILED
Reallocated_Sector_Ct：
Current_Pending_Sector：
Offline_Uncorrectable：
UDMA_CRC_Error_Count：
Media_and_Data_Integrity_Errors（NVMe）：
percentage_used（SSD）：
自检结果：

【3. RAID / 控制器】
型号：                     固件版本：
BBU/电池状态：
Cache Policy：             □WriteBack □WriteThrough
一致性校验：
Media/Other Error Count：

【4. 文件系统】
类型：                     状态：
内核 FS 错误：

【5. CPU / 温度】
MCE：
温度：

【6. 复现测试】
离线校验文件：             □干净 □损坏
run.sh 不清缓存：          □通过 □失败
run.sh --drop-cache：      □通过 □失败
run.sh：                   □通过 □失败
测试时长/负载：

【结论】
判定部件：
依据：
处置：
```
