# TDengine 兼容性升级 CI 工具集

## 概述

`CompatCI` 目录提供两个 CI 集成脚本，用于自动化验证 TDengine 的版本升级兼容性：

| 脚本 | 升级模式 | 说明 |
|------|----------|------|
| `hot_upgrade_task.py` | 滚动升级（Hot） | 自动查找最小基准版本，调用 CompatCheck 执行滚动升级测试 |
| `cold_upgrade_task.py` | 冷升级（Cold） | 对指定的一组基准版本逐一执行冷升级测试 |

两个脚本均依赖同目录同级的 `CompatCheck` 工具（`../CompatCheck/run/main.py`）执行实际的兼容性测试。

---

## hot_upgrade_task.py — 滚动升级测试

### 功能说明

脚本会自动完成以下步骤：

1. 从 `cmake/version.cmake` 读取当前（目标）版本号
2. 根据版本号的前三位（`major.minor.patch`），在绿色版本存储路径下查找匹配的基准版本
3. 若未找到匹配版本，输出提示并正常退出（无需测试）
4. 若找到多个匹配版本，选取版本号最小的作为基准版本
5. 将编译目录中的目标版本二进制文件复制到临时目录
6. 调用 CompatCheck（带 `--rollupdate` 标志）执行滚动升级测试
7. 测试完成后清理临时目录

### 使用方法

```bash
# 使用默认配置
python3 hot_upgrade_task.py

# 指定绿色版本存储路径
python3 hot_upgrade_task.py --green-path /tdengine/green_versions/

# 指定编译目录
python3 hot_upgrade_task.py --build-dir /path/to/debug

# 传递额外参数给 CompatCheck
python3 hot_upgrade_task.py -- --path /tmp/test --check-sysinfo

# 组合使用
python3 hot_upgrade_task.py \
    --green-path /tdengine/green_versions/ \
    --build-dir /path/to/debug \
    -- --check-sysinfo
```

### 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--green-path PATH` | `/tdengine/green_versions/` 或环境变量 `TD_GREEN_VERSIONS_PATH` | 绿色版本存储路径 |
| `--build-dir DIR` | `../../../../debug` | 编译目录路径 |
| `-- [额外参数]` | — | 透传给 CompatCheck 的额外参数 |

### 环境变量

| 变量 | 说明 |
|------|------|
| `TD_GREEN_VERSIONS_PATH` | 绿色版本存储路径（未通过 `--green-path` 指定时生效） |

### 版本匹配规则

脚本以当前版本号的前三位为前缀，查找绿色版本路径下的所有匹配子目录，并选取版本号最小的作为基准：

```
当前版本：  3.4.0.9
匹配前缀：  3.4.0
找到版本：  3.4.0.0、3.4.0.8
选用基准：  3.4.0.0（最小版本）
```

### 工作流程

```
读取版本号（cmake/version.cmake）
    ↓
查找匹配基准版本（greenVersionsPath/3.x.x.*）
    ↓ 未找到 → 正常退出（exit 0）
    ↓ 找到
选取最小版本作为基准
    ↓
复制编译产物到 /tmp/<currentVersion>/
    ↓
调用 CompatCheck --rollupdate -F <base> -T <target>
    ↓
清理临时目录，返回退出码
```

---

## cold_upgrade_task.py — 冷升级测试

### 功能说明

脚本会自动完成以下步骤：

1. 从 `cmake/version.cmake` 读取当前（目标）版本号
2. 将编译目录中的目标版本二进制文件复制到临时目录
3. 解析 `--versions` 参数，将所有指定的基准版本目录解析完毕（一次性报告所有缺失目录）
4. 按顺序对每个基准版本调用 CompatCheck（默认冷升级模式，不带 `--rollupdate`）执行测试
5. 汇总所有测试结果，若任意一项失败则返回非零退出码

与 hot 脚本不同，cold 脚本需要**显式指定**基准版本，不会自动扫描匹配版本。

### 使用方法

```bash
# 单个基准版本
python3 cold_upgrade_task.py \
    --versions 3.3.0.0

# 多个基准版本（逗号分隔）
python3 cold_upgrade_task.py \
    --versions 3.3.0.0,3.3.6.0

# 指定绿色版本路径和编译目录
python3 cold_upgrade_task.py \
    --green-path /tdengine/green_versions/ \
    --versions 3.3.0.0,3.3.6.0 \
    --build-dir /path/to/debug

# 传递额外参数给 CompatCheck
python3 cold_upgrade_task.py \
    --versions 3.3.0.0 \
    --options "-q --check-sysinfo"
```

### 命令行参数

| 参数 | 是否必填 | 默认值 | 说明 |
|------|----------|--------|------|
| `--versions VER[,VER,...]` | **必填** | — | 逗号分隔的基准版本列表，格式为 `x.x.x.x` |
| `--green-path PATH` | 可选 | `/tdengine/green_versions/` 或环境变量 `TD_GREEN_VERSIONS_PATH` | 绿色版本存储路径 |
| `--build-dir DIR` | 可选 | `../../../../debug` | 编译目录路径 |
| `--options "..."` | 可选 | — | 传递给 CompatCheck 的额外选项（作为整体字符串传入） |

### 环境变量

| 变量 | 说明 |
|------|------|
| `TD_GREEN_VERSIONS_PATH` | 绿色版本存储路径（未通过 `--green-path` 指定时生效） |

### 工作流程

```
读取版本号（cmake/version.cmake）
    ↓
复制编译产物到 /tmp/<currentVersion>/
    ↓
解析并校验所有基准版本目录（批量报告缺失）
    ↓
循环执行：
  对每个基准版本调用 CompatCheck -F <base> -T <target>
    ├─ PASS → 继续下一个
    └─ FAIL → 记录失败，继续下一个
    ↓
汇总：全部通过 exit 0 / 任意失败 exit 非零
```

---

## 共同说明

### 目录结构

绿色版本存储路径下须包含以版本号命名的子目录，每个目录中存放对应版本的二进制文件：

```
/tdengine/green_versions/
├── 3.3.0.0/
│   ├── taosd
│   └── libtaos.so
├── 3.3.6.0/
│   ├── taosd
│   └── libtaos.so
├── 3.4.0.0/
│   ├── taosd
│   └── libtaos.so
└── 3.4.0.8/
    ├── taosd
    └── libtaos.so
```

### 编译目录要求

编译目录下须包含以下文件（至少满足其一）：

```
<build-dir>/
└── build/
    ├── bin/
    │   └── taosd
    └── lib/
        ├── libtaosnative.so   ← 优先使用（重命名为 libtaos.so）
        └── libtaos.so         ← 次选
```

> **注意**：若 `libtaosnative.so` 存在，会优先将其复制并重命名为 `libtaos.so`；若不存在则退回使用 `libtaos.so`；两者均不存在时脚本报错退出。

### 退出码

| 退出码 | 含义 |
|--------|------|
| `0` | 全部测试通过，或无需执行测试（hot 模式下未找到匹配基准版本） |
| `1` | 测试失败或执行过程中发生错误 |

---

## 与 CI 系统集成

### 滚动升级

```bash
cd test/tools/CompatCI
python3 hot_upgrade_task.py \
    --green-path /tdengine/green_versions/ \
    --build-dir "$BUILD_DIR"

if [ $? -ne 0 ]; then
    echo "滚动升级测试失败"
    exit 1
fi
```

### 冷升级

```bash
cd test/tools/CompatCI
python3 cold_upgrade_task.py \
    --green-path /tdengine/green_versions/ \
    --versions 3.3.0.0,3.3.6.0 \
    --build-dir "$BUILD_DIR"

if [ $? -ne 0 ]; then
    echo "冷升级测试失败"
    exit 1
fi
```
