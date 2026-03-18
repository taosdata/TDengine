# TDengine 滚动升级 CI 任务

## 概述

`hot_upgrade_task.py` 是用于 CI 系统集成的滚动升级自动化测试脚本。它会自动完成以下步骤：

1. 从配置文件中读取引擎绿色版文件存储路径
2. 读取 `cmake/version.cmake` 文件获取当前版本号
3. 根据当前版本号前三位，在绿色版文件存储路径下查找匹配的基准版本
4. 如果没有找到匹配的文件夹，提示无需进行滚动升级测试
5. 如果找到多个匹配的文件夹，以版本最小的作为基准版本
6. 从编译目录下获取目标版本引擎绿色文件，存放在临时目录
7. 调用 CompatCheck 工具进行滚动升级测试

## 使用方法

### 基本用法

```bash
# 使用默认配置
python3 hot_upgrade_task.py

# 指定绿色版本存储路径
python3 hot_upgrade_task.py --green-versions-path /tdengine/green_versions/

# 指定编译目录
python3 hot_upgrade_task.py --build-dir /path/to/build

# 传递额外参数给 CompatCheck
python3 hot_upgrade_task.py -- --path /tmp/test --check-sysinfo
```

### 命令行参数

- `--config CONFIG`: 配置文件路径（可选）
- `--build-dir BUILD_DIR`: 编译目录路径，默认为 `../../../debug`
- `--green-versions-path PATH`: 绿色版本存储路径，默认为 `/tdengine/green_versions/`
- `-- [额外参数]`: 传递给 CompatCheck 工具的额外参数

### 环境变量

- `TD_GREEN_VERSIONS_PATH`: 绿色版本存储路径（如果未通过命令行参数指定）

## 目录结构

绿色版本存储路径下应包含按版本号命名的文件夹，例如：

```
/tdengine/green_versions/
├── 3.3.0.0/
│   ├── taosd
│   ├── libtaos.so
│   └── libtaosnative.so
├── 3.3.6.0/
│   ├── taosd
│   ├── libtaos.so
│   └── libtaosnative.so
├── 3.4.0.0/
│   ├── taosd
│   ├── libtaos.so
│   └── libtaosnative.so
└── 3.4.0.8/
    ├── taosd
    ├── libtaos.so
    └── libtaosnative.so
```

## 版本匹配规则

脚本会根据当前版本号的前三位（major.minor.patch）查找匹配的基准版本：

- 当前版本：`3.4.0.9`
- 匹配前缀：`3.4.0`
- 找到版本：`3.4.0.0`, `3.4.0.8`
- 选择基准：`3.4.0.0`（版本最小）

## 测试示例

使用测试目录进行测试：

```bash
# 使用 CompatCheck/test 目录作为绿色版本路径
python3 hot_upgrade_task.py \
    --green-versions-path /root/TDinternal/community/test/tools/CompatCheck/test \
    --build-dir /tmp/test_build
```

或使用提供的测试脚本：

```bash
./test_hot_upgrade.sh
```

## 工作流程

1. **读取版本号**: 从 `cmake/version.cmake` 解析当前版本
2. **查找基准版本**: 在绿色版本路径下查找匹配的版本文件夹
3. **准备目标版本**: 从编译目录复制文件到临时目录
4. **执行升级测试**: 调用 CompatCheck 工具进行滚动升级
5. **清理临时文件**: 测试完成后删除临时目录

## 退出码

- `0`: 测试成功或无需测试
- `1`: 测试失败或发生错误

## 注意事项

1. 确保绿色版本存储路径存在且包含正确的版本文件
2. 编译目录应包含 `build/bin/taosd` 和 `build/lib/libtaos*.so` 文件
3. 测试过程中会创建临时目录，测试完成后自动清理
4. 如果没有找到匹配的基准版本，脚本会正常退出（退出码 0）

## 与 CI 系统集成

在 CI 流程中，可以这样使用：

```bash
# 在编译完成后执行
cd test/tools/CompatCI
python3 hot_upgrade_task.py \
    --green-versions-path /tdengine/green_versions/ \
    --build-dir $BUILD_DIR

# 检查退出码
if [ $? -ne 0 ]; then
    echo "滚动升级测试失败"
    exit 1
fi
```
