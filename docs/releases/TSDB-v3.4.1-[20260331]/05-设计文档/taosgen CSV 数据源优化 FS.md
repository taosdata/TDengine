# taosgen CSV 数据源优化 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-18 |  | 1.0 | 裴亚明 | 初始版本 |

## 2. 背景

### 2.1 问题描述

taosgen 当前的 CSV 读取功能存在以下三个局限：
1. **性能问题：手写的逐字符状态机解析器使用 `std::ifstream` + `std::getline`，对大文件（GB 级别）的读取性能不佳， I/O 效率低。**
2. **单文件限制**：`file_path` 仅支持单个文件路径，无法批量读取多个 CSV 文件。用户需要手动合并多个数据文件，增加了数据准备的工作量。
3. **内存压力**：数据加载方式只有全量加载（preload），对于超大 CSV 文件会导致内存占用过高，限制了可处理的数据规模。

### 2.2 目标

本方案旨在解决上述问题，达成以下目标：
1. **提升性能**：替换为高性能 CSV 解析库，利用内存映射（mmap）I/O 提升大文件读取速度。
2. **简化多文件处理**：支持通过目录或通配符批量指定多个 CSV 文件，减少用户的数据预处理工作。
3. **降低内存占用**：引入 streaming 模式，实现逐行读取和处理，使 taosgen 能够处理远超内存容量的数据集。

## 3. 定义

| 术语 | 定义 |
| --- | --- |
| **Preload 模式** | 数据加载模式之一，将 CSV 数据全量加载到内存后进行处理。适用于数据量较小或需要重复读取的场景。 |
| **Streaming 模式** | 数据加载模式之一，逐行读取 CSV 数据并立即处理，不保留已处理的数据行。适用于大文件场景。 |
| **Glob 模式** | 一种通配符匹配模式，支持 `*`（匹配任意字符序列）和 `?`（匹配单个字符），用于批量匹配文件。例如：`data/*.csv` |

## 4. 行为    说明

### 4.1 新增配置参数

#### 4.1.1 loading_mode

**参数路径**: `schema.generation.loading_mode`
**描述**: 控制 CSV 数据的加载模式。
**有效值**:
- `"preload"`（默认）: 全量加载模式，将 CSV 数据全部加载到内存
- `"streaming"`: 流式加载模式，逐行读取处理
  
**配置示例**:
```yaml
schema:
  generation:
    loading_mode: streaming  # 或 preload
```


**约束**:
- streaming 模式下不支持 `tbname_index >= 0`

### 4.2 多文件支持

#### 4.2.1 file_path 扩展

**ColumnsCSV 和 TagsCSV 的 **`**file_path**`** 参数现支持以下格式**:
1. **单个文件路径**（原有行为，保持不变）:
  ```yaml
  columns:
    file_path: /path/to/data.csv
  ```

1. **目录路径**（新增）:
  ```yaml
  columns:
    file_path: /path/to/csv_directory/
  ```

  - 自动识别目录下所有 `.csv` 文件
  - 文件按字母顺序排序后依次读取
  - 要求所有 CSV 文件具有相同的列结构
1. **Glob 通配符**（新增）:
  ```yaml
  columns:
    file_path: /path/to/data_*.csv
  ```

  - 支持 `*` 匹配任意字符序列
  - 支持 `?` 匹配单个字符
  - 匹配的文件按字母顺序排序

#### 4.2.2 多文件读取行为

- 多个文件的数据被逻辑上串联为一个连续的数据流
- 除第一个文件外，后续文件的 header 行会被自动跳过
- 所有文件的列数必须与第一个文件保持一致，否则抛出错误
- 文件读取顺序按文件名字母升序排列
  
**示例配置**:
```yaml
schema:
  from_csv:
    columns:
      file_path: ./data/sensors_*.csv  # 匹配 sensors_001.csv, sensors_002.csv 等
      has_header: true
      timestamp_index: 0
      timestamp_precision: ms
```


### 4.3 Streaming 模式约束

#### 4.3.1 与 tbname_index 的互斥

streaming 模式下禁止按子表名分组加载数据，因此：
- 若 `loading_mode: streaming` 且 `from_csv.columns.tbname_index >= 0`，配置校验将抛出错误：
  ```plaintext
  loading_mode 'streaming' is incompatible with 'tbname_index' >= 0. 
  Streaming mode requires tbname_index = -1.
  ```

  
**原因**: streaming 模式需要逐行处理数据，而 tbname_index 需要先将数据按子表名分组缓存，两者设计冲突。

### 4.4 出错处理

#### 4.4.1 文件不存在

**错误信息**: `CSV file does not exist: <path>`
**排查方法**:
- 检查文件路径是否正确
- 确认文件权限是否允许读取
- 对于相对路径，确认当前工作目录

#### 4.4.2 目录为空或 glob 无匹配

**错误信息**: `No CSV files found matching path: <path>`
**排查方法**:
- 检查目录中是否存在 `.csv` 文件
- 对于 glob 模式，检查通配符是否正确
- 确认文件扩展名是否为 `.csv`（区分大小写）

#### 4.4.3 列数不一致

**错误信息**: `Column count mismatch in file '<file>': expected <n> but got <m>`
**排查方法**:
- 确保所有 CSV 文件具有相同的列数
- 检查是否存在多余的逗号或缺失的字段
- 对于带 header 的文件，确认 header 行格式正确

#### 4.4.4 配置冲突

**错误信息**: 见 4.3.1
**排查方法**:
- 若需使用 tbname_index 或 data_disorder，将 loading_mode 改为 preload

## 5. 性能

### 5.1 CSV 解析性能提升

**变更**: 将手写状态机解析器替换为 vincentlaucsb-csv-parser 库
**优化效果**:
- 使用内存映射（mmap）I/O 替代 `std::ifstream`，减少系统调用开销
- 优化的解析算法，降低 CPU 使用率
- 支持流式迭代器，减少内存分配
**适用场景**:
- 大文件（> 100MB）读取性能提升

### 5.2 treaming 模式内存优化

**变更**: 新增 streaming 加载模式
**优化效果**:
- 内存占用不再与 CSV 文件大小成正比
- 典型内存占用: 仅保留当前处理行的数据
- 可处理远超物理内存容量的数据集
  
**对比**:

| 模式 | 内存占用 | 适用场景 |
| --- | --- | --- |
| preload | 与 CSV 文件大小成正比 | 小文件、需要重复读取、使用 tbname_index |
| streaming | 常量 | 大文件、单次顺序处理 |

### 5.3 多文件读取优化

**变更**: 支持批量文件读取
**优化效果**:
- 减少用户手动合并文件的开销
- 文件切换时保持流式处理，避免多次打开/关闭文件的开销累积
- 按文件名排序保证数据处理的确定性顺序

## 6. 安全

### 6.1 文件路径安全

- 使用 `std::filesystem` 进行路径解析，避免路径遍历攻击
- Glob 匹配仅在指定的父目录内进行，不会递归进入子目录
- 符号链接会被正常处理，遵循操作系统的文件权限控制

### 6.2 内存安全

- vincentlaucsb-csv-parser 经过广泛测试，避免了手工解析器可能存在的缓冲区溢出问题
- streaming 模式通过限制同时驻留内存的数据量，降低了内存耗尽风险

### 6.3 数据校验

- 多文件读取时自动校验列数一致性，防止数据格式错误导致的未定义行为
- 严格的配置校验在启动阶段捕获不兼容的配置组合

## 7. 兼容性

### 7.1 向后兼容性

本变更保持**完全向后兼容**：
1. **单文件路径**: 原有配置无需修改即可继续工作
2. **默认行为**: `loading_mode` 默认为 `preload`，与原有行为一致
3. **API 兼容**: CSVReader 类仍支持单文件路径构造函数

### 7.2 配置迁移

对于需要使用新功能的用户：
**启用多文件支持**（无需修改其他配置）:
```yaml
schema:
  from_csv:
    columns:
      file_path: /data/*.csv  # 改为 glob 模式
```

**启用 streaming 模式**（需确保不违反约束）:
```yaml
schema:
  from_csv:
    columns:
      file_path: /data/large_file.csv
      tbname_index: -1  # 必须设为 -1
  generation:
    loading_mode: streaming
```

## 8. 运维

### 8.1 部署影响

无特殊影响。

### 8.2 日志和监控

- 多文件读取时，文件列表在 DEBUG 级别日志中输出
- streaming 模式下，内存使用量显著降低，可通过系统监控观察到

### 8.3 故障排查

若遇到性能问题：
1. 检查是否意外使用了 preload 模式处理大文件
2. 确认文件路径解析结果是否符合预期（通过 DEBUG 日志）
3. 使用 streaming 模式时，确认未启用 tbname_index

## 9. 使用场景

### 9.1 Use Case 1: 大文件数据导入

**场景**: 用户有一个 10GB 的 CSV 文件需要导入 TDengine，但服务器只有 8GB 内存。
**解决方案**:
```yaml
schema:
  from_csv:
    columns:
      file_path: /data/large_dataset.csv
      tbname_index: -1  # streaming 模式必须设为 -1
  generation:
    loading_mode: streaming
    rows_per_table: 1000000
```

**效果**: 成功处理 10GB 文件，内存占用保持在数常数级别。

### 9.2 Use Case 2: 多文件批量导入

**场景**: 用户有按日期分割的多个 CSV 文件（`data_20240101.csv`, `data_20240102.csv`...），需要批量导入。
**解决方案**:
```yaml
schema:
  from_csv:
    columns:
      file_path: /data/data_*.csv  # 使用 glob 匹配所有文件
      has_header: true
```

**效果**: 无需手动合并文件，taosgen 自动按文件名顺序处理所有文件。

### 9.3 Use Case 3: 目录监控导入

**场景**: 用户的 ETL 流程定期将 CSV 文件输出到指定目录，需要批量导入。
**解决方案**:
```yaml
schema:
  from_csv:
    columns:
      file_path: /etl_output/  # 指定目录
```

**效果**: taosgen 自动识别目录下所有 `.csv` 文件并依次处理。

### 9.4 Use Case 4: 高性能小文件处理

**场景**: 用户有大量（数千个）小 CSV 文件需要快速处理。
**解决方案**:
```yaml
schema:
  from_csv:
    columns:
      file_path: /data/split/*.csv
  generation:
    loading_mode: preload  # 小文件使用 preload 可获得更好性能
    generate_threads: 16
```

**效果**: 新的 CSV 解析库提供更高吞吐量，多线程并行处理。

## 10. 约束和限制

### 10.1 约束

1. **多文件列一致性**: 所有匹配的 CSV 文件必须具有相同的列数
2. **streaming 模式限制**: 不能与 tbname_index >= 0 同时使用
3. **文件编码**: 当前仅支持 UTF-8 编码的 CSV 文件
4. **换行符**: 支持 LF (\n) 和 CRLF (\r\n) 两种换行格式

### 10.2 限制

1. **多文件排序**: 文件按文件名字母顺序排序，无法控制其他排序方式
2. **目录递归**: glob 和目录模式不递归进入子目录
3. **streaming 随机访问**: streaming 模式不支持随机访问或倒带（除 reset 外）

## 11. 常见错误和排查

### 11.1 错误: `No CSV files found matching path`

**原因**: 
- 目录为空或不存在
- glob 模式没有匹配到任何文件
**排查**:
```bash

## 12. 检查目录内容

ls -la /path/to/dir/

## 13. 测试 glob 模式

ls /path/to/data_*.csv
```


### 13.1 错误: `Column count mismatch`

**原因**: 多个 CSV 文件的列数不一致
**排查**:
```bash

## 14. 检查各文件的列数

head -1 file1.csv | tr ',' '\n' | wc -l
head -1 file2.csv | tr ',' '\n' | wc -l
```

### 14.1 错误: `loading_mode 'streaming' is incompatible with 'tbname_index'`

**原因**: streaming 模式下尝试按子表名分组加载数据
**解决方案**: 
- 改为 `loading_mode: preload`，或
- 将 `tbname_index` 设为 `-1`

### 14.2 性能问题: 大文件处理缓慢

**排查**:
1. 检查是否使用了 preload 模式处理大文件
2. 检查 DEBUG 日志确认文件路径解析正确

## 15. 可观测性

**DEBUG 级别**:
- 多文件模式下列表解析结果: `Resolved <n> CSV files from path: <path>`
- 文件切换: `Switching to next CSV file: <filename>`

## 16. 安装和卸载

### 16.1 依赖变更

**新增 Conan 依赖**:
```plaintext
vincentlaucsb-csv-parser/2.3.0
```

该依赖为 header-only 库，无需额外的运行时依赖。

### 16.2 构建步骤

无需变更，原有构建流程自动处理新依赖:
```bash
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

### 16.3 卸载

无需特殊卸载步骤。

## 17. 文档

### 17.1 需要更新**官网文档**

- 新增 `loading_mode` 参数说明
- 更新 `file_path` 参数描述，增加多文件支持说明
- 添加 streaming 模式使用指南

## 18. 参考文档

1. [vincentlaucsb-csv-parser GitHub](https://github.com/vincentlaucsb/csv-parser) - CSV 解析库文档
2. [taosgen 官网文档](https://docs.tdengine.com/tdengine-reference/tools/taosgen/) - 现有配置参数说明
