---
name: tsdb-test-unittest-tmsg
description: "为 tmsg.c 中的结构体自动生成序列化/反序列化的 Google Test 单元测试代码。触发关键词: tmsg, 序列化测试, 单元测试, serialize, deserialize"
metadata:
  author: mmwang
  version: 1.0.0
  owner_team: engine
---

# tsdb-test-unittest-tmsg

## When to Use

当用户需要为 `tmsg.c` 中的结构体生成序列化和反序列化单元测试时使用此技能。

触发场景：
- 用户提供结构体名称，要求生成序列化/反序列化测试
- 用户说"为 XXX 结构体写单测"
- 用户说"生成 XXX 的序列化测试"

前置条件：
- 目标结构体在 `tmsg.c` 中已有 `tSerialize<StructName>` 和 `tDeserialize<StructName>` 函数
- 项目已集成 Google Test
- `tmsg.c` 同模块的 `test/tmsgTest.cpp` 文件已存在

## Input

必需信息：
- 结构体名称（如 `SVDeleteReq`、`SCreateDbReq`）

可选信息：
- 是否需要错误路径测试（默认：否）

## Output

生成两个文件操作：

1. **新建文件**：`test/<StructName>.cpp`
   - 路径规则：以 `tmsg.c` 所在目录为基准，向上两级，进入 `test/` 子目录
   - 示例：`tmsg.c` 在 `source/common/src/msg/tmsg.c`，则生成 `source/common/test/<StructName>.cpp`

2. **修改文件**：`test/tmsgTest.cpp`
   - 在 `main()` 函数定义行上方插入：`#include "<StructName>.cpp"`

测试文件结构：
```cpp
#include <gtest/gtest.h>
#include <vector>
#include <cstring>
#include "tmsg.h"

TEST(td_msg_test, <struct_name_lower>_codec) {
  // 1. 构造结构体并赋测试值
  // 2. 序列化（两阶段：先取长度，再填充缓冲区）
  // 3. 反序列化到新结构体
  // 4. 逐字段断言
  // 5. 释放动态分配的内存
}
```

## Execution Steps

### 步骤 1：定位 tmsg.c 并查找目标函数

1. 使用 Glob 工具查找 `tmsg.c`：
   ```
   pattern: **/tmsg.c
   ```

2. 使用 Grep 工具搜索序列化/反序列化函数：
   - 序列化函数：`tSerialize<StructName>`
   - 反序列化函数：`tDeserialize<StructName>`

3. 读取函数实现，关注：
   - 函数参数列表
   - 返回值语义
   - 动态分配内存的字段（需要 `taosMemoryFree` 释放）

4. 读取结构体定义（通常在对应 `.h` 头文件中）

### 步骤 2：确定测试文件路径

1. 以 `tmsg.c` 所在目录为基准，向上两级，进入 `test/` 子目录
2. 测试文件命名：`<StructName>.cpp`
3. 需要 include 到 `tmsgTest.cpp` 中

### 步骤 3：生成测试文件

生成 `test/<StructName>.cpp`，包含：

**测试函数命名**：
- `TEST(td_msg_test, <struct_name_lower>_codec)`
- 示例：`SVDeleteReq` → `sv_delete_req_codec`

**字段赋值规则**：
- 根据结构体定义为每个字段设置测试值
- 整型赋非零值
- 字符串赋字面量
- 布尔赋 `true/false`

**断言选择**：
- `int8_t/int32_t/uint64_t` 等整型 → `ASSERT_EQ`
- `char*/const char*` 字符串 → `ASSERT_STREQ`
- `float` → `ASSERT_FLOAT_EQ`
- `double` → `ASSERT_DOUBLE_EQ`
- 二进制 blob → `ASSERT_EQ(memcmp(...), 0)`

**内存管理**：
- 所有通过 `tDecodeCStr`/`tDecodeBinary` 分配的字段
- 必须在测试结束前调用 `taosMemoryFree` 释放

### 步骤 4：修改 tmsgTest.cpp

1. 读取 `tmsgTest.cpp` 完整内容
2. 找到 `int main(` 所在行
3. 在该行上方插入：`#include "<StructName>.cpp"`
4. 如果已有其他 `#include "xxx.cpp"` 行，则紧随其后插入

### 步骤 5：验证提示

告知用户编译运行命令：
```bash
cd build
cmake --build . --target tmsgTest
ctest -R tmsgTest
```

## Safety

- **只读** `tmsg.c`，不对其做任何修改
- 若找不到对应的序列化/反序列化函数，列出相近函数名供参考
- 生成的代码使用中文注释，与项目风格保持一致
- 不执行任何破坏性操作

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-test-unittest-tmsg version=0.1.0 author=mmwang`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

