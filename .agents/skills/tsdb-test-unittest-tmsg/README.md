# tmsg 序列化测试生成技能使用说明

## 简介

`SKILL.md` 定义了一个 Claude Code 技能，用于自动为 `tmsg.c` 中的任意结构体生成序列化与反序列化的 Google Test 单元测试代码，并将其自动挂载到现有测试入口文件中。

---

## 前置条件

| 条件 | 说明 |
|------|------|
| 目标结构体 | 在 `tmsg.c` 中已存在对应的 `tSerialize<StructName>` 和 `tDeserialize<StructName>` 函数 |
| 测试框架 | 项目已集成 Google Test（`gtest`） |
| 测试入口 | `tmsg.c` 同模块的 `test/tmsgTest.cpp` 文件已存在 |

---

## 使用方法

在 Claude Code 中直接用自然语言描述需求即可触发该技能，例如：

```
为结构体 SVDeleteReq 生成序列化/反序列化单元测试
```

```
帮我给 SCreateDbReq 写序列化的单测
```

---

## 技能执行流程

```
用户提供结构体名
        │
        ▼
① 在 tmsg.c 中定位 tSerialize / tDeserialize 函数
        │
        ▼
② 读取结构体定义，分析字段类型
        │
        ▼
③ 生成 test/<StructName>.cpp（Google Test 单测文件）
        │
        ▼
④ 在 tmsgTest.cpp 的 main() 函数上方插入 #include "<StructName>.cpp"
        │
        ▼
⑤ 告知用户文件路径及编译验证命令
```

---

## 生成文件说明

### 新建文件：`test/<StructName>.cpp`

路径规则：以 `tmsg.c` 所在目录为基准，向上两级，再进入 `test/` 子目录。

```
tmsg.c 路径：source/common/src/msg/tmsg.c
生成位置：  source/common/test/<StructName>.cpp
```

测试文件结构如下：

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

### 修改文件：`test/tmsgTest.cpp`

在 `main()` 函数定义行的**正上方**插入：

```cpp
#include "<StructName>.cpp"
```

---

## 断言规则速查

| 字段类型 | 使用的断言 |
|----------|-----------|
| `int8_t` / `int32_t` / `uint64_t` 等整型 | `ASSERT_EQ` |
| `char*` / `const char*` 字符串 | `ASSERT_STREQ` |
| `float` | `ASSERT_FLOAT_EQ` |
| `double` | `ASSERT_DOUBLE_EQ` |
| 二进制 blob | `ASSERT_EQ(memcmp(...), 0)` |

---

## 内存管理说明

反序列化函数内部会通过 `tDecodeCStr` / `tDecodeBinary` 为字符串和二进制字段动态分配内存，生成的测试代码会在测试结束前自动调用 `taosMemoryFree` 释放，防止内存泄漏。

---

## 编译与运行测试

```bash
cd build
cmake --build . --target tmsgTest
ctest -R tmsgTest
```

---

## 注意事项

- 技能**只读** `tmsg.c`，不会对其做任何修改。
- 若目标结构体在 `tmsg.c` 中不存在对应的序列化函数，技能会列出相近的函数名供参考。
- 若结构体包含嵌套结构体，技能会递归分析嵌套字段并生成完整的测试值和断言。
