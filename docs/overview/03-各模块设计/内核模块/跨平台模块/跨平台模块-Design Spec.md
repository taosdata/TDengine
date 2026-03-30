# 跨平台模块-Design Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-14 | 2026-01-14 | 1.0 | 程洪泽 | 初始版本创建 |

## 2. 引言

### 2.1 目的

本文档描述 TDengine 跨平台编码模块的设计方案，包括架构设计、数据结构、接口规范、性能优化和安全考虑等。目标是提供一套完整、高效、可靠的跨平台编码解决方案，确保 TDengine 在不同操作系统和平台上正确处理字符编码和本地化设置。

### 2.2 范围

本文档涵盖以下内容：
1. 跨平台编码模块的整体架构设计
2. 关键数据结构和接口设计
3. 字符编码转换机制
4. 本地化支持实现
5. 平台特定适配方案
6. 性能优化和安全考虑

### 2.3 受众

- TDengine 开发人员：了解跨平台编码模块的实现细节
- 架构师：评估编码模块的设计合理性和扩展性
- 测试人员：根据设计文档编写测试用例
- 运维人员：了解编码模块的运行机制和故障排查方法

## 3. 术语

| 术语 | 定义 |
| --- | --- |
| 跨平台编码 | 在不同操作系统和平台上正确处理字符编码、字符串操作和本地化设置的能力 |
| UCS4 | Unicode 字符编码格式，使用 32 位表示每个字符，TDengine 内部使用的统一字符表示 |
| UTF-8 | Unicode 转换格式，使用 1-4 字节表示每个字符，互联网和 Unix-like 系统的标准编码 |
| iconv | 字符编码转换库，用于不同字符集之间的转换 |
| locale | 本地化设置，包括语言、地区、字符集、数字格式、日期格式等信息 |
| 宽字符 (wchar_t) | 平台相关的宽字符类型，用于表示 Unicode 字符 |
| 字符集 (charset) | 字符编码方案，如 UTF-8、GBK、ISO-8859-1 等 |
| BOM (Byte Order Mark) | 字节顺序标记，用于标识 Unicode 文本的字节顺序和编码格式 |

## 4. 概述

### 4.1 架构

跨平台编码模块采用分层架构设计，分为以下层次：
<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TB\n    A[应用层] --\u003e B[接口层]\n    B --\u003e C[核心功能层]\n    C --\u003e D[平台适配层]\n    C --\u003e E[第三方依赖层]\n    \n    A --\u003e A1[TDengine 核心模块]\n    A --\u003e A2[客户端工具]\n    A --\u003e A3[管理界面]\n    \n    B --\u003e B1[字符串操作接口]\n    B --\u003e B2[编码转换接口]\n    B --\u003e B3[本地化接口]\n    \n    C --\u003e C1[编码转换引擎]\n    C --\u003e C2[本地化管理器]\n    C --\u003e C3[字符串处理器]\n    C --\u003e C4[错误处理器]\n    \n    D --\u003e D1[Windows 适配]\n    D --\u003e D2[Linux 适配]\n    D --\u003e D3[macOS 适配]\n    \n    E --\u003e E1[iconv 库]\n    E --\u003e E2[系统 locale API]\n    E --\u003e E3[C 标准库]\n    \n    style A fill:#e1f5fe\n    style B fill:#f3e5f5\n    style C fill:#e8f5e8\n    style D fill:#fff3e0\n    style E fill:#fce4ec\n","theme":"default","view":"chart"}"/>

### 4.2 技术

#### 4.2.1 核心技术

1. **iconv 库**：用于字符编码转换
2. **C 标准库**：提供基础字符串操作函数
3. **系统 locale API**：获取和设置本地化配置
4. **Unicode 标准**：遵循 Unicode 标准处理字符

#### 4.2.2 开发技术

1. **C 语言**：主要开发语言
2. **CMake**：构建系统
3. **条件编译**：处理平台差异
4. **单元测试**：确保代码质量

### 4.3 依赖项

#### 4.3.1 内部依赖

1. **TDengine 公共头文件**：`os.h`, `tutil.h` 等
2. **错误处理模块**：统一的错误码体系
3. **内存管理模块**：内存分配和释放

#### 4.3.2 外部依赖

1. **iconv 库**：字符编码转换（系统提供或内置）
2. **C 标准库**：字符串操作、本地化函数
3. **操作系统 API**：平台特定的本地化和字符串函数

## 5. 设计考虑

### 5.1 假设和限制

#### 5.1.1 假设

1. 目标系统支持 C99 标准或更高版本
2. 系统提供 iconv 库或可以使用内置实现
3. 用户数据主要使用常见字符编码（UTF-8、GBK 等）
4. 内存资源足够处理常规的编码转换操作

#### 5.1.2 限制

1. 不支持非常古老的字符编码
2. 超大字符串（超过可用内存）可能需要特殊处理
3. 某些平台可能对特定编码的支持有限

### 5.2 设计模式和原则

#### 5.2.1 设计模式

1. **适配器模式**：封装不同平台的字符串和本地化 API，提供统一接口
2. **工厂模式**：管理 iconv 转换器的创建和复用
3. **单例模式**：全局的本地化配置和转换器池
4. **策略模式**：根据不同平台选择不同的实现策略

#### 5.2.2 设计原则

1. **单一职责原则**：每个函数只负责一个功能
2. **开闭原则**：对扩展开放，对修改封闭
3. **依赖倒置原则**：依赖抽象而非具体实现
4. **接口隔离原则**：接口小而专，避免臃肿

### 5.3 风险和缓解措施

#### 5.3.1 风险 1: 编码转换性能瓶颈

**风险描述**：频繁的编码转换可能成为性能瓶颈
**缓解措施**：
1. 实现转换器池，避免频繁创建和销毁
2. 支持批量转换，减少函数调用开销
3. 提供性能监控和调优选项

#### 5.3.2 风险 2: 内存安全问题

**风险描述**：编码转换可能引发缓冲区溢出或内存泄漏
**缓解措施**：
1. 所有函数都进行缓冲区边界检查
2. 使用安全的内存管理函数
3. 实现内存泄漏检测机制

#### 5.3.3 风险 3: 平台兼容性问题

**风险描述**：不同平台的字符编码支持存在差异
**缓解措施**：
1. 使用条件编译处理平台差异
2. 提供平台特定的后备实现
3. 进行全面的跨平台测试

## 6. 详细设计

### 6.1 组件设计

#### 6.1.1 编码转换引擎

**职责**：管理字符编码转换，包括 UCS4 与多字节字符串的相互转换。
**核心数据结构**：
```c
typedef struct {
  iconv_t conv;    // iconv 转换器
  int8_t  inUse;   // 使用状态（0: 未使用, 1: 使用中）
} SConv;

typedef struct {
  SConv  *gConv[CM_NUM];           // 转换器数组
  int32_t convUsed[CM_NUM];        // 使用计数
  int32_t gConvMaxNum[CM_NUM];     // 最大转换器数量
  char    charset[TD_CHARSET_LEN]; // 字符集名称
} SConvInfo;
```

**关键函数**：
- `taosMbsToUcs4()`: 多字节字符串转 UCS4
- `taosUcs4ToMbs()`: UCS4 转多字节字符串
- `taosAcquireConv()`: 获取转换器
- `taosReleaseConv()`: 释放转换器

#### 6.1.2 本地化管理器

**职责**：管理系统本地化配置，包括 locale 和字符集。
**核心函数**：
- `taosGetSystemLocale()`: 获取系统 locale 和字符集
- `taosSetSystemLocale()`: 设置系统 locale
- `taosCharsetReplace()`: 字符集名称规范化

#### 6.1.3 字符串处理器

**职责**：提供跨平台兼容的字符串操作函数。
**核心函数**：
- `tstrncpy()`: 安全字符串复制
- `tstrdup()`, `tstrndup()`: 字符串复制（带内存分配）
- `taosStr2int64()` 等: 字符串到数字的转换
- `taosStrCaseStr()`: 不区分大小写的字符串查找

#### 6.1.4 错误处理器

**职责**：统一错误处理，提供错误码和错误信息。
**核心机制**：
```c
// 全局错误变量
extern int32_t terrno;

// 错误码定义
#define TSDB_CODE_SUCCESS          0      // 成功
#define TSDB_CODE_INVALID_PARA     0x8001 // 参数无效
#define TSDB_CODE_OUT_OF_MEMORY    0x8002 // 内存不足
#define TSDB_CODE_APP_ERROR        0x8003 // 应用错误

// 系统错误转换
#define TAOS_SYSTEM_ERROR(err) ((err) | 0x8000)
```

### 6.2 关键数据结构

#### 6.2.1 TdUcs4 类型

```c
typedef int32_t TdUcs4;
```

**说明**：32 位 Unicode 字符，TDengine 内部统一的字符表示。

#### 6.2.2 TdWchar 类型

```c
typedef wchar_t TdWchar;
```

**说明**：平台相关的宽字符类型，用于与系统 API 交互。

#### 6.2.3 转换类型枚举

```c
typedef enum { M2C = 0, C2M, CM_NUM } ConvType;
```

**说明**：
- `M2C`: 多字节字符串到 UCS4 的转换
- `C2M`: UCS4 到多字节字符串的转换
- `CM_NUM`: 转换类型数量

### 6.3 数据库设计（如适用）

不适用，跨平台编码模块不涉及数据库设计。

### 6.4 图表解释

#### 6.4.1 数据流图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph LR\n    A[客户端输入] --\u003e B[多字节字符串]\n    B --\u003e C{编码转换}\n    C --\u003e|转换为 UCS4| D[UCS4 内部表示]\n    D --\u003e E[TDengine 处理]\n    E --\u003e F{编码转换}\n    F --\u003e|转换为目标编码| G[多字节字符串输出]\n    G --\u003e H[客户端显示]\n    \n    I[系统 locale] --\u003e J[本地化管理器]\n    J --\u003e C\n    J --\u003e F\n    \n    K[iconv 转换器池] --\u003e C\n    K --\u003e F\n    \n    style A fill:#e1f5fe\n    style H fill:#e1f5fe\n    style D fill:#e8f5e8\n    style E fill:#e8f5e8\n","theme":"default","view":"chart"}"/>

#### 6.4.2 消息序列图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"sequenceDiagram\n    participant Client as 客户端\n    participant API as 编码接口层\n    participant Conv as 编码转换引擎\n    participant Pool as 转换器池\n    participant iconv as iconv 库\n    \n    Client-\u003e\u003eAPI: taosMbsToUcs4(mbs, charset)\n    API-\u003e\u003eConv: 请求编码转换\n    Conv-\u003e\u003ePool: taosAcquireConv(M2C)\n    Pool-\u003e\u003eiconv: iconv_open(charset, \"UCS-4\")\n    iconv--\u003e\u003ePool: 转换器句柄\n    Pool--\u003e\u003eConv: 转换器\n    Conv-\u003e\u003eiconv: iconv() 执行转换\n    iconv--\u003e\u003eConv: 转换结果\n    Conv-\u003e\u003ePool: taosReleaseConv()\n    Conv--\u003e\u003eAPI: UCS4 数据\n    API--\u003e\u003eClient: 转换成功\n","theme":"default","view":"chart"}"/>

#### 6.4.3 转换器池管理流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TD\n    A[开始获取转换器] --\u003e B{转换器池已初始化?}\n    B --\u003e|否| C[初始化转换器池]\n    C --\u003e D[分配转换器数组]\n    D --\u003e E[创建初始转换器]\n    E --\u003e F\n    B --\u003e|是| F{查找可用转换器}\n    F --\u003e|找到| G[标记为使用中]\n    G --\u003e H[返回转换器]\n    F --\u003e|未找到| I{已达最大数量?}\n    I --\u003e|否| J[创建新转换器]\n    J --\u003e G\n    I --\u003e|是| K[等待其他线程释放]\n    K --\u003e F\n    \n    L[开始释放转换器] --\u003e M{转换器来自池?}\n    M --\u003e|是| N[标记为未使用]\n    N --\u003e O[减少使用计数]\n    M --\u003e|否| P[直接关闭转换器]\n    P --\u003e Q[结束]\n    O --\u003e Q\n","theme":"default","view":"chart"}"/>

## 7. 接口规范

### 7.1 API 文档

#### 7.1.1 字符编码转换 API

##### 7.1.1.1 taosMbsToUcs4

```c
/**
 * 将多字节字符串转换为 UCS4 编码
 * 
 * @param mbs 输入的多字节字符串
 * @param mbsLength 输入字符串长度（字节数）
 * @param ucs4 输出的 UCS4 缓冲区
 * @param ucs4_max_len UCS4 缓冲区最大长度（字节数）
 * @param len 实际转换的 UCS4 字符数（输出参数，可为 NULL）
 * @param charsetCxt 字符集上下文，可为 NULL（使用全局上下文）
 * @return true 成功，false 失败
 */
bool taosMbsToUcs4(const char *mbs, size_t mbsLength, TdUcs4 *ucs4, 
                   int32_t ucs4_max_len, int32_t *len, void* charsetCxt);
```

##### 7.1.1.2 taosUcs4ToMbs

```c
/**
 * 将 UCS4 编码转换为多字节字符串
 * 
 * @param ucs4 输入的 UCS4 缓冲区
 * @param ucs4_max_len UCS4 缓冲区长度（字节数）
 * @param mbs 输出的多字节字符串缓冲区
 * @param charsetCxt 字符集上下文，可为 NULL（使用全局上下文）
 * @return 成功时返回写入的字节数，失败时返回错误码（负数）
 */
int32_t taosUcs4ToMbs(TdUcs4 *ucs4, int32_t ucs4_max_len, char *mbs, void* charsetCxt);
```

#### 7.1.2 字符串操作 API

##### 7.1.2.1 tstrncpy

```c
/**
 * 安全字符串复制宏
 * 确保目标缓冲区以 null 结尾，防止缓冲区溢出
 * 
 * @param dst 目标缓冲区
 * @param src 源字符串
 * @param size 目标缓冲区大小
 */
#define tstrncpy(dst, src, size)         \
  do {                                   \
    (void)strncpy((dst), (src), (size)); \
    (dst)[(size)-1] = 0;                 \
  } while (0)
```

##### 7.1.2.2 tstrdup

```c
/**
 * 复制字符串（分配内存）
 * 
 * @param src 源字符串
 * @return 新分配的字符串，失败返回 NULL
 */
char *tstrdup(const char *src);
```

#### 7.1.3 本地化 API

##### 7.1.3.1 taosGetSystemLocale

```c
/**
 * 获取系统 locale 和字符集
 * 
 * @param outLocale 输出的 locale 字符串缓冲区（至少 TD_LOCALE_LEN 字节）
 * @param outCharset 输出的字符集字符串缓冲区（至少 TD_CHARSET_LEN 字节）
 */
void taosGetSystemLocale(char *outLocale, char *outCharset);
```

##### 7.1.3.2 taosSetSystemLocale

```c
/**
 * 设置系统 locale
 * 
 * @param inLocale 要设置的 locale 字符串
 * @return 0 成功，非 0 错误码
 */
int32_t taosSetSystemLocale(const char *inLocale);
```

#### 7.1.4 工具函数 API

##### 7.1.4.1 taosHexEncode

```c
/**
 * 将二进制数据编码为十六进制字符串
 * 
 * @param src 输入的二进制数据
 * @param dst 输出的十六进制字符串缓冲区
 * @param len 输入数据长度（字节数）
 * @param bufSize 输出缓冲区大小
 * @return 0 成功，非 0 错误码
 */
int32_t taosHexEncode(const unsigned char *src, char *dst, int32_t len, int32_t bufSize);
```

### 7.2 用户界面（如适用）

不适用，跨平台编码模块是底层库，不直接提供用户界面。

## 8. 安全考虑（如适用）

### 8.1 安全要求

#### 8.1.1 输入验证

1. 验证所有输入参数的有效性
2. 检查字符串长度，防止缓冲区溢出
3. 验证字符编码名称，防止注入攻击

#### 8.1.2 内存安全

1. 检查内存分配是否成功
2. 确保所有分配的内存都被正确释放
3. 使用安全的内存操作函数

#### 8.1.3 错误处理安全

1. 错误信息不泄露敏感数据
2. 提供安全的错误恢复机制
3. 防止错误处理过程中的二次错误

### 8.2 漏洞缓解

#### 8.2.1 缓冲区溢出缓解

1. 所有字符串操作都进行边界检查
2. 使用安全的字符串函数替代不安全的函数
3. 对输入字符串长度进行合理限制

#### 8.2.2 内存泄漏缓解

1. 使用内存检测工具定期检查
2. 实现资源自动管理机制
3. 提供内存泄漏检测接口

#### 8.2.3 编码注入缓解

1. 严格验证输入的字符编码名称
2. 限制可用的字符编码范围
3. 对转换结果进行有效性检查

## 9. 性能和可扩展性（如适用）

### 9.1 性能要求

#### 9.1.1 编码转换性能

1. **转换速度**：单个字符编码转换操作应在微秒级别完成
2. **吞吐量**：支持高并发的编码转换请求
3. **内存效率**：编码转换过程中的内存使用应合理控制
4. **CPU 使用**：编码转换不应成为 CPU 瓶颈

#### 9.1.2 性能优化策略

1. **转换器池**：复用 iconv 转换器，避免频繁创建和销毁
2. **批量处理**：支持批量字符串转换，减少函数调用开销
3. **缓存机制**：缓存常用编码的转换器
4. **异步处理**：支持异步编码转换，避免阻塞主线程

### 9.2 可扩展性

#### 9.2.1 水平扩展

1. **多线程支持**：转换器池支持多线程并发访问
2. **分布式扩展**：支持在分布式环境中部署编码转换服务
3. **负载均衡**：支持多个编码转换实例间的负载均衡

#### 9.2.2 垂直扩展

1. **资源优化**：根据系统资源动态调整转换器池大小
2. **内存管理**：支持大内存系统的优化利用
3. **CPU 亲和性**：支持绑定 CPU 核心，提高缓存命中率

## 10. 部署和配置

### 10.1 部署流程

#### 10.1.1 编译部署

1. **依赖检查**：检查系统是否安装 iconv 库
2. **编译选项**：通过 CMake 选项控制编码功能
3. **安装部署**：作为 TDengine 核心部分一起部署

#### 10.1.2 运行时部署

1. **动态加载**：支持运行时加载编码转换插件
2. **热更新**：支持编码模块的热更新
3. **回滚机制**：支持部署失败时的回滚

### 10.2 版本控制

#### 10.2.1 向后兼容性

1. **API 兼容**：保持公共 API 的向后兼容性
2. **数据兼容**：确保新版本能够处理旧版本的数据
3. **配置兼容**：新版本兼容旧版本的配置文件

#### 10.2.2 发布说明

1. **版本号**：遵循语义化版本控制
2. **变更日志**：详细记录每个版本的变更
3. **迁移指南**：提供版本迁移的指导文档

#### 10.2.3 回滚策略

1. **备份机制**：部署前备份重要数据和配置
2. **快速回滚**：支持快速回滚到上一个稳定版本
3. **数据恢复**：确保回滚过程中的数据完整性

## 11. 监控和维护

### 11.1 日志记录和诊断

#### 11.1.1 日志级别

1. **错误日志**：记录编码转换失败和异常
2. **警告日志**：记录潜在问题和性能警告
3. **信息日志**：记录重要的编码转换操作
4. **调试日志**：记录详细的调试信息（开发环境）

#### 11.1.2 诊断工具

1. **编码检测工具**：检测字符串的编码格式
2. **性能分析工具**：分析编码转换的性能瓶颈
3. **内存检查工具**：检测内存泄漏和越界访问

## 12. 参考资料

无。

## 13. 附录

无。
