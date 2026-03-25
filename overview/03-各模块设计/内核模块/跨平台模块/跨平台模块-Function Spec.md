# 跨平台模块-Function Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-14 | 2026-01-14 | 1.0 | 程洪泽 | 初始版本创建 |

## 2. 背景

TDengine 作为高性能时序数据库，需要支持全球范围内的部署和使用。不同国家和地区使用不同的字符编码和本地化设置，这给跨平台开发带来了挑战：
1. **字符编码多样性**：不同平台默认使用不同的字符编码（如 Windows 使用 GBK/CP936，Linux/macOS 使用 UTF-8）
2. **本地化差异**：不同地区的数字格式、日期格式、排序规则等存在差异
3. **平台 API 不一致**：不同操作系统提供的字符串处理和本地化 API 存在差异
4. **性能要求**：编码转换操作需要高效，不能成为性能瓶颈
跨平台编码功能模块旨在解决这些问题，提供一套统一、高效、可靠的字符编码和本地化处理方案，确保 TDengine 在不同平台上的一致性和兼容性。

## 3. 定义

**跨平台编码**：在不同操作系统和平台上正确处理字符编码、字符串操作和本地化设置的能力
- **UCS4**：Unicode 字符编码格式，使用 32 位表示每个字符，TDengine 内部使用的统一字符表示
- **UTF-8**：Unicode 转换格式，使用 1-4 字节表示每个字符，互联网和 Unix-like 系统的标准编码
- **iconv**：字符编码转换库，用于不同字符集之间的转换
- **locale**：本地化设置，包括语言、地区、字符集、数字格式、日期格式等信息
- **宽字符 (wchar_t)**：平台相关的宽字符类型，用于表示 Unicode 字符
- **字符集 (charset)**：字符编码方案，如 UTF-8、GBK、ISO-8859-1 等

## 4. 行为说明

### 4.1 字符编码转换行为

#### 4.1.1 UCS4 与多字节字符串转换

TDengine 使用 UCS4 作为内部统一的字符表示，所有外部字符串在进入系统时转换为 UCS4，输出时再转换为目标编码。
```c
// 多字节字符串转换为 UCS4
bool taosMbsToUcs4(const char *mbs, size_t mbsLength, TdUcs4 *ucs4, 
                   int32_t ucs4_max_len, int32_t *len, void* charsetCxt);

// UCS4 转换为多字节字符串
int32_t taosUcs4ToMbs(TdUcs4 *ucs4, int32_t ucs4_max_len, char *mbs, void* charsetCxt);
```

**转换流程**：
1. 获取或创建 iconv 转换器
2. 执行编码转换
3. 处理转换错误
4. 释放转换器资源

#### 4.1.2 宽字符处理

针对需要直接处理宽字符的场景，提供宽字符相关函数：
```c
// 宽字符宽度计算（用于显示）
int32_t taosWcharWidth(TdWchar wchar);
int32_t taosWcharsWidth(TdWchar *pWchar, int32_t size);

// 多字节字符串与宽字符转换
int32_t taosMbToWchar(TdWchar *pWchar, const char *pStr, int32_t size);
int32_t taosMbsToWchars(TdWchar *pWchars, const char *pStrs, int32_t size);
int32_t taosWcharToMb(char *pStr, TdWchar wchar);
```

### 4.2 字符串操作行为

#### 4.2.1 安全字符串函数

提供跨平台兼容的安全字符串操作函数，避免缓冲区溢出：
```c
// 安全字符串复制（确保目标缓冲区以 null 结尾）
#define tstrncpy(dst, src, size)         \
  do {                                   \
    (void)strncpy((dst), (src), (size)); \
    (dst)[(size)-1] = 0;                 \
  } while (0)

// 字符串复制（带内存分配）
char *tstrdup(const char *src);
char *tstrndup(const char *str, int64_t size);
```

#### 4.2.2 数字字符串转换

提供安全的字符串到数字的转换函数，统一错误处理：
```c
// 字符串转换为各种整数类型
int32_t taosStr2int64(const char *str, int64_t *val);
int32_t taosStr2int32(const char *str, int32_t *val);
int32_t taosStr2int16(const char *str, int16_t *val);
int32_t taosStr2int8(const char *str, int8_t *val);

// 字符串转换为各种无符号整数类型
int32_t taosStr2Uint64(const char *str, uint64_t *val);
int32_t taosStr2Uint32(const char *str, uint32_t *val);
int32_t taosStr2Uint16(const char *str, uint16_t *val);
int32_t taosStr2Uint8(const char *str, uint8_t *val);

// 直接转换函数（返回转换结果）
int64_t taosStr2Int64(const char *str, char **pEnd, int32_t radix);
double taosStr2Double(const char *str, char **pEnd);
float taosStr2Float(const char *str, char **pEnd);
```

### 4.3 本地化支持行为

#### 4.3.1 系统 locale 管理

```c
// 获取系统 locale 和字符集
void taosGetSystemLocale(char *outLocale, char *outCharset);

// 设置系统 locale
int32_t taosSetSystemLocale(const char *inLocale);

// 字符集名称规范化
char *taosCharsetReplace(char *charsetstr);
```

**locale 获取流程**：
1. 调用 `setlocale(LC_CTYPE, "")` 获取系统当前 locale
2. 从 locale 字符串中提取字符集部分
3. 规范化字符集名称（如 "utf8" -> "UTF-8"）
4. 如果获取失败，使用默认值 "en_US.UTF-8"

#### 4.3.2 字符集验证

```c
// 验证字符编码名称是否有效
bool taosValidateEncodec(const char *encodec);
```

验证流程：尝试使用 iconv 打开该编码的转换器，如果成功则说明编码有效。

### 4.4 编码工具行为

#### 4.4.1 十六进制编码/解码

```c
// 二进制数据编码为十六进制字符串
int32_t taosHexEncode(const unsigned char *src, char *dst, int32_t len, int32_t bufSize);

// 十六进制字符串解码为二进制数据
int32_t taosHexDecode(const char *src, char *dst, int32_t len);

// 十六进制与 ASCII 互转
int32_t taosHex2Ascii(const char *z, uint32_t n, void **data, uint32_t *size);
int32_t taosAscii2Hex(const char *z, uint32_t n, void **data, uint32_t *size);
```

#### 4.4.2 字符编码转换器管理

```c
// 获取编码转换器
iconv_t taosAcquireConv(int32_t *idx, ConvType type, void* charsetCxt);

// 释放编码转换器
void taosReleaseConv(int32_t idx, iconv_t conv, ConvType type, void *charsetCxt);
```

**转换器管理机制**：
1. 维护一个转换器池，避免频繁创建和销毁
2. 支持多线程并发访问
3. 使用引用计数管理转换器使用状态

### 4.5 平台特定行为

#### 4.5.1 Windows 平台特殊处理

```c
// Windows 平台特有的字符串函数实现
#ifdef WINDOWS
char *strsep(char **stringp, const char *delim);
char *taosStrndupi(const char *s, int64_t size);
char *stpncpy(char *dest, const char *src, int n);
#endif
```

#### 4.5.2 Unix-like 平台优化

```c
// Linux/macOS 平台使用系统提供的函数
#ifndef WINDOWS
char *tstrndup(const char *str, int64_t size) {
  char *p = strndup(str, size);
  if (str != NULL && NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return p;
}
#endif
```

### 4.6 错误处理行为

所有函数都使用统一的错误码体系：
```c
// 错误码定义（部分）
#define TSDB_CODE_SUCCESS          0      // 成功
#define TSDB_CODE_INVALID_PARA     0x8001 // 参数无效
#define TSDB_CODE_OUT_OF_MEMORY    0x8002 // 内存不足
#define TSDB_CODE_APP_ERROR        0x8003 // 应用错误

// 设置错误码
extern int32_t terrno;

// 系统错误转换
#define TAOS_SYSTEM_ERROR(err) ((err) | 0x8000)
```

错误处理原则：
1. 函数返回错误码，0 表示成功
2. 设置全局错误变量 `terrno`
3. 提供详细的错误信息

## 5. 性能

### 5.1 编码转换性能优化

1. **转换器池**：避免频繁创建和销毁 iconv 转换器，提高性能
2. **并发访问**：转换器池支持多线程并发访问，减少锁竞争
3. **内存复用**：尽可能复用内存缓冲区，减少内存分配
4. **批量处理**：支持批量字符串转换，减少函数调用开销

### 5.2 内存使用优化

1. **缓冲区管理**：合理设置缓冲区大小，避免内存浪费
2. **内存池**：对频繁分配的小内存块使用内存池
3. **及时释放**：确保转换器和其他资源及时释放

### 5.3 性能监控指标

1. **编码转换耗时**：统计各种编码转换操作的平均耗时
2. **内存使用量**：监控编码转换过程中的内存使用情况
3. **并发性能**：测试多线程并发编码转换的性能表现
4. **缓存命中率**：监控转换器池的缓存命中率

## 6. 安全

### 6.1 缓冲区安全

1. **边界检查**：所有字符串操作函数都进行缓冲区边界检查
2. **空字符终止**：确保所有字符串以空字符终止
3. **长度限制**：对输入字符串长度进行合理限制

### 6.2 输入验证

1. **编码名称验证**：对输入的字符编码名称进行严格验证
2. **字符串有效性**：检查输入字符串的编码有效性
3. **参数范围检查**：对所有函数参数进行范围检查

### 6.3 内存安全

1. **内存分配检查**：检查内存分配是否成功
2. **内存释放**：确保所有分配的内存都被正确释放
3. **内存初始化**：对新分配的内存进行初始化

### 6.4 错误处理安全

1. **错误信息安全**：错误信息不泄露敏感信息
2. **错误恢复**：提供错误恢复机制，避免系统崩溃
3. **日志安全**：日志记录不包含敏感数据

## 7. 兼容性

### 7.1 平台兼容性

| 平台 | 支持状态 | 说明 |
| --- | --- | --- |
| Linux | 完全支持 | 支持主流发行版（Ubuntu、CentOS、Debian 等） |
| macOS | 完全支持 | 支持 macOS 10.14+ |
| Windows | 完全支持 | 支持 Windows 10+，提供特殊实现 |
| ARM Linux | 支持 | 支持 ARM64、ARMv7 架构 |

### 7.2 编译器兼容性

| 编译器 | 支持状态 | 说明 |
| --- | --- | --- |
| GCC | 完全支持 | 支持 GCC 7.5+ |
| Clang | 完全支持 | 支持 Clang 10+ |
| MSVC | 完全支持 | 支持 MSVC 2019+ |

### 7.3 编码兼容性

| 字符编码 | 支持状态 | 说明 |
| --- | --- | --- |
| UTF-8 | 完全支持 | 默认编码，推荐使用 |
| GBK/GB2312 | 支持 | 中文 Windows 默认编码 |
| BIG5 | 支持 | 繁体中文编码 |
| ISO-8859-1 | 支持 | 西欧语言编码 |
| UTF-16 | 支持 | Windows 内部使用 |
| UCS4 | 支持 | TDengine 内部使用 |

### 7.4 版本兼容性

保持 API 向后兼容，确保旧版本代码可以正常编译和运行。如果必须进行不兼容的更改，需要提供迁移指南和兼容层。

## 8. 运维

### 8.1 部署运维

1. **简化部署**：跨平台编码模块作为 TDengine 核心部分，无需额外部署
2. **配置管理**：支持通过配置文件调整编码相关参数
3. **版本管理**：提供编码模块版本信息查询接口

### 8.2 监控告警

1. **性能监控**：监控编码转换的性能指标
2. **错误监控**：监控编码转换错误率
3. **资源监控**：监控编码转换的内存和 CPU 使用

### 8.3 故障排查

1. **日志记录**：详细的编码转换日志，便于问题排查
2. **诊断工具**：提供编码诊断工具，检查编码问题
3. **问题排查指南**：提供常见编码问题的排查指南

## 9. 使用场景

### 9.1 多语言数据存储

**场景描述**：用户需要存储包含中文、日文、韩文等多语言字符的时间序列数据。
**解决方案**：
1. 客户端将数据转换为 UTF-8 编码发送到服务器
2. 服务器将 UTF-8 转换为 UCS4 进行内部处理
3. 查询时再将 UCS4 转换为客户端请求的编码

### 9.2 跨平台数据迁移

**场景描述**：用户需要将数据从 Windows 系统迁移到 Linux 系统。
**解决方案**：
1. 导出时使用 UTF-8 编码确保跨平台兼容性
2. 导入时自动检测源数据编码并进行转换
3. 确保数据在不同平台间的一致性

### 9.3 国际化界面

**场景描述**：TDengine 客户端工具需要支持多语言界面。
**解决方案**：
1. 使用 UTF-8 编码存储界面文本
2. 根据系统 locale 自动选择界面语言
3. 正确处理界面中的多语言字符显示

### 9.4 数据导出导入

**场景描述**：用户需要将数据导出为 CSV 文件，并在不同软件中打开。
**解决方案**：
1. 支持导出为多种编码格式（UTF-8、GBK、UTF-16 等）
2. 自动添加 BOM（字节顺序标记）帮助软件识别编码
3. 提供编码检测工具帮助用户确定文件编码

## 10. 约束和限制

### 10.1 约束

1. **iconv 依赖**：需要系统安装 iconv 库或使用内置实现
2. **内存限制**：大字符串编码转换需要较多内存
3. **性能限制**：频繁的编码转换可能影响性能

### 10.2 限制

1. **编码支持**：依赖 iconv 库支持的编码范围
2. **平台差异**：某些编码在某些平台上可能不可用
3. **字符串长度**：超长字符串可能需要分段处理

## 11. 常见错误和排查

### 11.1 构建错误

#### 11.1.1 错误 1: iconv 库未找到

**错误信息**: `error: iconv.h: No such file or directory`
**可能原因**: 系统未安装 iconv 开发库
**解决方案**:
- Ubuntu/Debian: `sudo apt-get install libiconv-dev`
- CentOS/RHEL: `sudo yum install libiconv-devel`
- macOS: `brew install libiconv`

#### 11.1.2 错误 2: 编码转换失败

**错误信息**: `Failed to convert string encoding`
**可能原因**: 不支持的字符编码或无效的输入
**解决方案**:
1. 检查输入的字符编码名称是否正确
2. 验证输入字符串的编码是否有效
3. 尝试使用 UTF-8 编码

### 11.2 运行时错误

#### 11.2.1 错误 1: 内存分配失败

**错误信息**: `Out of memory when converting string`
**可能原因**: 系统内存不足或字符串过长
**解决方案**:
1. 检查系统内存使用情况
2. 减少同时处理的字符串数量
3. 增加系统内存或优化程序内存使用

#### 11.2.2 错误 2: 编码不一致

**错误信息**: `Invalid byte sequence in conversion input`
**可能原因**: 实际编码与声明的编码不一致
**解决方案**:
1. 使用编码检测工具确定实际编码
2. 统一使用 UTF-8 编码
3. 添加 BOM 标记帮助识别编码

## 12. 可观测性

### 12.1 日志记录

编码模块提供详细的日志记录，包括：
1. **编码转换日志**：记录编码转换操作和结果
2. **性能日志**：记录编码转换的耗时和资源使用
3. **错误日志**：记录编码转换错误和异常

### 12.2 监控指标

提供以下监控指标：
1. **编码转换次数**：统计各种编码转换操作的次数
2. **转换成功率**：统计编码转换的成功率
3. **平均转换时间**：统计编码转换的平均耗时
4. **内存使用量**：监控编码转换过程中的内存使用

### 12.3 诊断工具

提供编码诊断工具，包括：
1. **编码检测工具**：自动检测字符串的编码格式
2. **转换验证工具**：验证编码转换的正确性
3. **性能分析工具**：分析编码转换的性能瓶颈
4. **内存检查工具**：检测编码转换过程中的内存问题

### 12.4 告警机制

1. **性能告警**：当编码转换性能低于阈值时触发告警
2. **错误率告警**：当编码转换错误率超过阈值时触发告警
3. **资源告警**：当内存或 CPU 使用超过阈值时触发告警
4. **健康检查告警**：当编码模块健康检查失败时触发告警

## 13. 安装和卸载

### 13.1 安装要求

#### 13.1.1 系统要求

- **Linux**: glibc 2.17+，内核 3.10+
- **macOS**: 10.14+
- **Windows**: Windows 10+

#### 13.1.2 依赖要求

- **iconv 库**: 系统提供或内置实现
- **C 编译器**: GCC 7.5+ / Clang 10+ / MSVC 2019+
- **构建工具**: CMake 3.16+

### 13.2 安装脚本

跨平台编码模块作为 TDengine 核心部分一起安装：
```bash

## 14. 构建 TDengine（包含编码模块）

mkdir build && cd build
cmake ..
make -j$(nproc)

## 15. 安装

sudo make install
```

### 15.1 卸载脚本

```bash

## 16. 卸载 TDengine（包含编码模块）

sudo make uninstall

## 17. 清理构建文件

rm -rf build
```

## 18. 文档

### 18.1 用户文档

1. **API 文档**：提供完整的 API 参考文档
2. **使用指南**：提供编码模块的使用指南
3. **配置指南**：提供配置参数说明
4. **故障排查**：提供常见问题解决方案

### 18.2 开发者文档

1. **架构设计**：提供模块架构设计文档
2. **代码规范**：提供编码规范和最佳实践
3. **测试指南**：提供单元测试和集成测试指南
4. **贡献指南**：提供代码贡献指南

### 18.3 运维文档

1. **部署指南**：提供部署和配置指南
2. **监控指南**：提供监控和告警配置指南
3. **维护指南**：提供日常维护和故障处理指南
4. **升级指南**：提供版本升级指南

## 19. 参考文档

1. **TDengine 源码**：
  - `community/include/os/osString.h`
  - `community/include/os/osLocale.h`
  - `community/source/os/src/osString.c`
  - `community/source/os/src/osLocale.c`
1. **相关标准**：
  - Unicode 标准：https://www.unicode.org/
  - ISO/IEC 10646：通用字符集标准
  - RFC 3629：UTF-8 编码标准
1. **第三方库文档**：
  - iconv 库文档：https://www.gnu.org/software/libiconv/
  - C 标准库文档
1. **相关工具**：
  - 编码检测工具：uchardet、enca
  - 性能分析工具：perf、valgrind
  - 内存检查工具：AddressSanitizer、Valgrind

## 20. 附录

无。
