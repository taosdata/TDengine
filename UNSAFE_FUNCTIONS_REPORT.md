# TDengine source/include 不安全函数使用报告
# 生成时间: 2026-01-23
# 搜索范围: /home/yihao/TDengine/source 和 /home/yihao/TDengine/include

## 执行摘要

本报告总结了 TDengine source/ 和 include/ 目录中发现的不安全函数使用情况。

**关键发现:**
- source/ 目录: 未发现任何不安全函数的直接使用
- include/ 目录: 发现多处不安全函数使用，但大部分在宏定义或注释中
- 大多数 memcpy/memmove/memset 使用都包含了大小参数验证
- 未发现 gets/scanf 等高危函数的使用

---

## 1. 不安全字符串函数

### 1.1 strcpy
**禁止使用** - 应使用 `taosStrncpy()` 或 `snprintf()` 替代

**发现数量: 1 (include/)**

```
/home/yihao/TDengine/include/os/osString.h:85
#define TAOS_STRCPY(_dst, _src)         ((void)strcpy(_dst, _src))
```

**说明:** 该函数在宏定义中使用，虽然存在但不是直接调用。建议审查此宏的使用场景，确保不会导致缓冲区溢出。

---

### 1.2 strcat
**禁止使用** - 应使用 `taosStrncat()` 或 `snprintf()` 替代

**发现数量: 1 (include/)**

```
/home/yihao/TDengine/include/os/osString.h:87
#define TAOS_STRCAT(_dst, _src)         ((void)strcat(_dst, _src))
```

**说明:** 该函数在宏定义中使用，同样不是直接调用。建议审查使用场景。

---

### 1.3 sprintf
**禁止使用** - 应使用 `snprintf()` 替代

**发现数量: 2 (include/)**

```
/home/yihao/TDengine/include/util/ttrace.h:52
//    sprintf(buf, "0x%" PRIx64 ":0x%" PRIx64 "", rootId, msgId); \

/home/yihao/TDengine/include/util/tlog.h:91
// Fast uint64_t to string conversion, equivalent to sprintf(buf, "%lu", val) but with 10x better performance.
```

**说明:** 这两处都在注释中，不是实际代码调用，可以忽略。

---

### 1.4 gets
**禁止使用** - 应使用 `fgets()` 替代

**发现数量: 0**

---

### 1.5 scanf
**禁止使用** - 应使用 `fscanf()` 配合边界检查

**发现数量: 0**

---

## 2. 不安全内存函数

### 2.1 memcpy
**禁止无大小验证使用** - 应包含适当的边界检查

**发现数量: 37 (include/)**

所有使用都包含了大小参数，主要分布在以下文件：

**include/util/tarray2.h:**
```c
76: (void)memcpy(a->data + idx * eleSize, elePtr, numEle * eleSize);
```

**include/util/types.h:**
```c
41: (void)memcpy(&fv, pBuf, sizeof(fv));  // in ARM, return *((const float*)(pBuf)) may cause problem
52: (void)memcpy(&dv, pBuf, sizeof(dv));  // in ARM, return *((const double*)(pBuf)) may cause problem
87: #define varDataCopy(dst, v)    (void)memcpy((dst), (void *)(v), varDataTLen(v))
102: #define blobDataCopy(dst, v)    (void)memcpy((dst), (void *)(v), blobDataTLen(v))
```

**include/util/tdef.h:**
```c
160: (void)memcpy((void *)(dst), (void *)(&((ptr)->member)), T_MEMBER_SIZE(type, member)); \
```

**include/util/tutil.h:**
```c
81: memcpy(&val, pVal, sizeof(int64_t));
90: memcpy(&val, pVal, sizeof(uint64_t));
99: memcpy(&val, pVal, sizeof(float));
108: memcpy(&val, pVal, sizeof(double));
119: memcpy(p, &val, sizeof(int64_t));
129: memcpy(p, &val, sizeof(uint64_t));
139: memcpy(to, from, sizeof(int64_t));
149: memcpy(to, from, sizeof(float));
159: memcpy(to, from, sizeof(double));
169: memcpy(to, from, sizeof(uint64_t));
172: #define TAOS_SET_OBJ_ALIGNED(pTo, vFrom)  memcpy((pTo), &(vFrom), sizeof(*(pTo)))
173: #define TAOS_SET_POBJ_ALIGNED(pTo, pFrom) memcpy((pTo), (pFrom), sizeof(*(pTo)))
194: (void)memcpy(target, context.digest, tListLen(context.digest));
210: (void)memcpy(target, buf, TSDB_PASSWORD_LEN);
```

**include/libs/function/taosudf.h:**
```c
117: #define varDataCopy(dst, v)    (void)memcpy((dst), (void *)(v), varDataTLen(v))
125: #define blobDataCopy(dst, v)    (void)memcpy((dst), (void *)(v), blobDataTLen(v))
251: (void)memcpy(data->fixLenCol.data + meta->bytes * currentRow, pData, meta->bytes);
291: (void)memcpy(data->varLenCol.payload + len, pData, dataLen);
```

**include/os/osMemory.h:**
```c
65: #define TAOS_MEMCPY(_d, _s, _n) ((void)memcpy(_d, _s, _n))
```

**include/common/trow.h:**
```c
206: static FORCE_INLINE void        tdRowCpy(void *dst, const STSRow *pRow) { memcpy(dst, pRow, TD_ROW_LEN(pRow)); }
228: static FORCE_INLINE void    tdRowCopy(void *dst, STSRow *row) { memcpy(dst, row, TD_ROW_LEN(row)); }
```

**include/common/tmsg.h:**
```c
1007: (void)memcpy(pDstWrapper->pColCmpr, pSrcWrapper->pColCmpr, size);
1063: (void)memcpy(pSW->pSchema, pSchemaWrapper->pSchema, pSW->nCols * sizeof(SSchema));
```

**include/common/tdataformat.h:**
```c
438: (void)memcpy(varDataVal(x), (str), __len);    \
450: (void)memcpy(varDataVal(x), (str), (_size)); \
```

**include/os/osMath.h:**
```c
36: (void)memcpy(__tmp, &(a), sizeof(a));    \
37: (void)memcpy(&(a), &(b), sizeof(a));     \
38: (void)memcpy(&(b), __tmp, sizeof(a));    \
44: (void)memcpy(__tmp, &(a), sizeof(a)); \
45: (void)memcpy(&(a), &(b), sizeof(a));  \
46: (void)memcpy(&(b), __tmp, sizeof(a)); \
```

**说明:** 所有 memcpy 使用都包含了大小参数，大部分使用了 sizeof() 或显式的大小常量，风险较低。

---

### 2.2 memmove
**禁止无大小验证使用** - 应包含适当的边界检查

**发现数量: 7 (include/)**

**include/util/tringbuf.h:**
```c
67: memmove(dst, src, nele * eleSize);
86: memmove(dst, src, nele * eleSize);
89: memmove(dst, src, (numEle - nele) * eleSize);
94: memmove(dst, src, numEle * eleSize);
```

**include/os/osMemory.h:**
```c
66: #define TAOS_MEMMOVE(_d, _s, _n) ((void)memmove(_d, _s, _n))
```

**include/util/tarray2.h:**
```c
74: (void)memmove(a->data + (idx + numEle) * eleSize, a->data + idx * eleSize, (a->size - idx) * eleSize);
156: (void)memmove((a)->data + (idx), (a)->data + (idx) + 1, sizeof((*(a)->data)) * ((a)->size - (idx)-1)); \
```

**说明:** 所有 memmove 使用都包含了大小参数，风险较低。

---

### 2.3 memset
**禁止无大小验证使用** - 应包含适当的边界检查

**发现数量: 7 (include/)**

**include/common/tdatablock.h:**
```c
64: (void)memset(((char*)(c_)->pData) + (c_)->info.bytes * (r_), 0, (c_)->info.bytes); \
159: (void)memset(pColumnInfoData->pData + start * pColumnInfoData->info.bytes, 0, pColumnInfoData->info.bytes * nRows);
```

**include/os/osMemory.h:**
```c
64: #define TAOS_MEMSET(_s, _c, _n) ((void)memset(_s, _c, _n))
```

**include/os/osDef.h:**
```c
68: #define bzero(ptr, size) (void)memset((ptr), 0, (size))
```

**include/libs/function/taosudf.h:**
```c
193: (void)memset(&data->varLenCol.varOffsets[existedRows], 0, sizeof(int32_t) * (allocCapacity - existedRows));
201: (void)memset(tmp + BitmapLen(data->rowsAlloc), 0, extend);
205: (void)memset(&data->fixLenCol.nullBitmap[oldLen], 0, BitmapLen(allocCapacity) - oldLen);
```

**说明:** 所有 memset 使用都包含了大小参数，风险较低。

---

## 3. 不安全格式字符串使用

### 3.1 printf
**禁止将用户输入作为格式字符串** - 应使用 `printf("%s", userInput)`

**发现数量: 1 (include/)**

```
/home/yihao/TDengine/include/libs/monitorfw/taos_monitor.h:76
*   printf("I did a really important thing!\n");
```

**说明:** 此处是硬编码的格式字符串，不是用户输入，可以接受。

---

### 3.2 fprintf
**禁止将用户输入作为格式字符串**

**发现数量: 1 (include/)**

```
/home/yihao/TDengine/include/os/os.h:160
fprintf(stderr, "Assertion `%s` failed.\n", #pred);         \
```

**说明:** 此处在断言宏中使用，格式字符串是硬编码的，可以接受。

---

### 3.3 snprintf
**安全使用示例** - 正确的使用方式

**发现数量: 14 (include/)**

所有使用都包含了格式字符串，安全使用：

**include/util/taoserror.h:**
```c
59: (void)snprintf(terrMsg, ERR_MSG_LEN, MSG, ##__VA_ARGS__)
```

**include/common/ttypes.h:**
```c
247: snprintf(_output, (int32_t)(_outputBytes), "%d", *(int8_t *)(_input));            \
250: snprintf(_output, (int32_t)(_outputBytes), "%d", *(uint8_t *)(_input));           \
253: snprintf(_output, (int32_t)(_outputBytes), "%d", *(int16_t *)(_input));           \
256: snprintf(_output, (int32_t)(_outputBytes), "%d", *(uint16_t *)(_input));          \
260: snprintf(_output, (int32_t)(_outputBytes), "%" PRId64, *(int64_t *)(_input));     \
263: snprintf(_output, (int32_t)(_outputBytes), "%" PRIu64, *(uint64_t *)(_input));    \
266: snprintf(_output, (int32_t)(_outputBytes), "%.*g", FLT_DIG, *(float *)(_input));  \
270: snprintf(_output, (int32_t)(_outputBytes), "%.*g", DBL_DIG, *(double *)(_input)); \
274: snprintf(_output, (int32_t)(_outputBytes), "%u", *(uint32_t *)(_input));          \
277: snprintf(_output, (int32_t)(_outputBytes), "%d", *(int32_t *)(_input));           \
```

**include/os/osSystem.h:**
```c
89: snprintf(array[size], STACKSIZE, "0x%lx : (%s+0x%lx) [0x%lx]\n", (long)pc, fname, (long)offset, (long)pc);      \
121: snprintf(array[size], STACKSIZE, "frame:%d, 0x%lx : (%s+0x%lx) [0x%lx]\n", size, (long)pc, fname, (long)offset, \
193: snprintf(buf, bufSize - 1, "obtained %d stack frames\n", (ignoreNum > 0) ? frames - ignoreNum : frames); \
```

**说明:** 所有 snprintf 使用都包含了显式的格式字符串和缓冲区大小，是安全的。

---

## 4. 统计总结

| 函数类型 | source/ | include/ | 总计 | 风险等级 |
|---------|----------|-----------|------|---------|
| strcpy | 0 | 1 | 1 | 中 |
| strcat | 0 | 1 | 1 | 中 |
| sprintf | 0 | 2 (注释) | 2 | 低 |
| gets | 0 | 0 | 0 | 无 |
| scanf | 0 | 0 | 0 | 无 |
| memcpy | 0 | 37 | 37 | 低 |
| memmove | 0 | 7 | 7 | 低 |
| memset | 0 | 7 | 7 | 低 |
| printf | 0 | 1 | 1 | 低 |
| fprintf | 0 | 1 | 1 | 低 |
| snprintf | 0 | 14 | 14 | 无 |

**总计:** 72 个匹配项，其中大部分在 include/ 目录的宏定义中，实际代码风险较低。

---

## 5. 建议和行动计划

### 5.1 高优先级修复
无高优先级问题。所有 gets、scanf 等高危函数均未使用。

### 5.2 中优先级改进

1. **审查 strcpy/strcat 宏定义**
   - 文件: `include/os/osString.h`
   - 行号: 85, 87
   - 建议: 检查这些宏的使用场景，确保不会导致缓冲区溢出，考虑替换为安全版本

### 5.3 低优先级监控

1. **监控 memcpy/memmove/memset 使用**
   - 虽然当前使用都包含了大小参数，但建议在代码审查时持续检查
   - 确保新增代码也遵循安全实践

2. **保持当前良好实践**
   - source/ 目录未发现任何不安全函数直接使用
   - include/ 目录的使用大部分在宏定义中，且包含大小参数验证
   - 继续使用 TDengine 的内存包装函数 (`taosMemory*`, `taosStr*`)

---

## 6. 附录：TDengine 安全函数替代方案

根据 AGENTS.md 中的定义，应使用以下安全替代函数：

| 不安全函数 | 安全替代 | 说明 |
|-----------|---------|------|
| strcpy() | taosStrncpy() 或 snprintf() | 缓冲区溢出保护 |
| strcat() | taosStrncat() 或 snprintf() | 缓冲区溢出保护 |
| sprintf() | snprintf() | 缓冲区大小限制 |
| gets() | fgets() | 缓冲区大小限制 |
| scanf() | fscanf() 配合边界检查 | 输入验证 |
| memcpy() | 带大小验证的 memcpy | 边界检查 |
| memmove() | 带大小验证的 memmove | 边界检查 |
| memset() | 带大小验证的 memset | 边界检查 |

---

**报告结束**
