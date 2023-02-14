/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _TD_OS_SYSTEM_H_
#define _TD_OS_SYSTEM_H_

#ifdef _ALPINE
#define UNW_LOCAL_ONLY
#include <libunwind.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define popen     POPEN_FUNC_TAOS_FORBID
#define pclose    PCLOSE_FUNC_TAOS_FORBID
#define tcsetattr TCSETATTR_FUNC_TAOS_FORBID
#define tcgetattr TCGETATTR_FUNC_TAOS_FORBID
#endif

typedef struct TdCmd *TdCmdPtr;

TdCmdPtr taosOpenCmd(const char *cmd);

int64_t taosGetsCmd(TdCmdPtr pCmd, int32_t maxSize, char *__restrict buf);

int64_t taosGetLineCmd(TdCmdPtr pCmd, char **__restrict ptrBuf);

int32_t taosEOFCmd(TdCmdPtr pCmd);

int64_t taosCloseCmd(TdCmdPtr *ppCmd);

void *taosLoadDll(const char *filename);

void *taosLoadSym(void *handle, char *name);

void taosCloseDll(void *handle);

int32_t taosSetConsoleEcho(bool on);

void taosSetTerminalMode();

int32_t taosGetOldTerminalMode();

void taosResetTerminalMode();

#define STACKSIZE 100

#if defined(_ALPINE)
#define taosLogTraceToBuf(buf, bufSize, ignoreNum)                                                                    \
  {                                                                                                                   \
    unw_cursor_t  cursor;                                                                                             \
    unw_context_t context;                                                                                            \
                                                                                                                      \
    unw_getcontext(&context);                                                                                         \
    unw_init_local(&cursor, &context);                                                                                \
                                                                                                                      \
    char   *array[STACKSIZE];                                                                                         \
    int32_t size = 0;                                                                                                 \
    int32_t ignores = ignoreNum;                                                                                      \
    int32_t offset = 0;                                                                                               \
    while (unw_step(&cursor) > 0 && size < STACKSIZE) {                                                               \
      unw_word_t offset, pc;                                                                                          \
      char       fname[64];                                                                                           \
      unw_get_reg(&cursor, UNW_REG_IP, &pc);                                                                          \
      fname[0] = '\0';                                                                                                \
      (void)unw_get_proc_name(&cursor, fname, sizeof(fname), &offset);                                                \
      size += 1;                                                                                                      \
      array[size] = (char *)taosMemoryMalloc(sizeof(char) * STACKSIZE + 1);                                           \
      snprintf(array[size], STACKSIZE, "0x%lx : (%s+0x%lx) [0x%lx]\n", (long)pc, fname, (long)offset, (long)pc);      \
    }                                                                                                                 \
    if (ignoreNum < size && size > 0) {                                                                               \
      offset = snprintf(buf, bufSize - 1, "obtained %d stack frames\n", (ignoreNum > 0) ? size - ignoreNum : size);   \
      for (int32_t i = (ignoreNum > 0) ? ignoreNum : 0; i < size; i++) {                                              \
        offset += snprintf(buf + offset, bufSize - 1 - offset, "frame:%d, %s\n", (ignoreNum > 0) ? i - ignoreNum : i, \
                           array[i]);                                                                                 \
      }                                                                                                               \
    }                                                                                                                 \
    for (int i = 0; i < size; i++) {                                                                                  \
      taosMemoryFree(array[i]);                                                                                       \
    }                                                                                                                 \
  }

#define taosPrintTrace(flags, level, dflag, ignoreNum)                                                                \
  {                                                                                                                   \
    unw_cursor_t  cursor;                                                                                             \
    unw_context_t context;                                                                                            \
                                                                                                                      \
    unw_getcontext(&context);                                                                                         \
    unw_init_local(&cursor, &context);                                                                                \
                                                                                                                      \
    char   *array[STACKSIZE];                                                                                         \
    int32_t size = 0;                                                                                                 \
    while (unw_step(&cursor) > 0 && size < STACKSIZE) {                                                               \
      unw_word_t offset, pc;                                                                                          \
      char       fname[64];                                                                                           \
      unw_get_reg(&cursor, UNW_REG_IP, &pc);                                                                          \
      fname[0] = '\0';                                                                                                \
      (void)unw_get_proc_name(&cursor, fname, sizeof(fname), &offset);                                                \
      size += 1;                                                                                                      \
      array[size] = (char *)taosMemoryMalloc(sizeof(char) * STACKSIZE + 1);                                           \
      snprintf(array[size], STACKSIZE, "frame:%d, 0x%lx : (%s+0x%lx) [0x%lx]\n", size, (long)pc, fname, (long)offset, \
               (long)pc);                                                                                             \
    }                                                                                                                 \
    if (ignoreNum < size && size > 0) {                                                                               \
      taosPrintLog(flags, level, dflag, "obtained %d stack frames", (ignoreNum > 0) ? size - ignoreNum : size);       \
      for (int32_t i = (ignoreNum > 0) ? ignoreNum : 0; i < size; i++) {                                              \
        taosPrintLog(flags, level, dflag, "frame:%d, %s", (ignoreNum > 0) ? i - ignoreNum : i, array[i]);             \
      }                                                                                                               \
    }                                                                                                                 \
    for (int i = 0; i < size; i++) {                                                                                  \
      taosMemoryFree(array[i]);                                                                                       \
    }                                                                                                                 \
  }

#elif !defined(WINDOWS)
#define taosLogTraceToBuf(buf, bufSize, ignoreNum)                                                                    \
  {                                                                                                                   \
    void   *array[STACKSIZE];                                                                                         \
    int32_t size = backtrace(array, STACKSIZE);                                                                       \
    char  **strings = backtrace_symbols(array, size);                                                                 \
    int32_t offset = 0;                                                                                               \
    if (strings != NULL) {                                                                                            \
      offset = snprintf(buf, bufSize - 1, "obtained %d stack frames\n", (ignoreNum > 0) ? size - ignoreNum : size);   \
      for (int32_t i = (ignoreNum > 0) ? ignoreNum : 0; i < size; i++) {                                              \
        offset += snprintf(buf + offset, bufSize - 1 - offset, "frame:%d, %s\n", (ignoreNum > 0) ? i - ignoreNum : i, \
                           strings[i]);                                                                               \
      }                                                                                                               \
    }                                                                                                                 \
                                                                                                                      \
    taosMemoryFree(strings);                                                                                          \
  }

#define taosPrintTrace(flags, level, dflag, ignoreNum)                                                          \
  {                                                                                                             \
    void   *array[STACKSIZE];                                                                                   \
    int32_t size = backtrace(array, STACKSIZE);                                                                 \
    char  **strings = backtrace_symbols(array, size);                                                           \
    if (strings != NULL) {                                                                                      \
      taosPrintLog(flags, level, dflag, "obtained %d stack frames", (ignoreNum > 0) ? size - ignoreNum : size); \
      for (int32_t i = (ignoreNum > 0) ? ignoreNum : 0; i < size; i++) {                                        \
        taosPrintLog(flags, level, dflag, "frame:%d, %s", (ignoreNum > 0) ? i - ignoreNum : i, strings[i]);     \
      }                                                                                                         \
    }                                                                                                           \
                                                                                                                \
    taosMemoryFree(strings);                                                                                    \
  }
#else

#include <dbghelp.h>
#include <windows.h>

#define taosLogTraceToBuf(buf, bufSize, ignoreNum)                                                                   \
  {                                                                                                                  \
    unsigned int   i;                                                                                                \
    void          *stack[STACKSIZE];                                                                                 \
    unsigned short frames;                                                                                           \
    SYMBOL_INFO   *symbol;                                                                                           \
    HANDLE         process;                                                                                          \
    int32_t        offset = 0;                                                                                       \
                                                                                                                     \
    process = GetCurrentProcess();                                                                                   \
                                                                                                                     \
    SymInitialize(process, NULL, TRUE);                                                                              \
                                                                                                                     \
    frames = CaptureStackBackTrace(0, STACKSIZE, stack, NULL);                                                       \
    symbol = (SYMBOL_INFO *)calloc(sizeof(SYMBOL_INFO) + 256 * sizeof(char), 1);                                     \
    if (symbol != NULL) {                                                                                            \
      symbol->MaxNameLen = 255;                                                                                      \
      symbol->SizeOfStruct = sizeof(SYMBOL_INFO);                                                                    \
                                                                                                                     \
      if (frames > 0) {                                                                                              \
        offset =                                                                                                     \
            snprintf(buf, bufSize - 1, "obtained %d stack frames\n", (ignoreNum > 0) ? frames - ignoreNum : frames); \
        for (i = (ignoreNum > 0) ? ignoreNum : 0; i < frames; i++) {                                                 \
          SymFromAddr(process, (DWORD64)(stack[i]), 0, symbol);                                                      \
          offset += snprintf(buf + offset, bufSize - 1 - offset, "frame:%i, %s - 0x%0X\n",                           \
                             (ignoreNum > 0) ? i - ignoreNum : i, symbol->Name, symbol->Address);                    \
        }                                                                                                            \
      }                                                                                                              \
      free(symbol);                                                                                                  \
    }                                                                                                                \
  }

#define taosPrintTrace(flags, level, dflag, ignoreNum)                                                     \
  {                                                                                                        \
    unsigned int   i;                                                                                      \
    void          *stack[STACKSIZE];                                                                       \
    unsigned short frames;                                                                                 \
    SYMBOL_INFO   *symbol;                                                                                 \
    HANDLE         process;                                                                                \
                                                                                                           \
    process = GetCurrentProcess();                                                                         \
                                                                                                           \
    SymInitialize(process, NULL, TRUE);                                                                    \
                                                                                                           \
    frames = CaptureStackBackTrace(0, STACKSIZE, stack, NULL);                                             \
    symbol = (SYMBOL_INFO *)calloc(sizeof(SYMBOL_INFO) + 256 * sizeof(char), 1);                           \
    if (symbol != NULL) {                                                                                  \
      symbol->MaxNameLen = 255;                                                                            \
      symbol->SizeOfStruct = sizeof(SYMBOL_INFO);                                                          \
                                                                                                           \
      if (frames > 0) {                                                                                    \
        taosPrintLog(flags, level, dflag, "obtained %d stack frames\n",                                    \
                     (ignoreNum > 0) ? frames - ignoreNum : frames);                                       \
        for (i = (ignoreNum > 0) ? ignoreNum : 0; i < frames; i++) {                                       \
          SymFromAddr(process, (DWORD64)(stack[i]), 0, symbol);                                            \
          taosPrintLog(flags, level, dflag, "frame:%i, %s - 0x%0X\n", (ignoreNum > 0) ? i - ignoreNum : i, \
                       symbol->Name, symbol->Address);                                                     \
        }                                                                                                  \
      }                                                                                                    \
      free(symbol);                                                                                        \
    }                                                                                                      \
  }
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_SYSTEM_H_*/
