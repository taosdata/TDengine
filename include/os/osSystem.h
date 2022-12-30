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

typedef struct TdCmd* TdCmdPtr;

TdCmdPtr taosOpenCmd(const char* cmd);
int64_t  taosGetsCmd(TdCmdPtr pCmd, int32_t maxSize, char* __restrict buf);
int64_t  taosGetLineCmd(TdCmdPtr pCmd, char** __restrict ptrBuf);
int32_t  taosEOFCmd(TdCmdPtr pCmd);
int64_t  taosCloseCmd(TdCmdPtr* ppCmd);

void* taosLoadDll(const char* filename);
void* taosLoadSym(void* handle, char* name);
void  taosCloseDll(void* handle);

int32_t taosSetConsoleEcho(bool on);
void    taosSetTerminalMode();
int32_t taosGetOldTerminalMode();
void    taosResetTerminalMode();

#if !defined(WINDOWS)
#define taosLogTraceToBuf(buf, bufSize, ignoreNum) {                                                                      \
  void*   array[100];                                                                                                     \
  int32_t size = backtrace(array, 100);                                                                                   \
  char**  strings = backtrace_symbols(array, size);                                                                       \
  int32_t offset = 0;                                                                                                     \
  if (strings != NULL) {                                                                                                  \
    offset = snprintf(buf, bufSize - 1, "obtained %d stack frames\n", (ignoreNum > 0) ? size - ignoreNum : size);           \
    for (int32_t i = (ignoreNum > 0) ? ignoreNum : 0; i < size; i++) {                                                    \
      offset += snprintf(buf + offset, bufSize - 1 - offset, "frame:%d, %s\n", (ignoreNum > 0) ? i - ignoreNum : i, strings[i]);     \
    }                                                                                                                     \
  }                                                                                                                       \
                                                                                                                          \
  taosMemoryFree(strings);                                                                                                \
}

#define taosPrintTrace(flags, level, dflag, ignoreNum)                                                          \
  {                                                                                                             \
    void*   array[100];                                                                                         \
    int32_t size = backtrace(array, 100);                                                                       \
    char**  strings = backtrace_symbols(array, size);                                                           \
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
#define taosLogTraceToBuf(buf, bufSize, ignoreNum) {                                                            \
  snprintf(buf, bufSize - 1,                                                                                    \
               "backtrace not implemented on windows, so detailed stack information cannot be printed");        \
}

#define taosPrintTrace(flags, level, dflag, ignoreNum)                                                         \
  {                                                                                                            \
    taosPrintLog(flags, level, dflag,                                                                          \
                 "backtrace not implemented on windows, so detailed stack information cannot be printed");     \
  }
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_SYSTEM_H_*/
