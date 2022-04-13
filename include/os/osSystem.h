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
    #define popen POPEN_FUNC_TAOS_FORBID
    #define pclose PCLOSE_FUNC_TAOS_FORBID
    #define tcsetattr TCSETATTR_FUNC_TAOS_FORBID
    #define tcgetattr TCGETATTR_FUNC_TAOS_FORBID
#endif

void* taosLoadDll(const char* filename);
void* taosLoadSym(void* handle, char* name);
void  taosCloseDll(void* handle);

int32_t taosSetConsoleEcho(bool on);
void setTerminalMode();
int32_t getOldTerminalMode();
void resetTerminalMode();

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_SYSTEM_H_*/
