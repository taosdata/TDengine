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

#ifndef _TD_OS_LOCALE_H_
#define _TD_OS_LOCALE_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define setlocale SETLOCALE_FUNC_TAOS_FORBID
#endif

char *taosCharsetReplace(char *charsetstr);
void  taosGetSystemLocale(char *outLocale, char *outCharset);
void  taosSetSystemLocale(const char *inLocale, const char *inCharSet);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_LOCALE_H_*/
