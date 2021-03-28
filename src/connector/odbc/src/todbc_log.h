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

#ifndef _todbc_log_h_
#define _todbc_log_h_

#include "os.h"

#ifdef __GNUC__
 __attribute__((format(printf, 6, 7)))
#endif
void todbc_log(const char *file, int line, const char *func, const char tag, int err, const char *fmt, ...);

#define OL(tag, err, fmt, ...) todbc_log(__FILE__, __LINE__, __func__, tag, err, "%s" fmt "", "", ##__VA_ARGS__)
#define OD(fmt, ...)      OL('D', 0,     fmt, ##__VA_ARGS__)
#define OE(fmt, ...)      OL('E', errno, fmt, ##__VA_ARGS__)
#define OW(fmt, ...)      OL('W', 0,     fmt, ##__VA_ARGS__)
#define OI(fmt, ...)      OL('I', 0,     fmt, ##__VA_ARGS__)
#define OV(fmt, ...)      OL('V', 0,     fmt, ##__VA_ARGS__)
#define OA(statement, fmt, ...)   do {                                        \
  if (statement) break;                                                       \
  OL('A', 0, "Assertion failure:[%s]; " fmt "", #statement, ##__VA_ARGS__);   \
  abort();                                                                    \
} while (0)
#define OILE(statement, fmt, ...)   OA(statement, "internal logic error: [" fmt "]", ##__VA_ARGS__)
#define ONIY(statement, fmt, ...)   OA(statement, "not implemented yet: [" fmt "]", ##__VA_ARGS__)
#define ONSP(statement, fmt, ...)   OA(statement, "not support yet: [" fmt "]", ##__VA_ARGS__)

#define D(fmt, ...)          OD(fmt, ##__VA_ARGS__)
#define E(fmt, ...)          OE(fmt, ##__VA_ARGS__)
#define DASSERT(statement)   OA(statement, "")
#define DASSERTX(statement, fmt, ...) OA(statement, fmt, ##__VA_ARGS__)


uint64_t todbc_get_threadid(void);



#endif // _todbc_log_h_

