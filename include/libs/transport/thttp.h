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

#ifndef _TD_UTIL_HTTP_H_
#define _TD_UTIL_HTTP_H_

#include "os.h"
#include "tref.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { HTTP_GZIP, HTTP_FLAT } EHttpCompFlag;

int32_t taosSendHttpReport(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                           EHttpCompFlag flag);

int32_t taosSendHttpReportWithQID(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                                  EHttpCompFlag flag, const char* qid);
int64_t taosInitHttpChan();
int32_t taosSendHttpReportByChan(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                                 EHttpCompFlag flag, int64_t chanId, const char* qid);
void    taosDestroyHttpChan(int64_t chanId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_UTIL_H_*/
