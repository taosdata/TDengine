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

#ifndef TDENGINE_ASTTOMSG_H
#define TDENGINE_ASTTOMSG_H

#include "parserInt.h"
#include "tmsg.h"

char* buildUserManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
char* buildAcctManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
char* buildDropUserMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
char* buildShowMsg(SShowInfo* pShowInfo, int32_t* outputLen, SParseContext* pParseCtx, SMsgBuf* pMsgBuf);
char* buildCreateDbMsg(SCreateDbInfo* pCreateDbInfo, int32_t* outputLen, SParseContext* pCtx, SMsgBuf* pMsgBuf);
char* buildCreateStbReq(SCreateTableSql* pCreateTableSql, int32_t* outputLen, SParseContext* pParseCtx, SMsgBuf* pMsgBuf);
char* buildDropStableReq(SSqlInfo* pInfo, int32_t* outputLen, SParseContext* pParseCtx, SMsgBuf* pMsgBuf);
char* buildCreateDnodeMsg(SSqlInfo* pInfo, int32_t* outputLen, SMsgBuf* pMsgBuf);
char* buildDropDnodeMsg(SSqlInfo* pInfo, int32_t* outputLen, SMsgBuf* pMsgBuf);

#endif  // TDENGINE_ASTTOMSG_H
