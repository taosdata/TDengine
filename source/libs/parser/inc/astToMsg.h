#ifndef TDENGINE_ASTTOMSG_H
#define TDENGINE_ASTTOMSG_H

#include "parserInt.h"
#include "taosmsg.h"

SCreateUserMsg* buildUserManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
SCreateAcctMsg* buildAcctManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
SDropUserMsg* buildDropUserMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
SShowMsg* buildShowMsg(SShowInfo* pShowInfo, SParseBasicCtx* pParseCtx, char* msgBuf, int32_t msgLen);
SCreateDbMsg* buildCreateDbMsg(SCreateDbInfo* pCreateDbInfo, SParseBasicCtx *pCtx, SMsgBuf* pMsgBuf);
SCreateStbMsg* buildCreateTableMsg(SCreateTableSql* pCreateTableSql, int32_t* len, SParseBasicCtx* pParseCtx, SMsgBuf* pMsgBuf);
SDropTableMsg* buildDropTableMsg(SSqlInfo* pInfo, int32_t* len, SParseBasicCtx* pParseCtx, SMsgBuf* pMsgBuf);

#endif  // TDENGINE_ASTTOMSG_H
