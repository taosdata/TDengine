#ifndef TDENGINE_ASTTOMSG_H
#define TDENGINE_ASTTOMSG_H

#include "parserInt.h"
#include "tmsg.h"


SCreateUserReq* buildUserManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
SCreateAcctReq* buildAcctManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
SDropUserReq* buildDropUserMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
SShowMsg* buildShowMsg(SShowInfo* pShowInfo, SParseBasicCtx* pParseCtx, char* msgBuf, int32_t msgLen);
SCreateDbMsg* buildCreateDbMsg(SCreateDbInfo* pCreateDbInfo, SParseBasicCtx *pCtx, SMsgBuf* pMsgBuf);
SCreateStbMsg* buildCreateTableMsg(SCreateTableSql* pCreateTableSql, int32_t* len, SParseBasicCtx* pParseCtx, SMsgBuf* pMsgBuf);
SDropStbMsg* buildDropStableMsg(SSqlInfo* pInfo, int32_t* len, SParseBasicCtx* pParseCtx, SMsgBuf* pMsgBuf);
SCreateDnodeMsg *buildCreateDnodeMsg(SSqlInfo* pInfo, int32_t* len, SMsgBuf* pMsgBuf);
SDropDnodeMsg *buildDropDnodeMsg(SSqlInfo* pInfo, int32_t* len, SMsgBuf* pMsgBuf);

#endif  // TDENGINE_ASTTOMSG_H
