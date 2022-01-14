#ifndef TDENGINE_ASTTOMSG_H
#define TDENGINE_ASTTOMSG_H

#include "parserInt.h"
#include "tmsg.h"


SCreateUserReq* buildUserManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
SCreateAcctReq* buildAcctManipulationMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
SDropUserReq* buildDropUserMsg(SSqlInfo* pInfo, int32_t* outputLen, int64_t id, char* msgBuf, int32_t msgLen);
SShowReq* buildShowMsg(SShowInfo* pShowInfo, SParseContext* pParseCtx, char* msgBuf, int32_t msgLen);
SCreateDbReq* buildCreateDbMsg(SCreateDbInfo* pCreateDbInfo, SParseContext *pCtx, SMsgBuf* pMsgBuf);
SMCreateStbReq*   buildCreateStbMsg(SCreateTableSql* pCreateTableSql, int32_t* len, SParseContext* pParseCtx, SMsgBuf* pMsgBuf);
SMDropStbReq* buildDropStableMsg(SSqlInfo* pInfo, int32_t* len, SParseContext* pParseCtx, SMsgBuf* pMsgBuf);
SCreateDnodeReq *buildCreateDnodeMsg(SSqlInfo* pInfo, int32_t* len, SMsgBuf* pMsgBuf);
SDropDnodeReq *buildDropDnodeMsg(SSqlInfo* pInfo, int32_t* len, SMsgBuf* pMsgBuf);

#endif  // TDENGINE_ASTTOMSG_H
