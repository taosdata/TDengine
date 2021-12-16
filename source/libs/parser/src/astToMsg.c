#include "parserInt.h"

SCreateUserMsg* buildUserManipulationMsg(SSqlInfo* pInfo, int64_t id, char* msgBuf, int32_t msgLen) {
  SCreateUserMsg *pMsg = (SCreateUserMsg *)calloc(1, sizeof(SCreateUserMsg));
  if (pMsg == NULL) {
//    tscError("0x%" PRIx64 " failed to malloc for query msg", id);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  SUserInfo *pUser = &pInfo->pMiscInfo->user;
  strncpy(pMsg->user, pUser->user.z, pUser->user.n);
  pMsg->type = pUser->type;
  pMsg->superUser = (int8_t)pUser->type;

  if (pUser->type == TSDB_ALTER_USER_PRIVILEGES) {
//    pMsg->privilege = (char)pCmd->count;
  } else {
    strncpy(pMsg->pass, pUser->passwd.z, pUser->passwd.n);
  }

  return pMsg;
}