#ifndef TDENGINE_ASTTOMSG_H
#define TDENGINE_ASTTOMSG_H

#include "parserInt.h"
#include "taosmsg.h"

SCreateUserMsg* buildUserManipulationMsg(SSqlInfo* pInfo, int64_t id, char* msgBuf, int32_t msgLen);

#endif  // TDENGINE_ASTTOMSG_H
