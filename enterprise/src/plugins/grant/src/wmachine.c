/*******************************************************************
 *           Copyright (c) 2017 by TAOS Technologies, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Technologies. 
 *  No part of this file may be reproduced, stored, transmitted, 
 *  disclosed or used in any form or by any means other than as 
 *  expressly provided by the written permission from Jianhui Tao
 *
 * ****************************************************************/
//version 2.0.4.0

#define _DEFAULT_SOURCE
#include "os.h"
#include "machine.h"

SGrantUniqObj grantObj;

void grantActiveSystem(const char *inputCfgFile, SGrantObj *pObj, SGrantConnObj *pConnObj) {
  grantObj.granted = 1;
  grantObj.officialVersion = 1;
  grantObj.basicExpireDay = GRANT_UNIQ_UNLIMITED;
  grantObj.limitDnodes = GRANT_UNIQ_UNLIMITED;
  grantObj.limitTimeSeries = GRANT_UNIQ_UNLIMITED;
  grantObj.limitStreams = GRANT_UNIQ_UNLIMITED;
  grantObj.limitTopics = GRANT_UNIQ_UNLIMITED;
  grantObj.streamExpireDay = GRANT_UNIQ_UNLIMITED;
  grantObj.topicExpireDay = GRANT_UNIQ_UNLIMITED;
  grantObj.multiTierExpireDay = GRANT_UNIQ_UNLIMITED;
  grantObj.auditExpireDay = GRANT_UNIQ_UNLIMITED;
  grantObj.bakRstExpireDay = GRANT_UNIQ_UNLIMITED;
  grantObj.replicaExpireDay = GRANT_UNIQ_UNLIMITED;
  for (int32_t i = 0; i < CONN_TYPE_MAX; ++i) {
    ins[i].number = GRANT_UNIQ_UNLIMITED;
    ins[i].speed = GRANT_UNIQ_UNLIMITED;
    ins[i].expire = GRANT_UNIQ_UNLIMITED;
  }
}

char *grantGetMachineSerials() { return "1234567890"; }
