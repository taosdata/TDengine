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

SGrantObj grantObj;

void grantActiveSystem(const char *inputCfgFile) {
  grantObj.granted = true;
  grantObj.officialVersion = 1;
  grantObj.expireTimeSec = GRANT_EXPIRE_TIME;
  grantObj.limitStorage = GRANT_STORAGE_LIMITS;
  grantObj.limitSpeed = GRANT_WRITING_SPEED_LIMITS;
  grantObj.limitTimeSeries = GRANT_TIME_SERIES_LIMITS;
  grantObj.limitQueryTime = GRANT_QUERY_TIME_LIMITS;
  grantObj.limitDbs = GRANT_DATABASE_LIMITS;
  grantObj.limitUsers = GRANT_USER_LIMITS;
  grantObj.limitConns = GRANT_CONNECTION_LIMITS;
  grantObj.limitStreams = GRANT_STREAM_LIMITS;
  grantObj.limitAccts = GRANT_ACCT_LIMITS;
  grantObj.limitDnodes = GRANT_DNODE_LIMITS;
  grantObj.limitCpuCores = GRANT_CPU_LIMITS;
}

char *grantGetMachineSerials() { return "1234567890"; }
