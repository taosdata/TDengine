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

#define _DEFAULT_SOURCE
#include "dnodeGrant.h"
#include "mgmtGrant.h"

void dnodeGrantInit() {
  mgmtAddTimeSeries = grantAddTimeSeries;
  mgmtRestoreTimeSeries = grantRestoreTimeSeries;
  mgmtCheckTimeSeries = grantCheckTimeSeries();
  mgmtCheckUserGrant = grantCheckUsers;
  mgmtCheckDbGrant = grantCheckDatabases;
  mgmtCheckExpired = grantCheckExpired();
}


void dclusterParseParameterK() {
  char *key = grantGetMachineSerials();
  if (key != NULL) {
    fprintf(stdout, "machine code: %s \n", key);
  } else {
    fprintf(stderr, "should generate machine code under root authority!\n");
  }
  exit(EXIT_SUCCESS);
}

int dnodeCheckSystemClusterImp() {
  char cfgFile[256];
  sprintf(cfgFile, "%s/taos.cfg", configDir);
  grantActiveSystem(cfgFile);

  /*
   * The cluster may not have a master, so the command may always be in progress
   */
  /*
  if (dnodeCheckConfig() != 0) {
    dError("TDengine initialization failed");
    return -1;
  }
  */

  return 0;
}


int32_t mgmtProcessDnodeGrantMsg(unsigned char *pMsg, int32_t msgLen, SDnodeObj *pObj) {
  grantUpdate(pMsg);

  int32_t code = (sdbMaster == 1 ? 0 : TSDB_CODE_REDIRECT);

  taosSendSimpleRsp(pObj->thandle, TSDB_MSG_TYPE_GRANT_RSP, code);

  return 0;
}
