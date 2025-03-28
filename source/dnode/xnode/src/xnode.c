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

#include "libs/tmqtt/xnode_mgmt_mqtt.h"
#include "xndInt.h"

int32_t xndOpen(const SXnodeOpt *pOption, SXnode **pXnode) {
  int32_t code = 0;

  *pXnode = taosMemoryCalloc(1, sizeof(SXnode));
  if (NULL == *pXnode) {
    xndError("calloc SXnode failed");
    return terrno;
  }

  (*pXnode)->msgCb = pOption->msgCb;
  (*pXnode)->dnodeId = pOption->dnodeId;

  // TODO: read config & start taosmqtt
  if ((code = mqttMgmtStartMqttd((*pXnode)->dnodeId)) != 0) {
    xndError("failed to start taosudf since %s", tstrerror(code));
    return code;
  }

  xndInfo("Xnode opened.");

  return TSDB_CODE_SUCCESS;
}

void xndClose(SXnode *pXnode) {
  mqttMgmtStopMqttd();

  taosMemoryFree(pXnode);

  xndInfo("Xnode closed.");
}
