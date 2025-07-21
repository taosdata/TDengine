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

#include "libs/tmqtt/tmqtt.h"
#include "bndInt.h"

int32_t bndOpen(const SBnodeOpt *pOption, SBnode **pBnode) {
  int32_t code = 0;

  *pBnode = taosMemoryCalloc(1, sizeof(SBnode));
  if (NULL == *pBnode) {
    bndError("calloc SBnode failed");
    code = terrno;
    TAOS_RETURN(code);
  }

  (*pBnode)->msgCb = pOption->msgCb;
  (*pBnode)->dnodeId = pOption->dnodeId;
  (*pBnode)->protocol = (int8_t)pOption->proto;

  if (TSDB_BNODE_OPT_PROTO_MQTT == (*pBnode)->protocol) {
    if ((code = mqttMgmtStartMqttd((*pBnode)->dnodeId)) != 0) {
      bndError("failed to start taosudf since %s", tstrerror(code));

      taosMemoryFree(*pBnode);
      TAOS_RETURN(code);
    }
  } else {
    bndError("Unknown bnode proto: %hhd.", (*pBnode)->protocol);

    taosMemoryFree(*pBnode);
    TAOS_RETURN(code);
  }

  bndInfo("Bnode opened.");

  return TSDB_CODE_SUCCESS;
}

void bndClose(SBnode *pBnode) {
  mqttMgmtStopMqttd();

  taosMemoryFree(pBnode);

  bndInfo("Bnode closed.");
}
