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
#include "mqttSystem.h"
#include "mqtt.h"
#include "mqttLog.h"
#include "os.h"
#include "taos.h"
#include "tglobal.h"
#include "tsocket.h"
#include "ttimer.h"

int32_t mqttGetReqCount() { return 0; }
int     mqttInitSystem() {
  mqttPrint("mqttInitSystem");
  return 0;
}

int mqttStartSystem() {
  mqttPrint("mqttStartSystem");
  return 0;
}

void mqttStopSystem() {
	mqttPrint("mqttStopSystem");
}

void mqttCleanUpSystem() { 
	mqttPrint("mqttCleanUpSystem");
}
