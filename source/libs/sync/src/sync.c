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

#include "sync.h"

int32_t syncInit() { return 0; }

void syncCleanUp() {}

SSyncNode* syncStart(const SSyncInfo* pInfo) { return NULL; }

void syncStop(const SSyncNode* pNode) {}

void syncReconfig(const SSyncNode* pNode, const SSyncCluster* pCfg) {}