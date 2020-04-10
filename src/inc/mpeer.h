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

#ifndef TDENGINE_MPEER_H
#define TDENGINE_MPEER_H

#ifdef __cplusplus
extern "C" {
#endif

enum _TSDB_MN_STATUS {
  TSDB_MN_STATUS_OFFLINE,
  TSDB_MN_STATUS_UNSYNCED,
  TSDB_MN_STATUS_SYNCING,
  TSDB_MN_STATUS_SERVING
};

enum _TSDB_MN_ROLE {
  TSDB_MN_ROLE_UNDECIDED,
  TSDB_MN_ROLE_SLAVE,
  TSDB_MN_ROLE_MASTER
};

int32_t mpeerInit();
void    mpeerCleanup();

bool    mpeerInServerStatus();   
bool    mpeerIsMaster();

bool    mpeerCheckRedirect(void *handle);
void    mpeerGetPrivateIpList(SRpcIpSet *ipSet);
void    mpeerGetPublicIpList(SRpcIpSet *ipSet);

#ifdef __cplusplus
}
#endif

#endif
