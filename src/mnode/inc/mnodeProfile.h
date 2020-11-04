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

#ifndef TDENGINE_MNODE_PROFILE_H
#define TDENGINE_MNODE_PROFILE_H

#ifdef __cplusplus
extern "C" {
#endif
#include "mnodeDef.h"

typedef struct {
  char     user[TSDB_USER_LEN];
  char     appName[TSDB_APPNAME_LEN];  // app name that invokes taosc
  uint32_t pid;                        // pid of app that invokes taosc
  int8_t   killed;
  uint16_t port;
  uint32_t ip;
  uint32_t connId;
  uint64_t stime;
  uint64_t lastAccess;
  uint32_t queryId;
  uint32_t streamId;
  int32_t  numOfQueries;
  int32_t  numOfStreams;
  SStreamDesc *pStreams;
  SQueryDesc * pQueries;
} SConnObj;

int32_t mnodeInitProfile();
void    mnodeCleanupProfile();

SConnObj *mnodeCreateConn(char *user, uint32_t ip, uint16_t port, int32_t pid, const char* app);
SConnObj *mnodeAccquireConn(int32_t connId, char *user, uint32_t ip, uint16_t port);
void      mnodeReleaseConn(SConnObj *pConn);
int32_t   mnodeSaveQueryStreamList(SConnObj *pConn, SHeartBeatMsg *pHBMsg);

#ifdef __cplusplus
}
#endif

#endif