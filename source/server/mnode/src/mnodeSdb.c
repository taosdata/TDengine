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
#include "os.h"
#include "mnodeInt.h"

int32_t mnodeInitSdb() { return 0; }
void    mnodeCleanupSdb() {}

int32_t mnodeDeploySdb() {

    // if (!taosMkDir())
  // if (pMinfos == NULL) {  // first deploy
  //   tsMint.dnodeId = 1;
  //   bool getuid = taosGetSystemUid(tsMint.clusterId);
  //   if (!getuid) {
  //     strcpy(tsMint.clusterId, "tdengine3.0");
  //     mError("deploy new mnode but failed to get uid, set to default val %s", tsMint.clusterId);
  //   } else {
  //     mDebug("deploy new mnode and uid is %s", tsMint.clusterId);
  //   }
  // } else {  // todo
  // }

  // if (mkdir(tsMnodeDir, 0755) != 0 && errno != EEXIST) {
  //   mError("failed to init mnode dir:%s, reason:%s", tsMnodeDir, strerror(errno));
  //   return -1;
  // }
  return 0;
}

void    mnodeUnDeploySdb() {}
int32_t mnodeReadSdb() { return 0; }
int32_t mnodeCommitSdb() { return 0; }