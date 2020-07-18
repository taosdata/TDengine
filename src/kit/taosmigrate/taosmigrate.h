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
#ifndef TAOS_MIGRATE_H
#define TAOS_MIGRATE_H

#ifdef __cplusplus
extern "C" {
#endif

#define _GNU_SOURCE

#ifndef _ALPINE
#include <error.h>
#endif

#include <argp.h>
#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "taosdef.h"
#include "tutil.h"
#include "twal.h"
#include "tchecksum.h"
#include "mnodeDef.h"
#include "mnodeSdb.h"
#include "cJSON.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "tsdb.h"

//#include "vnode.h"
#include "vnodeInt.h"

#define    MAX_DNODE_NUM       128


typedef struct _SdnodeIfo {
  int32_t    dnodeId;
  uint16_t   port;
  char       fqdn[TSDB_FQDN_LEN+1];
  char       ep[TSDB_EP_LEN+1];  
} SdnodeIfo;

typedef struct _SdnodeGroup {
  int32_t    dnodeNum;
  SdnodeIfo  dnodeArray[MAX_DNODE_NUM];
} SdnodeGroup;

int tSystemShell(const char * cmd); 
void taosMvFile(char* destFile, char *srcFile) ;
void walModWalFile(char* walfile);
SdnodeIfo* getDnodeInfo(int32_t      dnodeId);
void modDnodeEpSet(char* dnodeEpSet);
void modAllVnode(char *vnodeDir);

#endif
