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

#include "taosmigrate.h"


/* The options we understand. */
static struct argp_option options[] = {
  {0, 'r', "data dir",     0, "data dir",              0},
  {0, 'd', "dnodeId",      0, "dnode id",              1},
  {0, 'p', "port",         0, "dnode port",            1},
  {0, 'f', "fqdn",         0, "dnode fqdn",            1},
  {0, 'g', "multi dnodes", 0, "multi dnode info, e.g. \"2 7030 fqdn1, 3 8030 fqdn2\"",  2},
  {0}};

/* Used by main to communicate with parse_opt. */
struct arguments {
  char*      dataDir;
  int32_t    dnodeId;
  uint16_t   port;
  char*      fqdn;
  char*      dnodeGroups;
  char**     arg_list;
  int        arg_list_len;
};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  struct arguments *arguments = state->input;
  switch (key) {
    case 'r':
      arguments->dataDir = arg;
      break;
    case 'd':
      arguments->dnodeId = atoi(arg);
      break;
    case 'p':
      arguments->port = atoi(arg);
      break;
    case 'f':
      arguments->fqdn = arg;
      break;
    case 'g':
      arguments->dnodeGroups = arg;
      break;
    case ARGP_KEY_ARG:
      arguments->arg_list     = &state->argv[state->next - 1];
      arguments->arg_list_len = state->argc - state->next + 1;
      state->next             = state->argc;

      argp_usage(state);
      break;

    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

static struct argp argp = {options, parse_opt, 0, 0};
struct arguments arguments = {NULL, 0, 0, NULL, NULL, NULL, 0};
SdnodeGroup   tsDnodeGroup = {0};

int tSystemShell(const char * cmd) 
{ 
  FILE * fp; 
  int res; 
  char buf[1024]; 
  if (cmd == NULL) { 
    printf("tSystem cmd is NULL!\n");
    return -1;
  } 
  
  if ((fp = popen(cmd, "r") ) == NULL) { 
    printf("popen cmd:%s error: %s/n", cmd, strerror(errno)); 
    return -1; 
  } else {
    while(fgets(buf, sizeof(buf), fp))  { 
      printf("popen result:%s", buf); 
    } 

    if ((res = pclose(fp)) == -1) { 
      printf("close popen file pointer fp error!\n");
    } else { 
      printf("popen res is :%d\n", res);
    } 

    return res;
  }
}

void taosMvFile(char* destFile, char *srcFile) {
  char shellCmd[1024+1] = {0}; 
  
  //(void)snprintf(shellCmd, 1024, "cp -rf %s %s", srcDir, destDir);
  (void)snprintf(shellCmd, 1024, "mv -f %s %s", srcFile, destFile);
  tSystemShell(shellCmd);
}

SdnodeIfo* getDnodeInfo(int32_t      dnodeId)
{
  for (int32_t i = 0; i < tsDnodeGroup.dnodeNum; i++) {
    if (dnodeId == tsDnodeGroup.dnodeArray[i].dnodeId) {
      return &(tsDnodeGroup.dnodeArray[i]);
    }
  }

  return NULL;
}

void parseOneDnodeInfo(char* buf, SdnodeIfo* pDnodeInfo) 
{
  char *ptr;
  char *p;
  int32_t i = 0;
  ptr = strtok_r(buf, " ", &p);
  while(ptr != NULL) {
    if (0 == i) {
      pDnodeInfo->dnodeId = atoi(ptr);
    } else if  (1 == i) {
      pDnodeInfo->port = atoi(ptr);
    } else if  (2 == i) {
      tstrncpy(pDnodeInfo->fqdn, ptr, TSDB_FQDN_LEN);
    } else {
      printf("input parameter error near:%s\n", buf);
      exit(-1);
    } 
    i++;
    ptr = strtok_r(NULL, " ", &p);
  }

  snprintf(pDnodeInfo->ep, TSDB_EP_LEN, "%s:%d", pDnodeInfo->fqdn, pDnodeInfo->port);
}

void saveDnodeGroups()
{
  if ((NULL != arguments.fqdn) && (arguments.dnodeId > 0) && (0 != arguments.port)) {
    //printf("dnodeId:%d port:%d fqdn:%s ep:%s\n", arguments.dnodeId, arguments.port, arguments.fqdn, arguments.ep);
  
    tsDnodeGroup.dnodeArray[tsDnodeGroup.dnodeNum].dnodeId = arguments.dnodeId;
    tsDnodeGroup.dnodeArray[tsDnodeGroup.dnodeNum].port    = arguments.port;
    tstrncpy(tsDnodeGroup.dnodeArray[tsDnodeGroup.dnodeNum].fqdn, arguments.fqdn, TSDB_FQDN_LEN);
    snprintf(tsDnodeGroup.dnodeArray[tsDnodeGroup.dnodeNum].ep, TSDB_EP_LEN, "%s:%d", tsDnodeGroup.dnodeArray[tsDnodeGroup.dnodeNum].fqdn, tsDnodeGroup.dnodeArray[tsDnodeGroup.dnodeNum].port);
    
    tsDnodeGroup.dnodeNum++;
  }

  if (NULL == arguments.dnodeGroups) {
    return;
  }

  //printf("dnodeGroups:%s\n", arguments.dnodeGroups);
  
  char  buf[1024];
  char* str = NULL;
  char* start = arguments.dnodeGroups;
  while (NULL != (str = strstr(start, ","))) {
    memcpy(buf, start, str - start);
    // parse one dnode info: dnodeId port fqdn ep
    parseOneDnodeInfo(buf, &(tsDnodeGroup.dnodeArray[tsDnodeGroup.dnodeNum]));
    tsDnodeGroup.dnodeNum++;
    // next
    start = str + 1;
    str   = NULL;  
  }  

  if (strlen(start)) {  
    parseOneDnodeInfo(start, &(tsDnodeGroup.dnodeArray[tsDnodeGroup.dnodeNum]));
    tsDnodeGroup.dnodeNum++;
  }
}

int32_t main(int32_t argc, char *argv[]) {
  memset(&tsDnodeGroup, 0, sizeof(SdnodeGroup));
  
  argp_parse(&argp, argc, argv, 0, 0, &arguments);

  if ((NULL == arguments.dataDir) || ((NULL == arguments.dnodeGroups) 
    && (NULL == arguments.fqdn || arguments.dnodeId < 1 || 0 == arguments.port))) {
    printf("input parameter error!\n");
    return -1;
  }

  saveDnodeGroups();

  printf("===================arguments:==================\n");
  printf("oldWal:%s\n",  arguments.dataDir);
  for (int32_t i = 0; i < tsDnodeGroup.dnodeNum; i++) {
     printf("dnodeId:%d port:%d fqdn:%s ep:%s\n", tsDnodeGroup.dnodeArray[i].dnodeId,
                                                  tsDnodeGroup.dnodeArray[i].port,
                                                  tsDnodeGroup.dnodeArray[i].fqdn,
                                                  tsDnodeGroup.dnodeArray[i].ep);
  }    
  printf("===========================\n");

  // 1. modify wal for mnode
  char mnodeWal[TSDB_FILENAME_LEN*2] = {0};
  (void)snprintf(mnodeWal, TSDB_FILENAME_LEN*2, "%s/mnode/wal/wal0", arguments.dataDir);
  walModWalFile(mnodeWal);

  // 2. modfiy dnode config: mnodeEpSet.json
  char dnodeEpSet[TSDB_FILENAME_LEN*2] = {0};
  (void)snprintf(dnodeEpSet, TSDB_FILENAME_LEN*2, "%s/dnode/mnodeEpSet.json", arguments.dataDir);
  modDnodeEpSet(dnodeEpSet);

  // 3. modify vnode config: config.json
  char vnodeDir[TSDB_FILENAME_LEN*2] = {0};
  (void)snprintf(vnodeDir, TSDB_FILENAME_LEN*2, "%s/vnode", arguments.dataDir);  
  modAllVnode(vnodeDir);
  
  return 0;
}

