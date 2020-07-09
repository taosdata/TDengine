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

static void recordWrite(int fd, SWalHead *pHead) {
  
  taosCalcChecksumAppend(0, (uint8_t *)pHead, sizeof(SWalHead));
  
  int contLen = pHead->len + sizeof(SWalHead);

  if(write(fd, pHead, contLen) != contLen) {
    printf("failed to write(%s)", strerror(errno));
    exit(-1);
  }
}

static void recordMod(SWalHead* pHead) 
{
  SDnodeObj *pDnode;
  
  ESdbTable tableId = (ESdbTable)(pHead->msgType / 10); 

  switch (tableId) {
    case SDB_TABLE_DNODE:
    case SDB_TABLE_MNODE:
      pDnode = (SDnodeObj *)pHead->cont;
    
      printf("dnodeId:%d  port:%d  fqdn:%s  ep:%s\n", pDnode->dnodeId, pDnode->dnodePort, pDnode->dnodeFqdn, pDnode->dnodeEp);

      SdnodeIfo* pDnodeInfo = getDnodeInfo(pDnode->dnodeId);
      if (NULL == pDnodeInfo) {
        break;
      }
      
      pDnode->dnodePort = pDnodeInfo->port;
      tstrncpy(pDnode->dnodeFqdn, pDnodeInfo->fqdn, sizeof(pDnode->dnodeFqdn));
      tstrncpy(pDnode->dnodeEp, pDnodeInfo->ep, sizeof(pDnode->dnodeEp));
      break;
    #if 0
    case SDB_TABLE_ACCOUNT:
      SAcctObj *pAcct = (SDnodeObj *)pHead->cont;
      break;
    case SDB_TABLE_USER:
      SUserObj *pUser = (SDnodeObj *)pHead->cont;
      break;
    case SDB_TABLE_DB:
      SDbObj *pDb = (SDnodeObj *)pHead->cont;
      break;
    case SDB_TABLE_VGROUP:
      SVgObj *pVgroup = (SDnodeObj *)pHead->cont;
      break;
    case SDB_TABLE_STABLE:
      SSuperTableObj *pStable = (SDnodeObj *)pHead->cont;
      break;
    case SDB_TABLE_CTABLE:
      SChildTableObj *pCTable = (SDnodeObj *)pHead->cont;
      break;
    #endif
    default:
      break;
  }  
}

void walModWalFile(char* walfile) {
  char *buffer = malloc(1024000);  // size for one record
  if (buffer == NULL) {
    printf("failed to malloc:%s\n", strerror(errno));
    return ;
  }

  SWalHead *pHead = (SWalHead *)buffer;

  int rfd = open(walfile, O_RDONLY);
  if (rfd < 0) {
    printf("failed to open %s failed:%s\n", walfile, strerror(errno));
    free(buffer);
    return ;
  }

  char newWalFile[32] = "wal0";
  int wfd = open(newWalFile, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

  if (wfd < 0) {
    printf("wal:%s, failed to open(%s)\n", newWalFile, strerror(errno));
    free(buffer);
    close(rfd);
    return ;
  }

  printf("start to mod %s into %s\n", walfile, newWalFile);

  while (1) {
    memset(buffer, 0, 1024000);
    int ret = read(rfd, pHead, sizeof(SWalHead));
    if ( ret == 0)  break;  

    if (ret != sizeof(SWalHead)) {
      printf("wal:%s, failed to read head, skip, ret:%d(%s)\n", walfile, ret, strerror(errno));
      break;
    }

    if (!taosCheckChecksumWhole((uint8_t *)pHead, sizeof(SWalHead))) {
      printf("wal:%s, cksum is messed up, skip the rest of file\n", walfile);
      break;
    } 

    if (pHead->len >= 1024000 - sizeof(SWalHead)) {
      printf("wal:%s, SWalHead.len(%d) overflow, skip the rest of file\n", walfile, pHead->len);
      break;
    } 

    ret = read(rfd, pHead->cont, pHead->len);
    if ( ret != pHead->len) {
      printf("wal:%s, failed to read body, skip, len:%d ret:%d\n", walfile, pHead->len, ret);
      break;
    }

    recordMod(pHead);
    recordWrite(wfd, pHead);
  }

  close(rfd);
  close(wfd);
  free(buffer);

  taosMvFile(walfile, newWalFile);

  return ;
}



