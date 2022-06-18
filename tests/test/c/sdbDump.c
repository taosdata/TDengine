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
#include "dmMgmt.h"
#include "mndInt.h"
#include "sdb.h"
#include "tconfig.h"
#include "tjson.h"

#define TMP_DNODE_DIR           TD_TMP_DIR_PATH "dumpsdb"
#define TMP_MNODE_DIR           TD_TMP_DIR_PATH "dumpsdb" TD_DIRSEP "mnode"
#define TMP_SDB_DATA_DIR        TD_TMP_DIR_PATH "dumpsdb" TD_DIRSEP "mnode" TD_DIRSEP "data"
#define TMP_SDB_SYNC_DIR        TD_TMP_DIR_PATH "dumpsdb" TD_DIRSEP "mnode" TD_DIRSEP "sync"
#define TMP_SDB_DATA_FILE       TD_TMP_DIR_PATH "dumpsdb" TD_DIRSEP "mnode" TD_DIRSEP "data" TD_DIRSEP "sdb.data"
#define TMP_SDB_RAFT_CFG_FILE   TD_TMP_DIR_PATH "dumpsdb" TD_DIRSEP "mnode" TD_DIRSEP "sync" TD_DIRSEP "raft_config.json"
#define TMP_SDB_RAFT_STORE_FILE TD_TMP_DIR_PATH "dumpsdb" TD_DIRSEP "mnode" TD_DIRSEP "sync" TD_DIRSEP "raft_store.json"

void reportStartup(const char *name, const char *desc) {}

void sendRsp(SRpcMsg *pMsg) { rpcFreeCont(pMsg->pCont); }

int32_t sendReq(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  terrno = TSDB_CODE_INVALID_PTR;
  return -1;
}

char *i642str(int64_t val) {
  static char str[24] = {0};
  snprintf(str, sizeof(str), "%" PRId64, val);
  return str;
}

void dumpFunc(SSdb *pSdb, SJson *json) {}

void dumpDb(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "dbs", items);

  while (1) {
    SDbObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "db", item);

    tjsonAddStringToObject(item, "name", pObj->name);
    tjsonAddStringToObject(item, "acct", pObj->acct);
    tjsonAddStringToObject(item, "createUser", pObj->createUser);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    tjsonAddIntegerToObject(item, "cfgVersion", pObj->cfgVersion);
    tjsonAddIntegerToObject(item, "vgVersion", pObj->vgVersion);
    tjsonAddIntegerToObject(item, "numOfVgroups", pObj->cfg.numOfVgroups);
    tjsonAddIntegerToObject(item, "numOfStables", pObj->cfg.numOfStables);
    tjsonAddIntegerToObject(item, "buffer", pObj->cfg.buffer);
    tjsonAddIntegerToObject(item, "pageSize", pObj->cfg.pageSize);
    tjsonAddIntegerToObject(item, "pages", pObj->cfg.pages);
    tjsonAddIntegerToObject(item, "daysPerFile", pObj->cfg.daysPerFile);
    tjsonAddIntegerToObject(item, "daysToKeep0", pObj->cfg.daysToKeep0);
    tjsonAddIntegerToObject(item, "daysToKeep1", pObj->cfg.daysToKeep1);
    tjsonAddIntegerToObject(item, "daysToKeep2", pObj->cfg.daysToKeep2);
    tjsonAddIntegerToObject(item, "minRows", pObj->cfg.minRows);
    tjsonAddIntegerToObject(item, "maxRows", pObj->cfg.maxRows);
    tjsonAddIntegerToObject(item, "fsyncPeriod", pObj->cfg.fsyncPeriod);
    tjsonAddIntegerToObject(item, "walLevel", pObj->cfg.walLevel);
    tjsonAddIntegerToObject(item, "precision", pObj->cfg.precision);
    tjsonAddIntegerToObject(item, "compression", pObj->cfg.compression);
    tjsonAddIntegerToObject(item, "replications", pObj->cfg.replications);
    tjsonAddIntegerToObject(item, "strict", pObj->cfg.strict);
    tjsonAddIntegerToObject(item, "cacheLastRow", pObj->cfg.cacheLastRow);
    tjsonAddIntegerToObject(item, "hashMethod", pObj->cfg.hashMethod);
    tjsonAddIntegerToObject(item, "numOfRetensions", pObj->cfg.numOfRetensions);
    tjsonAddIntegerToObject(item, "schemaless", pObj->cfg.schemaless);

    sdbRelease(pSdb, pObj);
  }
}

void dumpStb(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "stbs", items);

  while (1) {
    SStbObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "stb", item);

    tjsonAddStringToObject(item, "name", pObj->name);
    tjsonAddStringToObject(item, "db", pObj->db);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    tjsonAddIntegerToObject(item, "tagVer", pObj->tagVer);
    tjsonAddIntegerToObject(item, "colVer", pObj->colVer);
    tjsonAddIntegerToObject(item, "nextColId", pObj->nextColId);
    tjsonAddIntegerToObject(item, "xFilesFactor", pObj->xFilesFactor * 10000);
    tjsonAddIntegerToObject(item, "delay", pObj->delay);
    tjsonAddIntegerToObject(item, "ttl", pObj->ttl);
    tjsonAddIntegerToObject(item, "numOfColumns", pObj->numOfColumns);
    tjsonAddIntegerToObject(item, "numOfTags", pObj->numOfTags);
    tjsonAddIntegerToObject(item, "commentLen", pObj->commentLen);
    tjsonAddIntegerToObject(item, "ast1Len", pObj->ast1Len);
    tjsonAddIntegerToObject(item, "ast2Len", pObj->ast2Len);

    sdbRelease(pSdb, pObj);
  }
}

void dumpSma(SSdb *pSdb, SJson *json) {}

void dumpVgroup(SSdb *pSdb, SJson *json) {}

void dumpTopic(SSdb *pSdb, SJson *json) {}

void dumpConsumber(SSdb *pSdb, SJson *json) {}

void dumpSubscribe(SSdb *pSdb, SJson *json) {}

void dumpOffset(SSdb *pSdb, SJson *json) {}

void dumpStream(SSdb *pSdb, SJson *json) {}

void dumpAcct(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "accts", items);

  while (1) {
    SAcctObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_ACCT, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "acct", item);

    tjsonAddStringToObject(item, "acct", pObj->acct);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddIntegerToObject(item, "acctId", pObj->acctId);

    sdbRelease(pSdb, pObj);
  }
}

void dumpAuth(SSdb *pSdb, SJson *json) {}

void dumpUser(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "users", items);

  while (1) {
    SUserObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "user", item);

    tjsonAddStringToObject(item, "name", pObj->user);
    tjsonAddStringToObject(item, "pass", pObj->pass);
    tjsonAddStringToObject(item, "acct", pObj->acct);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddIntegerToObject(item, "superUser", pObj->superUser);
    tjsonAddIntegerToObject(item, "authVersion", pObj->authVersion);
    tjsonAddIntegerToObject(item, "numOfReadDbs", taosHashGetSize(pObj->readDbs));
    tjsonAddIntegerToObject(item, "numOfWriteDbs", taosHashGetSize(pObj->writeDbs));

    sdbRelease(pSdb, pObj);
  }
}

void dumpDnode(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "dnodes", items);

  while (1) {
    SDnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "dnode", item);

    tjsonAddIntegerToObject(item, "id", pObj->id);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddIntegerToObject(item, "port", pObj->port);
    tjsonAddStringToObject(item, "fqdn", pObj->fqdn);

    sdbRelease(pSdb, pObj);
  }
}

void dumpBnode(SSdb *pSdb, SJson *json) {}

void dumpSnode(SSdb *pSdb, SJson *json) {}

void dumpQnode(SSdb *pSdb, SJson *json) {}

void dumpMnode(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "mnodes", items);

  while (1) {
    SMnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "mnode", item);

    tjsonAddIntegerToObject(item, "id", pObj->id);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));

    sdbRelease(pSdb, pObj);
  }
}

void dumpCluster(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "clusters", items);

  while (1) {
    SClusterObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_CLUSTER, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "cluster", item);

    tjsonAddStringToObject(item, "id", i642str(pObj->id));
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "name", pObj->name);

    sdbRelease(pSdb, pObj);
  }
}

void dumpTrans(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "transactions", items);

  while (1) {
    STrans *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_TRANS, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "trans", item);

    tjsonAddIntegerToObject(item, "id", pObj->id);
    tjsonAddIntegerToObject(item, "stage", pObj->stage);
    tjsonAddIntegerToObject(item, "policy", pObj->policy);
    tjsonAddIntegerToObject(item, "conflict", pObj->conflict);
    tjsonAddIntegerToObject(item, "exec", pObj->exec);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "dbname1", pObj->dbname1);
    tjsonAddStringToObject(item, "dbname2", pObj->dbname2);
    tjsonAddIntegerToObject(item, "commitLogNum", taosArrayGetSize(pObj->commitActions));
    tjsonAddIntegerToObject(item, "redoActionNum", taosArrayGetSize(pObj->redoActions));
    tjsonAddIntegerToObject(item, "undoActionNum", taosArrayGetSize(pObj->undoActions));

    sdbRelease(pSdb, pObj);
  }
}

void dumpHeader(SSdb *pSdb, SJson *json) {
  tjsonAddIntegerToObject(json, "sver", 1);
  tjsonAddStringToObject(json, "applyIndex", i642str(pSdb->applyIndex));
  tjsonAddStringToObject(json, "applyTerm", i642str(pSdb->applyTerm));
  tjsonAddStringToObject(json, "applyConfig", i642str(pSdb->applyConfig));

  SJson *maxIdsJson = tjsonCreateObject();
  tjsonAddItemToObject(json, "maxIds", maxIdsJson);
  for (int32_t i = 0; i < SDB_MAX; ++i) {
    int64_t maxId = 0;
    if (i < SDB_MAX) {
      maxId = pSdb->maxId[i];
    }
    tjsonAddStringToObject(maxIdsJson, sdbTableName(i), i642str(maxId));
  }

  SJson *tableVersJson = tjsonCreateObject();
  tjsonAddItemToObject(json, "tableVers", tableVersJson);
  for (int32_t i = 0; i < SDB_MAX; ++i) {
    int64_t tableVer = 0;
    if (i < SDB_MAX) {
      tableVer = pSdb->tableVer[i];
    }
    tjsonAddStringToObject(tableVersJson, sdbTableName(i), i642str(tableVer));
  }
}

int32_t dumpSdb() {
  wDebugFlag = 0;
  mDebugFlag = 0;
  sDebugFlag = 0;

  SMsgCb msgCb = {0};
  msgCb.reportStartupFp = reportStartup;
  msgCb.sendReqFp = sendReq;
  msgCb.sendRspFp = sendRsp;
  msgCb.mgmt = (SMgmtWrapper *)(&msgCb);  // hack
  tmsgSetDefault(&msgCb);
  walInit();
  syncInit();

  SMnodeOpt opt = {.msgCb = msgCb};
  SMnode   *pMnode = mndOpen(TMP_MNODE_DIR, &opt);
  if (pMnode == NULL) return -1;

  SSdb  *pSdb = pMnode->pSdb;
  SJson *json = tjsonCreateObject();
  dumpHeader(pSdb, json);
  dumpFunc(pSdb, json);
  dumpDb(pSdb, json);
  dumpStb(pSdb, json);
  dumpSma(pSdb, json);
  dumpVgroup(pSdb, json);
  dumpTopic(pSdb, json);
  dumpConsumber(pSdb, json);
  dumpSubscribe(pSdb, json);
  dumpOffset(pSdb, json);
  dumpStream(pSdb, json);
  dumpAcct(pSdb, json);
  dumpAuth(pSdb, json);
  dumpUser(pSdb, json);
  dumpDnode(pSdb, json);
  dumpBnode(pSdb, json);
  dumpSnode(pSdb, json);
  dumpQnode(pSdb, json);
  dumpMnode(pSdb, json);
  dumpCluster(pSdb, json);
  dumpTrans(pSdb, json);

  char     *pCont = tjsonToString(json);
  int32_t   contLen = strlen(pCont);
  char      file[] = "sdb.json";
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to write %s since %s", file, terrstr());
    return -1;
  }
  taosWriteFile(pFile, pCont, contLen);
  taosWriteFile(pFile, "\n", 1);
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  tjsonDelete(json);
  taosMemoryFree(pCont);
  taosRemoveDir(TMP_DNODE_DIR);
  return 0;
}

int32_t parseArgs(int32_t argc, char *argv[]) {
  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("config file path overflow");
          return -1;
        }
        tstrncpy(configDir, argv[i], PATH_MAX);
      } else {
        printf("'-c' requires a parameter, default is %s\n", configDir);
        return -1;
      }
    } else {
      printf("-c Configuration directory. \n");
      return -1;
    }
  }

  if (taosCreateLog("dumplog", 1, configDir, NULL, NULL, NULL, NULL, 1) != 0) {
    printf("failed to dump since init log error\n");
    return -1;
  }

  if (taosInitCfg(configDir, NULL, NULL, NULL, NULL, 0) != 0) {
    printf("failed to dump since read config error\n");
    return -1;
  }

  char dataFile[PATH_MAX] = {0};
  char raftCfgFile[PATH_MAX] = {0};
  char raftStoreFile[PATH_MAX] = {0};
  snprintf(dataFile, PATH_MAX, "%s" TD_DIRSEP "mnode" TD_DIRSEP "data" TD_DIRSEP "sdb.data", tsDataDir);
  snprintf(raftCfgFile, PATH_MAX, "%s" TD_DIRSEP "mnode" TD_DIRSEP "sync" TD_DIRSEP "raft_config.json", tsDataDir);
  snprintf(raftStoreFile, PATH_MAX, "%s" TD_DIRSEP "mnode" TD_DIRSEP "sync" TD_DIRSEP "raft_store.json", tsDataDir);

  char cmd[PATH_MAX * 2] = {0};
  snprintf(cmd, sizeof(cmd), "rm -rf %s", TMP_DNODE_DIR);
  system(cmd);
#ifdef WINDOWS
  taosMulMkDir(TMP_SDB_DATA_DIR);
  taosMulMkDir(TMP_SDB_SYNC_DIR);
  snprintf(cmd, sizeof(cmd), "cp %s %s 2>nul", dataFile, TMP_SDB_DATA_FILE);
  system(cmd);
  snprintf(cmd, sizeof(cmd), "cp %s %s 2>nul", raftCfgFile, TMP_SDB_RAFT_CFG_FILE);
  system(cmd);
  snprintf(cmd, sizeof(cmd), "cp %s %s 2>nul", raftStoreFile, TMP_SDB_RAFT_STORE_FILE);
  system(cmd);
#else
  snprintf(cmd, sizeof(cmd), "mkdir -p %s", TMP_SDB_DATA_DIR);
  system(cmd);
  snprintf(cmd, sizeof(cmd), "mkdir -p %s", TMP_SDB_SYNC_DIR);
  system(cmd);
  snprintf(cmd, sizeof(cmd), "cp %s %s 2>/dev/null", dataFile, TMP_SDB_DATA_FILE);
  system(cmd);
  snprintf(cmd, sizeof(cmd), "cp %s %s 2>/dev/null", raftCfgFile, TMP_SDB_RAFT_CFG_FILE);
  system(cmd);
  snprintf(cmd, sizeof(cmd), "cp %s %s 2>/dev/null", raftStoreFile, TMP_SDB_RAFT_STORE_FILE);
  system(cmd);
#endif

  strcpy(tsDataDir, TMP_DNODE_DIR);
  return 0;
}

int32_t main(int32_t argc, char *argv[]) {
  if (parseArgs(argc, argv) != 0) {
    return -1;
  }

  return dumpSdb();
}
