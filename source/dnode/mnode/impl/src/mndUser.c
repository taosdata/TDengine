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
// clang-format off
#include <uv.h>
#include "mndUser.h"
#include "audit.h"
#include "mndDb.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "tbase64.h"

// clang-format on

#define USER_VER_NUMBER   6
#define USER_RESERVE_SIZE 64

#define BIT_FLAG_MASK(n)              (1 << n)
#define BIT_FLAG_SET_MASK(val, mask)  ((val) |= (mask))
#define BIT_FLAG_TEST_MASK(val, mask) (((val) & (mask)) != 0)

#define PRIVILEGE_TYPE_ALL       BIT_FLAG_MASK(0)
#define PRIVILEGE_TYPE_READ      BIT_FLAG_MASK(1)
#define PRIVILEGE_TYPE_WRITE     BIT_FLAG_MASK(2)
#define PRIVILEGE_TYPE_SUBSCRIBE BIT_FLAG_MASK(3)
#define PRIVILEGE_TYPE_ALTER     BIT_FLAG_MASK(4)

#define ALTER_USER_ADD_PRIVS(_type) ((_type) == TSDB_ALTER_USER_ADD_PRIVILEGES)
#define ALTER_USER_DEL_PRIVS(_type) ((_type) == TSDB_ALTER_USER_DEL_PRIVILEGES)

#define ALTER_USER_ALL_PRIV(_priv) (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALL))
#define ALTER_USER_READ_PRIV(_priv) (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_READ) || BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALL))
#define ALTER_USER_WRITE_PRIV(_priv) (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_WRITE) || BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALL))
#define ALTER_USER_ALTER_PRIV(_priv) (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALTER) || BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALL))
#define ALTER_USER_SUBSCRIBE_PRIV(_priv) (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_SUBSCRIBE))

#define ALTER_USER_TARGET_DB(_tbname) (0 == (_tbname)[0])
#define ALTER_USER_TARGET_TB(_tbname) (0 != (_tbname)[0])

#define ALTER_USER_ADD_READ_DB_PRIV(_type, _priv, _tbname) (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_READ_DB_PRIV(_type, _priv, _tbname) (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_WRITE_DB_PRIV(_type, _priv, _tbname) (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_WRITE_DB_PRIV(_type, _priv, _tbname) (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_ALTER_DB_PRIV(_type, _priv, _tbname) (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_ALTER_DB_PRIV(_type, _priv, _tbname) (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_ALL_DB_PRIV(_type, _priv, _tbname) (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_ALL_DB_PRIV(_type, _priv, _tbname) (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))

#define ALTER_USER_ADD_READ_TB_PRIV(_type, _priv, _tbname) (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_READ_TB_PRIV(_type, _priv, _tbname) (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_WRITE_TB_PRIV(_type, _priv, _tbname) (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_WRITE_TB_PRIV(_type, _priv, _tbname) (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_ALTER_TB_PRIV(_type, _priv, _tbname) (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_ALTER_TB_PRIV(_type, _priv, _tbname) (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_ALL_TB_PRIV(_type, _priv, _tbname) (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_ALL_TB_PRIV(_type, _priv, _tbname) (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))

#define ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(_type, _priv) (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_SUBSCRIBE_PRIV(_priv))
#define ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(_type, _priv) (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_SUBSCRIBE_PRIV(_priv))


static SIpWhiteList *createDefaultIpWhiteList();
SIpWhiteList        *createIpWhiteList(void *buf, int32_t len);
static bool          updateIpWhiteList(SIpWhiteList *pOld, SIpWhiteList *pNew);
static bool          isIpWhiteListEqual(SIpWhiteList *a, SIpWhiteList *b);
static bool          isIpRangeEqual(SIpV4Range *a, SIpV4Range *b);

void destroyIpWhiteTab(SHashObj *pIpWhiteTab);

#define MND_MAX_USE_HOST (TSDB_PRIVILEDGE_HOST_LEN / 24)

static int32_t  mndCreateDefaultUsers(SMnode *pMnode);
static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw);
static int32_t  mndUserActionInsert(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionDelete(SSdb *pSdb, SUserObj *pUser);
static int32_t  mndUserActionUpdate(SSdb *pSdb, SUserObj *pOld, SUserObj *pNew);
static int32_t  mndCreateUser(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SRpcMsg *pReq);
static int32_t  mndProcessCreateUserReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterUserReq(SRpcMsg *pReq);
static int32_t  mndProcessDropUserReq(SRpcMsg *pReq);
static int32_t  mndProcessGetUserAuthReq(SRpcMsg *pReq);
static int32_t  mndProcessGetUserWhiteListReq(SRpcMsg *pReq);
static int32_t  mndRetrieveUsers(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextUser(SMnode *pMnode, void *pIter);
static int32_t  mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter);
SHashObj       *mndFetchAllIpWhite(SMnode *pMnode);
static int32_t  mndProcesSRetrieveIpWhiteReq(SRpcMsg *pReq);
bool            mndUpdateIpWhiteImpl(SHashObj *pIpWhiteTab, char *user, char *fqdn, int8_t type);

void ipWhiteMgtUpdateAll(SMnode *pMnode);
typedef struct {
  SHashObj      *pIpWhiteTab;
  int64_t        ver;
  TdThreadRwlock rw;
} SIpWhiteMgt;

static SIpWhiteMgt ipWhiteMgt;

const static SIpV4Range defaultIpRange = {.ip = 16777343, .mask = 32};

void ipWhiteMgtInit() {
  ipWhiteMgt.pIpWhiteTab = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), 1, HASH_ENTRY_LOCK);
  ipWhiteMgt.ver = 0;
  taosThreadRwlockInit(&ipWhiteMgt.rw, NULL);
}
void ipWhiteMgtCleanup() {
  destroyIpWhiteTab(ipWhiteMgt.pIpWhiteTab);
  taosThreadRwlockDestroy(&ipWhiteMgt.rw);
}

int32_t ipWhiteMgtUpdate(SMnode *pMnode, char *user, SIpWhiteList *pNew) {
  bool update = true;
  taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  SIpWhiteList **ppList = taosHashGet(ipWhiteMgt.pIpWhiteTab, user, strlen(user));

  if (ppList == NULL || *ppList == NULL) {
    SIpWhiteList *p = cloneIpWhiteList(pNew);
    taosHashPut(ipWhiteMgt.pIpWhiteTab, user, strlen(user), &p, sizeof(void *));
  } else {
    SIpWhiteList *pOld = *ppList;
    if (isIpWhiteListEqual(pOld, pNew)) {
      update = false;
    } else {
      taosMemoryFree(pOld);
      SIpWhiteList *p = cloneIpWhiteList(pNew);
      taosHashPut(ipWhiteMgt.pIpWhiteTab, user, strlen(user), &p, sizeof(void *));
    }
  }
  SArray *fqdns = mndGetAllDnodeFqdns(pMnode);

  for (int i = 0; i < taosArrayGetSize(fqdns); i++) {
    char *fqdn = taosArrayGetP(fqdns, i);
    update |= mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, TSDB_DEFAULT_USER, fqdn, IP_WHITE_ADD);
    update |= mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, user, fqdn, IP_WHITE_ADD);
  }

  for (int i = 0; i < taosArrayGetSize(fqdns); i++) {
    char *fqdn = taosArrayGetP(fqdns, i);
    taosMemoryFree(fqdn);
  }
  taosArrayDestroy(fqdns);

  // for (int i = 0; i < taosArrayGetSize(pUserNames); i++) {
  //   taosMemoryFree(taosArrayGetP(pUserNames, i));
  // }
  // taosArrayDestroy(pUserNames);

  if (update) ipWhiteMgt.ver++;

  taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  return 0;
}
int32_t ipWhiteMgtRemove(char *user) {
  bool update = true;
  taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  SIpWhiteList **ppList = taosHashGet(ipWhiteMgt.pIpWhiteTab, user, strlen(user));
  if (ppList == NULL || *ppList == NULL) {
    update = false;
  } else {
    taosMemoryFree(*ppList);
    taosHashRemove(ipWhiteMgt.pIpWhiteTab, user, strlen(user));
  }

  if (update) ipWhiteMgt.ver++;
  taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  return 0;
}

bool isRangeInWhiteList(SIpWhiteList *pList, SIpV4Range *range) {
  for (int i = 0; i < pList->num; i++) {
    if (isIpRangeEqual(&pList->pIpRange[i], range)) {
      return true;
    }
  }
  return false;
}
int32_t ipWhiteUpdateForAllUser(SIpWhiteList *pList) {
  taosThreadRwlockWrlock(&ipWhiteMgt.rw);

  SHashObj *pIpWhiteTab = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), 1, HASH_ENTRY_LOCK);
  void     *pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, NULL);

  while (pIter) {
    SIpWhiteList *p = *(SIpWhiteList **)pIter;
    SIpWhiteList *clone = cloneIpWhiteList(pList);
    int32_t       idx = 0;
    for (int i = 0; i < pList->num; i++) {
      SIpV4Range *e = &pList->pIpRange[i];
      if (!isRangeInWhiteList(p, e)) {
        clone->pIpRange[idx] = *e;
        idx++;
      }
    }
    clone->num = idx;

    SIpWhiteList *val = NULL;
    if (clone->num != 0) {
      int32_t sz = clone->num + p->num;
      val = taosMemoryCalloc(1, sizeof(SIpWhiteList) + sz * sizeof(SIpV4Range));
      memcpy(val->pIpRange, p->pIpRange, sizeof(SIpV4Range) * p->num);
      memcpy(((char *)val->pIpRange) + sizeof(SIpV4Range) * p->num, (char *)clone->pIpRange,
             sizeof(SIpV4Range) * clone->num);

    } else {
      val = cloneIpWhiteList(p);
    }
    taosMemoryFree(clone);

    size_t klen;
    void  *key = taosHashGetKey(pIter, &klen);
    taosHashPut(pIpWhiteTab, key, klen, val, sizeof(void *));
  }

  destroyIpWhiteTab(ipWhiteMgt.pIpWhiteTab);

  ipWhiteMgt.pIpWhiteTab = pIpWhiteTab;
  ipWhiteMgt.ver++;
  taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  return 0;
}

void ipWhiteMgtUpdateAll(SMnode *pMnode) {
  ipWhiteMgt.ver++;
  SHashObj *pNew = mndFetchAllIpWhite(pMnode);
  SHashObj *pOld = ipWhiteMgt.pIpWhiteTab;

  ipWhiteMgt.pIpWhiteTab = pNew;

  destroyIpWhiteTab(pOld);
}
void ipWhiteMgtUpdate2(SMnode *pMnode) {
  taosThreadRwlockWrlock(&ipWhiteMgt.rw);

  ipWhiteMgtUpdateAll(pMnode);

  taosThreadRwlockUnlock(&ipWhiteMgt.rw);
}

int64_t mndGetIpWhiteVer(SMnode *pMnode) {
  int64_t ver = 0;
  taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  if (ipWhiteMgt.ver == 0) {
    // get user and dnode ip white list
    ipWhiteMgtUpdateAll(pMnode);
    ipWhiteMgt.ver = taosGetTimestampMs();
  }
  ver = ipWhiteMgt.ver;
  taosThreadRwlockUnlock(&ipWhiteMgt.rw);

  if (mndEnableIpWhiteList(pMnode) == 0 || tsEnableWhiteList == false) {
    ver = 0;
  }
  mDebug("ip-white-list on mnode ver: %" PRId64 "", ver);
  return ver;
}

bool mndUpdateIpWhiteImpl(SHashObj *pIpWhiteTab, char *user, char *fqdn, int8_t type) {
  bool       update = false;
  SIpV4Range range = {.ip = taosGetIpv4FromFqdn(fqdn), .mask = 32};
  mDebug("ip-white-list may update for user: %s, fqdn: %s", user, fqdn);
  SIpWhiteList **ppList = taosHashGet(pIpWhiteTab, user, strlen(user));
  SIpWhiteList  *pList = NULL;
  if (ppList != NULL && *ppList != NULL) {
    pList = *ppList;
  }

  if (type == IP_WHITE_ADD) {
    if (pList == NULL) {
      SIpWhiteList *pNewList = taosMemoryCalloc(1, sizeof(SIpWhiteList) + sizeof(SIpV4Range));
      memcpy(pNewList->pIpRange, &range, sizeof(SIpV4Range));
      pNewList->num = 1;

      taosHashPut(pIpWhiteTab, user, strlen(user), &pNewList, sizeof(void *));
      update = true;
    } else {
      if (!isRangeInWhiteList(pList, &range)) {
        int32_t       sz = sizeof(SIpWhiteList) + sizeof(SIpV4Range) * (pList->num + 1);
        SIpWhiteList *pNewList = taosMemoryCalloc(1, sz);
        memcpy(pNewList->pIpRange, pList->pIpRange, sizeof(SIpV4Range) * (pList->num));
        pNewList->pIpRange[pList->num].ip = range.ip;
        pNewList->pIpRange[pList->num].mask = range.mask;

        pNewList->num = pList->num + 1;

        taosHashPut(pIpWhiteTab, user, strlen(user), &pNewList, sizeof(void *));
        taosMemoryFree(pList);
        update = true;
      }
    }
  } else if (type == IP_WHITE_DROP) {
    if (pList != NULL) {
      if (isRangeInWhiteList(pList, &range)) {
        if (pList->num == 1) {
          taosHashRemove(pIpWhiteTab, user, strlen(user));
          taosMemoryFree(pList);
        } else {
          int32_t       idx = 0;
          int32_t       sz = sizeof(SIpWhiteList) + sizeof(SIpV4Range) * (pList->num - 1);
          SIpWhiteList *pNewList = taosMemoryCalloc(1, sz);
          for (int i = 0; i < pList->num; i++) {
            SIpV4Range *e = &pList->pIpRange[i];
            if (!isIpRangeEqual(e, &range)) {
              pNewList->pIpRange[idx].ip = e->ip;
              pNewList->pIpRange[idx].mask = e->mask;
              idx++;
            }
          }
          pNewList->num = idx;
          taosHashPut(pIpWhiteTab, user, strlen(user), &pNewList, sizeof(void *));
          taosMemoryFree(pList);
        }
        update = true;
      }
    }
  }
  if (update) {
    mDebug("ip-white-list update for user: %s, fqdn: %s", user, fqdn);
  }

  return update;
}

int32_t mndRefreshUserIpWhiteList(SMnode *pMnode) {
  taosThreadRwlockWrlock(&ipWhiteMgt.rw);

  ipWhiteMgtUpdateAll(pMnode);
  ipWhiteMgt.ver = taosGetTimestampMs();
  taosThreadRwlockUnlock(&ipWhiteMgt.rw);

  return 0;
}
void mndUpdateIpWhiteForAllUser(SMnode *pMnode, char *user, char *fqdn, int8_t type, int8_t lock) {
  if (lock) {
    taosThreadRwlockWrlock(&ipWhiteMgt.rw);
    if (ipWhiteMgt.ver == 0) {
      ipWhiteMgtUpdateAll(pMnode);
      ipWhiteMgt.ver = taosGetTimestampMs();
      mInfo("ip-white-list, user: %" PRId64 "", ipWhiteMgt.ver);
    }
  }

  bool update = mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, user, fqdn, type);

  void *pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, NULL);
  while (pIter) {
    size_t klen = 0;
    char  *key = taosHashGetKey(pIter, &klen);

    char *keyDup = taosMemoryCalloc(1, klen + 1);
    memcpy(keyDup, key, klen);
    update |= mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, keyDup, fqdn, type);
    taosMemoryFree(keyDup);

    pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, pIter);
  }

  if (update) ipWhiteMgt.ver++;

  if (lock) taosThreadRwlockUnlock(&ipWhiteMgt.rw);
}
int64_t ipWhiteMgtFillMsg(SUpdateIpWhite *pUpdate) {
  int64_t ver = 0;
  taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  ver = ipWhiteMgt.ver;
  int32_t num = taosHashGetSize(ipWhiteMgt.pIpWhiteTab);

  pUpdate->pUserIpWhite = taosMemoryCalloc(1, num * sizeof(SUpdateUserIpWhite));

  void   *pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, NULL);
  int32_t i = 0;
  while (pIter) {
    SUpdateUserIpWhite *pUser = &pUpdate->pUserIpWhite[i];
    SIpWhiteList       *list = *(SIpWhiteList **)pIter;

    size_t klen;
    char  *key = taosHashGetKey(pIter, &klen);
    if (list->num != 0) {
      pUser->ver = ver;
      memcpy(pUser->user, key, klen);
      pUser->numOfRange = list->num;
      pUser->pIpRanges = taosMemoryCalloc(1, list->num * sizeof(SIpV4Range));
      memcpy(pUser->pIpRanges, list->pIpRange, list->num * sizeof(SIpV4Range));
      i++;
    }
    pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, pIter);
  }
  pUpdate->numOfUser = i;
  pUpdate->ver = ver;

  taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  return 0;
}

void destroyIpWhiteTab(SHashObj *pIpWhiteTab) {
  if (pIpWhiteTab == NULL) return;

  void *pIter = taosHashIterate(pIpWhiteTab, NULL);
  while (pIter) {
    SIpWhiteList *list = *(SIpWhiteList **)pIter;
    taosMemoryFree(list);
    pIter = taosHashIterate(pIpWhiteTab, pIter);
  }

  taosHashCleanup(pIpWhiteTab);
}
SHashObj *mndFetchAllIpWhite(SMnode *pMnode) {
  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  SHashObj *pIpWhiteTab = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), 1, HASH_ENTRY_LOCK);

  SArray *pUserNames = taosArrayInit(8, sizeof(void *));
  while (1) {
    SUserObj *pUser = NULL;
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    SIpWhiteList *pWhiteList = cloneIpWhiteList(pUser->pIpWhiteList);
    taosHashPut(pIpWhiteTab, pUser->user, strlen(pUser->user), &pWhiteList, sizeof(void *));

    char *name = taosStrdup(pUser->user);
    taosArrayPush(pUserNames, &name);

    sdbRelease(pSdb, pUser);
  }

  bool found = false;
  for (int i = 0; i < taosArrayGetSize(pUserNames); i++) {
    char *name = taosArrayGetP(pUserNames, i);
    if (strlen(name) == strlen(TSDB_DEFAULT_USER) && strncmp(name, TSDB_DEFAULT_USER, strlen(TSDB_DEFAULT_USER)) == 0) {
      found = true;
      break;
    }
  }
  if (found == false) {
    char *name = taosStrdup(TSDB_DEFAULT_USER);
    taosArrayPush(pUserNames, &name);
  }

  SArray *fqdns = mndGetAllDnodeFqdns(pMnode);

  for (int i = 0; i < taosArrayGetSize(fqdns); i++) {
    char *fqdn = taosArrayGetP(fqdns, i);

    for (int j = 0; j < taosArrayGetSize(pUserNames); j++) {
      char *name = taosArrayGetP(pUserNames, j);
      mndUpdateIpWhiteImpl(pIpWhiteTab, name, fqdn, IP_WHITE_ADD);
    }
  }

  for (int i = 0; i < taosArrayGetSize(fqdns); i++) {
    char *fqdn = taosArrayGetP(fqdns, i);
    taosMemoryFree(fqdn);
  }
  taosArrayDestroy(fqdns);

  for (int i = 0; i < taosArrayGetSize(pUserNames); i++) {
    taosMemoryFree(taosArrayGetP(pUserNames, i));
  }
  taosArrayDestroy(pUserNames);

  return pIpWhiteTab;
}

int32_t mndInitUser(SMnode *pMnode) {
  ipWhiteMgtInit();

  SSdbTable table = {
      .sdbType = SDB_USER,
      .keyType = SDB_KEY_BINARY,
      .deployFp = (SdbDeployFp)mndCreateDefaultUsers,
      .encodeFp = (SdbEncodeFp)mndUserActionEncode,
      .decodeFp = (SdbDecodeFp)mndUserActionDecode,
      .insertFp = (SdbInsertFp)mndUserActionInsert,
      .updateFp = (SdbUpdateFp)mndUserActionUpdate,
      .deleteFp = (SdbDeleteFp)mndUserActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_USER, mndProcessCreateUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_USER, mndProcessAlterUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_USER, mndProcessDropUserReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_USER_AUTH, mndProcessGetUserAuthReq);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_USER_WHITELIST, mndProcessGetUserWhiteListReq);

  mndSetMsgHandle(pMnode, TDMT_MND_RETRIEVE_IP_WHITE, mndProcesSRetrieveIpWhiteReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_USER, mndRetrieveUsers);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_USER, mndCancelGetNextUser);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndRetrievePrivileges);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndCancelGetNextPrivileges);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupUser(SMnode *pMnode) { ipWhiteMgtCleanup(); }

static void ipRangeToStr(SIpV4Range *range, char *buf) {
  struct in_addr addr;
  addr.s_addr = range->ip;

  uv_inet_ntop(AF_INET, &addr, buf, 32);
  if (range->mask != 32) {
    sprintf(buf + strlen(buf), "/%d", range->mask);
  }
  return;
}
static bool isDefaultRange(SIpV4Range *pRange) {
  static SIpV4Range val = {.ip = 16777343, .mask = 32};
  return pRange->ip == val.ip && pRange->mask == val.mask;
}
static int32_t ipRangeListToStr(SIpV4Range *range, int32_t num, char *buf) {
  int32_t len = 0;
  for (int i = 0; i < num; i++) {
    char        tbuf[36] = {0};
    SIpV4Range *pRange = &range[i];

    ipRangeToStr(&range[i], tbuf);
    len += sprintf(buf + len, "%s,", tbuf);
  }
  if (len > 0) buf[len - 1] = 0;
  return len;
}

static bool isIpRangeEqual(SIpV4Range *a, SIpV4Range *b) {
  // equal or not
  return a->ip == b->ip && a->mask == b->mask;
}
static bool isRangeInIpWhiteList(SIpWhiteList *pList, SIpV4Range *tgt) {
  for (int i = 0; i < pList->num; i++) {
    if (isIpRangeEqual(&pList->pIpRange[i], tgt)) return true;
  }
  return false;
}
static bool isIpWhiteListEqual(SIpWhiteList *a, SIpWhiteList *b) {
  if (a->num != b->num) {
    return false;
  }
  for (int i = 0; i < a->num; i++) {
    if (!isIpRangeEqual(&a->pIpRange[i], &b->pIpRange[i])) {
      return false;
    }
  }
  return true;
}
int32_t convertIpWhiteListToStr(SIpWhiteList *pList, char **buf) {
  if (pList->num == 0) {
    *buf = NULL;
    return 0;
  }
  *buf = taosMemoryCalloc(1, pList->num * 36);
  int32_t len = ipRangeListToStr(pList->pIpRange, pList->num, *buf);
  if (len == 0) {
    taosMemoryFree(*buf);
    return 0;
  }
  return strlen(*buf);
}
int32_t tSerializeIpWhiteList(void *buf, int32_t len, SIpWhiteList *pList) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, len);

  if (tStartEncode(&encoder) < 0) return -1;
  if (tEncodeI32(&encoder, pList->num) < 0) return -1;

  for (int i = 0; i < pList->num; i++) {
    SIpV4Range *pRange = &(pList->pIpRange[i]);
    if (tEncodeU32(&encoder, pRange->ip) < 0) return -1;
    if (tEncodeU32(&encoder, pRange->mask) < 0) return -1;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDerializeIpWhileList(void *buf, int32_t len, SIpWhiteList *pList) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, len);

  if (tStartDecode(&decoder) < 0) return -1;
  if (tDecodeI32(&decoder, &pList->num) < 0) return -1;

  for (int i = 0; i < pList->num; i++) {
    SIpV4Range *pRange = &(pList->pIpRange[i]);
    if (tDecodeU32(&decoder, &pRange->ip) < 0) return -1;
    if (tDecodeU32(&decoder, &pRange->mask) < 0) return -1;
  }
  tEndDecode(&decoder);
  tDecoderClear(&decoder);

  return 0;
}
SIpWhiteList *createIpWhiteList(void *buf, int32_t len) {
  int32_t  num = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, len);

  if (tStartDecode(&decoder) < 0) return NULL;
  if (tDecodeI32(&decoder, &num) < 0) return NULL;
  tEndDecode(&decoder);
  tDecoderClear(&decoder);

  SIpWhiteList *p = taosMemoryCalloc(1, sizeof(SIpWhiteList) + num * sizeof(SIpV4Range));
  tDerializeIpWhileList(buf, len, p);
  return p;
}

static SIpWhiteList *createDefaultIpWhiteList() {
  SIpWhiteList *pWhiteList = taosMemoryCalloc(1, sizeof(SIpWhiteList) + sizeof(SIpV4Range) * 1);
  pWhiteList->num = 1;
  SIpV4Range *range = &(pWhiteList->pIpRange[0]);

  struct in_addr addr;
  if (uv_inet_pton(AF_INET, "127.0.0.1", &addr) == 0) {
    range->ip = addr.s_addr;
    range->mask = 32;
  }
  return pWhiteList;
}

static int32_t mndCreateDefaultUser(SMnode *pMnode, char *acct, char *user, char *pass) {
  SUserObj userObj = {0};
  taosEncryptPass_c((uint8_t *)pass, strlen(pass), userObj.pass);
  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.sysInfo = 1;
  userObj.enable = 1;
  userObj.ipWhiteListVer = taosGetTimestampMs();
  userObj.pIpWhiteList = createDefaultIpWhiteList();
  if (strcmp(user, TSDB_DEFAULT_USER) == 0) {
    userObj.superUser = 1;
  }

  SSdbRaw *pRaw = mndUserActionEncode(&userObj);
  if (pRaw == NULL) goto _ERROR;
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mInfo("user:%s, will be created when deploying, raw:%p", userObj.user, pRaw);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-user");
  if (pTrans == NULL) {
    sdbFreeRaw(pRaw);
    mError("user:%s, failed to create since %s", userObj.user, terrstr());
    goto _ERROR;
  }
  mInfo("trans:%d, used to create user:%s", pTrans->id, userObj.user);

  if (mndTransAppendCommitlog(pTrans, pRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _ERROR;
  }
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _ERROR;
  }

  mndTransDrop(pTrans);
  taosMemoryFree(userObj.pIpWhiteList);
  return 0;
_ERROR:
  taosMemoryFree(userObj.pIpWhiteList);
  return -1;
}

static int32_t mndCreateDefaultUsers(SMnode *pMnode) {
  if (mndCreateDefaultUser(pMnode, TSDB_DEFAULT_USER, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS) != 0) {
    return -1;
  }

  return 0;
}

SSdbRaw *mndUserActionEncode(SUserObj *pUser) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t ipWhiteReserve =
      pUser->pIpWhiteList ? (sizeof(SIpV4Range) * pUser->pIpWhiteList->num + sizeof(SIpWhiteList) + 4) : 16;
  int32_t numOfReadDbs = taosHashGetSize(pUser->readDbs);
  int32_t numOfWriteDbs = taosHashGetSize(pUser->writeDbs);
  int32_t numOfReadTbs = taosHashGetSize(pUser->readTbs);
  int32_t numOfWriteTbs = taosHashGetSize(pUser->writeTbs);
  int32_t numOfAlterTbs = taosHashGetSize(pUser->alterTbs);
  int32_t numOfReadViews = taosHashGetSize(pUser->readViews);
  int32_t numOfWriteViews = taosHashGetSize(pUser->writeViews);
  int32_t numOfAlterViews = taosHashGetSize(pUser->alterViews);
  int32_t numOfTopics = taosHashGetSize(pUser->topics);
  int32_t numOfUseDbs = taosHashGetSize(pUser->useDbs);
  int32_t size = sizeof(SUserObj) + USER_RESERVE_SIZE + (numOfReadDbs + numOfWriteDbs) * TSDB_DB_FNAME_LEN +
                 numOfTopics * TSDB_TOPIC_FNAME_LEN + ipWhiteReserve;

  char *stb = taosHashIterate(pUser->readTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->readTbs, stb);
  }

  stb = taosHashIterate(pUser->writeTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->writeTbs, stb);
  }

  stb = taosHashIterate(pUser->alterTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->alterTbs, stb);
  }

  stb = taosHashIterate(pUser->readViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->readViews, stb);
  }

  stb = taosHashIterate(pUser->writeViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->writeViews, stb);
  }

  stb = taosHashIterate(pUser->alterViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    size += sizeof(int32_t);
    size += valueLen;
    stb = taosHashIterate(pUser->alterViews, stb);
  }

  int32_t *useDb = taosHashIterate(pUser->useDbs, NULL);
  while (useDb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(useDb, &keyLen);
    size += sizeof(int32_t);
    size += keyLen;
    size += sizeof(int32_t);
    useDb = taosHashIterate(pUser->useDbs, useDb);
  }

  SSdbRaw *pRaw = sdbAllocRaw(SDB_USER, USER_VER_NUMBER, size);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pUser->pass, TSDB_PASSWORD_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->updateTime, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->superUser, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->sysInfo, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->enable, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->reserve, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pUser->authVersion, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pUser->passVersion, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfReadDbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfWriteDbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfTopics, _OVER)

  char *db = taosHashIterate(pUser->readDbs, NULL);
  while (db != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER);
    db = taosHashIterate(pUser->readDbs, db);
  }

  db = taosHashIterate(pUser->writeDbs, NULL);
  while (db != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER);
    db = taosHashIterate(pUser->writeDbs, db);
  }

  char *topic = taosHashIterate(pUser->topics, NULL);
  while (topic != NULL) {
    SDB_SET_BINARY(pRaw, dataPos, topic, TSDB_TOPIC_FNAME_LEN, _OVER);
    topic = taosHashIterate(pUser->topics, topic);
  }

  SDB_SET_INT32(pRaw, dataPos, numOfReadTbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfWriteTbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfAlterTbs, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfReadViews, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfWriteViews, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfAlterViews, _OVER)
  SDB_SET_INT32(pRaw, dataPos, numOfUseDbs, _OVER)

  stb = taosHashIterate(pUser->readTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->readTbs, stb);
  }

  stb = taosHashIterate(pUser->writeTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->writeTbs, stb);
  }

  stb = taosHashIterate(pUser->alterTbs, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->alterTbs, stb);
  }

  stb = taosHashIterate(pUser->readViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->readViews, stb);
  }

  stb = taosHashIterate(pUser->writeViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->writeViews, stb);
  }

  stb = taosHashIterate(pUser->alterViews, NULL);
  while (stb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(stb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    size_t valueLen = 0;
    valueLen = strlen(stb) + 1;
    SDB_SET_INT32(pRaw, dataPos, valueLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, stb, valueLen, _OVER);
    stb = taosHashIterate(pUser->alterViews, stb);
  }

  useDb = taosHashIterate(pUser->useDbs, NULL);
  while (useDb != NULL) {
    size_t keyLen = 0;
    void  *key = taosHashGetKey(useDb, &keyLen);
    SDB_SET_INT32(pRaw, dataPos, keyLen, _OVER)
    SDB_SET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

    SDB_SET_INT32(pRaw, dataPos, *useDb, _OVER)
    useDb = taosHashIterate(pUser->useDbs, useDb);
  }

  // save white list
  int32_t num = pUser->pIpWhiteList->num;
  int32_t tlen = sizeof(SIpWhiteList) + num * sizeof(SIpV4Range) + 4;
  char   *buf = taosMemoryCalloc(1, tlen);
  int32_t len = tSerializeIpWhiteList(buf, tlen, pUser->pIpWhiteList);

  SDB_SET_INT32(pRaw, dataPos, len, _OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, len, _OVER);
  taosMemoryFree(buf);

  SDB_SET_INT64(pRaw, dataPos, pUser->ipWhiteListVer, _OVER);

  SDB_SET_RESERVE(pRaw, dataPos, USER_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("user:%s, failed to encode to raw:%p since %s", pUser->user, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("user:%s, encode to raw:%p, row:%p", pUser->user, pRaw, pUser);
  return pRaw;
}

static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow  *pRow = NULL;
  SUserObj *pUser = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver < 1 || sver > USER_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SUserObj));
  if (pRow == NULL) goto _OVER;

  pUser = sdbGetRowObj(pRow);
  if (pUser == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pUser->pass, TSDB_PASSWORD_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->updateTime, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->superUser, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->sysInfo, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->enable, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->reserve, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pUser->authVersion, _OVER)
  if (sver >= 4) {
    SDB_GET_INT32(pRaw, dataPos, &pUser->passVersion, _OVER)
  }

  int32_t numOfReadDbs = 0;
  int32_t numOfWriteDbs = 0;
  int32_t numOfTopics = 0;
  SDB_GET_INT32(pRaw, dataPos, &numOfReadDbs, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &numOfWriteDbs, _OVER)
  if (sver >= 2) {
    SDB_GET_INT32(pRaw, dataPos, &numOfTopics, _OVER)
  }

  pUser->readDbs = taosHashInit(numOfReadDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pUser->writeDbs =
      taosHashInit(numOfWriteDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  pUser->topics = taosHashInit(numOfTopics, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pUser->readDbs == NULL || pUser->writeDbs == NULL || pUser->topics == NULL) goto _OVER;

  for (int32_t i = 0; i < numOfReadDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER)
    int32_t len = strlen(db) + 1;
    taosHashPut(pUser->readDbs, db, len, db, TSDB_DB_FNAME_LEN);
  }

  for (int32_t i = 0; i < numOfWriteDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER)
    int32_t len = strlen(db) + 1;
    taosHashPut(pUser->writeDbs, db, len, db, TSDB_DB_FNAME_LEN);
  }

  if (sver >= 2) {
    for (int32_t i = 0; i < numOfTopics; ++i) {
      char topic[TSDB_TOPIC_FNAME_LEN] = {0};
      SDB_GET_BINARY(pRaw, dataPos, topic, TSDB_TOPIC_FNAME_LEN, _OVER)
      int32_t len = strlen(topic) + 1;
      taosHashPut(pUser->topics, topic, len, topic, TSDB_TOPIC_FNAME_LEN);
    }
  }

  if (sver >= 3) {
    int32_t numOfReadTbs = 0;
    int32_t numOfWriteTbs = 0;
    int32_t numOfAlterTbs = 0;
    int32_t numOfReadViews = 0;
    int32_t numOfWriteViews = 0;
    int32_t numOfAlterViews = 0;
    int32_t numOfUseDbs = 0;
    SDB_GET_INT32(pRaw, dataPos, &numOfReadTbs, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &numOfWriteTbs, _OVER)
    if (sver >= 6) {
      SDB_GET_INT32(pRaw, dataPos, &numOfAlterTbs, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &numOfReadViews, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &numOfWriteViews, _OVER)
      SDB_GET_INT32(pRaw, dataPos, &numOfAlterViews, _OVER)
    }
    SDB_GET_INT32(pRaw, dataPos, &numOfUseDbs, _OVER)

    pUser->readTbs =
        taosHashInit(numOfReadTbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    pUser->writeTbs =
        taosHashInit(numOfWriteTbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    pUser->alterTbs =
        taosHashInit(numOfAlterTbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

    pUser->readViews =
        taosHashInit(numOfReadViews, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    pUser->writeViews =
        taosHashInit(numOfWriteViews, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    pUser->alterViews =
        taosHashInit(numOfAlterViews, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

    pUser->useDbs = taosHashInit(numOfUseDbs, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);

    for (int32_t i = 0; i < numOfReadTbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      char *key = taosMemoryCalloc(keyLen, sizeof(char));
      memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t valuelen = 0;
      SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
      char *value = taosMemoryCalloc(valuelen, sizeof(char));
      memset(value, 0, valuelen);
      SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

      taosHashPut(pUser->readTbs, key, keyLen, value, valuelen);

      taosMemoryFree(key);
      taosMemoryFree(value);
    }

    for (int32_t i = 0; i < numOfWriteTbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      char *key = taosMemoryCalloc(keyLen, sizeof(char));
      memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t valuelen = 0;
      SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
      char *value = taosMemoryCalloc(valuelen, sizeof(char));
      memset(value, 0, valuelen);
      SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

      taosHashPut(pUser->writeTbs, key, keyLen, value, valuelen);

      taosMemoryFree(key);
      taosMemoryFree(value);
    }

    if (sver >= 6) {
      for (int32_t i = 0; i < numOfAlterTbs; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        char *key = taosMemoryCalloc(keyLen, sizeof(char));
        memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        char *value = taosMemoryCalloc(valuelen, sizeof(char));
        memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        taosHashPut(pUser->alterTbs, key, keyLen, value, valuelen);

        taosMemoryFree(key);
        taosMemoryFree(value);
      }

      for (int32_t i = 0; i < numOfReadViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        char *key = taosMemoryCalloc(keyLen, sizeof(char));
        memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        char *value = taosMemoryCalloc(valuelen, sizeof(char));
        memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        taosHashPut(pUser->readViews, key, keyLen, value, valuelen);

        taosMemoryFree(key);
        taosMemoryFree(value);
      }

      for (int32_t i = 0; i < numOfWriteViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        char *key = taosMemoryCalloc(keyLen, sizeof(char));
        memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        char *value = taosMemoryCalloc(valuelen, sizeof(char));
        memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        taosHashPut(pUser->writeViews, key, keyLen, value, valuelen);

        taosMemoryFree(key);
        taosMemoryFree(value);
      }

      for (int32_t i = 0; i < numOfAlterViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        char *key = taosMemoryCalloc(keyLen, sizeof(char));
        memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        char *value = taosMemoryCalloc(valuelen, sizeof(char));
        memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        taosHashPut(pUser->alterViews, key, keyLen, value, valuelen);

        taosMemoryFree(key);
        taosMemoryFree(value);
      }
    }

    for (int32_t i = 0; i < numOfUseDbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      char *key = taosMemoryCalloc(keyLen, sizeof(char));
      memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t ref = 0;
      SDB_GET_INT32(pRaw, dataPos, &ref, _OVER);

      taosHashPut(pUser->useDbs, key, keyLen, &ref, sizeof(ref));
      taosMemoryFree(key);
    }
  }
  // decoder white list
  if (sver >= 5) {
    int32_t len = 0;
    SDB_GET_INT32(pRaw, dataPos, &len, _OVER);

    char *buf = taosMemoryMalloc(len);
    if (buf == NULL) goto _OVER;
    SDB_GET_BINARY(pRaw, dataPos, buf, len, _OVER);

    pUser->pIpWhiteList = createIpWhiteList(buf, len);
    taosMemoryFree(buf);

    SDB_GET_INT64(pRaw, dataPos, &pUser->ipWhiteListVer, _OVER);
  }

  if (pUser->pIpWhiteList == NULL) {
    pUser->pIpWhiteList = createDefaultIpWhiteList();
    pUser->ipWhiteListVer = taosGetTimestampMs();
  }

  SDB_GET_RESERVE(pRaw, dataPos, USER_RESERVE_SIZE, _OVER)
  taosInitRWLatch(&pUser->lock);

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("user:%s, failed to decode from raw:%p since %s", pUser == NULL ? "null" : pUser->user, pRaw, terrstr());
    if (pUser != NULL) {
      taosHashCleanup(pUser->readDbs);
      taosHashCleanup(pUser->writeDbs);
      taosHashCleanup(pUser->topics);
      taosHashCleanup(pUser->readTbs);
      taosHashCleanup(pUser->writeTbs);
      taosHashCleanup(pUser->alterTbs);
      taosHashCleanup(pUser->readViews);
      taosHashCleanup(pUser->writeViews);
      taosHashCleanup(pUser->alterViews);
      taosHashCleanup(pUser->useDbs);
      taosMemoryFreeClear(pUser->pIpWhiteList);
    }
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("user:%s, decode from raw:%p, row:%p", pUser->user, pRaw, pUser);
  return pRow;
}

static int32_t mndUserActionInsert(SSdb *pSdb, SUserObj *pUser) {
  mTrace("user:%s, perform insert action, row:%p", pUser->user, pUser);

  SAcctObj *pAcct = sdbAcquire(pSdb, SDB_ACCT, pUser->acct);
  if (pAcct == NULL) {
    terrno = TSDB_CODE_MND_ACCT_NOT_EXIST;
    mError("user:%s, failed to perform insert action since %s", pUser->user, terrstr());
    return -1;
  }
  pUser->acctId = pAcct->acctId;
  sdbRelease(pSdb, pAcct);

  return 0;
}

SHashObj *mndDupTableHash(SHashObj *pOld) {
  SHashObj *pNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pNew == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  char *tb = taosHashIterate(pOld, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    char  *key = taosHashGetKey(tb, &keyLen);

    int32_t valueLen = strlen(tb) + 1;
    if (taosHashPut(pNew, key, keyLen, tb, valueLen) != 0) {
      taosHashCancelIterate(pOld, tb);
      taosHashCleanup(pNew);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    tb = taosHashIterate(pOld, tb);
  }

  return pNew;
}

SHashObj *mndDupUseDbHash(SHashObj *pOld) {
  SHashObj *pNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pNew == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t *db = taosHashIterate(pOld, NULL);
  while (db != NULL) {
    size_t keyLen = 0;
    char  *key = taosHashGetKey(db, &keyLen);

    if (taosHashPut(pNew, key, keyLen, db, sizeof(*db)) != 0) {
      taosHashCancelIterate(pOld, db);
      taosHashCleanup(pNew);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    db = taosHashIterate(pOld, db);
  }

  return pNew;
}

int32_t mndUserDupObj(SUserObj *pUser, SUserObj *pNew) {
  memcpy(pNew, pUser, sizeof(SUserObj));
  pNew->authVersion++;
  pNew->updateTime = taosGetTimestampMs();

  taosRLockLatch(&pUser->lock);
  pNew->readDbs = mndDupDbHash(pUser->readDbs);
  pNew->writeDbs = mndDupDbHash(pUser->writeDbs);
  pNew->readTbs = mndDupTableHash(pUser->readTbs);
  pNew->writeTbs = mndDupTableHash(pUser->writeTbs);
  pNew->alterTbs = mndDupTableHash(pUser->alterTbs);
  pNew->readViews = mndDupTableHash(pUser->readViews);
  pNew->writeViews = mndDupTableHash(pUser->writeViews);
  pNew->alterViews = mndDupTableHash(pUser->alterViews);
  pNew->topics = mndDupTopicHash(pUser->topics);
  pNew->useDbs = mndDupUseDbHash(pUser->useDbs);
  pNew->pIpWhiteList = cloneIpWhiteList(pUser->pIpWhiteList);

  taosRUnLockLatch(&pUser->lock);

  if (pNew->readDbs == NULL || pNew->writeDbs == NULL || pNew->topics == NULL) {
    return -1;
  }
  return 0;
}

void mndUserFreeObj(SUserObj *pUser) {
  taosHashCleanup(pUser->readDbs);
  taosHashCleanup(pUser->writeDbs);
  taosHashCleanup(pUser->topics);
  taosHashCleanup(pUser->readTbs);
  taosHashCleanup(pUser->writeTbs);
  taosHashCleanup(pUser->alterTbs);
  taosHashCleanup(pUser->readViews);
  taosHashCleanup(pUser->writeViews);
  taosHashCleanup(pUser->alterViews);
  taosHashCleanup(pUser->useDbs);
  taosMemoryFreeClear(pUser->pIpWhiteList);
  pUser->readDbs = NULL;
  pUser->writeDbs = NULL;
  pUser->topics = NULL;
  pUser->readTbs = NULL;
  pUser->writeTbs = NULL;
  pUser->alterTbs = NULL;
  pUser->readViews = NULL;
  pUser->writeViews = NULL;
  pUser->alterViews = NULL;
  pUser->useDbs = NULL;
}

static int32_t mndUserActionDelete(SSdb *pSdb, SUserObj *pUser) {
  mTrace("user:%s, perform delete action, row:%p", pUser->user, pUser);
  mndUserFreeObj(pUser);
  return 0;
}

static int32_t mndUserActionUpdate(SSdb *pSdb, SUserObj *pOld, SUserObj *pNew) {
  mTrace("user:%s, perform update action, old row:%p new row:%p", pOld->user, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  pOld->authVersion = pNew->authVersion;
  pOld->passVersion = pNew->passVersion;
  pOld->sysInfo = pNew->sysInfo;
  pOld->enable = pNew->enable;
  memcpy(pOld->pass, pNew->pass, TSDB_PASSWORD_LEN);
  TSWAP(pOld->readDbs, pNew->readDbs);
  TSWAP(pOld->writeDbs, pNew->writeDbs);
  TSWAP(pOld->topics, pNew->topics);
  TSWAP(pOld->readTbs, pNew->readTbs);
  TSWAP(pOld->writeTbs, pNew->writeTbs);
  TSWAP(pOld->alterTbs, pNew->alterTbs);
  TSWAP(pOld->readViews, pNew->readViews);
  TSWAP(pOld->writeViews, pNew->writeViews);
  TSWAP(pOld->alterViews, pNew->alterViews);
  TSWAP(pOld->useDbs, pNew->useDbs);

  int32_t sz = sizeof(SIpWhiteList) + pNew->pIpWhiteList->num * sizeof(SIpV4Range);
  pOld->pIpWhiteList = taosMemoryRealloc(pOld->pIpWhiteList, sz);
  memcpy(pOld->pIpWhiteList, pNew->pIpWhiteList, sz);
  pOld->ipWhiteListVer = pNew->ipWhiteListVer;

  taosWUnLockLatch(&pOld->lock);

  return 0;
}

SUserObj *mndAcquireUser(SMnode *pMnode, const char *userName) {
  SSdb     *pSdb = pMnode->pSdb;
  SUserObj *pUser = sdbAcquire(pSdb, SDB_USER, userName);
  if (pUser == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    } else {
      terrno = TSDB_CODE_MND_USER_NOT_AVAILABLE;
    }
  }
  return pUser;
}

void mndReleaseUser(SMnode *pMnode, SUserObj *pUser) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pUser);
}

static int32_t mndCreateUser(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SRpcMsg *pReq) {
  SUserObj userObj = {0};
  taosEncryptPass_c((uint8_t *)pCreate->pass, strlen(pCreate->pass), userObj.pass);
  tstrncpy(userObj.user, pCreate->user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.superUser = 0;  // pCreate->superUser;
  userObj.sysInfo = pCreate->sysInfo;
  userObj.enable = pCreate->enable;

  if (pCreate->numIpRanges == 0) {
    userObj.pIpWhiteList = createDefaultIpWhiteList();

  } else {
    SHashObj *pUniqueTab = taosHashInit(64, MurmurHash3_32, false, HASH_NO_LOCK);
    int32_t   dummpy = 0;
    for (int i = 0; i < pCreate->numIpRanges; i++) {
      SIpV4Range range = {.ip = pCreate->pIpRanges[i].ip, .mask = pCreate->pIpRanges[i].mask};
      taosHashPut(pUniqueTab, &range, sizeof(range), &dummpy, sizeof(dummpy));
    }
    taosHashPut(pUniqueTab, &defaultIpRange, sizeof(defaultIpRange), &dummpy, sizeof(dummpy));

    if (taosHashGetSize(pUniqueTab) > MND_MAX_USE_HOST) {
      terrno = TSDB_CODE_MND_TOO_MANY_USER_HOST;
      taosHashCleanup(pUniqueTab);
      return terrno;
    }

    int32_t       numOfRanges = taosHashGetSize(pUniqueTab);
    SIpWhiteList *p = taosMemoryCalloc(1, sizeof(SIpWhiteList) + numOfRanges * sizeof(SIpV4Range));
    void         *pIter = taosHashIterate(pUniqueTab, NULL);
    int32_t       i = 0;
    while (pIter) {
      size_t      len = 0;
      SIpV4Range *key = taosHashGetKey(pIter, &len);
      p->pIpRange[i].ip = key->ip;
      p->pIpRange[i].mask = key->mask;
      pIter = taosHashIterate(pUniqueTab, pIter);

      i++;
    }

    taosHashCleanup(pUniqueTab);
    p->num = numOfRanges;
    userObj.pIpWhiteList = p;
  }

  userObj.ipWhiteListVer = taosGetTimestampMs();

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "create-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to create since %s", pCreate->user, terrstr());

    taosMemoryFree(userObj.pIpWhiteList);
    return -1;
  }
  mInfo("trans:%d, used to create user:%s", pTrans->id, pCreate->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(&userObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    goto _OVER;
  }
  ipWhiteMgtUpdate(pMnode, userObj.user, userObj.pIpWhiteList);
  taosMemoryFree(userObj.pIpWhiteList);

  mndTransDrop(pTrans);
  return 0;
_OVER:
  taosMemoryFree(userObj.pIpWhiteList);

  return -1;
}

static int32_t mndProcessCreateUserReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = -1;
  SUserObj      *pUser = NULL;
  SUserObj      *pOperUser = NULL;
  SCreateUserReq createReq = {0};

  if (tDeserializeSCreateUserReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("user:%s, start to create", createReq.user);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_USER) != 0) {
    goto _OVER;
  }

  if (createReq.user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    goto _OVER;
  }

  if (createReq.pass[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_PASS_FORMAT;
    goto _OVER;
  }

  if (strlen(createReq.pass) >= TSDB_PASSWORD_LEN) {
    terrno = TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, createReq.user);
  if (pUser != NULL) {
    terrno = TSDB_CODE_MND_USER_ALREADY_EXIST;
    goto _OVER;
  }

  pOperUser = mndAcquireUser(pMnode, pReq->info.conn.user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto _OVER;
  }

  if ((terrno = grantCheck(TSDB_GRANT_USER)) != 0) {
    code = terrno;
    goto _OVER;
  }

  code = mndCreateUser(pMnode, pOperUser->acct, &createReq, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char detail[1000] = {0};
  sprintf(detail, "enable:%d, superUser:%d, sysInfo:%d, password:xxx",
          createReq.enable, createReq.superUser, createReq.sysInfo);

  auditRecord(pReq, pMnode->clusterId, "createUser", "", createReq.user, detail, strlen(detail));

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to create since %s", createReq.user, terrstr());
  }

  mndReleaseUser(pMnode, pUser);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSCreateUserReq(&createReq);

  return code;
}

int32_t mndProcessGetUserWhiteListReq(SRpcMsg *pReq) {
  SMnode              *pMnode = pReq->info.node;
  int32_t              code = -1;
  SUserObj            *pUser = NULL;
  SGetUserWhiteListReq wlReq = {0};
  SGetUserWhiteListRsp wlRsp = {0};

  if (tDeserializeSGetUserWhiteListReq(pReq->pCont, pReq->contLen, &wlReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }
  mTrace("user: %s, start to get whitelist", wlReq.user);

  pUser = mndAcquireUser(pMnode, wlReq.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    goto _OVER;
  }

  code = mndSetUserWhiteListRsp(pMnode, pUser, &wlRsp);
  if (code) {
    goto _OVER;
  }
  int32_t contLen = tSerializeSGetUserWhiteListRsp(NULL, 0, &wlRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  tSerializeSGetUserWhiteListRsp(pRsp, contLen, &wlRsp);

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;
  code = 0;
_OVER:
  mndReleaseUser(pMnode, pUser);
  tFreeSGetUserWhiteListRsp(&wlRsp);
  return code;
}

int32_t mndProcesSRetrieveIpWhiteReq(SRpcMsg *pReq) {
  // impl later
  SRetrieveIpWhiteReq req = {0};
  if (tDeserializeRetrieveIpWhite(pReq->pCont, pReq->contLen, &req) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  SUpdateIpWhite ipWhite = {0};
  ipWhiteMgtFillMsg(&ipWhite);

  int32_t len = tSerializeSUpdateIpWhite(NULL, 0, &ipWhite);

  void *pRsp = rpcMallocCont(len);
  tSerializeSUpdateIpWhite(pRsp, len, &ipWhite);

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = len;
  //}
  tFreeSUpdateIpWhiteReq(&ipWhite);

  return 0;
_OVER:
  return -1;
}

static int32_t mndAlterUser(SMnode *pMnode, SUserObj *pOld, SUserObj *pNew, SRpcMsg *pReq) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "alter-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to alter since %s", pOld->user, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to alter user:%s", pTrans->id, pOld->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pNew);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  ipWhiteMgtUpdate(pMnode, pNew->user, pNew->pIpWhiteList);
  mndTransDrop(pTrans);
  return 0;
}

SHashObj *mndDupObjHash(SHashObj *pOld, int32_t dataLen) {
  SHashObj *pNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pNew == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  char *db = taosHashIterate(pOld, NULL);
  while (db != NULL) {
    int32_t len = strlen(db) + 1;
    if (taosHashPut(pNew, db, len, db, dataLen) != 0) {
      taosHashCancelIterate(pOld, db);
      taosHashCleanup(pNew);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    db = taosHashIterate(pOld, db);
  }

  return pNew;
}

SHashObj *mndDupDbHash(SHashObj *pOld) { return mndDupObjHash(pOld, TSDB_DB_FNAME_LEN); }

SHashObj *mndDupTopicHash(SHashObj *pOld) { return mndDupObjHash(pOld, TSDB_TOPIC_FNAME_LEN); }

static int32_t mndTablePriviledge(SMnode *pMnode, SHashObj *hash, SHashObj *useDbHash, SAlterUserReq *alterReq,
                                  SSdb *pSdb) {
  void *pIter = NULL;
  char  tbFName[TSDB_TABLE_FNAME_LEN] = {0};

  snprintf(tbFName, sizeof(tbFName), "%s.%s", alterReq->objname, alterReq->tabName);
  int32_t len = strlen(tbFName) + 1;

  if (alterReq->tagCond != NULL && alterReq->tagCondLen != 0) {
    char *value = taosHashGet(hash, tbFName, len);
    if (value != NULL) {
      terrno = TSDB_CODE_MND_PRIVILEDGE_EXIST;
      return -1;
    }

    int32_t condLen = alterReq->tagCondLen;
    if (taosHashPut(hash, tbFName, len, alterReq->tagCond, condLen) != 0) {
      return -1;
    }
  } else {
    if (taosHashPut(hash, tbFName, len, alterReq->isView ? "v" : "t", 2) != 0) {
      return -1;
    }
  }

  int32_t  dbKeyLen = strlen(alterReq->objname) + 1;
  int32_t  ref = 1;
  int32_t *currRef = taosHashGet(useDbHash, alterReq->objname, dbKeyLen);
  if (NULL != currRef) {
    ref = (*currRef) + 1;
  }
  if (taosHashPut(useDbHash, alterReq->objname, dbKeyLen, &ref, sizeof(ref)) != 0) {
    return -1;
  }

  return 0;
}

static int32_t mndRemoveTablePriviledge(SMnode *pMnode, SHashObj *hash, SHashObj *useDbHash, SAlterUserReq *alterReq,
                                        SSdb *pSdb) {
  void *pIter = NULL;
  char  tbFName[TSDB_TABLE_FNAME_LEN] = {0};
  snprintf(tbFName, sizeof(tbFName), "%s.%s", alterReq->objname, alterReq->tabName);
  int32_t len = strlen(tbFName) + 1;

  if (taosHashRemove(hash, tbFName, len) != 0) {
    return -1;
  }

  int32_t  dbKeyLen = strlen(alterReq->objname) + 1;
  int32_t *currRef = taosHashGet(useDbHash, alterReq->objname, dbKeyLen);
  if (NULL == currRef) {
    return 0;
  }
  
  if (1 == *currRef) {
    if (taosHashRemove(useDbHash, alterReq->objname, dbKeyLen) != 0) {
      return -1;
    }
    return 0;
  }
  int32_t ref = (*currRef) - 1;
  if (taosHashPut(useDbHash, alterReq->objname, dbKeyLen, &ref, sizeof(ref)) != 0) {
    return -1;
  }

  return 0;
}

static char *mndUserAuditTypeStr(int32_t type) {
  if (type == TSDB_ALTER_USER_PASSWD) {
    return "changePassword";
  }
  if (type == TSDB_ALTER_USER_SUPERUSER) {
    return "changeSuperUser";
  }
  if (type == TSDB_ALTER_USER_ENABLE) {
    return "enableUser";
  }
  if (type == TSDB_ALTER_USER_SYSINFO) {
    return "userSysInfo";
  }
  return "error";
}

static int32_t mndProcessAlterUserPrivilegesReq(SAlterUserReq *pAlterReq, SMnode *pMnode, SUserObj* pNewUser) {
  SSdb         *pSdb = pMnode->pSdb;
  void         *pIter = NULL;

  if (ALTER_USER_ADD_READ_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) || 
     ALTER_USER_ADD_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        return -1;
      }
      if (taosHashPut(pNewUser->readDbs, pAlterReq->objname, len, pAlterReq->objname, TSDB_DB_FNAME_LEN) != 0) {
        mndReleaseDb(pMnode, pDb);
        return -1;
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      while (1) {
        SDbObj *pDb = NULL;
        pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
        if (pIter == NULL) break;
        int32_t len = strlen(pDb->name) + 1;
        taosHashPut(pNewUser->readDbs, pDb->name, len, pDb->name, TSDB_DB_FNAME_LEN);
        sdbRelease(pSdb, pDb);
      }
    }
  }

  if (ALTER_USER_ADD_WRITE_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) || ALTER_USER_ADD_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        return -1;
      }
      if (taosHashPut(pNewUser->writeDbs, pAlterReq->objname, len, pAlterReq->objname, TSDB_DB_FNAME_LEN) != 0) {
        mndReleaseDb(pMnode, pDb);
        return -1;
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      while (1) {
        SDbObj *pDb = NULL;
        pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
        if (pIter == NULL) break;
        int32_t len = strlen(pDb->name) + 1;
        taosHashPut(pNewUser->writeDbs, pDb->name, len, pDb->name, TSDB_DB_FNAME_LEN);
        sdbRelease(pSdb, pDb);
      }
    }
  }

  if (ALTER_USER_DEL_READ_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) || ALTER_USER_DEL_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        return -1;
      }
      taosHashRemove(pNewUser->readDbs, pAlterReq->objname, len);
      mndReleaseDb(pMnode, pDb);
    } else {
      taosHashClear(pNewUser->readDbs);
    }
  }

  if (ALTER_USER_DEL_WRITE_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) || ALTER_USER_DEL_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        return -1;
      }
      taosHashRemove(pNewUser->writeDbs, pAlterReq->objname, len);
      mndReleaseDb(pMnode, pDb);
    } else {
      taosHashClear(pNewUser->writeDbs);
    }
  }

  SHashObj* pReadTbs = pNewUser->readTbs;
  SHashObj* pWriteTbs = pNewUser->writeTbs;
  SHashObj* pAlterTbs = pNewUser->alterTbs;

#ifdef TD_ENTERPRISE
  if (pAlterReq->isView) {
    pReadTbs = pNewUser->readViews;
    pWriteTbs = pNewUser->writeViews;
    pAlterTbs = pNewUser->alterViews;
  }
#endif

  if (ALTER_USER_ADD_READ_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) || ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (mndTablePriviledge(pMnode, pReadTbs, pNewUser->useDbs, pAlterReq, pSdb) != 0) return -1;
  }

  if (ALTER_USER_ADD_WRITE_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) || ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (mndTablePriviledge(pMnode, pWriteTbs, pNewUser->useDbs, pAlterReq, pSdb) != 0) return -1;
  }

  if (ALTER_USER_ADD_ALTER_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) || ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (mndTablePriviledge(pMnode, pAlterTbs, pNewUser->useDbs, pAlterReq, pSdb) != 0) return -1;
  }

  if (ALTER_USER_DEL_READ_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (mndRemoveTablePriviledge(pMnode, pReadTbs, pNewUser->useDbs, pAlterReq, pSdb) != 0) return -1;
  }

  if (ALTER_USER_DEL_WRITE_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (mndRemoveTablePriviledge(pMnode, pWriteTbs, pNewUser->useDbs, pAlterReq, pSdb) != 0) return -1;
  }

  if (ALTER_USER_DEL_ALTER_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (mndRemoveTablePriviledge(pMnode, pAlterTbs, pNewUser->useDbs, pAlterReq, pSdb) != 0) return -1;
  }

  if (ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(pAlterReq->alterType, pAlterReq->privileges)) {
    int32_t      len = strlen(pAlterReq->objname) + 1;
    SMqTopicObj *pTopic = mndAcquireTopic(pMnode, pAlterReq->objname);
    if (pTopic == NULL) {
      mndReleaseTopic(pMnode, pTopic);
      return -1;
    }
    taosHashPut(pNewUser->topics, pTopic->name, len, pTopic->name, TSDB_TOPIC_FNAME_LEN);
    mndReleaseTopic(pMnode, pTopic);
  }

  if (ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(pAlterReq->alterType, pAlterReq->privileges)) {
    int32_t      len = strlen(pAlterReq->objname) + 1;
    SMqTopicObj *pTopic = mndAcquireTopic(pMnode, pAlterReq->objname);
    if (pTopic == NULL) {
      mndReleaseTopic(pMnode, pTopic);
      return -1;
    }
    taosHashRemove(pNewUser->topics, pAlterReq->objname, len);
    mndReleaseTopic(pMnode, pTopic);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mndProcessAlterUserReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SSdb         *pSdb = pMnode->pSdb;
  void         *pIter = NULL;
  int32_t       code = -1;
  SUserObj     *pUser = NULL;
  SUserObj     *pOperUser = NULL;
  SUserObj      newUser = {0};
  SAlterUserReq alterReq = {0};

  if (tDeserializeSAlterUserReq(pReq->pCont, pReq->contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("user:%s, start to alter", alterReq.user);

  if (alterReq.user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    goto _OVER;
  }

  if (TSDB_ALTER_USER_PASSWD == alterReq.alterType &&
      (alterReq.pass[0] == 0 || strlen(alterReq.pass) >= TSDB_PASSWORD_LEN)) {
    terrno = TSDB_CODE_MND_INVALID_PASS_FORMAT;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, alterReq.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    goto _OVER;
  }

  pOperUser = mndAcquireUser(pMnode, pReq->info.conn.user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto _OVER;
  }

  if (mndCheckAlterUserPrivilege(pOperUser, pUser, &alterReq) != 0) {
    goto _OVER;
  }

  if (mndUserDupObj(pUser, &newUser) != 0) goto _OVER;

  if (alterReq.alterType == TSDB_ALTER_USER_PASSWD) {
    char pass[TSDB_PASSWORD_LEN + 1] = {0};
    taosEncryptPass_c((uint8_t *)alterReq.pass, strlen(alterReq.pass), pass);
    memcpy(newUser.pass, pass, TSDB_PASSWORD_LEN);
    if (0 != strncmp(pUser->pass, pass, TSDB_PASSWORD_LEN)) {
      ++newUser.passVersion;
    }
  }

  if (alterReq.alterType == TSDB_ALTER_USER_SUPERUSER) {
    newUser.superUser = alterReq.superUser;
  }

  if (alterReq.alterType == TSDB_ALTER_USER_ENABLE) {
    newUser.enable = alterReq.enable;
  }

  if (alterReq.alterType == TSDB_ALTER_USER_SYSINFO) {
    newUser.sysInfo = alterReq.sysInfo;
  }

  if (ALTER_USER_ADD_PRIVS(alterReq.alterType) || ALTER_USER_DEL_PRIVS(alterReq.alterType)) {
    if (0 != mndProcessAlterUserPrivilegesReq(&alterReq, pMnode, &newUser)) goto _OVER;
  }

  if (alterReq.alterType == TSDB_ALTER_USER_ADD_WHITE_LIST) {
    taosMemoryFreeClear(newUser.pIpWhiteList);

    int32_t num = pUser->pIpWhiteList->num + alterReq.numIpRanges;

    SIpWhiteList *pNew = taosMemoryCalloc(1, sizeof(SIpWhiteList) + sizeof(SIpV4Range) * num);
    int32_t       idx = pUser->pIpWhiteList->num;

    bool exist = false;
    memcpy(pNew->pIpRange, pUser->pIpWhiteList->pIpRange, sizeof(SIpV4Range) * idx);
    for (int i = 0; i < alterReq.numIpRanges; i++) {
      SIpV4Range *range = &(alterReq.pIpRanges[i]);
      if (!isRangeInIpWhiteList(pUser->pIpWhiteList, range)) {
        // already exist, just ignore;
        memcpy(&pNew->pIpRange[idx], range, sizeof(SIpV4Range));
        idx++;
        continue;
      } else {
        exist = true;
      }
    }
    if (exist) {
      taosMemoryFree(pNew);
      terrno = TSDB_CODE_MND_USER_HOST_EXIST;
      code = terrno;
      goto _OVER;
    }
    pNew->num = idx;
    newUser.pIpWhiteList = pNew;
    newUser.ipWhiteListVer = pUser->ipWhiteListVer + 1;

    if (pNew->num > MND_MAX_USE_HOST) {
      terrno = TSDB_CODE_MND_TOO_MANY_USER_HOST;
      code = terrno;
      goto _OVER;
    }
  }
  if (alterReq.alterType == TSDB_ALTER_USER_DROP_WHITE_LIST) {
    taosMemoryFreeClear(newUser.pIpWhiteList);

    int32_t       num = pUser->pIpWhiteList->num;
    SIpWhiteList *pNew = taosMemoryCalloc(1, sizeof(SIpWhiteList) + sizeof(SIpV4Range) * num);
    bool          noexist = true;
    bool          localHost = false;

    if (pUser->pIpWhiteList->num > 0) {
      int idx = 0;
      for (int i = 0; i < pUser->pIpWhiteList->num; i++) {
        SIpV4Range *oldRange = &pUser->pIpWhiteList->pIpRange[i];
        bool        found = false;
        for (int j = 0; j < alterReq.numIpRanges; j++) {
          SIpV4Range *range = &alterReq.pIpRanges[j];
          if (isDefaultRange(range)) {
            localHost = true;
            break;
          }
          if (isIpRangeEqual(oldRange, range)) {
            found = true;
            break;
          }
        }
        if (localHost) break;

        if (found == false) {
          memcpy(&pNew->pIpRange[idx], oldRange, sizeof(SIpV4Range));
          idx++;
        } else {
          noexist = false;
        }
      }
      pNew->num = idx;
      newUser.pIpWhiteList = pNew;
      newUser.ipWhiteListVer = pUser->ipWhiteListVer + 1;

    } else {
      pNew->num = 0;
      newUser.pIpWhiteList = pNew;
      newUser.ipWhiteListVer = pUser->ipWhiteListVer + 1;
    }

    if (localHost) {
      terrno = TSDB_CODE_MND_USER_LOCAL_HOST_NOT_DROP;
      code = terrno;
      goto _OVER;
    }
    if (noexist) {
      terrno = TSDB_CODE_MND_USER_HOST_NOT_EXIST;
      code = terrno;
      goto _OVER;
    }
  }

  code = mndAlterUser(pMnode, pUser, &newUser, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if(alterReq.alterType == TSDB_ALTER_USER_PASSWD){
    char detail[1000] = {0};
    sprintf(detail, "alterType:%s, enable:%d, superUser:%d, sysInfo:%d, tabName:%s, password:xxx",
            mndUserAuditTypeStr(alterReq.alterType), alterReq.enable, alterReq.superUser, alterReq.sysInfo,
            alterReq.tabName);
    auditRecord(pReq, pMnode->clusterId, "alterUser", "", alterReq.user, detail, strlen(detail));
  }
  else if(alterReq.alterType == TSDB_ALTER_USER_SUPERUSER ||
          alterReq.alterType == TSDB_ALTER_USER_ENABLE ||
          alterReq.alterType == TSDB_ALTER_USER_SYSINFO){
    auditRecord(pReq, pMnode->clusterId, "alterUser", "", alterReq.user, alterReq.sql, alterReq.sqlLen);
  }
  else if(ALTER_USER_ADD_READ_DB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName)||
          ALTER_USER_ADD_WRITE_DB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName)||
          ALTER_USER_ADD_ALL_DB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName)||
          ALTER_USER_ADD_READ_TB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName)||
          ALTER_USER_ADD_WRITE_TB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName)||
          ALTER_USER_ADD_ALL_TB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName)){
    if (strcmp(alterReq.objname, "1.*") != 0){
      SName name = {0};
      tNameFromString(&name, alterReq.objname, T_NAME_ACCT | T_NAME_DB);
      auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", name.dbname, alterReq.user,
                  alterReq.sql, alterReq.sqlLen);
    }else{
      auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", "", alterReq.user,
                  alterReq.sql, alterReq.sqlLen);
    }
  }
  else if(ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(alterReq.alterType, alterReq.privileges)){
    auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", alterReq.objname, alterReq.user,
                    alterReq.sql, alterReq.sqlLen);
  }
  else if(ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(alterReq.alterType, alterReq.privileges)){
    auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", alterReq.objname, alterReq.user,
                alterReq.sql, alterReq.sqlLen);
  }
  else{
    if (strcmp(alterReq.objname, "1.*") != 0){
      SName name = {0};
      tNameFromString(&name, alterReq.objname, T_NAME_ACCT | T_NAME_DB);
      auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", name.dbname, alterReq.user,
                  alterReq.sql, alterReq.sqlLen);
    }else{
      auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", "", alterReq.user,
                  alterReq.sql, alterReq.sqlLen);
    }
  }

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to alter since %s", alterReq.user, terrstr());
  }

  tFreeSAlterUserReq(&alterReq);
  mndReleaseUser(pMnode, pOperUser);
  mndReleaseUser(pMnode, pUser);
  mndUserFreeObj(&newUser);

  return code;
}

static int32_t mndDropUser(SMnode *pMnode, SRpcMsg *pReq, SUserObj *pUser) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "drop-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to drop since %s", pUser->user, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to drop user:%s", pTrans->id, pUser->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pUser);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  ipWhiteMgtRemove(pUser->user);

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessDropUserReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = -1;
  SUserObj    *pUser = NULL;
  SDropUserReq dropReq = {0};

  if (tDeserializeSDropUserReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("user:%s, start to drop", dropReq.user);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_USER) != 0) {
    goto _OVER;
  }

  if (dropReq.user[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_USER_FORMAT;
    goto _OVER;
  }

  pUser = mndAcquireUser(pMnode, dropReq.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    goto _OVER;
  }

  code = mndDropUser(pMnode, pReq, pUser);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  auditRecord(pReq, pMnode->clusterId, "dropUser", "", dropReq.user, dropReq.sql, dropReq.sqlLen);

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to drop since %s", dropReq.user, terrstr());
  }

  mndReleaseUser(pMnode, pUser);
  tFreeSDropUserReq(&dropReq);
  return code;
}

static int32_t mndProcessGetUserAuthReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  int32_t         code = -1;
  SUserObj       *pUser = NULL;
  SGetUserAuthReq authReq = {0};
  SGetUserAuthRsp authRsp = {0};

  if (tDeserializeSGetUserAuthReq(pReq->pCont, pReq->contLen, &authReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mTrace("user:%s, start to get auth", authReq.user);

  pUser = mndAcquireUser(pMnode, authReq.user);
  if (pUser == NULL) {
    terrno = TSDB_CODE_MND_USER_NOT_EXIST;
    goto _OVER;
  }

  code = mndSetUserAuthRsp(pMnode, pUser, &authRsp);
  if (code) {
    goto _OVER;
  }

  int32_t contLen = tSerializeSGetUserAuthRsp(NULL, 0, &authRsp);
  void   *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  tSerializeSGetUserAuthRsp(pRsp, contLen, &authRsp);

  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;
  code = 0;

_OVER:

  mndReleaseUser(pMnode, pUser);
  tFreeSGetUserAuthRsp(&authRsp);

  return code;
}

static int32_t mndRetrieveUsers(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SUserObj *pUser = NULL;
  int32_t   cols = 0;
  char     *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
    colDataSetVal(pColInfo, numOfRows, (const char *)name, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pUser->superUser, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pUser->enable, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pUser->sysInfo, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pUser->createdTime, false);

    cols++;

    char   *buf = NULL;
    int32_t tlen = convertIpWhiteListToStr(pUser->pIpWhiteList, &buf);
    // int32_t tlen = mndFetchIpWhiteList(pUser->pIpWhiteList, &buf);
    if (tlen != 0) {
      char *varstr = taosMemoryCalloc(1, VARSTR_HEADER_SIZE + tlen);
      varDataSetLen(varstr, tlen);
      memcpy(varDataVal(varstr), buf, tlen);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      colDataSetVal(pColInfo, numOfRows, (const char *)varstr, false);

      taosMemoryFree(varstr);
      taosMemoryFree(buf);
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      colDataSetVal(pColInfo, numOfRows, (const char *)NULL, true);
    }

    numOfRows++;
    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextUser(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static void mndLoopHash(SHashObj *hash, char *priType, SSDataBlock *pBlock, int32_t *numOfRows, char *user,
                        SShowObj *pShow) {
  char   *value = taosHashIterate(hash, NULL);
  int32_t cols = 0;

  while (value != NULL) {
    cols = 0;
    char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(userName, user, pShow->pMeta->pSchemas[cols].bytes);
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)userName, false);

    char privilege[20] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(privilege, priType, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)privilege, false);

    size_t keyLen = 0;
    void  *key = taosHashGetKey(value, &keyLen);

    char dbName[TSDB_DB_NAME_LEN] = {0};
    mndExtractShortDbNameFromStbFullName(key, dbName);
    char dbNameContent[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(dbNameContent, dbName, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)dbNameContent, false);

    char tableName[TSDB_TABLE_NAME_LEN] = {0};
    mndExtractTbNameFromStbFullName(key, tableName, TSDB_TABLE_NAME_LEN);
    char tableNameContent[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(tableNameContent, tableName, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, *numOfRows, (const char *)tableNameContent, false);

    if (strcmp("t", value) != 0 && strcmp("v", value) != 0) {
      SNode  *pAst = NULL;
      int32_t sqlLen = 0;
      size_t  bufSz = strlen(value) + 1;
      char   *sql = taosMemoryMalloc(bufSz + 1);
      char   *obj = taosMemoryMalloc(TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);

      if (sql != NULL && obj != NULL && nodesStringToNode(value, &pAst) == 0) {
        nodesNodeToSQL(pAst, sql, bufSz, &sqlLen);
        nodesDestroyNode(pAst);
      } else {
        sqlLen = 5;
        sprintf(sql, "error");
      }

      STR_WITH_MAXSIZE_TO_VARSTR(obj, sql, pShow->pMeta->pSchemas[cols].bytes);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, *numOfRows, (const char *)obj, false);
      taosMemoryFree(obj);
      taosMemoryFree(sql);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, *numOfRows, (const char *)notes, false);
    } else {
      char *condition = taosMemoryMalloc(TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, *numOfRows, (const char *)condition, false);
      taosMemoryFree(condition);

      char notes[64 + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, value[0] == 'v' ? "view" : "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, *numOfRows, (const char *)notes, false);
    }

    (*numOfRows)++;
    value = taosHashIterate(hash, value);
  }
}

static int32_t mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SUserObj *pUser = NULL;
  int32_t   cols = 0;
  char     *pWrite;

  bool fetchNextUser = pShow->restore ? false : true;
  pShow->restore = false;

  while (numOfRows < rows) {
    if (fetchNextUser) {
      pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
      if (pShow->pIter == NULL) break;
    } else {
      fetchNextUser = true;
      void *pKey = taosHashGetKey(pShow->pIter, NULL);
      pUser = sdbAcquire(pSdb, SDB_USER, pKey);
      if (!pUser) {
        continue;
      }
    }

    int32_t numOfReadDbs = taosHashGetSize(pUser->readDbs);
    int32_t numOfWriteDbs = taosHashGetSize(pUser->writeDbs);
    int32_t numOfTopics = taosHashGetSize(pUser->topics);
    int32_t numOfReadTbs = taosHashGetSize(pUser->readTbs);
    int32_t numOfWriteTbs = taosHashGetSize(pUser->writeTbs);
    int32_t numOfAlterTbs = taosHashGetSize(pUser->alterTbs);
    int32_t numOfReadViews = taosHashGetSize(pUser->readViews);
    int32_t numOfWriteViews = taosHashGetSize(pUser->writeViews);
    int32_t numOfAlterViews = taosHashGetSize(pUser->alterViews);
    if (numOfRows + numOfReadDbs + numOfWriteDbs + numOfTopics + numOfReadTbs + numOfWriteTbs + numOfAlterTbs + numOfReadViews + numOfWriteViews + numOfAlterViews >= rows) {
      mInfo(
          "will restore. current num of rows: %d, read dbs %d, write dbs %d, topics %d, read tables %d, write tables "
          "%d, alter tables %d, read views %d, write views %d, alter views %d",
          numOfRows, numOfReadDbs, numOfWriteDbs, numOfTopics, numOfReadTbs, numOfWriteTbs, numOfAlterTbs, numOfReadViews, numOfWriteViews, numOfAlterViews);
      pShow->restore = true;
      sdbRelease(pSdb, pUser);
      break;
    }

    if (pUser->superUser) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)userName, false);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "all", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)privilege, false);

      char objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(objName, "all", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)objName, false);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)tableName, false);

      char *condition = taosMemoryMalloc(TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)condition, false);
      taosMemoryFree(condition);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)notes, false);

      numOfRows++;
    }

    char *db = taosHashIterate(pUser->readDbs, NULL);
    while (db != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)userName, false);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "read", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)privilege, false);

      SName name = {0};
      char  objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      tNameFromString(&name, db, T_NAME_ACCT | T_NAME_DB);
      tNameGetDbName(&name, varDataVal(objName));
      varDataSetLen(objName, strlen(varDataVal(objName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)objName, false);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)tableName, false);

      char *condition = taosMemoryMalloc(TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)condition, false);
      taosMemoryFree(condition);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)notes, false);

      numOfRows++;
      db = taosHashIterate(pUser->readDbs, db);
    }

    db = taosHashIterate(pUser->writeDbs, NULL);
    while (db != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)userName, false);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "write", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)privilege, false);

      SName name = {0};
      char  objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      tNameFromString(&name, db, T_NAME_ACCT | T_NAME_DB);
      tNameGetDbName(&name, varDataVal(objName));
      varDataSetLen(objName, strlen(varDataVal(objName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)objName, false);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)tableName, false);

      char *condition = taosMemoryMalloc(TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)condition, false);
      taosMemoryFree(condition);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)notes, false);

      numOfRows++;
      db = taosHashIterate(pUser->writeDbs, db);
    }

    mndLoopHash(pUser->readTbs, "read", pBlock, &numOfRows, pUser->user, pShow);

    mndLoopHash(pUser->writeTbs, "write", pBlock, &numOfRows, pUser->user, pShow);

    mndLoopHash(pUser->alterTbs, "alter", pBlock, &numOfRows, pUser->user, pShow);

    mndLoopHash(pUser->readViews, "read", pBlock, &numOfRows, pUser->user, pShow);

    mndLoopHash(pUser->writeViews, "write", pBlock, &numOfRows, pUser->user, pShow);

    mndLoopHash(pUser->alterViews, "alter", pBlock, &numOfRows, pUser->user, pShow);

    char *topic = taosHashIterate(pUser->topics, NULL);
    while (topic != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)userName, false);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "subscribe", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)privilege, false);

      char topicName[TSDB_TOPIC_NAME_LEN + VARSTR_HEADER_SIZE + 5] = {0};
      tstrncpy(varDataVal(topicName), mndGetDbStr(topic), TSDB_TOPIC_NAME_LEN - 2);
      varDataSetLen(topicName, strlen(varDataVal(topicName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)topicName, false);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)tableName, false);

      char *condition = taosMemoryMalloc(TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)condition, false);
      taosMemoryFree(condition);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      colDataSetVal(pColInfo, numOfRows, (const char *)notes, false);

      numOfRows++;
      topic = taosHashIterate(pUser->topics, topic);
    }

    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

int32_t mndValidateUserAuthInfo(SMnode *pMnode, SUserAuthVersion *pUsers, int32_t numOfUses, void **ppRsp,
                                int32_t *pRspLen) {
  SUserAuthBatchRsp batchRsp = {0};
  batchRsp.pArray = taosArrayInit(numOfUses, sizeof(SGetUserAuthRsp));
  if (batchRsp.pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int32_t code = 0;
  for (int32_t i = 0; i < numOfUses; ++i) {
    SUserObj *pUser = mndAcquireUser(pMnode, pUsers[i].user);
    if (pUser == NULL) {
      if (TSDB_CODE_MND_USER_NOT_EXIST == terrno) {
        SGetUserAuthRsp rsp = {.dropped = 1};
        memcpy(rsp.user, pUsers[i].user, TSDB_USER_LEN);
        taosArrayPush(batchRsp.pArray, &rsp);
      }
      mError("user:%s, failed to auth user since %s", pUsers[i].user, terrstr());
      continue;
    }

    pUsers[i].version = ntohl(pUsers[i].version);
    if (pUser->authVersion <= pUsers[i].version) {
      mndReleaseUser(pMnode, pUser);
      continue;
    }

    SGetUserAuthRsp rsp = {0};
    code = mndSetUserAuthRsp(pMnode, pUser, &rsp);
    if (code) {
      mndReleaseUser(pMnode, pUser);
      tFreeSGetUserAuthRsp(&rsp);
      goto _OVER;
    }

    taosArrayPush(batchRsp.pArray, &rsp);
    mndReleaseUser(pMnode, pUser);
  }

  if (taosArrayGetSize(batchRsp.pArray) <= 0) {
    *ppRsp = NULL;
    *pRspLen = 0;

    tFreeSUserAuthBatchRsp(&batchRsp);
    return 0;
  }

  int32_t rspLen = tSerializeSUserAuthBatchRsp(NULL, 0, &batchRsp);
  void   *pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tFreeSUserAuthBatchRsp(&batchRsp);
    return -1;
  }
  tSerializeSUserAuthBatchRsp(pRsp, rspLen, &batchRsp);

  *ppRsp = pRsp;
  *pRspLen = rspLen;

  tFreeSUserAuthBatchRsp(&batchRsp);
  return 0;

_OVER:

  *ppRsp = NULL;
  *pRspLen = 0;

  tFreeSUserAuthBatchRsp(&batchRsp);
  return code;
}

int32_t mndUserRemoveDb(SMnode *pMnode, STrans *pTrans, char *db) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(db) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};

  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    code = -1;
    if (mndUserDupObj(pUser, &newUser) != 0) {
      break;
    }

    bool inRead = (taosHashGet(newUser.readDbs, db, len) != NULL);
    bool inWrite = (taosHashGet(newUser.writeDbs, db, len) != NULL);
    if (inRead || inWrite) {
      (void)taosHashRemove(newUser.readDbs, db, len);
      (void)taosHashRemove(newUser.writeDbs, db, len);

      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
        break;
      }
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
    code = 0;
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  return code;
}

int32_t mndUserRemoveStb(SMnode *pMnode, STrans *pTrans, char *stb) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(stb) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};

  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    code = -1;
    if (mndUserDupObj(pUser, &newUser) != 0) {
      break;
    }

    bool inRead = (taosHashGet(newUser.readTbs, stb, len) != NULL);
    bool inWrite = (taosHashGet(newUser.writeTbs, stb, len) != NULL);
    bool inAlter = (taosHashGet(newUser.alterTbs, stb, len) != NULL);
    if (inRead || inWrite || inAlter) {
      (void)taosHashRemove(newUser.readTbs, stb, len);
      (void)taosHashRemove(newUser.writeTbs, stb, len);
      (void)taosHashRemove(newUser.alterTbs, stb, len);

      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
        break;
      }
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
    code = 0;
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  return code;
}

int32_t mndUserRemoveView(SMnode *pMnode, STrans *pTrans, char *view) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(view) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};

  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    code = -1;
    if (mndUserDupObj(pUser, &newUser) != 0) {
      break;
    }

    bool inRead = (taosHashGet(newUser.readViews, view, len) != NULL);
    bool inWrite = (taosHashGet(newUser.writeViews, view, len) != NULL);
    bool inAlter = (taosHashGet(newUser.alterViews, view, len) != NULL);
    if (inRead || inWrite || inAlter) {
      (void)taosHashRemove(newUser.readViews, view, len);
      (void)taosHashRemove(newUser.writeViews, view, len);
      (void)taosHashRemove(newUser.alterViews, view, len);

      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
        break;
      }
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
    code = 0;
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  return code;
}


int32_t mndUserRemoveTopic(SMnode *pMnode, STrans *pTrans, char *topic) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   len = strlen(topic) + 1;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  SUserObj  newUser = {0};

  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) {
      break;
    }

    code = -1;
    if (mndUserDupObj(pUser, &newUser) != 0) {
      break;
    }

    bool inTopic = (taosHashGet(newUser.topics, topic, len) != NULL);
    if (inTopic) {
      (void)taosHashRemove(newUser.topics, topic, len);
      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
        break;
      }
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
    code = 0;
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  return code;
}
