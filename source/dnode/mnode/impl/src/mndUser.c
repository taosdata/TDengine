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
#define ALTER_USER_READ_PRIV(_priv) \
  (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_READ) || BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALL))
#define ALTER_USER_WRITE_PRIV(_priv) \
  (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_WRITE) || BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALL))
#define ALTER_USER_ALTER_PRIV(_priv) \
  (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALTER) || BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_ALL))
#define ALTER_USER_SUBSCRIBE_PRIV(_priv) (BIT_FLAG_TEST_MASK((_priv), PRIVILEGE_TYPE_SUBSCRIBE))

#define ALTER_USER_TARGET_DB(_tbname) (0 == (_tbname)[0])
#define ALTER_USER_TARGET_TB(_tbname) (0 != (_tbname)[0])

#define ALTER_USER_ADD_READ_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_READ_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_WRITE_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_WRITE_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_ALTER_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_ALTER_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_ADD_ALL_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))
#define ALTER_USER_DEL_ALL_DB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_DB(_tbname))

#define ALTER_USER_ADD_READ_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_READ_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_READ_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_WRITE_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_WRITE_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_WRITE_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_ALTER_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_ALTER_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALTER_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_ADD_ALL_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))
#define ALTER_USER_DEL_ALL_TB_PRIV(_type, _priv, _tbname) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_ALL_PRIV(_priv) && ALTER_USER_TARGET_TB(_tbname))

#define ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(_type, _priv) \
  (ALTER_USER_ADD_PRIVS(_type) && ALTER_USER_SUBSCRIBE_PRIV(_priv))
#define ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(_type, _priv) \
  (ALTER_USER_DEL_PRIVS(_type) && ALTER_USER_SUBSCRIBE_PRIV(_priv))

static int32_t createDefaultIpWhiteList(SIpWhiteList **ppWhiteList);
static int32_t createIpWhiteList(void *buf, int32_t len, SIpWhiteList **ppWhiteList);
static bool    updateIpWhiteList(SIpWhiteList *pOld, SIpWhiteList *pNew);
static bool    isIpWhiteListEqual(SIpWhiteList *a, SIpWhiteList *b);
static bool    isIpRangeEqual(SIpV4Range *a, SIpV4Range *b);

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
static int32_t  mndRetrieveUsersFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextUser(SMnode *pMnode, void *pIter);
static int32_t  mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter);
static int32_t  mndFetchAllIpWhite(SMnode *pMnode, SHashObj **ppIpWhiteTab);
static int32_t  mndProcesSRetrieveIpWhiteReq(SRpcMsg *pReq);
static int32_t  mndUpdateIpWhiteImpl(SHashObj *pIpWhiteTab, char *user, char *fqdn, int8_t type, bool *pUpdate);

static int32_t ipWhiteMgtUpdateAll(SMnode *pMnode);
typedef struct {
  SHashObj      *pIpWhiteTab;
  int64_t        ver;
  TdThreadRwlock rw;
} SIpWhiteMgt;

static SIpWhiteMgt ipWhiteMgt;

const static SIpV4Range defaultIpRange = {.ip = 16777343, .mask = 32};

static int32_t ipWhiteMgtInit() {
  ipWhiteMgt.pIpWhiteTab = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), 1, HASH_ENTRY_LOCK);
  if (ipWhiteMgt.pIpWhiteTab == NULL) {
    TAOS_RETURN(terrno);
  }
  ipWhiteMgt.ver = 0;
  (void)taosThreadRwlockInit(&ipWhiteMgt.rw, NULL);
  TAOS_RETURN(0);
}
void ipWhiteMgtCleanup() {
  destroyIpWhiteTab(ipWhiteMgt.pIpWhiteTab);
  (void)taosThreadRwlockDestroy(&ipWhiteMgt.rw);
}

int32_t ipWhiteMgtUpdate(SMnode *pMnode, char *user, SIpWhiteList *pNew) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    update = true;
  SArray *fqdns = NULL;
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  SIpWhiteList **ppList = taosHashGet(ipWhiteMgt.pIpWhiteTab, user, strlen(user));

  if (ppList == NULL || *ppList == NULL) {
    SIpWhiteList *p = cloneIpWhiteList(pNew);
    if (p == NULL) {
      update = false;
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
    if ((code = taosHashPut(ipWhiteMgt.pIpWhiteTab, user, strlen(user), &p, sizeof(void *))) != 0) {
      update = false;
      taosMemoryFree(p);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
  } else {
    SIpWhiteList *pOld = *ppList;
    if (isIpWhiteListEqual(pOld, pNew)) {
      update = false;
    } else {
      taosMemoryFree(pOld);
      SIpWhiteList *p = cloneIpWhiteList(pNew);
      if (p == NULL) {
        update = false;
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
      }
      if ((code = taosHashPut(ipWhiteMgt.pIpWhiteTab, user, strlen(user), &p, sizeof(void *))) != 0) {
        update = false;
        taosMemoryFree(p);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
    }
  }

  fqdns = mndGetAllDnodeFqdns(pMnode);  // TODO: update this line after refactor api
  if (fqdns == NULL) {
    update = false;
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }

  for (int i = 0; i < taosArrayGetSize(fqdns); i++) {
    char *fqdn = taosArrayGetP(fqdns, i);
    bool  upd = false;
    TAOS_CHECK_GOTO(mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, TSDB_DEFAULT_USER, fqdn, IP_WHITE_ADD, &upd), &lino,
                    _OVER);
    update |= upd;
    TAOS_CHECK_GOTO(mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, user, fqdn, IP_WHITE_ADD, &upd), &lino, _OVER);
    update |= upd;
  }

  // for (int i = 0; i < taosArrayGetSize(pUserNames); i++) {
  //   taosMemoryFree(taosArrayGetP(pUserNames, i));
  // }
  // taosArrayDestroy(pUserNames);

  if (update) ipWhiteMgt.ver++;

_OVER:
  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  taosArrayDestroyP(fqdns, (FDelete)taosMemoryFree);
  if (code < 0) {
    mError("failed to update ip white list for user: %s at line %d since %s", user, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}
int32_t ipWhiteMgtRemove(char *user) {
  bool update = true;
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  SIpWhiteList **ppList = taosHashGet(ipWhiteMgt.pIpWhiteTab, user, strlen(user));
  if (ppList == NULL || *ppList == NULL) {
    update = false;
  } else {
    taosMemoryFree(*ppList);
    (void)taosHashRemove(ipWhiteMgt.pIpWhiteTab, user, strlen(user));
  }

  if (update) ipWhiteMgt.ver++;
  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
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
#if 0
int32_t ipWhiteUpdateForAllUser(SIpWhiteList *pList) {
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);

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
      (void)memcpy(val->pIpRange, p->pIpRange, sizeof(SIpV4Range) * p->num);
      (void)memcpy(((char *)val->pIpRange) + sizeof(SIpV4Range) * p->num, (char *)clone->pIpRange,
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
  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  return 0;
}
#endif

static int32_t ipWhiteMgtUpdateAll(SMnode *pMnode) {
  SHashObj *pNew = NULL;
  TAOS_CHECK_RETURN(mndFetchAllIpWhite(pMnode, &pNew));

  SHashObj *pOld = ipWhiteMgt.pIpWhiteTab;

  ipWhiteMgt.pIpWhiteTab = pNew;
  ipWhiteMgt.ver++;

  destroyIpWhiteTab(pOld);
  TAOS_RETURN(0);
}

#if 0
void ipWhiteMgtUpdate2(SMnode *pMnode) {
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);

  ipWhiteMgtUpdateAll(pMnode);

  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
}
#endif

int64_t mndGetIpWhiteVer(SMnode *pMnode) {
  int64_t ver = 0;
  int32_t code = 0;
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  if (ipWhiteMgt.ver == 0) {
    // get user and dnode ip white list
    if ((code = ipWhiteMgtUpdateAll(pMnode)) != 0) {
      (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
      mError("%s failed to update ip white list since %s", __func__, tstrerror(code));
      return ver;
    }
    ipWhiteMgt.ver = taosGetTimestampMs();
  }
  ver = ipWhiteMgt.ver;
  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);

  if (mndEnableIpWhiteList(pMnode) == 0 || tsEnableWhiteList == false) {
    ver = 0;
  }
  mDebug("ip-white-list on mnode ver: %" PRId64 "", ver);
  return ver;
}

int32_t mndUpdateIpWhiteImpl(SHashObj *pIpWhiteTab, char *user, char *fqdn, int8_t type, bool *pUpdate) {
  int32_t    lino = 0;
  bool       update = false;
  SIpV4Range range = {.ip = 0, .mask = 32};
  int32_t    code = taosGetIpv4FromFqdn(fqdn, &range.ip);
  if (code) {
    //TODO
  }
  mDebug("ip-white-list may update for user: %s, fqdn: %s", user, fqdn);
  SIpWhiteList **ppList = taosHashGet(pIpWhiteTab, user, strlen(user));
  SIpWhiteList  *pList = NULL;
  if (ppList != NULL && *ppList != NULL) {
    pList = *ppList;
  }

  if (type == IP_WHITE_ADD) {
    if (pList == NULL) {
      SIpWhiteList *pNewList = taosMemoryCalloc(1, sizeof(SIpWhiteList) + sizeof(SIpV4Range));
      if (pNewList == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memcpy(pNewList->pIpRange, &range, sizeof(SIpV4Range));
      pNewList->num = 1;

      if ((code = taosHashPut(pIpWhiteTab, user, strlen(user), &pNewList, sizeof(void *))) != 0) {
        taosMemoryFree(pNewList);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
      update = true;
    } else {
      if (!isRangeInWhiteList(pList, &range)) {
        int32_t       sz = sizeof(SIpWhiteList) + sizeof(SIpV4Range) * (pList->num + 1);
        SIpWhiteList *pNewList = taosMemoryCalloc(1, sz);
        if (pNewList == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memcpy(pNewList->pIpRange, pList->pIpRange, sizeof(SIpV4Range) * (pList->num));
        pNewList->pIpRange[pList->num].ip = range.ip;
        pNewList->pIpRange[pList->num].mask = range.mask;

        pNewList->num = pList->num + 1;

        if ((code = taosHashPut(pIpWhiteTab, user, strlen(user), &pNewList, sizeof(void *))) != 0) {
          taosMemoryFree(pNewList);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
        taosMemoryFree(pList);
        update = true;
      }
    }
  } else if (type == IP_WHITE_DROP) {
    if (pList != NULL) {
      if (isRangeInWhiteList(pList, &range)) {
        if (pList->num == 1) {
          (void)taosHashRemove(pIpWhiteTab, user, strlen(user));
          taosMemoryFree(pList);
        } else {
          int32_t       idx = 0;
          int32_t       sz = sizeof(SIpWhiteList) + sizeof(SIpV4Range) * (pList->num - 1);
          SIpWhiteList *pNewList = taosMemoryCalloc(1, sz);
          if (pNewList == NULL) {
            TAOS_CHECK_GOTO(terrno, &lino, _OVER);
          }
          for (int i = 0; i < pList->num; i++) {
            SIpV4Range *e = &pList->pIpRange[i];
            if (!isIpRangeEqual(e, &range)) {
              pNewList->pIpRange[idx].ip = e->ip;
              pNewList->pIpRange[idx].mask = e->mask;
              idx++;
            }
          }
          pNewList->num = idx;
          if ((code = taosHashPut(pIpWhiteTab, user, strlen(user), &pNewList, sizeof(void *)) != 0)) {
            taosMemoryFree(pNewList);
            TAOS_CHECK_GOTO(code, &lino, _OVER);
          }
          taosMemoryFree(pList);
        }
        update = true;
      }
    }
  }
  if (update) {
    mDebug("ip-white-list update for user: %s, fqdn: %s", user, fqdn);
  }

_OVER:
  if (pUpdate) *pUpdate = update;
  if (code < 0) {
    mError("failed to update ip-white-list for user: %s, fqdn: %s at line %d since %s", user, fqdn, lino,
           tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t mndRefreshUserIpWhiteList(SMnode *pMnode) {
  int32_t code = 0;
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);

  if ((code = ipWhiteMgtUpdateAll(pMnode)) != 0) {
    (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
    TAOS_RETURN(code);
  }
  ipWhiteMgt.ver = taosGetTimestampMs();
  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);

  TAOS_RETURN(code);
}

int32_t mndUpdateIpWhiteForAllUser(SMnode *pMnode, char *user, char *fqdn, int8_t type, int8_t lock) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    update = false;

  if (lock) {
    (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);
    if (ipWhiteMgt.ver == 0) {
      TAOS_CHECK_GOTO(ipWhiteMgtUpdateAll(pMnode), &lino, _OVER);
      ipWhiteMgt.ver = taosGetTimestampMs();
      mInfo("update ip-white-list, user: %s, ver: %" PRId64, user, ipWhiteMgt.ver);
    }
  }

  TAOS_CHECK_GOTO(mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, user, fqdn, type, &update), &lino, _OVER);

  void *pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, NULL);
  while (pIter) {
    size_t klen = 0;
    char  *key = taosHashGetKey(pIter, &klen);

    char *keyDup = taosMemoryCalloc(1, klen + 1);
    if (keyDup == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
    (void)memcpy(keyDup, key, klen);
    bool upd = false;
    code = mndUpdateIpWhiteImpl(ipWhiteMgt.pIpWhiteTab, keyDup, fqdn, type, &upd);
    update |= upd;
    if (code < 0) {
      taosMemoryFree(keyDup);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    taosMemoryFree(keyDup);

    pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, pIter);
  }

_OVER:
  if (update) ipWhiteMgt.ver++;
  if (lock) (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  if (code < 0) {
    mError("failed to update ip-white-list for user: %s, fqdn: %s at line %d since %s", user, fqdn, lino,
           tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int64_t ipWhiteMgtFillMsg(SUpdateIpWhite *pUpdate) {
  int64_t ver = 0;
  (void)taosThreadRwlockWrlock(&ipWhiteMgt.rw);
  ver = ipWhiteMgt.ver;
  int32_t num = taosHashGetSize(ipWhiteMgt.pIpWhiteTab);

  pUpdate->pUserIpWhite = taosMemoryCalloc(1, num * sizeof(SUpdateUserIpWhite));
  if (pUpdate->pUserIpWhite == NULL) {
    (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
    TAOS_RETURN(terrno);
  }

  void   *pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, NULL);
  int32_t i = 0;
  while (pIter) {
    SUpdateUserIpWhite *pUser = &pUpdate->pUserIpWhite[i];
    SIpWhiteList       *list = *(SIpWhiteList **)pIter;

    size_t klen;
    char  *key = taosHashGetKey(pIter, &klen);
    if (list->num != 0) {
      pUser->ver = ver;
      (void)memcpy(pUser->user, key, klen);
      pUser->numOfRange = list->num;
      pUser->pIpRanges = taosMemoryCalloc(1, list->num * sizeof(SIpV4Range));
      if (pUser->pIpRanges == NULL) {
        (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
        TAOS_RETURN(terrno);
      }
      (void)memcpy(pUser->pIpRanges, list->pIpRange, list->num * sizeof(SIpV4Range));
      i++;
    }
    pIter = taosHashIterate(ipWhiteMgt.pIpWhiteTab, pIter);
  }
  pUpdate->numOfUser = i;
  pUpdate->ver = ver;

  (void)taosThreadRwlockUnlock(&ipWhiteMgt.rw);
  TAOS_RETURN(0);
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
int32_t mndFetchAllIpWhite(SMnode *pMnode, SHashObj **ppIpWhiteTab) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  SHashObj *pIpWhiteTab = NULL;
  SArray   *pUserNames = NULL;
  SArray   *fqdns = NULL;

  pIpWhiteTab = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), 1, HASH_ENTRY_LOCK);
  if (pIpWhiteTab == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  pUserNames = taosArrayInit(8, sizeof(void *));
  if (pUserNames == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  while (1) {
    SUserObj *pUser = NULL;
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pIter == NULL) break;

    SIpWhiteList *pWhiteList = cloneIpWhiteList(pUser->pIpWhiteList);
    if (pWhiteList == NULL) {
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
    if ((code = taosHashPut(pIpWhiteTab, pUser->user, strlen(pUser->user), &pWhiteList, sizeof(void *))) != 0) {
      taosMemoryFree(pWhiteList);
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }

    char *name = taosStrdup(pUser->user);
    if (name == NULL) {
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
    if (taosArrayPush(pUserNames, &name) == NULL) {
      taosMemoryFree(name);
      sdbRelease(pSdb, pUser);
      sdbCancelFetch(pSdb, pIter);
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

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
    if (name == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
    }
    if (taosArrayPush(pUserNames, &name) == NULL) {
      taosMemoryFree(name);
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
  }

  fqdns = mndGetAllDnodeFqdns(pMnode);  // TODO: refactor this line after refactor api
  if (fqdns == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }

  for (int i = 0; i < taosArrayGetSize(fqdns); i++) {
    char *fqdn = taosArrayGetP(fqdns, i);

    for (int j = 0; j < taosArrayGetSize(pUserNames); j++) {
      char *name = taosArrayGetP(pUserNames, j);
      TAOS_CHECK_GOTO(mndUpdateIpWhiteImpl(pIpWhiteTab, name, fqdn, IP_WHITE_ADD, NULL), &lino, _OVER);
    }
  }

_OVER:
  taosArrayDestroyP(fqdns, taosMemoryFree);
  taosArrayDestroyP(pUserNames, taosMemoryFree);

  if (code < 0) {
    mError("failed to fetch all ip white list at line %d since %s", lino, tstrerror(code));
    destroyIpWhiteTab(pIpWhiteTab);
    pIpWhiteTab = NULL;
  }
  *ppIpWhiteTab = pIpWhiteTab;
  TAOS_RETURN(code);
}

int32_t mndInitUser(SMnode *pMnode) {
  TAOS_CHECK_RETURN(ipWhiteMgtInit());

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
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_USER_FULL, mndRetrieveUsersFull);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_USER_FULL, mndCancelGetNextUser);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndRetrievePrivileges);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_PRIVILEGES, mndCancelGetNextPrivileges);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupUser(SMnode *pMnode) { ipWhiteMgtCleanup(); }

static void ipRangeToStr(SIpV4Range *range, char *buf) {
  struct in_addr addr;
  addr.s_addr = range->ip;

  (void)uv_inet_ntop(AF_INET, &addr, buf, 32);
  if (range->mask != 32) {
    (void)sprintf(buf + strlen(buf), "/%d", range->mask);
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
  if (*buf == NULL) {
    return 0;
  }
  int32_t len = ipRangeListToStr(pList->pIpRange, pList->num, *buf);
  if (len == 0) {
    taosMemoryFreeClear(*buf);
    return 0;
  }
  return strlen(*buf);
}
int32_t tSerializeIpWhiteList(void *buf, int32_t len, SIpWhiteList *pList, uint32_t *pLen) {
  int32_t  code = 0;
  int32_t  lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, len);

  TAOS_CHECK_GOTO(tStartEncode(&encoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tEncodeI32(&encoder, pList->num), &lino, _OVER);

  for (int i = 0; i < pList->num; i++) {
    SIpV4Range *pRange = &(pList->pIpRange[i]);
    TAOS_CHECK_GOTO(tEncodeU32(&encoder, pRange->ip), &lino, _OVER);
    TAOS_CHECK_GOTO(tEncodeU32(&encoder, pRange->mask), &lino, _OVER);
  }

  tEndEncode(&encoder);

  tlen = encoder.pos;
_OVER:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("failed to serialize ip white list at line %d since %s", lino, tstrerror(code));
  }
  if (pLen) *pLen = tlen;
  TAOS_RETURN(code);
}

int32_t tDerializeIpWhileList(void *buf, int32_t len, SIpWhiteList *pList) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, len);

  TAOS_CHECK_GOTO(tStartDecode(&decoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tDecodeI32(&decoder, &pList->num), &lino, _OVER);

  for (int i = 0; i < pList->num; i++) {
    SIpV4Range *pRange = &(pList->pIpRange[i]);
    TAOS_CHECK_GOTO(tDecodeU32(&decoder, &pRange->ip), &lino, _OVER);
    TAOS_CHECK_GOTO(tDecodeU32(&decoder, &pRange->mask), &lino, _OVER);
  }

_OVER:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("failed to deserialize ip white list at line %d since %s", lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t createIpWhiteList(void *buf, int32_t len, SIpWhiteList **ppList) {
  int32_t       code = 0;
  int32_t       lino = 0;
  int32_t       num = 0;
  SIpWhiteList *p = NULL;
  SDecoder      decoder = {0};
  tDecoderInit(&decoder, buf, len);

  TAOS_CHECK_GOTO(tStartDecode(&decoder), &lino, _OVER);
  TAOS_CHECK_GOTO(tDecodeI32(&decoder, &num), &lino, _OVER);

  p = taosMemoryCalloc(1, sizeof(SIpWhiteList) + num * sizeof(SIpV4Range));
  if (p == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  TAOS_CHECK_GOTO(tDerializeIpWhileList(buf, len, p), &lino, _OVER);

_OVER:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    taosMemoryFreeClear(p);
    mError("failed to create ip white list at line %d since %s", lino, tstrerror(code));
  }
  *ppList = p;
  TAOS_RETURN(code);
}

static int32_t createDefaultIpWhiteList(SIpWhiteList **ppWhiteList) {
  *ppWhiteList = taosMemoryCalloc(1, sizeof(SIpWhiteList) + sizeof(SIpV4Range) * 1);
  if (*ppWhiteList == NULL) {
    TAOS_RETURN(terrno);
  }
  (*ppWhiteList)->num = 1;
  SIpV4Range *range = &((*ppWhiteList)->pIpRange[0]);

  struct in_addr addr;
  if (uv_inet_pton(AF_INET, "127.0.0.1", &addr) == 0) {
    range->ip = addr.s_addr;
    range->mask = 32;
  }
  return 0;
}

static int32_t mndCreateDefaultUser(SMnode *pMnode, char *acct, char *user, char *pass) {
  int32_t  code = 0;
  SUserObj userObj = {0};
  taosEncryptPass_c((uint8_t *)pass, strlen(pass), userObj.pass);
  tstrncpy(userObj.user, user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.sysInfo = 1;
  userObj.enable = 1;
  userObj.ipWhiteListVer = taosGetTimestampMs();
  TAOS_CHECK_RETURN(createDefaultIpWhiteList(&userObj.pIpWhiteList));
  if (strcmp(user, TSDB_DEFAULT_USER) == 0) {
    userObj.superUser = 1;
    userObj.createdb = 1;
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
  TAOS_RETURN(terrno ? terrno : TSDB_CODE_APP_ERROR);
}

static int32_t mndCreateDefaultUsers(SMnode *pMnode) {
  return mndCreateDefaultUser(pMnode, TSDB_DEFAULT_USER, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS);
}

SSdbRaw *mndUserActionEncode(SUserObj *pUser) {
  int32_t code = 0;
  int32_t lino = 0;
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
  char    *buf = NULL;
  SSdbRaw *pRaw = NULL;

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

  pRaw = sdbAllocRaw(SDB_USER, USER_VER_NUMBER, size);
  if (pRaw == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pUser->pass, TSDB_PASSWORD_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pUser->updateTime, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->superUser, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->sysInfo, _OVER)
  SDB_SET_INT8(pRaw, dataPos, pUser->enable, _OVER)
  SDB_SET_UINT8(pRaw, dataPos, pUser->flag, _OVER)
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
  if ((buf = taosMemoryCalloc(1, tlen)) == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _OVER);
  }
  int32_t len = 0;
  TAOS_CHECK_GOTO(tSerializeIpWhiteList(buf, tlen, pUser->pIpWhiteList, &len), &lino, _OVER);

  SDB_SET_INT32(pRaw, dataPos, len, _OVER);
  SDB_SET_BINARY(pRaw, dataPos, buf, len, _OVER);

  SDB_SET_INT64(pRaw, dataPos, pUser->ipWhiteListVer, _OVER);

  SDB_SET_RESERVE(pRaw, dataPos, USER_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

_OVER:
  taosMemoryFree(buf);
  if (code < 0) {
    mError("user:%s, failed to encode user action to raw:%p at line %d since %s", pUser->user, pRaw, lino,
           tstrerror(code));
    sdbFreeRaw(pRaw);
    pRaw = NULL;
    terrno = code;
  }

  mTrace("user:%s, encode user action to raw:%p, row:%p", pUser->user, pRaw, pUser);
  return pRaw;
}

static SSdbRow *mndUserActionDecode(SSdbRaw *pRaw) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SSdbRow  *pRow = NULL;
  SUserObj *pUser = NULL;
  char     *key = NULL;
  char     *value = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_PTR, &lino, _OVER);
  }

  if (sver < 1 || sver > USER_VER_NUMBER) {
    TAOS_CHECK_GOTO(TSDB_CODE_SDB_INVALID_DATA_VER, &lino, _OVER);
  }

  pRow = sdbAllocRow(sizeof(SUserObj));
  if (pRow == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  pUser = sdbGetRowObj(pRow);
  if (pUser == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pUser->user, TSDB_USER_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pUser->pass, TSDB_PASSWORD_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pUser->acct, TSDB_USER_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pUser->updateTime, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->superUser, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->sysInfo, _OVER)
  SDB_GET_INT8(pRaw, dataPos, &pUser->enable, _OVER)
  SDB_GET_UINT8(pRaw, dataPos, &pUser->flag, _OVER)
  if (pUser->superUser) pUser->createdb = 1;
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
  if (pUser->readDbs == NULL || pUser->writeDbs == NULL || pUser->topics == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    goto _OVER;
  }

  for (int32_t i = 0; i < numOfReadDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER)
    int32_t len = strlen(db) + 1;
    TAOS_CHECK_GOTO(taosHashPut(pUser->readDbs, db, len, db, TSDB_DB_FNAME_LEN), &lino, _OVER);
  }

  for (int32_t i = 0; i < numOfWriteDbs; ++i) {
    char db[TSDB_DB_FNAME_LEN] = {0};
    SDB_GET_BINARY(pRaw, dataPos, db, TSDB_DB_FNAME_LEN, _OVER)
    int32_t len = strlen(db) + 1;
    TAOS_CHECK_GOTO(taosHashPut(pUser->writeDbs, db, len, db, TSDB_DB_FNAME_LEN), &lino, _OVER);
  }

  if (sver >= 2) {
    for (int32_t i = 0; i < numOfTopics; ++i) {
      char topic[TSDB_TOPIC_FNAME_LEN] = {0};
      SDB_GET_BINARY(pRaw, dataPos, topic, TSDB_TOPIC_FNAME_LEN, _OVER)
      int32_t len = strlen(topic) + 1;
      TAOS_CHECK_GOTO(taosHashPut(pUser->topics, topic, len, topic, TSDB_TOPIC_FNAME_LEN), &lino, _OVER);
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

    if (pUser->readTbs == NULL || pUser->writeTbs == NULL || pUser->alterTbs == NULL || pUser->readViews == NULL ||
        pUser->writeViews == NULL || pUser->alterViews == NULL || pUser->useDbs == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      goto _OVER;
    }

    for (int32_t i = 0; i < numOfReadTbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t valuelen = 0;
      SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
      TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
      if (value == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(value, 0, valuelen);
      SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

      TAOS_CHECK_GOTO(taosHashPut(pUser->readTbs, key, keyLen, value, valuelen), &lino, _OVER);
    }

    for (int32_t i = 0; i < numOfWriteTbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t valuelen = 0;
      SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
      TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
      if (value == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(value, 0, valuelen);
      SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

      TAOS_CHECK_GOTO(taosHashPut(pUser->writeTbs, key, keyLen, value, valuelen), &lino, _OVER);
    }

    if (sver >= 6) {
      for (int32_t i = 0; i < numOfAlterTbs; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(pUser->alterTbs, key, keyLen, value, valuelen), &lino, _OVER);
      }

      for (int32_t i = 0; i < numOfReadViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(pUser->readViews, key, keyLen, value, valuelen), &lino, _OVER);
      }

      for (int32_t i = 0; i < numOfWriteViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(pUser->writeViews, key, keyLen, value, valuelen), &lino, _OVER);
      }

      for (int32_t i = 0; i < numOfAlterViews; ++i) {
        int32_t keyLen = 0;
        SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

        TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
        if (key == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(key, 0, keyLen);
        SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

        int32_t valuelen = 0;
        SDB_GET_INT32(pRaw, dataPos, &valuelen, _OVER);
        TAOS_MEMORY_REALLOC(value, valuelen * sizeof(char));
        if (value == NULL) {
          TAOS_CHECK_GOTO(terrno, &lino, _OVER);
        }
        (void)memset(value, 0, valuelen);
        SDB_GET_BINARY(pRaw, dataPos, value, valuelen, _OVER)

        TAOS_CHECK_GOTO(taosHashPut(pUser->alterViews, key, keyLen, value, valuelen), &lino, _OVER);
      }
    }

    for (int32_t i = 0; i < numOfUseDbs; ++i) {
      int32_t keyLen = 0;
      SDB_GET_INT32(pRaw, dataPos, &keyLen, _OVER);

      TAOS_MEMORY_REALLOC(key, keyLen * sizeof(char));
      if (key == NULL) {
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);
      }
      (void)memset(key, 0, keyLen);
      SDB_GET_BINARY(pRaw, dataPos, key, keyLen, _OVER);

      int32_t ref = 0;
      SDB_GET_INT32(pRaw, dataPos, &ref, _OVER);

      TAOS_CHECK_GOTO(taosHashPut(pUser->useDbs, key, keyLen, &ref, sizeof(ref)), &lino, _OVER);
    }
  }
  // decoder white list
  if (sver >= 5) {
    int32_t len = 0;
    SDB_GET_INT32(pRaw, dataPos, &len, _OVER);

    TAOS_MEMORY_REALLOC(key, len);
    if (key == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }
    SDB_GET_BINARY(pRaw, dataPos, key, len, _OVER);

    TAOS_CHECK_GOTO(createIpWhiteList(key, len, &pUser->pIpWhiteList), &lino, _OVER);

    SDB_GET_INT64(pRaw, dataPos, &pUser->ipWhiteListVer, _OVER);
  }

  if (pUser->pIpWhiteList == NULL) {
    TAOS_CHECK_GOTO(createDefaultIpWhiteList(&pUser->pIpWhiteList), &lino, _OVER);
    pUser->ipWhiteListVer = taosGetTimestampMs();
  }

  SDB_GET_RESERVE(pRaw, dataPos, USER_RESERVE_SIZE, _OVER)
  taosInitRWLatch(&pUser->lock);

_OVER:
  taosMemoryFree(key);
  taosMemoryFree(value);
  if (code < 0) {
    terrno = code;
    mError("user:%s, failed to decode at line %d from raw:%p since %s", pUser == NULL ? "null" : pUser->user, lino,
           pRaw, tstrerror(code));
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
    TAOS_RETURN(terrno);
  }
  pUser->acctId = pAcct->acctId;
  sdbRelease(pSdb, pAcct);

  return 0;
}

int32_t mndDupTableHash(SHashObj *pOld, SHashObj **ppNew) {
  int32_t code = 0;
  *ppNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (*ppNew == NULL) {
    TAOS_RETURN(terrno);
  }

  char *tb = taosHashIterate(pOld, NULL);
  while (tb != NULL) {
    size_t keyLen = 0;
    char  *key = taosHashGetKey(tb, &keyLen);

    int32_t valueLen = strlen(tb) + 1;
    if ((code = taosHashPut(*ppNew, key, keyLen, tb, valueLen)) != 0) {
      taosHashCancelIterate(pOld, tb);
      taosHashCleanup(*ppNew);
      TAOS_RETURN(code);
    }
    tb = taosHashIterate(pOld, tb);
  }

  TAOS_RETURN(code);
}

int32_t mndDupUseDbHash(SHashObj *pOld, SHashObj **ppNew) {
  int32_t code = 0;
  *ppNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (*ppNew == NULL) {
    TAOS_RETURN(terrno);
  }

  int32_t *db = taosHashIterate(pOld, NULL);
  while (db != NULL) {
    size_t keyLen = 0;
    char  *key = taosHashGetKey(db, &keyLen);

    if ((code = taosHashPut(*ppNew, key, keyLen, db, sizeof(*db))) != 0) {
      taosHashCancelIterate(pOld, db);
      taosHashCleanup(*ppNew);
      TAOS_RETURN(code);
    }
    db = taosHashIterate(pOld, db);
  }

  TAOS_RETURN(code);
}

int32_t mndUserDupObj(SUserObj *pUser, SUserObj *pNew) {
  int32_t code = 0;
  (void)memcpy(pNew, pUser, sizeof(SUserObj));
  pNew->authVersion++;
  pNew->updateTime = taosGetTimestampMs();

  taosRLockLatch(&pUser->lock);
  TAOS_CHECK_GOTO(mndDupDbHash(pUser->readDbs, &pNew->readDbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupDbHash(pUser->writeDbs, &pNew->writeDbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->readTbs, &pNew->readTbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->writeTbs, &pNew->writeTbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->alterTbs, &pNew->alterTbs), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->readViews, &pNew->readViews), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->writeViews, &pNew->writeViews), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTableHash(pUser->alterViews, &pNew->alterViews), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupTopicHash(pUser->topics, &pNew->topics), NULL, _OVER);
  TAOS_CHECK_GOTO(mndDupUseDbHash(pUser->useDbs, &pNew->useDbs), NULL, _OVER);
  pNew->pIpWhiteList = cloneIpWhiteList(pUser->pIpWhiteList);
  if (pNew->pIpWhiteList == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

_OVER:
  taosRUnLockLatch(&pUser->lock);
  TAOS_RETURN(code);
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
  pOld->flag = pNew->flag;
  (void)memcpy(pOld->pass, pNew->pass, TSDB_PASSWORD_LEN);
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
  TAOS_MEMORY_REALLOC(pOld->pIpWhiteList, sz);
  if (pOld->pIpWhiteList == NULL) {
    taosWUnLockLatch(&pOld->lock);
    return terrno;
  }
  (void)memcpy(pOld->pIpWhiteList, pNew->pIpWhiteList, sz);
  pOld->ipWhiteListVer = pNew->ipWhiteListVer;

  taosWUnLockLatch(&pOld->lock);

  return 0;
}

int32_t mndAcquireUser(SMnode *pMnode, const char *userName, SUserObj **ppUser) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  *ppUser = sdbAcquire(pSdb, SDB_USER, userName);
  if (*ppUser == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      code = TSDB_CODE_MND_USER_NOT_EXIST;
    } else {
      code = TSDB_CODE_MND_USER_NOT_AVAILABLE;
    }
  }
  TAOS_RETURN(code);
}

void mndReleaseUser(SMnode *pMnode, SUserObj *pUser) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pUser);
}

static int32_t mndCreateUser(SMnode *pMnode, char *acct, SCreateUserReq *pCreate, SRpcMsg *pReq) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SUserObj userObj = {0};
  if (pCreate->isImport != 1) {
    taosEncryptPass_c((uint8_t *)pCreate->pass, strlen(pCreate->pass), userObj.pass);
  } else {
    // mInfo("pCreate->pass:%s", pCreate->pass)
    strncpy(userObj.pass, pCreate->pass, TSDB_PASSWORD_LEN);
  }
  tstrncpy(userObj.user, pCreate->user, TSDB_USER_LEN);
  tstrncpy(userObj.acct, acct, TSDB_USER_LEN);
  userObj.createdTime = taosGetTimestampMs();
  userObj.updateTime = userObj.createdTime;
  userObj.superUser = 0;  // pCreate->superUser;
  userObj.sysInfo = pCreate->sysInfo;
  userObj.enable = pCreate->enable;
  userObj.createdb = pCreate->createDb;

  if (pCreate->numIpRanges == 0) {
    TAOS_CHECK_RETURN(createDefaultIpWhiteList(&userObj.pIpWhiteList));
  } else {
    SHashObj *pUniqueTab = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
    if (pUniqueTab == NULL) {
      TAOS_RETURN(terrno);
    }
    int32_t dummpy = 0;
    for (int i = 0; i < pCreate->numIpRanges; i++) {
      SIpV4Range range = {.ip = pCreate->pIpRanges[i].ip, .mask = pCreate->pIpRanges[i].mask};
      if ((code = taosHashPut(pUniqueTab, &range, sizeof(range), &dummpy, sizeof(dummpy))) != 0) {
        taosHashCleanup(pUniqueTab);
        TAOS_RETURN(code);
      }
    }
    if ((code = taosHashPut(pUniqueTab, &defaultIpRange, sizeof(defaultIpRange), &dummpy, sizeof(dummpy))) != 0) {
      taosHashCleanup(pUniqueTab);
      TAOS_RETURN(code);
    }

    if (taosHashGetSize(pUniqueTab) > MND_MAX_USE_HOST) {
      taosHashCleanup(pUniqueTab);
      TAOS_RETURN(TSDB_CODE_MND_TOO_MANY_USER_HOST);
    }

    int32_t       numOfRanges = taosHashGetSize(pUniqueTab);
    SIpWhiteList *p = taosMemoryCalloc(1, sizeof(SIpWhiteList) + numOfRanges * sizeof(SIpV4Range));
    if (p == NULL) {
      taosHashCleanup(pUniqueTab);
      TAOS_RETURN(terrno);
    }
    void   *pIter = taosHashIterate(pUniqueTab, NULL);
    int32_t i = 0;
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
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  mInfo("trans:%d, used to create user:%s", pTrans->id, pCreate->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(&userObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  if ((code = ipWhiteMgtUpdate(pMnode, userObj.user, userObj.pIpWhiteList)) != 0) {
    mndTransDrop(pTrans);
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  taosMemoryFree(userObj.pIpWhiteList);
  mndTransDrop(pTrans);
  return 0;
_OVER:
  taosMemoryFree(userObj.pIpWhiteList);

  TAOS_RETURN(code);
}

static int32_t mndProcessCreateUserReq(SRpcMsg *pReq) {
  SMnode        *pMnode = pReq->info.node;
  int32_t        code = 0;
  int32_t        lino = 0;
  SUserObj      *pUser = NULL;
  SUserObj      *pOperUser = NULL;
  SCreateUserReq createReq = {0};

  if (tDeserializeSCreateUserReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _OVER);
  }

  mInfo("user:%s, start to create, createdb:%d, is_import:%d", createReq.user, createReq.isImport, createReq.createDb);

#ifndef TD_ENTERPRISE
  if (createReq.isImport == 1) {
    TAOS_CHECK_GOTO(TSDB_CODE_OPS_NOT_SUPPORT, &lino, _OVER);  // enterprise feature
  }
#endif

  if (createReq.isImport != 1) {
    TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_USER), &lino, _OVER);
  } else {
    if (strcmp(pReq->info.conn.user, "root") != 0) {
      mError("The operation is not permitted, user:%s", pReq->info.conn.user);
      TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_RIGHTS, &lino, _OVER);
    }
  }

  if (createReq.user[0] == 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_USER_FORMAT, &lino, _OVER);
  }

  if (createReq.pass[0] == 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_PASS_FORMAT, &lino, _OVER);
  }

  if (createReq.isImport != 1) {
    if (strlen(createReq.pass) >= TSDB_PASSWORD_LEN) {
      TAOS_CHECK_GOTO(TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG, &lino, _OVER);
    }
  }

  code = mndAcquireUser(pMnode, createReq.user, &pUser);
  if (pUser != NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_ALREADY_EXIST, &lino, _OVER);
  }

  code = mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser);
  if (pOperUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_USER_FROM_CONN, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(grantCheck(TSDB_GRANT_USER), &lino, _OVER);

  code = mndCreateUser(pMnode, pOperUser->acct, &createReq, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  char detail[1000] = {0};
  (void)sprintf(detail, "enable:%d, superUser:%d, sysInfo:%d, password:xxx", createReq.enable, createReq.superUser,
                createReq.sysInfo);
  char operation[15] = {0};
  if (createReq.isImport == 1) {
    (void)strcpy(operation, "importUser");
  } else {
    (void)strcpy(operation, "createUser");
  }

  auditRecord(pReq, pMnode->clusterId, operation, "", createReq.user, detail, strlen(detail));

_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to create at line %d since %s", createReq.user, lino, tstrerror(code));
  }

  mndReleaseUser(pMnode, pUser);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSCreateUserReq(&createReq);

  TAOS_RETURN(code);
}

int32_t mndProcessGetUserWhiteListReq(SRpcMsg *pReq) {
  SMnode              *pMnode = pReq->info.node;
  int32_t              code = 0;
  int32_t              lino = 0;
  int32_t              contLen = 0;
  void                *pRsp = NULL;
  SUserObj            *pUser = NULL;
  SGetUserWhiteListReq wlReq = {0};
  SGetUserWhiteListRsp wlRsp = {0};

  if (tDeserializeSGetUserWhiteListReq(pReq->pCont, pReq->contLen, &wlReq) != 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &lino, _OVER);
  }
  mTrace("user: %s, start to get whitelist", wlReq.user);

  code = mndAcquireUser(pMnode, wlReq.user, &pUser);
  if (pUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_NOT_EXIST, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(mndSetUserWhiteListRsp(pMnode, pUser, &wlRsp), &lino, _OVER);

  contLen = tSerializeSGetUserWhiteListRsp(NULL, 0, &wlRsp);
  if (contLen < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }
  pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  contLen = tSerializeSGetUserWhiteListRsp(pRsp, contLen, &wlRsp);
  if (contLen < 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _OVER);
  }

_OVER:
  mndReleaseUser(pMnode, pUser);
  tFreeSGetUserWhiteListRsp(&wlRsp);
  if (code < 0) {
    mError("user:%s, failed to get whitelist at line %d since %s", wlReq.user, lino, tstrerror(code));
    rpcFreeCont(pRsp);
    pRsp = NULL;
    contLen = 0;
  }
  pReq->code = code;
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;

  TAOS_RETURN(code);
}

int32_t mndProcesSRetrieveIpWhiteReq(SRpcMsg *pReq) {
  int32_t        code = 0;
  int32_t        lino = 0;
  int32_t        len = 0;
  void          *pRsp = NULL;
  SUpdateIpWhite ipWhite = {0};

  // impl later
  SRetrieveIpWhiteReq req = {0};
  if (tDeserializeRetrieveIpWhite(pReq->pCont, pReq->contLen, &req) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    TAOS_CHECK_GOTO(code, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(ipWhiteMgtFillMsg(&ipWhite), &lino, _OVER);

  len = tSerializeSUpdateIpWhite(NULL, 0, &ipWhite);
  if (len < 0) {
    TAOS_CHECK_GOTO(len, &lino, _OVER);
  }

  pRsp = rpcMallocCont(len);
  if (!pRsp) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  len = tSerializeSUpdateIpWhite(pRsp, len, &ipWhite);
  if (len < 0) {
    TAOS_CHECK_GOTO(len, &lino, _OVER);
  }

_OVER:
  if (code < 0) {
    mError("failed to process retrieve ip white request at line %d since %s", lino, tstrerror(code));
    rpcFreeCont(pRsp);
    pRsp = NULL;
    len = 0;
  }
  pReq->code = code;
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = len;

  tFreeSUpdateIpWhiteReq(&ipWhite);
  TAOS_RETURN(code);
}

static int32_t mndAlterUser(SMnode *pMnode, SUserObj *pOld, SUserObj *pNew, SRpcMsg *pReq) {
  int32_t code = 0;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "alter-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to alter since %s", pOld->user, terrstr());
    TAOS_RETURN(terrno);
  }
  mInfo("trans:%d, used to alter user:%s", pTrans->id, pOld->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pNew);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  if ((code = ipWhiteMgtUpdate(pMnode, pNew->user, pNew->pIpWhiteList)) != 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }
  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndDupObjHash(SHashObj *pOld, int32_t dataLen, SHashObj **ppNew) {
  int32_t code = 0;

  *ppNew =
      taosHashInit(taosHashGetSize(pOld), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (*ppNew == NULL) {
    code = terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY;
    TAOS_RETURN(code);
  }

  char *db = taosHashIterate(pOld, NULL);
  while (db != NULL) {
    int32_t len = strlen(db) + 1;
    if ((code = taosHashPut(*ppNew, db, len, db, dataLen)) != 0) {
      taosHashCancelIterate(pOld, db);
      taosHashCleanup(*ppNew);
      TAOS_RETURN(code);
    }
    db = taosHashIterate(pOld, db);
  }

  TAOS_RETURN(code);
}

int32_t mndDupDbHash(SHashObj *pOld, SHashObj **ppNew) { return mndDupObjHash(pOld, TSDB_DB_FNAME_LEN, ppNew); }

int32_t mndDupTopicHash(SHashObj *pOld, SHashObj **ppNew) { return mndDupObjHash(pOld, TSDB_TOPIC_FNAME_LEN, ppNew); }

static int32_t mndTablePriviledge(SMnode *pMnode, SHashObj *hash, SHashObj *useDbHash, SAlterUserReq *alterReq,
                                  SSdb *pSdb) {
  void *pIter = NULL;
  char  tbFName[TSDB_TABLE_FNAME_LEN] = {0};

  (void)snprintf(tbFName, sizeof(tbFName), "%s.%s", alterReq->objname, alterReq->tabName);
  int32_t len = strlen(tbFName) + 1;

  if (alterReq->tagCond != NULL && alterReq->tagCondLen != 0) {
    char *value = taosHashGet(hash, tbFName, len);
    if (value != NULL) {
      TAOS_RETURN(TSDB_CODE_MND_PRIVILEDGE_EXIST);
    }

    int32_t condLen = alterReq->tagCondLen;
    TAOS_CHECK_RETURN(taosHashPut(hash, tbFName, len, alterReq->tagCond, condLen));
  } else {
    TAOS_CHECK_RETURN(taosHashPut(hash, tbFName, len, alterReq->isView ? "v" : "t", 2));
  }

  int32_t  dbKeyLen = strlen(alterReq->objname) + 1;
  int32_t  ref = 1;
  int32_t *currRef = taosHashGet(useDbHash, alterReq->objname, dbKeyLen);
  if (NULL != currRef) {
    ref = (*currRef) + 1;
  }
  TAOS_CHECK_RETURN(taosHashPut(useDbHash, alterReq->objname, dbKeyLen, &ref, sizeof(ref)));

  TAOS_RETURN(0);
}

static int32_t mndRemoveTablePriviledge(SMnode *pMnode, SHashObj *hash, SHashObj *useDbHash, SAlterUserReq *alterReq,
                                        SSdb *pSdb) {
  void *pIter = NULL;
  char  tbFName[TSDB_TABLE_FNAME_LEN] = {0};
  (void)snprintf(tbFName, sizeof(tbFName), "%s.%s", alterReq->objname, alterReq->tabName);
  int32_t len = strlen(tbFName) + 1;

  if (taosHashRemove(hash, tbFName, len) != 0) {
    TAOS_RETURN(0);  // not found
  }

  int32_t  dbKeyLen = strlen(alterReq->objname) + 1;
  int32_t *currRef = taosHashGet(useDbHash, alterReq->objname, dbKeyLen);
  if (NULL == currRef) {
    return 0;
  }

  if (1 == *currRef) {
    if (taosHashRemove(useDbHash, alterReq->objname, dbKeyLen) != 0) {
      TAOS_RETURN(0);  // not found
    }
    return 0;
  }
  int32_t ref = (*currRef) - 1;
  TAOS_CHECK_RETURN(taosHashPut(useDbHash, alterReq->objname, dbKeyLen, &ref, sizeof(ref)));

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
  if (type == TSDB_ALTER_USER_CREATEDB) {
    return "userCreateDB";
  }
  return "error";
}

static int32_t mndProcessAlterUserPrivilegesReq(SAlterUserReq *pAlterReq, SMnode *pMnode, SUserObj *pNewUser) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int32_t code = 0;
  int32_t lino = 0;

  if (ALTER_USER_ADD_READ_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      if ((code = taosHashPut(pNewUser->readDbs, pAlterReq->objname, len, pAlterReq->objname, TSDB_DB_FNAME_LEN)) !=
          0) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      while (1) {
        SDbObj *pDb = NULL;
        pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
        if (pIter == NULL) break;
        int32_t len = strlen(pDb->name) + 1;
        if ((code = taosHashPut(pNewUser->readDbs, pDb->name, len, pDb->name, TSDB_DB_FNAME_LEN)) != 0) {
          sdbRelease(pSdb, pDb);
          sdbCancelFetch(pSdb, pIter);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
        sdbRelease(pSdb, pDb);
      }
    }
  }

  if (ALTER_USER_ADD_WRITE_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      if ((code = taosHashPut(pNewUser->writeDbs, pAlterReq->objname, len, pAlterReq->objname, TSDB_DB_FNAME_LEN)) !=
          0) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(code, &lino, _OVER);
      }
      mndReleaseDb(pMnode, pDb);
    } else {
      while (1) {
        SDbObj *pDb = NULL;
        pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
        if (pIter == NULL) break;
        int32_t len = strlen(pDb->name) + 1;
        if ((code = taosHashPut(pNewUser->writeDbs, pDb->name, len, pDb->name, TSDB_DB_FNAME_LEN)) != 0) {
          sdbRelease(pSdb, pDb);
          sdbCancelFetch(pSdb, pIter);
          TAOS_CHECK_GOTO(code, &lino, _OVER);
        }
        sdbRelease(pSdb, pDb);
      }
    }
  }

  if (ALTER_USER_DEL_READ_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      (void)taosHashRemove(pNewUser->readDbs, pAlterReq->objname, len);
      mndReleaseDb(pMnode, pDb);
    } else {
      taosHashClear(pNewUser->readDbs);
    }
  }

  if (ALTER_USER_DEL_WRITE_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_DB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    if (strcmp(pAlterReq->objname, "1.*") != 0) {
      int32_t len = strlen(pAlterReq->objname) + 1;
      SDbObj *pDb = mndAcquireDb(pMnode, pAlterReq->objname);
      if (pDb == NULL) {
        mndReleaseDb(pMnode, pDb);
        TAOS_CHECK_GOTO(terrno, &lino, _OVER);  // TODO: refactor the terrno to code
      }
      (void)taosHashRemove(pNewUser->writeDbs, pAlterReq->objname, len);
      mndReleaseDb(pMnode, pDb);
    } else {
      taosHashClear(pNewUser->writeDbs);
    }
  }

  SHashObj *pReadTbs = pNewUser->readTbs;
  SHashObj *pWriteTbs = pNewUser->writeTbs;
  SHashObj *pAlterTbs = pNewUser->alterTbs;

#ifdef TD_ENTERPRISE
  if (pAlterReq->isView) {
    pReadTbs = pNewUser->readViews;
    pWriteTbs = pNewUser->writeViews;
    pAlterTbs = pNewUser->alterViews;
  }
#endif

  if (ALTER_USER_ADD_READ_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndTablePriviledge(pMnode, pReadTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_ADD_WRITE_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndTablePriviledge(pMnode, pWriteTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_ADD_ALTER_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_ADD_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndTablePriviledge(pMnode, pAlterTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_DEL_READ_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndRemoveTablePriviledge(pMnode, pReadTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_DEL_WRITE_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndRemoveTablePriviledge(pMnode, pWriteTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_DEL_ALTER_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName) ||
      ALTER_USER_DEL_ALL_TB_PRIV(pAlterReq->alterType, pAlterReq->privileges, pAlterReq->tabName)) {
    TAOS_CHECK_GOTO(mndRemoveTablePriviledge(pMnode, pAlterTbs, pNewUser->useDbs, pAlterReq, pSdb), &lino, _OVER);
  }

  if (ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(pAlterReq->alterType, pAlterReq->privileges)) {
    int32_t      len = strlen(pAlterReq->objname) + 1;
    SMqTopicObj *pTopic = NULL;
    if ((code = mndAcquireTopic(pMnode, pAlterReq->objname, &pTopic)) != 0) {
      mndReleaseTopic(pMnode, pTopic);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    if ((code = taosHashPut(pNewUser->topics, pTopic->name, len, pTopic->name, TSDB_TOPIC_FNAME_LEN)) != 0) {
      mndReleaseTopic(pMnode, pTopic);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    mndReleaseTopic(pMnode, pTopic);
  }

  if (ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(pAlterReq->alterType, pAlterReq->privileges)) {
    int32_t      len = strlen(pAlterReq->objname) + 1;
    SMqTopicObj *pTopic = NULL;
    if ((code = mndAcquireTopic(pMnode, pAlterReq->objname, &pTopic)) != 0) {
      mndReleaseTopic(pMnode, pTopic);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    (void)taosHashRemove(pNewUser->topics, pAlterReq->objname, len);
    mndReleaseTopic(pMnode, pTopic);
  }

_OVER:
  if (code < 0) {
    mError("user:%s, failed to alter user privileges at line %d since %s", pAlterReq->user, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessAlterUserReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  SSdb         *pSdb = pMnode->pSdb;
  void         *pIter = NULL;
  int32_t       code = 0;
  int32_t       lino = 0;
  SUserObj     *pUser = NULL;
  SUserObj     *pOperUser = NULL;
  SUserObj      newUser = {0};
  SAlterUserReq alterReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSAlterUserReq(pReq->pCont, pReq->contLen, &alterReq), &lino, _OVER);

  mInfo("user:%s, start to alter", alterReq.user);

  if (alterReq.user[0] == 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_USER_FORMAT, &lino, _OVER);
  }

  if (TSDB_ALTER_USER_PASSWD == alterReq.alterType &&
      (alterReq.pass[0] == 0 || strlen(alterReq.pass) >= TSDB_PASSWORD_LEN)) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_PASS_FORMAT, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, alterReq.user, &pUser), &lino, _OVER);

  (void)mndAcquireUser(pMnode, pReq->info.conn.user, &pOperUser);
  if (pOperUser == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_NO_USER_FROM_CONN, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(mndCheckAlterUserPrivilege(pOperUser, pUser, &alterReq), &lino, _OVER);

  TAOS_CHECK_GOTO(mndUserDupObj(pUser, &newUser), &lino, _OVER);

  if (alterReq.alterType == TSDB_ALTER_USER_PASSWD) {
    char pass[TSDB_PASSWORD_LEN + 1] = {0};
    taosEncryptPass_c((uint8_t *)alterReq.pass, strlen(alterReq.pass), pass);
    (void)memcpy(newUser.pass, pass, TSDB_PASSWORD_LEN);
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

  if (alterReq.alterType == TSDB_ALTER_USER_CREATEDB) {
    newUser.createdb = alterReq.createdb;
  }

  if (ALTER_USER_ADD_PRIVS(alterReq.alterType) || ALTER_USER_DEL_PRIVS(alterReq.alterType)) {
    TAOS_CHECK_GOTO(mndProcessAlterUserPrivilegesReq(&alterReq, pMnode, &newUser), &lino, _OVER);
  }

  if (alterReq.alterType == TSDB_ALTER_USER_ADD_WHITE_LIST) {
    taosMemoryFreeClear(newUser.pIpWhiteList);

    int32_t       num = pUser->pIpWhiteList->num + alterReq.numIpRanges;
    int32_t       idx = pUser->pIpWhiteList->num;
    SIpWhiteList *pNew = taosMemoryCalloc(1, sizeof(SIpWhiteList) + sizeof(SIpV4Range) * num);

    if (pNew == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

    bool exist = false;
    (void)memcpy(pNew->pIpRange, pUser->pIpWhiteList->pIpRange, sizeof(SIpV4Range) * idx);
    for (int i = 0; i < alterReq.numIpRanges; i++) {
      SIpV4Range *range = &(alterReq.pIpRanges[i]);
      if (!isRangeInIpWhiteList(pUser->pIpWhiteList, range)) {
        // already exist, just ignore;
        (void)memcpy(&pNew->pIpRange[idx], range, sizeof(SIpV4Range));
        idx++;
        continue;
      } else {
        exist = true;
      }
    }
    if (exist) {
      taosMemoryFree(pNew);
      TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_HOST_EXIST, &lino, _OVER);
    }
    pNew->num = idx;
    newUser.pIpWhiteList = pNew;
    newUser.ipWhiteListVer = pUser->ipWhiteListVer + 1;

    if (pNew->num > MND_MAX_USE_HOST) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_TOO_MANY_USER_HOST, &lino, _OVER);
    }
  }
  if (alterReq.alterType == TSDB_ALTER_USER_DROP_WHITE_LIST) {
    taosMemoryFreeClear(newUser.pIpWhiteList);

    int32_t       num = pUser->pIpWhiteList->num;
    bool          noexist = true;
    bool          localHost = false;
    SIpWhiteList *pNew = taosMemoryCalloc(1, sizeof(SIpWhiteList) + sizeof(SIpV4Range) * num);

    if (pNew == NULL) {
      TAOS_CHECK_GOTO(terrno, &lino, _OVER);
    }

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
          (void)memcpy(&pNew->pIpRange[idx], oldRange, sizeof(SIpV4Range));
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
      TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_LOCAL_HOST_NOT_DROP, &lino, _OVER);
    }
    if (noexist) {
      TAOS_CHECK_GOTO(TSDB_CODE_MND_USER_HOST_NOT_EXIST, &lino, _OVER);
    }
  }

  code = mndAlterUser(pMnode, pUser, &newUser, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (alterReq.alterType == TSDB_ALTER_USER_PASSWD) {
    char detail[1000] = {0};
    (void)sprintf(detail, "alterType:%s, enable:%d, superUser:%d, sysInfo:%d, createdb:%d, tabName:%s, password:xxx",
                  mndUserAuditTypeStr(alterReq.alterType), alterReq.enable, alterReq.superUser, alterReq.sysInfo,
                  alterReq.createdb ? 1 : 0, alterReq.tabName);
    auditRecord(pReq, pMnode->clusterId, "alterUser", "", alterReq.user, detail, strlen(detail));
  } else if (alterReq.alterType == TSDB_ALTER_USER_SUPERUSER || alterReq.alterType == TSDB_ALTER_USER_ENABLE ||
             alterReq.alterType == TSDB_ALTER_USER_SYSINFO || alterReq.alterType == TSDB_ALTER_USER_CREATEDB) {
    auditRecord(pReq, pMnode->clusterId, "alterUser", "", alterReq.user, alterReq.sql, alterReq.sqlLen);
  } else if (ALTER_USER_ADD_READ_DB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName) ||
             ALTER_USER_ADD_WRITE_DB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName) ||
             ALTER_USER_ADD_ALL_DB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName) ||
             ALTER_USER_ADD_READ_TB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName) ||
             ALTER_USER_ADD_WRITE_TB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName) ||
             ALTER_USER_ADD_ALL_TB_PRIV(alterReq.alterType, alterReq.privileges, alterReq.tabName)) {
    if (strcmp(alterReq.objname, "1.*") != 0) {
      SName name = {0};
      (void)tNameFromString(&name, alterReq.objname, T_NAME_ACCT | T_NAME_DB);
      auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", name.dbname, alterReq.user, alterReq.sql,
                  alterReq.sqlLen);
    } else {
      auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", "", alterReq.user, alterReq.sql, alterReq.sqlLen);
    }
  } else if (ALTER_USER_ADD_SUBSCRIBE_TOPIC_PRIV(alterReq.alterType, alterReq.privileges)) {
    auditRecord(pReq, pMnode->clusterId, "GrantPrivileges", alterReq.objname, alterReq.user, alterReq.sql,
                alterReq.sqlLen);
  } else if (ALTER_USER_DEL_SUBSCRIBE_TOPIC_PRIV(alterReq.alterType, alterReq.privileges)) {
    auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", alterReq.objname, alterReq.user, alterReq.sql,
                alterReq.sqlLen);
  } else {
    if (strcmp(alterReq.objname, "1.*") != 0) {
      SName name = {0};
      (void)tNameFromString(&name, alterReq.objname, T_NAME_ACCT | T_NAME_DB);
      auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", name.dbname, alterReq.user, alterReq.sql,
                  alterReq.sqlLen);
    } else {
      auditRecord(pReq, pMnode->clusterId, "RevokePrivileges", "", alterReq.user, alterReq.sql, alterReq.sqlLen);
    }
  }

_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to alter at line %d since %s", alterReq.user, lino, tstrerror(code));
  }

  tFreeSAlterUserReq(&alterReq);
  mndReleaseUser(pMnode, pOperUser);
  mndReleaseUser(pMnode, pUser);
  mndUserFreeObj(&newUser);

  TAOS_RETURN(code);
}

static int32_t mndDropUser(SMnode *pMnode, SRpcMsg *pReq, SUserObj *pUser) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "drop-user");
  if (pTrans == NULL) {
    mError("user:%s, failed to drop since %s", pUser->user, terrstr());
    TAOS_RETURN(terrno);
  }
  mInfo("trans:%d, used to drop user:%s", pTrans->id, pUser->user);

  SSdbRaw *pCommitRaw = mndUserActionEncode(pUser);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(terrno);
  }
  (void)ipWhiteMgtRemove(pUser->user);

  mndTransDrop(pTrans);
  TAOS_RETURN(0);
}

static int32_t mndProcessDropUserReq(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  int32_t      code = 0;
  int32_t      lino = 0;
  SUserObj    *pUser = NULL;
  SDropUserReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSDropUserReq(pReq->pCont, pReq->contLen, &dropReq), &lino, _OVER);

  mInfo("user:%s, start to drop", dropReq.user);
  TAOS_CHECK_GOTO(mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_USER), &lino, _OVER);

  if (dropReq.user[0] == 0) {
    TAOS_CHECK_GOTO(TSDB_CODE_MND_INVALID_USER_FORMAT, &lino, _OVER);
  }

  TAOS_CHECK_GOTO(mndAcquireUser(pMnode, dropReq.user, &pUser), &lino, _OVER);

  TAOS_CHECK_GOTO(mndDropUser(pMnode, pReq, pUser), &lino, _OVER);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  auditRecord(pReq, pMnode->clusterId, "dropUser", "", dropReq.user, dropReq.sql, dropReq.sqlLen);

_OVER:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to drop at line %d since %s", dropReq.user, lino, tstrerror(code));
  }

  mndReleaseUser(pMnode, pUser);
  tFreeSDropUserReq(&dropReq);
  TAOS_RETURN(code);
}

static int32_t mndProcessGetUserAuthReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  int32_t         code = 0;
  int32_t         lino = 0;
  int32_t         contLen = 0;
  void           *pRsp = NULL;
  SUserObj       *pUser = NULL;
  SGetUserAuthReq authReq = {0};
  SGetUserAuthRsp authRsp = {0};

  TAOS_CHECK_EXIT(tDeserializeSGetUserAuthReq(pReq->pCont, pReq->contLen, &authReq));
  mTrace("user:%s, start to get auth", authReq.user);

  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, authReq.user, &pUser));

  TAOS_CHECK_EXIT(mndSetUserAuthRsp(pMnode, pUser, &authRsp));

  contLen = tSerializeSGetUserAuthRsp(NULL, 0, &authRsp);
  if (contLen < 0) {
    TAOS_CHECK_EXIT(contLen);
  }
  pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  contLen = tSerializeSGetUserAuthRsp(pRsp, contLen, &authRsp);
  if (contLen < 0) {
    TAOS_CHECK_EXIT(contLen);
  }

_exit:
  mndReleaseUser(pMnode, pUser);
  tFreeSGetUserAuthRsp(&authRsp);
  if (code < 0) {
    mError("user:%s, failed to get auth at line %d since %s", authReq.user, lino, tstrerror(code));
    rpcFreeCont(pRsp);
    pRsp = NULL;
    contLen = 0;
  }
  pReq->info.rsp = pRsp;
  pReq->info.rspLen = contLen;
  pReq->code = code;

  TAOS_RETURN(code);
}

static int32_t mndRetrieveUsers(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   code = 0;
  int32_t   lino = 0;
  int32_t   numOfRows = 0;
  SUserObj *pUser = NULL;
  int32_t   cols = 0;
  int8_t    flag = 0;
  char     *pWrite = NULL;
  char     *buf = NULL;
  char     *varstr = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)name, false, pUser, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->superUser, false, pUser, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->enable, false, pUser, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->sysInfo, false, pUser, _exit);

    cols++;
    flag = pUser->createdb ? 1 : 0;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&flag, false, pUser, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->createdTime, false, pUser, _exit);

    cols++;

    int32_t tlen = convertIpWhiteListToStr(pUser->pIpWhiteList, &buf);
    // int32_t tlen = mndFetchIpWhiteList(pUser->pIpWhiteList, &buf);
    if (tlen != 0) {
      TAOS_MEMORY_REALLOC(varstr, VARSTR_HEADER_SIZE + tlen);
      if (varstr == NULL) {
        sdbRelease(pSdb, pUser);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      varDataSetLen(varstr, tlen);
      (void)memcpy(varDataVal(varstr), buf, tlen);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)varstr, false, pUser, _exit);

      taosMemoryFreeClear(buf);
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)NULL, true, pUser, _exit);
    }

    numOfRows++;
    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(buf);
  taosMemoryFreeClear(varstr);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static int32_t mndRetrieveUsersFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t numOfRows = 0;
#ifdef TD_ENTERPRISE
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  SUserObj *pUser = NULL;
  int32_t   code = 0;
  int32_t   lino = 0;
  int32_t   cols = 0;
  int8_t    flag = 0;
  char     *pWrite = NULL;
  char     *buf = NULL;
  char     *varstr = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_USER, pShow->pIter, (void **)&pUser);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)name, false, pUser, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->superUser, false, pUser, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->enable, false, pUser, _exit);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pUser->sysInfo, false, pUser, _exit);

    cols++;
    flag = pUser->createdb ? 1 : 0;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&flag, false, pUser, _exit);

    // mInfo("pUser->pass:%s", pUser->pass);
    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char pass[TSDB_PASSWORD_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(pass, pUser->pass, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)pass, false, pUser, _exit);

    cols++;

    int32_t tlen = convertIpWhiteListToStr(pUser->pIpWhiteList, &buf);
    // int32_t tlen = mndFetchIpWhiteList(pUser->pIpWhiteList, &buf);
    if (tlen != 0) {
      TAOS_MEMORY_REALLOC(varstr, VARSTR_HEADER_SIZE + tlen);
      if (varstr == NULL) {
        sdbRelease(pSdb, pUser);
        TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
      }
      varDataSetLen(varstr, tlen);
      (void)memcpy(varDataVal(varstr), buf, tlen);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)varstr, false, pUser, _exit);

      taosMemoryFreeClear(buf);
    } else {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      COL_DATA_SET_VAL_GOTO((const char *)NULL, true, pUser, _exit);
    }

    numOfRows++;
    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(buf);
  taosMemoryFreeClear(varstr);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
#endif
  return numOfRows;
}

static void mndCancelGetNextUser(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_USER);
}

static int32_t mndLoopHash(SHashObj *hash, char *priType, SSDataBlock *pBlock, int32_t *pNumOfRows, SSdb *pSdb,
                           SUserObj *pUser, SShowObj *pShow, char **condition, char **sql) {
  char   *value = taosHashIterate(hash, NULL);
  char   *user = pUser->user;
  int32_t code = 0;
  int32_t lino = 0;
  int32_t cols = 0;
  int32_t numOfRows = *pNumOfRows;

  while (value != NULL) {
    cols = 0;
    char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(userName, user, pShow->pMeta->pSchemas[cols].bytes);
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)userName, false, NULL, _exit);

    char privilege[20] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(privilege, priType, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)privilege, false, NULL, _exit);

    size_t keyLen = 0;
    void  *key = taosHashGetKey(value, &keyLen);

    char dbName[TSDB_DB_NAME_LEN] = {0};
    (void)mndExtractShortDbNameFromStbFullName(key, dbName);
    char dbNameContent[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(dbNameContent, dbName, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)dbNameContent, false, NULL, _exit);

    char tableName[TSDB_TABLE_NAME_LEN] = {0};
    mndExtractTbNameFromStbFullName(key, tableName, TSDB_TABLE_NAME_LEN);
    char tableNameContent[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(tableNameContent, tableName, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)tableNameContent, false, NULL, _exit);

    if (strcmp("t", value) != 0 && strcmp("v", value) != 0) {
      SNode  *pAst = NULL;
      int32_t sqlLen = 0;
      size_t  bufSz = strlen(value) + 1;
      if (bufSz < 5) bufSz = 5;
      TAOS_MEMORY_REALLOC(*sql, bufSz + 1);
      if (*sql == NULL) {
        code = terrno;
        goto _exit;
      }
      TAOS_MEMORY_REALLOC(*condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if ((*condition) == NULL) {
        code = terrno;
        goto _exit;
      }

      if (nodesStringToNode(value, &pAst) == 0) {
        if (nodesNodeToSQL(pAst, *sql, bufSz, &sqlLen) != 0) {
          sqlLen = 5;
          (void)sprintf(*sql, "error");
        }
        nodesDestroyNode(pAst);
      } else {
        sqlLen = 5;
        (void)sprintf(*sql, "error");
      }

      STR_WITH_MAXSIZE_TO_VARSTR((*condition), (*sql), pShow->pMeta->pSchemas[cols].bytes);

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)(*condition), false, NULL, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, NULL, _exit);
    } else {
      TAOS_MEMORY_REALLOC(*condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if ((*condition) == NULL) {
        code = terrno;
        goto _exit;
      }
      STR_WITH_MAXSIZE_TO_VARSTR((*condition), "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)(*condition), false, NULL, _exit);

      char notes[64 + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, value[0] == 'v' ? "view" : "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, NULL, _exit);
    }

    numOfRows++;
    value = taosHashIterate(hash, value);
  }
  *pNumOfRows = numOfRows;
_exit:
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    sdbRelease(pSdb, pUser);
  }
  TAOS_RETURN(code);
}

static int32_t mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SUserObj *pUser = NULL;
  int32_t   cols = 0;
  char     *pWrite = NULL;
  char     *condition = NULL;
  char     *sql = NULL;

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
    if (numOfRows + numOfReadDbs + numOfWriteDbs + numOfTopics + numOfReadTbs + numOfWriteTbs + numOfAlterTbs +
            numOfReadViews + numOfWriteViews + numOfAlterViews >=
        rows) {
      mInfo(
          "will restore. current num of rows: %d, read dbs %d, write dbs %d, topics %d, read tables %d, write tables "
          "%d, alter tables %d, read views %d, write views %d, alter views %d",
          numOfRows, numOfReadDbs, numOfWriteDbs, numOfTopics, numOfReadTbs, numOfWriteTbs, numOfAlterTbs,
          numOfReadViews, numOfWriteViews, numOfAlterViews);
      pShow->restore = true;
      sdbRelease(pSdb, pUser);
      break;
    }

    if (pUser->superUser) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "all", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, _exit);

      char objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(objName, "all", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)objName, false, pUser, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, _exit);

      numOfRows++;
    }

    char *db = taosHashIterate(pUser->readDbs, NULL);
    while (db != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "read", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, _exit);

      SName name = {0};
      char  objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      (void)tNameFromString(&name, db, T_NAME_ACCT | T_NAME_DB);
      (void)tNameGetDbName(&name, varDataVal(objName));
      varDataSetLen(objName, strlen(varDataVal(objName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)objName, false, pUser, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, _exit);

      numOfRows++;
      db = taosHashIterate(pUser->readDbs, db);
    }

    db = taosHashIterate(pUser->writeDbs, NULL);
    while (db != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "write", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, _exit);

      SName name = {0};
      char  objName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      (void)tNameFromString(&name, db, T_NAME_ACCT | T_NAME_DB);
      (void)tNameGetDbName(&name, varDataVal(objName));
      varDataSetLen(objName, strlen(varDataVal(objName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)objName, false, pUser, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, _exit);

      numOfRows++;
      db = taosHashIterate(pUser->writeDbs, db);
    }

    TAOS_CHECK_EXIT(mndLoopHash(pUser->readTbs, "read", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->writeTbs, "write", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->alterTbs, "alter", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->readViews, "read", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->writeViews, "write", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    TAOS_CHECK_EXIT(mndLoopHash(pUser->alterViews, "alter", pBlock, &numOfRows, pSdb, pUser, pShow, &condition, &sql));

    char *topic = taosHashIterate(pUser->topics, NULL);
    while (topic != NULL) {
      cols = 0;
      char userName[TSDB_USER_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(userName, pUser->user, pShow->pMeta->pSchemas[cols].bytes);
      SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)userName, false, pUser, _exit);

      char privilege[20] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(privilege, "subscribe", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)privilege, false, pUser, _exit);

      char topicName[TSDB_TOPIC_NAME_LEN + VARSTR_HEADER_SIZE + 5] = {0};
      tstrncpy(varDataVal(topicName), mndGetDbStr(topic), TSDB_TOPIC_NAME_LEN - 2);
      varDataSetLen(topicName, strlen(varDataVal(topicName)));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)topicName, false, pUser, _exit);

      char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(tableName, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)tableName, false, pUser, _exit);

      TAOS_MEMORY_REALLOC(condition, TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE);
      if (condition == NULL) {
        sdbRelease(pSdb, pUser);
        TAOS_CHECK_GOTO(terrno, &lino, _exit);
      }
      STR_WITH_MAXSIZE_TO_VARSTR(condition, "", pShow->pMeta->pSchemas[cols].bytes);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)condition, false, pUser, _exit);

      char notes[2] = {0};
      STR_WITH_MAXSIZE_TO_VARSTR(notes, "", sizeof(notes));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      COL_DATA_SET_VAL_GOTO((const char *)notes, false, pUser, _exit);

      numOfRows++;
      topic = taosHashIterate(pUser->topics, topic);
    }

    sdbRelease(pSdb, pUser);
  }

  pShow->numOfRows += numOfRows;
_exit:
  taosMemoryFreeClear(condition);
  taosMemoryFreeClear(sql);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextPrivileges(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_USER);
}

int32_t mndValidateUserAuthInfo(SMnode *pMnode, SUserAuthVersion *pUsers, int32_t numOfUses, void **ppRsp,
                                int32_t *pRspLen, int64_t ipWhiteListVer) {
  int32_t           code = 0;
  int32_t           lino = 0;
  int32_t           rspLen = 0;
  void             *pRsp = NULL;
  SUserAuthBatchRsp batchRsp = {0};

  batchRsp.pArray = taosArrayInit(numOfUses, sizeof(SGetUserAuthRsp));
  if (batchRsp.pArray == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }

  for (int32_t i = 0; i < numOfUses; ++i) {
    SUserObj *pUser = NULL;
    code = mndAcquireUser(pMnode, pUsers[i].user, &pUser);
    if (pUser == NULL) {
      if (TSDB_CODE_MND_USER_NOT_EXIST == code) {
        SGetUserAuthRsp rsp = {.dropped = 1};
        (void)memcpy(rsp.user, pUsers[i].user, TSDB_USER_LEN);
        TSDB_CHECK_NULL(taosArrayPush(batchRsp.pArray, &rsp), code, lino, _OVER, TSDB_CODE_OUT_OF_MEMORY);
      }
      mError("user:%s, failed to auth user since %s", pUsers[i].user, tstrerror(code));
      code = 0;
      continue;
    }

    pUsers[i].version = ntohl(pUsers[i].version);
    if (pUser->authVersion <= pUsers[i].version && ipWhiteListVer == pMnode->ipWhiteVer) {
      mndReleaseUser(pMnode, pUser);
      continue;
    }

    SGetUserAuthRsp rsp = {0};
    code = mndSetUserAuthRsp(pMnode, pUser, &rsp);
    if (code) {
      mndReleaseUser(pMnode, pUser);
      tFreeSGetUserAuthRsp(&rsp);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }

    if (!(taosArrayPush(batchRsp.pArray, &rsp))) {
      code = terrno;
      mndReleaseUser(pMnode, pUser);
      tFreeSGetUserAuthRsp(&rsp);
      TAOS_CHECK_GOTO(code, &lino, _OVER);
    }
    mndReleaseUser(pMnode, pUser);
  }

  if (taosArrayGetSize(batchRsp.pArray) <= 0) {
    *ppRsp = NULL;
    *pRspLen = 0;

    tFreeSUserAuthBatchRsp(&batchRsp);
    return 0;
  }

  rspLen = tSerializeSUserAuthBatchRsp(NULL, 0, &batchRsp);
  if (rspLen < 0) {
    TAOS_CHECK_GOTO(rspLen, &lino, _OVER);
  }
  pRsp = taosMemoryMalloc(rspLen);
  if (pRsp == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _OVER);
  }
  rspLen = tSerializeSUserAuthBatchRsp(pRsp, rspLen, &batchRsp);
  if (rspLen < 0) {
    TAOS_CHECK_GOTO(rspLen, &lino, _OVER);
  }
_OVER:
  tFreeSUserAuthBatchRsp(&batchRsp);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    taosMemoryFreeClear(pRsp);
    rspLen = 0;
  }
  *ppRsp = pRsp;
  *pRspLen = rspLen;

  TAOS_RETURN(code);
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

    if ((code = mndUserDupObj(pUser, &newUser)) != 0) {
      break;
    }

    bool inRead = (taosHashGet(newUser.readDbs, db, len) != NULL);
    bool inWrite = (taosHashGet(newUser.writeDbs, db, len) != NULL);
    if (inRead || inWrite) {
      (void)taosHashRemove(newUser.readDbs, db, len);
      (void)taosHashRemove(newUser.writeDbs, db, len);

      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
  }

_OVER:
  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
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

    if ((code = mndUserDupObj(pUser, &newUser)) != 0) {
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
      if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
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

    if ((code = mndUserDupObj(pUser, &newUser)) != 0) {
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
      if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
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

    if ((code = mndUserDupObj(pUser, &newUser)) != 0) {
      break;
    }

    bool inTopic = (taosHashGet(newUser.topics, topic, len) != NULL);
    if (inTopic) {
      (void)taosHashRemove(newUser.topics, topic, len);
      SSdbRaw *pCommitRaw = mndUserActionEncode(&newUser);
      if (pCommitRaw == NULL || (code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        break;
      }
      (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
    }

    mndUserFreeObj(&newUser);
    sdbRelease(pSdb, pUser);
  }

  if (pUser != NULL) sdbRelease(pSdb, pUser);
  if (pIter != NULL) sdbCancelFetch(pSdb, pIter);
  mndUserFreeObj(&newUser);
  TAOS_RETURN(code);
}

int64_t mndGetUserIpWhiteListVer(SMnode *pMnode, SUserObj *pUser) {
  // ver = 0, disable ip white list
  // ver > 0, enable ip white list
  return tsEnableWhiteList ? pUser->ipWhiteListVer : 0;
}
