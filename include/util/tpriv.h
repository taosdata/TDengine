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

#ifndef _TD_UTIL_PRIV_H_
#define _TD_UTIL_PRIV_H_

#include "os.h"
#include "taosdef.h"
#include "tarray.h"
#include "thash.h"

#ifdef __cplusplus
extern "C" {
#endif

#define T_ROLE_SYSDBA       0x01
#define T_ROLE_SYSSEC       0x02
#define T_ROLE_SYSAUDIT     0x04
#define T_ROLE_SYSAUDIT_LOG 0x08
#define T_ROLE_SYSINFO_0    0x10
#define T_ROLE_SYSINFO_1    0x20

#define TSDB_ROLE_SYSDBA       "SYSDBA"
#define TSDB_ROLE_SYSSEC       "SYSSEC"
#define TSDB_ROLE_SYSAUDIT     "SYSAUDIT"
#define TSDB_ROLE_SYSAUDIT_LOG "SYSAUDIT_LOG"
#define TSDB_ROLE_SYSINFO_0    "SYSINFO_0"
#define TSDB_ROLE_SYSINFO_1    "SYSINFO_1"
#define TSDB_ROLE_DEFAULT      TSDB_ROLE_SYSINFO_1

#define TSDB_WORD_AUDIT      "audit"
#define TSDB_WORD_BASIC      "basic"
#define TSDB_WORD_DEBUG      "debug"
#define TSDB_WORD_PASS       "pass"
#define TSDB_WORD_PRIVILEGED "privileged"
#define TSDB_WORD_SECURITY   "security"
#define TSDB_WORD_SELF       "self"
#define TSDB_WORD_SYSTEM     "system"
#define TSDB_WORD_VARIABLE   "variable"
#define TSDB_WORD_VARIABLES  "variables"
#define TSDB_WORD_INFORMATION "information"

#define PRIV_INFO_TABLE_VERSION 3
typedef enum {
  PRIV_TYPE_UNKNOWN = -1,
  // ==================== Common Privilege ====================
  PRIV_CM_ALL = 0,          // ALL PRIVILEGES
  PRIV_CM_ALTER = 1,        // ALTER PRIVILEGE
  PRIV_CM_DROP = 2,         // DROP PRIVILEGE
  PRIV_CM_SHOW = 3,         // SHOW PRIVILEGE
  PRIV_CM_SHOW_CREATE = 4,  // SHOW CREATE PRIVILEGE
  PRIV_CM_START = 5,        // START PRIVILEGE
  PRIV_CM_STOP = 6,         // STOP PRIVILEGE
  PRIV_CM_RECALC = 7,       // RECALC PRIVILEGE
  PRIV_CM_KILL = 8,         // KILL PRIVILEGE
  PRIV_CM_SUBSCRIBE = 9,    // SUBSCRIBE PRIVILEGE
  PRIV_CM_MAX = 29,         // MAX COMMON PRIVILEGE
  // ==================== DB Privileges(5~49) ====================
  PRIV_DB_CREATE = 30,  // CREATE DATABASE
  PRIV_DB_USE,          // USE DATABASE
  PRIV_DB_FLUSH,        // FLUSH DATABASE
  PRIV_DB_COMPACT,      // COMPACT DATABASE
  PRIV_DB_TRIM,         // TRIM DATABASE
  PRIV_DB_ROLLUP,       // ROLLUP DATABASE
  PRIV_DB_SCAN,         // SCAN DATABASE
  PRIV_DB_SSMIGRATE,    // SSMIGRATE DATABASE

  // VGroup Operations
  PRIV_VG_BALANCE = 50,    // BALANCE VGROUP
  PRIV_VG_BALANCE_LEADER,  // BALANCE VGROUP LEADER
  PRIV_VG_MERGE,           // MERGE VGROUP
  PRIV_VG_REDISTRIBUTE,    // REDISTRIBUTE VGROUP
  PRIV_VG_SPLIT,           // SPLIT VGROUP

  // DB Show Operations
  PRIV_SHOW_VNODES = 60,  // SHOW VNODES
  PRIV_SHOW_VGROUPS,      // SHOW VGROUPS
  PRIV_SHOW_COMPACTS,     // SHOW COMPACTS
  PRIV_SHOW_RETENTIONS,   // SHOW RETENTIONS
  PRIV_SHOW_SCANS,        // SHOW SCANS
  PRIV_SHOW_SSMIGRATES,   // SHOW SSMIGRATES

  // ==================== Table Privileges(50-69)  ================
  PRIV_TBL_CREATE = 70,  // CREATE TABLE
  PRIV_TBL_SELECT = 71,  // SELECT TABLE
  PRIV_TBL_INSERT = 72,  // INSERT TABLE
  PRIV_TBL_UPDATE = 73,  // UPDATE TABLE(reserved)
  PRIV_TBL_DELETE = 74,  // DELETE TABLE

  // ==================== Other Privileges ================
  // function management
  PRIV_FUNC_CREATE = 80,  // CREATE FUNCTION
  PRIV_FUNC_DROP,         // DROP FUNCTION
  PRIV_FUNC_SHOW,         // SHOW FUNCTIONS

  // index management
  PRIV_IDX_CREATE = 84,  // CREATE INDEX

  // view management
  PRIV_VIEW_CREATE = 88,  // CREATE VIEW
  PRIV_VIEW_SELECT,       // SELECT VIEW

  // SMA management
  PRIV_RSMA_CREATE = 95,  // CREATE RSMA
  PRIV_TSMA_CREATE,       // CREATE TSMA

  // mount management
  PRIV_MOUNT_CREATE = 100,  // CREATE MOUNT
  PRIV_MOUNT_DROP,          // DROP MOUNT
  PRIV_MOUNT_SHOW,          // SHOW MOUNTS

  // password management
  PRIV_PASS_ALTER = 105,  // ALTER PASS
  PRIV_PASS_ALTER_SELF,   // ALTER SELF PASS

  // role management
  PRIV_ROLE_CREATE = 110,  // CREATE ROLE
  PRIV_ROLE_DROP,          // DROP ROLE
  PRIV_ROLE_SHOW,          // SHOW ROLES
  PRIV_ROLE_LOCK,          // LOCK ROLE
  PRIV_ROLE_UNLOCK,        // UNLOCK ROLE

  // user management
  PRIV_USER_CREATE = 130,  // CREATE USER
  PRIV_USER_DROP,          // DROP USER
  PRIV_USER_SET_SECURITY,  // SET USER SECURITY INFO
  PRIV_USER_SET_AUDIT,     // SET USER AUDIT INFO
  PRIV_USER_SET_BASIC,     // SET USER BASIC INFO
  PRIV_USER_UNLOCK,        // UNLOCK USER
  PRIV_USER_LOCK,          // LOCK USER
  PRIV_USER_SHOW,          // SHOW USERS
  PRIV_USER_ALTER,         // ALTER USER
  PRIV_USER_SHOW_SECURITY, // SHOW USERS SECURITY INFO

  // audit management
  PRIV_AUDIT_DB_DROP = 140,  // DROP AUDIT DATABASE
  PRIV_AUDIT_DB_ALTER,       // ALTER AUDIT DATABASE
  PRIV_AUDIT_DB_USE,         // USE AUDIT DATABASE(reserved for future use)
  PRIV_AUDIT_TBL_CREATE,     // CREATE AUDIT TABLE
  PRIV_AUDIT_TBL_SELECT,     // SELECT AUDIT TABLE
  PRIV_AUDIT_TBL_INSERT,     // INSERT AUDIT TABLE

  // token management
  PRIV_TOKEN_CREATE = 150,  // CREATE TOKEN
  PRIV_TOKEN_DROP,          // DROP TOKEN
  PRIV_TOKEN_ALTER,         // ALTER TOKEN
  PRIV_TOKEN_SHOW,          // SHOW TOKENS

  // key management
  PRIV_KEY_UPDATE = 160,  // UPDATE KEY
  PRIV_TOTP_CREATE,       // CREATE TOTP
  PRIV_TOTP_DROP,         // DROP TOTP

  // grant/revoke privileges
  PRIV_GRANT_PRIVILEGE = 170,  // GRANT PRIVILEGE
  PRIV_REVOKE_PRIVILEGE,       // REVOKE PRIVILEGE
  PRIV_SHOW_PRIVILEGES,        // SHOW PRIVILEGES
  PRIV_GRANT_SYSDBA,           // GRANT SYSDBA PRIVILEGE
  PRIV_REVOKE_SYSDBA,          // REVOKE SYSDBA PRIVILEGE
  PRIV_GRANT_SYSSEC,           // GRANT SYSSEC PRIVILEGE
  PRIV_REVOKE_SYSSEC,          // REVOKE SYSSEC PRIVILEGE
  PRIV_GRANT_SYSAUDIT,         // GRANT SYSAUDIT PRIVILEGE
  PRIV_REVOKE_SYSAUDIT,        // REVOKE SYSAUDIT PRIVILEGE

  // node management
  PRIV_NODE_CREATE = 190,  // CREATE NODE
  PRIV_NODE_DROP,          // DROP NODE
  PRIV_NODES_SHOW,         // SHOW NODES

  // system variables
  PRIV_VAR_SECURITY_ALTER = 200,  // ALTER SECURITY VARIABLE
  PRIV_VAR_AUDIT_ALTER,           // ALTER AUDIT VARIABLE
  PRIV_VAR_SYSTEM_ALTER,          // ALTER SYSTEM VARIABLE
  PRIV_VAR_DEBUG_ALTER,           // ALTER DEBUG VARIABLE
  PRIV_VAR_SECURITY_SHOW,         // SHOW SECURITY VARIABLES
  PRIV_VAR_AUDIT_SHOW,            // SHOW AUDIT VARIABLES
  PRIV_VAR_SYSTEM_SHOW,           // SHOW SYSTEM VARIABLES
  PRIV_VAR_DEBUG_SHOW,            // SHOW DEBUG VARIABLES

  // topic management
  PRIV_TOPIC_CREATE = 210,  // CREATE TOPIC
  PRIV_CONSUMER_SHOW,       // SHOW CONSUMERS
  PRIV_SUBSCRIPTION_SHOW,   // SHOW SUBSCRIPTIONS

  // stream management
  PRIV_STREAM_CREATE = 220,  // CREATE STREAM

  // system operation management
  PRIV_TRANS_SHOW = 230,  // SHOW TRANS
  PRIV_TRANS_KILL,        // KILL TRANS
  PRIV_CONN_SHOW,         // SHOW CONNECTIONS
  PRIV_CONN_KILL,         // KILL CONNECTION
  PRIV_QUERY_SHOW,        // SHOW QUERIES
  PRIV_QUERY_KILL,        // KILL QUERY

  // system info
  PRIV_INFO_SCHEMA_READ_BASIC = 240, // READ INFORMATION_SCHEMA BASIC
  PRIV_INFO_SCHEMA_READ_SEC,         // READ INFORMATION_SCHEMA SECURITY
  PRIV_INFO_SCHEMA_READ_AUDIT,       // READ INFORMATION_SCHEMA AUDIT
  PRIV_INFO_SCHEMA_READ_PRIVILEGED,  // READ INFORMATION_SCHEMA PRIVILEGED
  PRIV_PERF_SCHEMA_READ_BASIC,       // READ PERFORMANCE_SCHEMA BASIC
  PRIV_PERF_SCHEMA_READ_PRIVILEGED,  // READ PERFORMANCE_SCHEMA PRIVILEGED
  PRIV_GRANTS_SHOW,                  // SHOW GRANTS
  PRIV_CLUSTER_SHOW,                 // SHOW CLUSTER
  PRIV_APPS_SHOW,                    // SHOW APPS

  // extended privileges can be defined here (255 bits reserved in total)
  // ==================== Maximum Privilege Bit ====================
  MAX_PRIV_TYPE = 255
} EPrivType;

#define PRIV_GROUP_CNT ((MAX_PRIV_TYPE + 63) / 64)
typedef struct {
  uint64_t set[PRIV_GROUP_CNT];
} SPrivSet;

#define PRIV_GROUP(type)  ((type) / 64)
#define PRIV_OFFSET(type) ((type) & 63)
#define PRIV_TYPE(type)   ((SPrivSet){.set[PRIV_GROUP(type)] = 1ULL << PRIV_OFFSET(type)})

#define PRIV_HAS(privSet, type) (((privSet)->set[PRIV_GROUP(type)] & (1ULL << PRIV_OFFSET(type))) != 0)

#define PRIV_SET(privSet, type, val)   \
  do {                                 \
    if (PRIV_HAS((privSet), (type))) { \
      (val) = 1;                       \
    }                                  \
  } while (0)

typedef struct {
  SPrivSet privSet;
  int32_t  nPrivArgs;
  int16_t  objType;     // EPrivObjType
  void*    selectCols;  // SNodeList*
  void*    insertCols;  // SNodeList*
  void*    updateCols;  // SNodeList*
} SPrivSetArgs;

#define PRIV_SET_TYPE(type)                                                   \
  ((SPrivSetArgs){.nPrivArgs = 1,                                             \
                  .privSet.set[PRIV_GROUP(type)] = 1ULL << PRIV_OFFSET(type), \
                  .selectCols = NULL,                                         \
                  .insertCols = NULL,                                         \
                  .updateCols = NULL})

#define PRIV_SET_COLS(type, select, insert, update)                                                \
  ((SPrivSetArgs){.nPrivArgs = 1,                                                                  \
                  .privSet = ((select) == NULL && (insert) == NULL && (update) == NULL             \
                                  ? (SPrivSet){.set[PRIV_GROUP(type)] = 1ULL << PRIV_OFFSET(type)} \
                                  : (SPrivSet){0}),                                                \
                  .selectCols = (void*)(select),                                                   \
                  .insertCols = (void*)(insert),                                                   \
                  .updateCols = (void*)(update)})

typedef enum {
  PRIV_CATEGORY_UNKNOWN = -1,
  PRIV_CATEGORY_SYSTEM,
  PRIV_CATEGORY_OBJECT,
  PRIV_CATEGORY_COMMON,
  PRIV_CATEGORY_MAX,
} EPrivCategory;

typedef enum {
  PRIV_OBJ_UNKNOWN = -1,
  PRIV_OBJ_CLUSTER = 0,
  PRIV_OBJ_NODE = 1,
  PRIV_OBJ_DB = 2,
  PRIV_OBJ_TBL = 3,
  PRIV_OBJ_FUNC = 4,
  PRIV_OBJ_IDX = 5,
  PRIV_OBJ_VIEW = 6,
  PRIV_OBJ_USER = 7,
  PRIV_OBJ_ROLE = 8,
  PRIV_OBJ_RSMA = 9,
  PRIV_OBJ_TSMA = 10,
  PRIV_OBJ_TOPIC = 11,
  PRIV_OBJ_STREAM = 12,
  PRIV_OBJ_MOUNT = 13,
  PRIV_OBJ_AUDIT = 14,
  PRIV_OBJ_TOKEN = 15,
  PRIV_OBJ_MAX = 16,
} EPrivObjType;

typedef enum {
  PRIV_RW_ATTR_UNKNOWN = 0,
  PRIV_RW_ATTR_READ = 0x01,
  PRIV_RW_ATTR_WRITE = 0x02,
  PRIV_RW_ATTR_RW = 0x03,
} EPrivRwAttr;

typedef struct {
  int16_t     privType;  // EPrivType
  int8_t      category;  // EPrivCategory
  int8_t      objType;   // EPrivObjType
  int8_t      objLevel;  // 0: DB, function, 1: table, view
  uint8_t     sysType;
  uint8_t     rwAttr;  // 0x01 for read, 0x02 for write, 0x03 for read/write
  const char* dbName;  // "" for *, otherwise specific db name
  const char* name;
} SPrivInfo;

/*
 * The SPrivTblPolicy only applies to tables, such as READ/WRITE/DELETE TABLE.
 * It defines the row-level or table-level privileges for a table, maybe combined with columns and tag conditions.
 * It's recommended to decrease the number of SPrivPolicy considering the performance.
 */
typedef struct {
  SArray* cols;  // SColNameFlag, NULL means all columns, sorted by colId.
  char*   cond;  // NULL if no condition of columns or tags.
  int32_t condLen;
  int64_t updateUs;
} SPrivTblPolicy;

typedef struct {
  SArray* policy;  // element of SArray: SPrivTblPolicy, only support 1 element currently
} SPrivTblPolicies;

typedef struct {
  SPrivSet policy;
} SPrivObjPolicies;

typedef struct {
  SPrivSet* privSet;
  uint64_t  curPriv;
  int32_t   groupIndex;
} SPrivIter;

typedef struct {
  SPrivInfo* privInfo;
  int32_t    privInfoCnt;
  int32_t    index;
} SPrivInfoIter;

static FORCE_INLINE void privAddSet(SPrivSet* privSet1, SPrivSet* privSet2) {
  for (int32_t i = 0; i < PRIV_GROUP_CNT; ++i) {
    if (privSet2->set[i]) {
      privSet1->set[i] |= privSet2->set[i];
    }
  }
}

static FORCE_INLINE void privAddType(SPrivSet* privSet, EPrivType type) {
  if (type < 0 || type > MAX_PRIV_TYPE) return;
  privSet->set[PRIV_GROUP(type)] |= 1ULL << PRIV_OFFSET(type);
}

static FORCE_INLINE void privRemoveSet(SPrivSet* privSet1, const SPrivSet* privSet2) {
  for (int i = 0; i < PRIV_GROUP_CNT; i++) {
    privSet1->set[i] &= ~privSet2->set[i];
  }
}

static FORCE_INLINE void privRemoveType(SPrivSet* privSet, EPrivType type) {
  if (type < 0 || type > MAX_PRIV_TYPE) return;
  privSet->set[PRIV_GROUP(type)] &= ~(1ULL << PRIV_OFFSET(type));
}

static FORCE_INLINE bool privIsEmptySet(SPrivSet* privSet) {
  for (int i = 0; i < PRIV_GROUP_CNT; i++) {
    if (privSet->set[i] != 0) return false;
  }
  return true;
}

static FORCE_INLINE void privTblPolicyFree(void* policy) {
  taosMemoryFree(((SPrivTblPolicy*)policy)->cond);
  taosArrayDestroy(((SPrivTblPolicy*)policy)->cols);
}

static FORCE_INLINE void privTblPoliciesFree(void* pTblPolicies) {
  SPrivTblPolicies* tbPolicies = (SPrivTblPolicies*)pTblPolicies;
  taosArrayDestroyEx(tbPolicies->policy, privTblPolicyFree);
}

static FORCE_INLINE bool privDbKeyMatch(const char* key, const char* dbFName, int32_t len) {
  const char* p = strchr(key, '.');
  if (p) ++p;
  return p && strncmp(p, dbFName, len) == 0 && (p[len] == '.' || p[len] == '\0');
}

static FORCE_INLINE int32_t privPopCnt(SPrivSet* privSet) {
  int32_t count = 0;
  for (int32_t i = 0; i < PRIV_GROUP_CNT; ++i) {
    uint64_t chunk = privSet->set[i];
    while (chunk != 0) {
      chunk &= (chunk - 1);
      ++count;
    }
  }
  return count;
}

static FORCE_INLINE int32_t privTblPrivCnt(SHashObj* privTbls) {
  int32_t nPrivs = 0;
  void*   pIter = NULL;
  while ((pIter = taosHashIterate(privTbls, pIter))) {
    nPrivs += taosArrayGetSize(((SPrivTblPolicies*)pIter)->policy);
  }
  return nPrivs;
}

int32_t privCheckConflicts(const SPrivSet* privSet, EPrivCategory* pCategory, EPrivObjType* pObjType,
                           uint8_t* pObjLevel, EPrivType* conflict0, EPrivType* conflict1);
int32_t privExpandAll(SPrivSet* privSet, EPrivObjType pObjType, uint8_t pObjLevel);
int32_t privUpgradeRwDb(SHashObj* objPrivs, const char* dbFName, const char* tbName, uint8_t rwAttr);
void    privIterInit(SPrivIter* pIter, SPrivSet* privSet);
bool    privIterNext(SPrivIter* iter, SPrivInfo** ppPrivInfo);
void    privInfoIterInit(SPrivInfoIter* pIter);
bool    privInfoIterNext(SPrivInfoIter* iter, SPrivInfo** ppPrivInfo);

int32_t privTblPolicyCopy(SPrivTblPolicy* dest, SPrivTblPolicy* src);
int32_t privTblPoliciesAdd(SPrivTblPolicies* dest, SPrivTblPolicies* src, bool deepCopy, bool setUpdateTimeMax);
int32_t privTblPoliciesMerge(SPrivTblPolicies* dest, SPrivTblPolicies* src, bool updateWithLatest);

int32_t privObjKeyF(const SPrivInfo* pPrivInfo, const char* fname, const char* tb, char* buf, int32_t bufLen);
int32_t privObjKey(const SPrivInfo* pPrivInfo, int32_t acctId, const char* name, const char* tb, char* buf,
                   int32_t bufLen);
int32_t privObjKeyParse(const char* str, EPrivObjType* pObjType, char* db, int32_t dbLen, char* tb, int32_t tbLen,
                        bool fullDb);

const char*      privObjGetName(EPrivObjType objType);
int32_t          privObjGetLevel(EPrivObjType objType);
const char*      privInfoGetName(EPrivType privType);
const SPrivInfo* privInfoGet(EPrivType privType);
int32_t          getSysRoleType(const char* roleName);
bool             isPrivInheritName(const char* name);
bool             privHasObjPrivilege(SHashObj* privs, int32_t acctId, const char* objName, const char* tbName,
                                     const SPrivInfo* privInfo, bool recursive);
SPrivTblPolicy* privGetConstraintTblPrivileges(SHashObj* privs, int32_t acctId, const char* objName, const char* tbName,
                                               const SPrivInfo* privInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_PRIV_H_*/
