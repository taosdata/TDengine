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

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  // ==================== Legacy Privilege ====================
  PRIV_TYPE_ALL = 0,        // ALL PRIVILEGES
  PRIV_TYPE_READ = 1,       // READ PRIVILEGE
  PRIV_TYPE_WRITE = 2,      // WRITE PRIVILEGE
  PRIV_TYPE_SUBSCRIBE = 3,  // SUBSCRIBE PRIVILEGE
  PRIV_TYPE_ALTER = 4,      // ALTER PRIVILEGE
  // ==================== DB Privileges(5~49) ====================
  PRIV_DB_CREATE = 5,      // CREATE DATABASE
  PRIV_DB_ALTER,           // ALTER DATABASE
  PRIV_DB_DROP,            // DROP DATABASE
  PRIV_DB_USE,             // USE DATABASE
  PRIV_DB_FLUSH,           // FLUSH DATABASE
  PRIV_DB_COMPACT,         // COMPACT DATABASE
  PRIV_DB_TRIM,            // TRIM DATABASE
  PRIV_DB_ROLLUP,          // ROLLUP DATABASE
  PRIV_DB_SCAN,            // SCAN DATABASE
  PRIV_DB_SSMIGRATE = 15,  // SSMIGRATE DATABASE

  // VGroup Operations
  PRIV_VG_BALANCE = 25,    // BALANCE VGROUP
  PRIV_VG_BALANCE_LEADER,  // BALANCE VGROUP LEADER
  PRIV_VG_MERGER,          // MERGER VGROUP
  PRIV_VG_REDISTRIBUTE,    // REDISTRIBUTE VGROUP
  PRIV_VG_SPLIT = 29,      // SPLIT VGROUP

  // DB Show Operations
  PRIV_SHOW_DATABASES = 30,   // SHOW DATABASES
  PRIV_SHOW_VNODES,           // SHOW VNODES
  PRIV_SHOW_VGROUPS,          // SHOW VGROUPS
  PRIV_SHOW_COMPACTS,         // SHOW COMPACTS
  PRIV_SHOW_RETENTIONS,       // SHOW RETENTIONS
  PRIV_SHOW_SCANS,            // SHOW SCANS
  PRIV_SHOW_SSMIGRATES = 36,  // SHOW SSMIGRATES

  // ==================== Table Privileges(50-69)  ================
  PRIV_TBL_CREATE = 50,  // CREATE TABLE
  PRIV_TBL_DROP,         // DROP TABLE
  PRIV_TBL_ALTER,        // ALTER TABLE
  PRIV_TBL_SHOW,         // SHOW TABLES
  PRIV_TBL_SHOW_CREATE,  // SHOW CREATE TABLE
  PRIV_TBL_READ,         // READ TABLE(equivalent to SELECT TABLE)
  PRIV_TBL_WRITE,        // WRITE TABLE(equivalent to INSERT TABLE)
  PRIV_TBL_UPDATE,       // UPDATE TABLE(reserved)
  PRIV_TBL_DELETE = 58,  // DELETE TABLE

  // ==================== Other Privileges ================
  // function management
  PRIV_FUNC_CREATE = 70,  // CREATE FUNCTION
  PRIV_FUNC_DROP,         // DROP FUNCTION
  PRIV_FUNC_SHOW,         // SHOW FUNCTIONS

  // index management
  PRIV_IDX_CREATE = 73,  // CREATE INDEX
  PRIV_IDX_DROP,         //  DROP INDEX
  PRIV_IDX_SHOW,         //  SHOW INDEXES

  // view management
  PRIV_VIEW_CREATE = 76,  // CREATE VIEW
  PRIV_VIEW_DROP,         // DROP VIEW
  PRIV_VIEW_SHOW,         // SHOW VIEWS
  PRIV_VIEW_READ,         // READ VIEW

  // SMA management
  PRIV_RSMA_CREATE = 80,  // CREATE RSMA
  PRIV_RSMA_DROP,         // DROP RSMA
  PRIV_RSMA_ALTER,        // ALTER RSMA
  PRIV_RSMA_SHOW,         // SHOW RSMAS
  PRIV_RSMA_SHOW_CREATE,  // SHOW CREATE RSMA
  PRIV_TSMA_CREATE,       // CREATE TSMA
  PRIV_TSMA_DROP,         // DROP TSMA
  PRIV_TSMA_SHOW,         // SHOW TSMAS

  // mount management
  PRIV_MOUNT_CREATE = 90,  // CREATE MOUNT
  PRIV_MOUNT_DROP,         // DROP MOUNT
  PRIV_MOUNT_SHOW,         // SHOW MOUNTS

  // password management
  PRIV_PASS_ALTER = 93,  // ALTER PASS
  PRIV_PASS_ALTER_SELF,  // ALTER SELF PASS

  // role management
  PRIV_ROLE_CREATE = 100,  // CREATE ROLE
  PRIV_ROLE_DROP,          // DROP ROLE
  PRIV_ROLE_SHOW,          // SHOW ROLES

  // user management
  PRIV_USER_CREATE = 110,  // CREATE USER
  PRIV_USER_DROP,          // DROP USER
  PRIV_USER_SET_SECURITY,  // SET USER SECURITY INFO
  PRIV_USER_SET_AUDIT,     // SET USER AUDIT INFO
  PRIV_USER_SET_BASIC,     // SET USER BASIC INFO
  PRIV_USER_ENABLE,        // ENABLE USER
  PRIV_USER_DISABLE,       // DISABLE USER
  PRIV_USER_SHOW,          // SHOW USERS

  // audit management
  PRIV_AUDIT_DB_CREATE = 120,  // CREATE AUDIT DATABASE
  PRIV_AUDIT_DB_DROP,          // DROP AUDIT DATABASE
  PRIV_AUDIT_DB_ALTER,         // ALTER AUDIT DATABASE
  PRIV_AUDIT_DB_READ,          // READ AUDIT DATABASE
  PRIV_AUDIT_DB_WRITE,         // WRITE AUDIT DATABASE

  // token management
  PRIV_TOKEN_CREATE = 130,  // CREATE TOKEN
  PRIV_TOKEN_DROP,          // DROP TOKEN
  PRIV_TOKEN_ALTER,         // ALTER TOKEN
  PRIV_TOKEN_SHOW,          // SHOW TOKENS

  // key management
  PRIV_KEY_UPDATE = 140,  // UPDATE KEY
  PRIV_TOTP_CREATE,       // CREATE TOTP
  PRIV_TOTP_DROP,         // DROP TOTP
  PRIV_TOTP_UPDATE,       // UPDATE TOTP

  // grant/revoke privileges
  PRIV_GRANT_PRIVILEGE = 150,  // GRANT PRIVILEGE
  PRIV_REVOKE_PRIVILEGE,       // REVOKE PRIVILEGE
  PRIV_GRANT_SYSDBA,           // GRANT SYSDBA PRIVILEGE
  PRIV_REVOKE_SYSDBA,          // REVOKE SYSDBA PRIVILEGE
  PRIV_GRANT_SYSSEC,           // GRANT SYSSEC PRIVILEGE
  PRIV_REVOKE_SYSSEC,          // REVOKE SYSSEC PRIVILEGE
  PRIV_GRANT_SYSAUDIT,         // GRANT SYSAUDIT PRIVILEGE
  PRIV_REVOKE_SYSAUDIT,        // REVOKE SYSAUDIT PRIVILEGE

  // node management
  PRIV_NODE_CREATE = 160,  // CREATE NODE
  PRIV_NODE_DROP,          // DROP NODE
  PRIV_NODES_SHOW,         // SHOW NODES

  // system variables
  PRIV_VAR_SECURITY_ALTER = 190,  // ALTER SECURITY VARIABLE
  PRIV_VAR_AUDIT_ALTER,           // ALTER AUDIT VARIABLE
  PRIV_VAR_SYSTEM_ALTER,          // ALTER SYSTEM VARIABLE
  PRIV_VAR_DEBUG_ALTER,           // ALTER DEBUG VARIABLE
  PRIV_VAR_SECURITY_SHOW,         // SHOW SECURITY VARIABLES
  PRIV_VAR_AUDIT_SHOW,            // SHOW AUDIT VARIABLES
  PRIV_VAR_SYSTEM_SHOW,           // SHOW SYSTEM VARIABLES
  PRIV_VAR_DEBUG_SHOW,            // SHOW DEBUG VARIABLES

  // topic management
  PRIV_TOPIC_CREATE = 200,  // CREATE TOPIC
  PRIV_TOPIC_DROP,          // DROP TOPIC
  PRIV_TOPIC_SHOW,          // SHOW TOPICS
  PRIV_CONSUMER_SHOW,       // SHOW CONSUMERS
  PRIV_SUBSCRIPTION_SHOW,   // SHOW SUBSCRIPTIONS

  // stream management
  PRIV_STREAM_CREATE = 210,  // CREATE STREAM
  PRIV_STREAM_DROP,          // DROP STREAM
  PRIV_STREAM_SHOW,          // SHOW STREAMS
  PRIV_STREAM_START,         // START STREAM
  PRIV_STREAM_STOP,          // STOP STREAM
  PRIV_STREAM_RECALC,        // RECALC STREAM

  // system operation management
  PRIV_TRANS_SHOW = 220,  // SHOW TRANS
  PRIV_TRANS_KILL,        // KILL TRANS
  PRIV_CONNECTION_SHOW,   // SHOW CONNECTIONS
  PRIV_CONNECTION_KILL,   // KILL CONNECTION
  PRIV_QUERY_SHOW,        // SHOW QUERIES
  PRIV_QUERY_KILL,        // KILL QUERY

  // system info
  PRIV_INFO_SCHEMA_USE = 230,   // USE INFORMATION_SCHEMA
  PRIV_PERF_SCHEMA_USE,         // USE PERFORMANCE_SCHEMA
  PRIV_INFO_SCHEMA_READ_LIMIT,  // READ INFORMATION_SCHEMA LIMIT
  PRIV_INFO_SCHEMA_READ_SEC,    // READ INFORMATION_SCHEMA SECURITY
  PRIV_INFO_SCHEMA_READ_AUDIT,  // READ INFORMATION_SCHEMA AUDIT
  PRIV_INFO_SCHEMA_READ_BASIC,  // READ INFORMATION_SCHEMA BASIC
  PRIV_PERF_SCHEMA_READ_LIMIT,  // READ PERFORMANCE_SCHEMA LIMIT
  PRIV_PERF_SCHEMA_READ_BASIC,  // READ PERFORMANCE_SCHEMA BASIC
  PRIV_GRANTS_SHOW,             // SHOW GRANTS
  PRIV_CLUSTER_SHOW,            // SHOW CLUSTER
  PRIV_APPS_SHOW,               // SHOW APPS

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
#define PRIV_TYPE(type) \
  (SPrivSet) { .set[PRIV_GROUP(type)] = 1ULL << PRIV_OFFSET(type) }

#define PRIV_HAS(privSet, type) (((privSet)->set[PRIV_GROUP(type)] & (1ULL << PRIV_OFFSET(type))) != 0)

typedef enum {
  PRIV_CATEGORY_UNKNOWN = -1,
  PRIV_CATEGORY_SYSTEM,
  PRIV_CATEGORY_OBJECT,
  PRIV_CATEGORY_LEGACY,
  PRIV_CATEGORY_MAX,
} EPrivCategory;

typedef enum {
  OBJ_TYPE_UNKNOWN = -1,
  OBJ_TYPE_CLUSTER,
  OBJ_TYPE_NODE,
  OBJ_TYPE_DB,
  OBJ_TYPE_TABLE,
  OBJ_TYPE_FUNCTION,
  OBJ_TYPE_INDEX,
  OBJ_TYPE_VIEW,
  OBJ_TYPE_USER,
  OBJ_TYPE_ROLE,
  OBJ_TYPE_RSMA,
  OBJ_TYPE_TSMA,
  OBJ_TYPE_TOPIC,
  OBJ_TYPE_STREAM,
  OBJ_TYPE_MOUNT,
  OBJ_TYPE_AUDIT,
  OBJ_TYPE_TOKEN,
  OBJ_TYPE_MAX,
} EObjType;

typedef struct {
  EPrivType     privType;
  EPrivCategory category;
  EObjType      objType;
  const char*   name;
} SPrivInfo;

typedef struct {
  SPrivSet* privSet;
  uint64_t  curPriv;
  int32_t   groupIndex;
} SPrivIter;

static FORCE_INLINE SPrivSet privAdd(SPrivSet privSet1, SPrivSet privSet2) {
  SPrivSet merged = privSet1;
  for (int32_t i = 0; i < PRIV_GROUP_CNT; ++i) {
    if (privSet2.set[i]) {
      merged.set[i] |= privSet2.set[i];
    }
  }
  return merged;
}

int32_t checkPrivConflicts(const SPrivSet* privSet, EPrivCategory* pCategory, EObjType* pObjType);
void    privIterInit(SPrivIter* pIter, SPrivSet* privSet);
bool    privIterNext(SPrivIter* iter, SPrivInfo** ppPrivInfo);

int32_t privObjKey(EObjType objType, const char* db, const char* tb, char* buf, size_t bufLen);
int32_t privRowKey(ETableType tbType, const char* db, const char* tb, int64_t tsStart, int64_t tsEnd, char* buf,
                   size_t bufLen);
int32_t privColKey(ETableType tbType, const char* db, const char* tb, const char* col, char* buf, size_t bufLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_PRIV_H_*/
