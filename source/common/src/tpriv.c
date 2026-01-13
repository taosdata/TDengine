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
#include "tpriv.h"
#include "tmsg.h"
#include "tutil.h"

static TdThreadOnce privInit = PTHREAD_ONCE_INIT;

typedef struct {
  const char* name;
  int32_t     level;
} SPrivObjInfo;

static const SPrivObjInfo __privObjInfo[] = {
    {"CLUSTER", 0}, {"NODE", 0},  {"DATABASE", 0}, {"TABLE", 1}, {"FUNCTION", 0}, {"INDEX", 1},
    {"VIEW", 1},    {"USER", 0},  {"ROLE", 0},     {"RSMA", 1},  {"TSMA", 1},     {"TOPIC", 1},
    {"STREAM", 1},  {"MOUNT", 0}, {"AUDIT", 0},    {"TOKEN", 0},
};

/**
 * N.B. increase the macro PRIV_INFO_TABLE_VERSION for any update of privInfoTable
 */

#define SYS_ADMIN_BASIC_ROLES (T_ROLE_SYSDBA | T_ROLE_SYSSEC | T_ROLE_SYSAUDIT)
#define SYS_ADMIN_INFO1_ROLES (SYS_ADMIN_BASIC_ROLES | T_ROLE_SYSINFO_1)
#define SYS_ADMIN_INFO_ROLES  (SYS_ADMIN_BASIC_ROLES | T_ROLE_SYSINFO_0 | T_ROLE_SYSINFO_1)
#define SYS_ADMIN_ALL_ROLES   (SYS_ADMIN_BASIC_ROLES | T_ROLE_SYSAUDIT_LOG | T_ROLE_SYSINFO_0 | T_ROLE_SYSINFO_1)

static SPrivInfo privInfoTable[] = {
    // ==================== common privileges ====================
    {PRIV_CM_ALL, PRIV_CATEGORY_COMMON, 0, 0, 0, "ALL"},
    {PRIV_CM_ALTER, PRIV_CATEGORY_COMMON, 0, 0, 0, "ALTER"},
    {PRIV_CM_DROP, PRIV_CATEGORY_COMMON, 0, 0, 0, "DROP"},
    {PRIV_CM_SHOW, PRIV_CATEGORY_COMMON, 0, 0, 0, "SHOW"},
    {PRIV_CM_SHOW_CREATE, PRIV_CATEGORY_COMMON, 0, 0, 0, "SHOW CREATE"},
    {PRIV_CM_START, PRIV_CATEGORY_COMMON, 0, 0, 0, "START"},
    {PRIV_CM_STOP, PRIV_CATEGORY_COMMON, 0, 0, 0, "STOP"},
    {PRIV_CM_RECALC, PRIV_CATEGORY_COMMON, 0, 0, 0, "RECALCULATE"},
    {PRIV_CM_KILL, PRIV_CATEGORY_COMMON, 0, 0, 0, "KILL"},
    {PRIV_CM_SUBSCRIBE, PRIV_CATEGORY_COMMON, 0, 0, 0, "SUBSCRIBE"},

    // ==================== system privileges ====================
    // Database Management
    {PRIV_DB_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "CREATE DATABASE"},
    {PRIV_VG_BALANCE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "BALANCE VGROUP"},
    {PRIV_VG_BALANCE_LEADER, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "BALANCE VGROUP LEADER"},
    {PRIV_VG_MERGE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "MERGE VGROUP"},
    {PRIV_VG_REDISTRIBUTE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "REDISTRIBUTE VGROUP"},
    {PRIV_VG_SPLIT, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "SPLIT VGROUP"},

    // Function Privileges
    {PRIV_FUNC_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "CREATE FUNCTION"},
    {PRIV_FUNC_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "DROP FUNCTION"},
    {PRIV_FUNC_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW FUNCTIONS"},

    // Mount Privileges
    {PRIV_MOUNT_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "CREATE MOUNT"},
    {PRIV_MOUNT_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "DROP MOUNT"},
    {PRIV_MOUNT_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW MOUNTS"},

    // User Management
    {PRIV_USER_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "CREATE USER"},
    {PRIV_USER_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "DROP USER"},
    {PRIV_USER_SET_SECURITY, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSSEC, "SET USER SECURITY INFO"},
    {PRIV_USER_SET_AUDIT, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSAUDIT, "SET USER AUDIT INFO"},
    {PRIV_USER_SET_BASIC, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "SET USER BASIC INFO"},
    {PRIV_USER_UNLOCK, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "UNLOCK USER"},
    {PRIV_USER_LOCK, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "LOCK USER"},
    {PRIV_USER_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW USERS"},

    // Role Management
    {PRIV_ROLE_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "CREATE ROLE"},
    {PRIV_ROLE_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "DROP ROLE"},
    {PRIV_ROLE_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW ROLES"},

    // Token Privileges
    {PRIV_TOKEN_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSSEC, "CREATE TOKEN"},
    {PRIV_TOKEN_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSSEC, "DROP TOKEN"},
    {PRIV_TOKEN_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSSEC, "ALTER TOKEN"},
    {PRIV_TOKEN_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_BASIC_ROLES, "SHOW TOKENS"},

    // Node Management
    {PRIV_NODE_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "CREATE NODE"},
    {PRIV_NODE_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "DROP NODE"},
    {PRIV_NODES_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW NODES"},

    // System Variables
    {PRIV_VAR_SECURITY_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "ALTER SECURITY VARIABLE"},
    {PRIV_VAR_AUDIT_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "ALTER AUDIT VARIABLE"},
    {PRIV_VAR_SYSTEM_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "ALTER SYSTEM VARIABLE"},
    {PRIV_VAR_DEBUG_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "ALTER DEBUG VARIABLE"},
    {PRIV_VAR_SECURITY_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW SECURITY VARIABLE"},
    {PRIV_VAR_AUDIT_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW AUDIT VARIABLE"},
    {PRIV_VAR_SYSTEM_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW SYSTEM VARIABLE"},
    {PRIV_VAR_DEBUG_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW DEBUG VARIABLE"},

    // Key/Password Management
    {PRIV_KEY_UPDATE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "UPDATE KEY"},
    {PRIV_TOTP_CREATE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSSEC, "CREATE TOTP"},
    {PRIV_TOTP_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSSEC, "DROP TOTP"},
    {PRIV_PASS_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "ALTER PASS"},
    {PRIV_PASS_ALTER_SELF, PRIV_CATEGORY_SYSTEM, 0, 0, 0, "ALTER SELF PASS"},

    // Grant/Revoke Privileges
    {PRIV_GRANT_PRIVILEGE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSSEC, "GRANT PRIVILEGE"},
    {PRIV_REVOKE_PRIVILEGE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSSEC, "REVOKE PRIVILEGE"},
    {PRIV_SHOW_PRIVILEGES, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW PRIVILEGES"},
    {PRIV_GRANT_SYSDBA, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "GRANT SYSDBA PRIVILEGE"},
    {PRIV_REVOKE_SYSDBA, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "REVOKE SYSDBA PRIVILEGE"},
    {PRIV_GRANT_SYSSEC, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSSEC, "GRANT SYSSEC PRIVILEGE"},
    {PRIV_REVOKE_SYSSEC, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSSEC, "REVOKE SYSSEC PRIVILEGE"},
    {PRIV_GRANT_SYSAUDIT, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSAUDIT, "GRANT SYSAUDIT PRIVILEGE"},
    {PRIV_REVOKE_SYSAUDIT, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSAUDIT, "REVOKE SYSAUDIT PRIVILEGE"},

    // Audit Management
    {PRIV_AUDIT_DB_DROP, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "DROP AUDIT DATABASE"},
    {PRIV_AUDIT_DB_ALTER, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "ALTER AUDIT DATABASE"},
    {PRIV_AUDIT_DB_USE, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSAUDIT | T_ROLE_SYSAUDIT_LOG, "USE AUDIT DATABASE"},

    // System Administration
    {PRIV_TRANS_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW TRANS"},
    {PRIV_TRANS_KILL, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "KILL TRANS"},
    {PRIV_CONN_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW CONNECTIONS"},
    {PRIV_CONN_KILL, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "KILL CONNECTION"},
    {PRIV_QUERY_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW QUERIES"},
    {PRIV_QUERY_KILL, PRIV_CATEGORY_SYSTEM, 0, 0, T_ROLE_SYSDBA, "KILL QUERY"},
    {PRIV_GRANTS_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW GRANTS"},
    {PRIV_CLUSTER_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO1_ROLES, "SHOW CLUSTER"},
    {PRIV_APPS_SHOW, PRIV_CATEGORY_SYSTEM, 0, 0, SYS_ADMIN_INFO_ROLES, "SHOW APPS"},

    // ==================== object privileges ====================
    // Database Privileges
    {PRIV_CM_ALTER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA, "ALTER DATABASE"},
    {PRIV_CM_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA, "DROP DATABASE"},
    {PRIV_DB_USE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA | T_ROLE_SYSSEC, "USE DATABASE"},
    {PRIV_DB_FLUSH, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA, "FLUSH DATABASE"},
    {PRIV_DB_COMPACT, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA, "COMPACT DATABASE"},
    {PRIV_DB_TRIM, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA, "TRIM DATABASE"},
    {PRIV_DB_ROLLUP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA, "ROLLUP DATABASE"},
    {PRIV_DB_SCAN, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA, "SCAN DATABASE"},
    {PRIV_DB_SSMIGRATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA, "SSMIGRATE DATABASE"},
    {PRIV_CM_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO_ROLES, "SHOW DATABASES"},
    {PRIV_CM_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO_ROLES, "SHOW CREATE DATABASE"},
    {PRIV_SHOW_VNODES, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW VNODES"},
    {PRIV_SHOW_VGROUPS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW VGROUPS"},
    {PRIV_SHOW_COMPACTS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW COMPACTS"},
    {PRIV_SHOW_RETENTIONS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW RETENTIONS"},
    {PRIV_SHOW_SCANS, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW SCANS"},
    {PRIV_SHOW_SSMIGRATES, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, SYS_ADMIN_INFO1_ROLES, "SHOW SSMIGRATES"},

    // Table Privileges
    {PRIV_TBL_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA | T_ROLE_SYSAUDIT_LOG, "CREATE TABLE"},
    {PRIV_CM_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TBL, 1, T_ROLE_SYSDBA, "DROP TABLE"},
    {PRIV_CM_ALTER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TBL, 1, T_ROLE_SYSDBA, "ALTER TABLE"},
    {PRIV_CM_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TBL, 1, SYS_ADMIN_INFO_ROLES, "SHOW TABLES"},
    {PRIV_CM_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TBL, 1, SYS_ADMIN_INFO_ROLES, "SHOW CREATE TABLE"},
    {PRIV_TBL_SELECT, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TBL, 1, T_ROLE_SYSAUDIT, "SELECT"},
    {PRIV_TBL_INSERT, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TBL, 1, T_ROLE_SYSAUDIT_LOG, "INSERT"},
    {PRIV_TBL_UPDATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TBL, 1, 0, "UPDATE"},
    {PRIV_TBL_DELETE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TBL, 1, 0, "DELETE"},

    // Index Privileges
    {PRIV_IDX_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TBL, 1, T_ROLE_SYSDBA, "CREATE INDEX"},
    {PRIV_CM_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_IDX, 1, T_ROLE_SYSDBA, "DROP INDEX"},
    {PRIV_CM_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_IDX, 1, SYS_ADMIN_INFO_ROLES, "SHOW INDEXES"},
    {PRIV_CM_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_IDX, 1, SYS_ADMIN_INFO_ROLES, "SHOW CREATE INDEX"},

    // RSMA Privileges
    {PRIV_RSMA_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TBL, 1, T_ROLE_SYSDBA, "CREATE RSMA"},
    {PRIV_CM_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, 1, T_ROLE_SYSDBA, "DROP RSMA"},
    {PRIV_CM_ALTER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, 1, T_ROLE_SYSDBA, "ALTER RSMA"},
    {PRIV_CM_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, 1, SYS_ADMIN_INFO_ROLES, "SHOW RSMAS"},
    {PRIV_CM_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_RSMA, 1, SYS_ADMIN_INFO_ROLES, "SHOW CREATE RSMA"},

    // TSMA Privileges
    {PRIV_TSMA_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TBL, 1, T_ROLE_SYSDBA, "CREATE TSMA"},
    {PRIV_CM_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TSMA, 1, T_ROLE_SYSDBA, "DROP TSMA"},
    {PRIV_CM_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TSMA, 1, SYS_ADMIN_INFO_ROLES, "SHOW TSMAS"},
    {PRIV_CM_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TSMA, 1, SYS_ADMIN_INFO_ROLES, "SHOW CREATE TSMA"},

    // View Privileges
    {PRIV_VIEW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA, "CREATE VIEW"},
    {PRIV_CM_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, 1, T_ROLE_SYSDBA, "DROP VIEW"},
    {PRIV_CM_ALTER, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, 1, T_ROLE_SYSDBA, "ALTER VIEW"},
    {PRIV_CM_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, 1, SYS_ADMIN_INFO_ROLES, "SHOW VIEWS"},
    {PRIV_CM_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, 1, SYS_ADMIN_INFO_ROLES, "SHOW CREATE VIEW"},
    {PRIV_VIEW_SELECT, PRIV_CATEGORY_OBJECT, PRIV_OBJ_VIEW, 1, 0, "SELECT VIEW"},

    // Topic Privileges
    {PRIV_TOPIC_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA, "CREATE TOPIC"},
    {PRIV_CM_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, 1, T_ROLE_SYSDBA, "DROP TOPIC"},
    {PRIV_CM_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, 1, SYS_ADMIN_INFO_ROLES, "SHOW TOPICS"},
    {PRIV_CM_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, 1, SYS_ADMIN_INFO_ROLES, "SHOW CREATE TOPIC"},
    {PRIV_CM_SUBSCRIBE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, 1, 0, "SUBSCRIBE TOPIC"},
    {PRIV_CONSUMER_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, 1, SYS_ADMIN_INFO_ROLES, "SHOW CONSUMERS"},
    {PRIV_SUBSCRIPTION_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_TOPIC, 1, SYS_ADMIN_INFO_ROLES, "SHOW SUBSCRIPTIONS"},

    // Stream Privileges
    {PRIV_STREAM_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_DB, 0, T_ROLE_SYSDBA, "CREATE STREAM"},
    {PRIV_CM_DROP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, 1, T_ROLE_SYSDBA, "DROP STREAM"},
    {PRIV_CM_SHOW, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, 1, SYS_ADMIN_INFO_ROLES, "SHOW STREAMS"},
    {PRIV_CM_SHOW_CREATE, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, 1, SYS_ADMIN_INFO_ROLES, "SHOW CREATE STREAM"},
    {PRIV_CM_START, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, 1, T_ROLE_SYSDBA, "START STREAM"},
    {PRIV_CM_STOP, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, 1, T_ROLE_SYSDBA, "STOP STREAM"},
    {PRIV_CM_RECALC, PRIV_CATEGORY_OBJECT, PRIV_OBJ_STREAM, 1, T_ROLE_SYSDBA, "RECALC STREAM"},
};

static SPrivInfo* privLookup[MAX_PRIV_TYPE + 1] = {0};

static void initPrivLookup(void) {
  for (size_t i = 0; i < sizeof(privInfoTable) / sizeof(privInfoTable[0]); ++i) {
    EPrivType privType = privInfoTable[i].privType;
    if (privType <= MAX_PRIV_TYPE &&
        !privLookup[privType]) {  // duplicated items for other purpose, e.g. createDefaultRoles.
      privLookup[privType] = &privInfoTable[i];
    }
  }
}

int32_t privExpandAll(SPrivSet* privSet, EPrivObjType pObjType, uint8_t pObjLevel) {
  (void)taosThreadOnce(&privInit, initPrivLookup);

  if (!privSet) return TSDB_CODE_APP_ERROR;
  if (!PRIV_HAS(privSet, PRIV_CM_ALL)) return TSDB_CODE_SUCCESS;

  SPrivInfoIter iter = {0};
  privInfoIterInit(&iter);

  SPrivInfo* pPrivInfo = NULL;
  while (privInfoIterNext(&iter, &pPrivInfo)) {
    if (pPrivInfo->objType != pObjType) continue;
    if (pPrivInfo->objLevel != pObjLevel) {
      uWarn("privExpandAll: skip privType %d due to level mismatch, expected %d vs %d", pPrivInfo->privType, pObjLevel,
            pPrivInfo->objLevel);
      continue;
    }
    if (pPrivInfo->category != PRIV_CATEGORY_OBJECT) {
      uWarn("privExpandAll: skip privType %d due to category mismatch, expected OBJECT vs %d", pPrivInfo->privType,
            pPrivInfo->category);
      continue;
    }
    privAddType(privSet, pPrivInfo->privType);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t privCheckConflicts(const SPrivSet* privSet, EPrivCategory* pCategory, EPrivObjType* pObjType,
                           uint8_t* pObjLevel, EPrivType* conflict0, EPrivType* conflict1) {
  (void)taosThreadOnce(&privInit, initPrivLookup);

  if (!privSet) return TSDB_CODE_APP_ERROR;

  int32_t          code = 0;
  const SPrivInfo* privInfo = NULL;

  bool hasSystemPriv = false;
  bool hasObjectPriv = false;
  bool hasCommonPriv = false;

  EPrivType lastPriv = PRIV_TYPE_UNKNOWN;

  for (int32_t i = 0; i < PRIV_GROUP_CNT; ++i) {
    uint64_t chunk = privSet->set[i];
    if (chunk == 0) continue;

    while (chunk != 0) {
      int32_t   bitPos = BUILDIN_CTZL(chunk);
      EPrivType privType = (i << 6) + bitPos;
      chunk &= ~(1ULL << bitPos);

      privInfo = privLookup[privType];
      if (privInfo == NULL) continue;

      switch (privInfo->category) {
        case PRIV_CATEGORY_SYSTEM: {
          if (hasObjectPriv || hasCommonPriv) {
            code = 1;
            goto _exit;
          }
          hasSystemPriv = true;
          lastPriv = privInfo->privType;
          break;
        }
        case PRIV_CATEGORY_OBJECT: {
          if (hasSystemPriv) {
            code = 1;
            goto _exit;
          }
          if (*pObjType != privInfo->objType) {
            code = 2;
            goto _exit;
          }
          if (*pObjLevel != privInfo->objLevel) {
            code = 3;
            goto _exit;
          }
          hasObjectPriv = true;
          lastPriv = privInfo->privType;
          break;
        }
        case PRIV_CATEGORY_COMMON: {
          if (hasSystemPriv) {
            code = 1;
            goto _exit;
          }
          hasCommonPriv = true;
          lastPriv = privInfo->privType;
          break;
        }
        default:
          break;
      }
    }
  }

_exit:
  if (code != 0) {
    if (conflict0) *conflict0 = lastPriv;
    if (conflict1 && privInfo) *conflict1 = privInfo->privType;
    return code;
  }

  if (pCategory) {
    *pCategory = hasSystemPriv   ? PRIV_CATEGORY_SYSTEM
                 : hasObjectPriv ? PRIV_CATEGORY_OBJECT
                 : hasCommonPriv ? PRIV_CATEGORY_COMMON
                                 : PRIV_CATEGORY_UNKNOWN;
  }

  return 0;
}

void privIterInit(SPrivIter* pIter, SPrivSet* privSet) {
  (void)taosThreadOnce(&privInit, initPrivLookup);
  pIter->privSet = privSet;
  pIter->groupIndex = 0;
  pIter->curPriv = privSet->set[0];
}

bool privIterNext(SPrivIter* iter, SPrivInfo** ppPrivInfo) {
_loop:
  while (iter->curPriv == 0) {
    if (++iter->groupIndex >= PRIV_GROUP_CNT) {
      return false;
    }
    iter->curPriv = iter->privSet->set[iter->groupIndex];
  }
  int32_t   bitPos = BUILDIN_CTZL(iter->curPriv);
  EPrivType privType = (iter->groupIndex << 6) + bitPos;
  iter->curPriv &= ~(1ULL << bitPos);
  if (ppPrivInfo) {
    *ppPrivInfo = privLookup[privType];
    if (!(*ppPrivInfo)) goto _loop;
  }
  return true;
}

void privInfoIterInit(SPrivInfoIter* pIter) {
  pIter->privInfo = privInfoTable;
  pIter->privInfoCnt = sizeof(privInfoTable) / sizeof(privInfoTable[0]);
  pIter->index = 0;
}

bool privInfoIterNext(SPrivInfoIter* iter, SPrivInfo** ppPrivInfo) {
  if (iter->index < iter->privInfoCnt) {
    if (ppPrivInfo) {
      *ppPrivInfo = &iter->privInfo[iter->index++];
    } else {
      iter->index++;
    }
    return true;
  }

  return false;
}
// objType.1.db.tb or objType.1.db
int32_t privObjKeyF(const SPrivInfo* pPrivInfo, const char* fname, const char* tb, char* buf, int32_t bufLen) {
  return (pPrivInfo->objLevel == 0)
             ? snprintf(buf, bufLen, "%d.%s", pPrivInfo->objType, fname ? fname : "")
             : snprintf(buf, bufLen, "%d.%s.%s", pPrivInfo->objType, fname ? fname : "", tb ? tb : "");
}

int32_t privObjKey(const SPrivInfo* pPrivInfo, int32_t acctId, const char* name, const char* tb, char* buf,
                   int32_t bufLen) {
  return (pPrivInfo->objLevel == 0)
             ? snprintf(buf, bufLen, "%d.%d.%s", pPrivInfo->objType, acctId, name ? name : "")
             : snprintf(buf, bufLen, "%d.%d.%s.%s", pPrivInfo->objType, acctId, name ? name : "", tb ? tb : "");
}

// objType.1.db or objType.1.db.tb
int32_t privObjKeyParse(const char* str, EPrivObjType* pObjType, char* db, int32_t dbLen, char* tb, int32_t tbLen,
                        bool fullDb) {
  char* p = strchr(str, '.');
  if (!p) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  *pObjType = taosStr2Int32(str, NULL, 10);
  if (errno == ERANGE || *pObjType < PRIV_OBJ_CLUSTER || *pObjType >= PRIV_OBJ_MAX) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  char* pNext = strchr(p + 1, '.');
  if (!pNext) {
    return TSDB_CODE_INVALID_DATA_FMT;
  }
  char* qNext = strchr(pNext + 1, '.');
  if (qNext) {
    size_t dbLength = qNext - (fullDb ? (p + 1) : (pNext + 1));
    if (dbLength >= (size_t)dbLen) {
      return TSDB_CODE_INVALID_DATA_FMT;
    }
    strncpy(db, fullDb ? (p + 1) : (pNext + 1), dbLength);
    db[dbLength] = '\0';
    strncpy(tb, qNext + 1, tbLen);
  } else {
    strcpy(db, fullDb ? (p + 1) : (pNext + 1));
    tb[0] = '\0';
  }
  return TSDB_CODE_SUCCESS;
}

int32_t privTblKey(const char* db, const char* tb, char* buf, int32_t bufLen) {
  return snprintf(buf, bufLen, "%s.%s", db ? db : "", tb ? tb : "");
}

const char* privObjGetName(EPrivObjType objType) {
  if (objType < PRIV_OBJ_CLUSTER || objType >= PRIV_OBJ_MAX) {
    return "UNKNOWN";
  }
  return __privObjInfo[objType].name;
}

int32_t privObjGetLevel(EPrivObjType objType) {
  if (objType < PRIV_OBJ_CLUSTER || objType >= PRIV_OBJ_MAX) {
    return -1;
  }
  return __privObjInfo[objType].level;
}

int32_t getSysRoleType(const char* roleName) {
  if (strcmp(roleName, TSDB_ROLE_SYSDBA) == 0) {
    return T_ROLE_SYSDBA;
  } else if (strcmp(roleName, TSDB_ROLE_SYSSEC) == 0) {
    return T_ROLE_SYSSEC;
  } else if (strcmp(roleName, TSDB_ROLE_SYSAUDIT) == 0) {
    return T_ROLE_SYSAUDIT;
  } else if (strcmp(roleName, TSDB_ROLE_SYSAUDIT_LOG) == 0) {
    return T_ROLE_SYSAUDIT_LOG;
  } else if (strcmp(roleName, TSDB_ROLE_SYSINFO_0) == 0) {
    return T_ROLE_SYSINFO_0;
  } else if (strcmp(roleName, TSDB_ROLE_SYSINFO_1) == 0) {
    return T_ROLE_SYSINFO_1;
  }
  return 0;
}

bool isPrivInheritName(const char* name) {
  if (name && (name[0] == 'S' || name[0] == 's')) {
    if (taosStrncasecmp(name, TSDB_ROLE_SYSDBA, 7) == 0 || taosStrncasecmp(name, TSDB_ROLE_SYSSEC, 7) == 0 ||
        taosStrncasecmp(name, TSDB_ROLE_SYSAUDIT, 9) == 0 || taosStrncasecmp(name, TSDB_ROLE_SYSAUDIT_LOG, 13) == 0 ||
        taosStrncasecmp(name, TSDB_ROLE_SYSINFO_0, 10) == 0 || taosStrncasecmp(name, TSDB_ROLE_SYSINFO_1, 10) == 0) {
      return true;
    }
  }
  return false;
}

int32_t privTblPolicyCopy(SPrivTblPolicy* dest, SPrivTblPolicy* src) {
  dest->updateUs = src->updateUs;
  dest->condLen = src->condLen;
  if (src->cond) {
    if (!(dest->cond = taosStrdup(src->cond))) {
      return terrno;
    }
  } else {
    dest->cond = NULL;
  }

  if (taosArrayGetSize(src->cols) > 0) {
    dest->cols = taosArrayDup(src->cols, NULL);
    if (!dest->cols) return terrno;
  } else {
    dest->cols = NULL;
  }
  return 0;
}

int32_t privTblPoliciesAdd(SPrivTblPolicies* dest, SPrivTblPolicies* src, bool deepCopy, bool setUpdateTimeMax) {
  int32_t code = 0, lino = 0;
  if (deepCopy) {
    if (taosArrayGetSize(src->policy) == 0) {
      dest->policy = NULL;
      goto _exit;
    }
    if (!dest->policy) {
      dest->policy = taosArrayInit(TARRAY_SIZE(src->policy), sizeof(SPrivTblPolicy));
      if (!dest->policy) {
        TAOS_CHECK_EXIT(terrno);
      }
    }
    for (int32_t j = 0; j < TARRAY_SIZE(src->policy); ++j) {
      SPrivTblPolicy* pSrcPolicy = (SPrivTblPolicy*)TARRAY_GET_ELEM(src->policy, j);
      SPrivTblPolicy  destPolicy = {0};
      if ((code = privTblPolicyCopy(&destPolicy, pSrcPolicy))) {
        privTblPolicyFree(&destPolicy);
        TAOS_CHECK_EXIT(code);
      }
      if (setUpdateTimeMax) destPolicy.updateUs = INT64_MAX;
      if (!taosArrayPush(dest->policy, &destPolicy)) {
        privTblPolicyFree(&destPolicy);
        TAOS_CHECK_EXIT(terrno);
      }
    }
  } else {
    dest->policy = src->policy;
  }
_exit:
  return code;
}

int32_t privTblPoliciesMerge(SPrivTblPolicies* dest, SPrivTblPolicies* src, bool updateWithLatest) {
  int32_t code = 0, lino = 0;
  if (taosArrayGetSize(src->policy) == 0) {
    dest->policy = NULL;
    goto _exit;
  }
  if (!dest->policy) {
    dest->policy = taosArrayInit(TARRAY_SIZE(src->policy), sizeof(SPrivTblPolicy));
    if (!dest->policy) {
      TAOS_CHECK_EXIT(terrno);
    }
  }
  // only 1 element exist in the table policy array currently
  for (int32_t j = 0; j < TARRAY_SIZE(src->policy); ++j) {
    SPrivTblPolicy* pSrcPolicy = (SPrivTblPolicy*)TARRAY_GET_ELEM(src->policy, j);
    SPrivTblPolicy* pDestPolicy = taosArrayGet(dest->policy, 0);

    bool needAdd = false;
    if (!pDestPolicy) {
      needAdd = true;
    } else if (updateWithLatest && (pSrcPolicy->updateUs > pDestPolicy->updateUs)) {
      taosArrayClearEx(dest->policy, privTblPolicyFree);
      needAdd = true;
    }
    if (!needAdd) {
      break;
    }

    SPrivTblPolicy destPolicy = {0};
    if ((code = privTblPolicyCopy(&destPolicy, pSrcPolicy))) {
      privTblPolicyFree(&destPolicy);
      TAOS_CHECK_EXIT(code);
    }
    if (!taosArrayPush(dest->policy, &destPolicy)) {
      privTblPolicyFree(&destPolicy);
      TAOS_CHECK_EXIT(terrno);
    }
  }
_exit:
  return code;
}

bool privHasObjPrivilege(SHashObj* privs, int32_t acctId, const char* objName, const char* tbName,
                         const SPrivInfo* privInfo, bool recursive) {
#if 0  // debug info, remove when release
  uInfo("--------------------------------");
  uInfo("%s:%d check db:%s tb:%s, privType:%d, privObj:%d, privLevel:%d, privName:%s", __func__, __LINE__,
         objName ? objName : "", tbName ? tbName : "", privInfo->privType, privInfo->objType, privInfo->objLevel,
         privInfoGetName(privInfo->privType));
  SPrivObjPolicies* pp = NULL;
  while ((pp = taosHashIterate(privs, pp))) {
    char* pKey = taosHashGetKey(pp, NULL);
    uInfo("%s:%d key is %s", __func__, __LINE__, pKey);

    SPrivIter privIter = {0};
    privIterInit(&privIter, &pp->policy);
    SPrivInfo* pPrivInfoIter = NULL;
    while (privIterNext(&privIter, &pPrivInfoIter)) {
      uInfo("    has privType:%d, privObj:%d, privLevel:%d, privName:%s", pPrivInfoIter->privType,
             pPrivInfoIter->objType, pPrivInfoIter->objLevel, privInfoGetName(pPrivInfoIter->privType));
    }
  }
#endif
  if (tbName != NULL) {
    if (privInfo->objLevel == 0 || privInfo->objType <= PRIV_OBJ_DB) {
      uError("invalid privilege info for table level check, privType:%d, objType:%d, objLevel:%d\n", privInfo->privType,
             privInfo->objType, privInfo->objLevel);
    }
  }

  if (taosHashGetSize(privs) == 0) return false;

  const char* pObjName = objName;
  const char* pTbName = tbName;
  char        key[TSDB_PRIV_MAX_KEY_LEN] = {0};
  int32_t     klen = 0;

_retry:
  klen = privObjKey(privInfo, acctId, pObjName, pTbName, key, sizeof(key));

  SPrivObjPolicies* policies = taosHashGet(privs, key, klen + 1);
  if (policies && PRIV_HAS(&policies->policy, privInfo->privType)) {
#if 0  // debug info, remove when release
    uInfo("%s:%d check db:%s tb:%s, privType:%d, privObj:%d, privLevel:%d, privName:%s, key:%s TRUE", __func__, __LINE__,
           objName ? objName : "", tbName ? tbName : "", privInfo->privType, privInfo->objType, privInfo->objLevel,
           privInfoGetName(privInfo->privType), key);
#endif
    return true;
  }

  if (!recursive) goto _exit;

  if (pTbName && !(pTbName[0] == '*' && pTbName[1] == 0)) {
    pTbName = "*";
    goto _retry;
  }
  if (pObjName && !(pObjName[0] == '*' && pObjName[1] == 0)) {
    pObjName = "*";
    goto _retry;
  }
_exit:
#if 0  // debug info, remove when release
  uInfo("%s:%d check db:%s tb:%s, privType:%d, privObj:%d, privLevel:%d, privName:%s, key:%s, FALSE", __func__, __LINE__,
         objName ? objName : "", tbName ? tbName : "", privInfo->privType, privInfo->objType, privInfo->objLevel,
         privInfoGetName(privInfo->privType), key);
#endif
  return false;
}

SPrivTblPolicy* privGetConstraintTblPrivileges(SHashObj* privs, int32_t acctId, const char* objName, const char* tbName,
                                               SPrivInfo* privInfo) {
#if 0  // debug info, remove when release
  SPrivObjPolicies* pp = NULL;
  while ((pp = taosHashIterate(privs, pp))) {
    char* pKey = taosHashGetKey(pp, NULL);
    printf("%s:%d key is %s\n", __func__, __LINE__, pKey);
  }
#endif

  if (taosHashGetSize(privs) == 0) return NULL;

  const char* pObjName = objName;
  const char* pTbName = tbName;
  char        key[TSDB_PRIV_MAX_KEY_LEN] = {0};
  int32_t     klen = 0;

  klen = privObjKey(privInfo, acctId, pObjName, pTbName, key, sizeof(key));

  SPrivTblPolicies* policies = taosHashGet(privs, key, klen + 1);
  if (policies) {
    return (SPrivTblPolicy*)taosArrayGet(policies->policy, 0);
  }

  return NULL;
}

const SPrivInfo* privInfoGet(EPrivType privType) {
  (void)taosThreadOnce(&privInit, initPrivLookup);
  SPrivInfo* result = (privType >= 0 && privType <= MAX_PRIV_TYPE) ? privLookup[privType] : NULL;
  if (!result) terrno = TSDB_CODE_APP_ERROR;
  return result;
}

const char* privInfoGetName(EPrivType privType) {
  const SPrivInfo* privInfo = privInfoGet(privType);
  return privInfo ? privInfo->name : "privUnkown";
}