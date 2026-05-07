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

// extConnector.h  –  public API for the federated query external connector
//
// Location: include/libs/extconnector/extConnector.h
// Included by: include/libs/nodes/querynodes.h

#ifndef _TD_EXT_CONNECTOR_H_
#define _TD_EXT_CONNECTOR_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tcommon.h"  // SSDataBlock
#include "tmsg.h"   // EExtSourceType, TSDB_* length constants

// ---------------------------------------------------------------------------
// Forward declarations for types defined in other headers
// ---------------------------------------------------------------------------
typedef struct SExtTableNode         SExtTableNode;         // querynodes.h
typedef struct SFederatedScanPhysiNode SFederatedScanPhysiNode;  // plannodes.h
typedef struct SExtColTypeMapping    SExtColTypeMapping;    // plannodes.h

// ---------------------------------------------------------------------------
// Opaque handle types
// ---------------------------------------------------------------------------
typedef struct SExtConnectorHandle   SExtConnectorHandle;
typedef struct SExtQueryHandle       SExtQueryHandle;

// ---------------------------------------------------------------------------
// EExtSQLDialect  [DS §6.2.6, global-interface.md §1.6]
// ---------------------------------------------------------------------------
typedef enum EExtSQLDialect {
  EXT_SQL_DIALECT_MYSQL    = 0,
  EXT_SQL_DIALECT_POSTGRES = 1,
  EXT_SQL_DIALECT_INFLUXQL = 2,
} EExtSQLDialect;

// ---------------------------------------------------------------------------
// SExtSourceCapability  [DS §6.2.2]
// Filled by extConnectorGetCapabilities; stored in SExtTableNode.
// NOTE: The struct is defined in tmsg.h (included above) to avoid a circular
// include between extConnector.h and tmsg.h.
// (SExtSourceCapability is used by SExtSourceInfo in tmsg.h.)

// ---------------------------------------------------------------------------
// SExtConnectorError  [DS §5.3.11]
// Passed as an output parameter to exec/fetch functions.
// The Connector fills this on failure; the Executor formats it into
// pRequest->msgBuf so that taos_errstr() can surface the remote error.
// ---------------------------------------------------------------------------
typedef struct SExtConnectorError {
  int32_t tdCode;                             // TSDB_CODE_EXT_* mapped error code
  int8_t  sourceType;                         // EExtSourceType
  char    sourceName[TSDB_EXT_SOURCE_NAME_LEN];    // external source name
  int32_t remoteCode;                         // MySQL errno / gRPC status code
  char    remoteSqlstate[8];                  // PG SQLSTATE (5 chars + NUL); empty for others
  int32_t httpStatus;                         // InfluxDB HTTP status; 0 for non-HTTP sources
  char    remoteMessage[512];                 // raw remote error text
} SExtConnectorError;

// ---------------------------------------------------------------------------
// SExtColumnDef  [DS §6.2.6.6]
// ---------------------------------------------------------------------------
typedef struct SExtColumnDef {
  char colName[TSDB_COL_NAME_LEN];         // TDengine-side column name (may differ from remote)
  char remoteColName[TSDB_COL_NAME_LEN];   // original column name on the remote source; empty = same as colName
  char extTypeName[64];   // original type name from the external source
  bool nullable;
  bool isTag;             // InfluxDB only
  bool isPrimaryKey;      // true if this column maps to the TDengine primary key (timestamp)
} SExtColumnDef;

// ---------------------------------------------------------------------------
// SExtTableMeta  [DS §6.2.6.6]
// Returned by extConnectorGetTableSchema; caller frees via extConnectorFreeTableSchema.
// ---------------------------------------------------------------------------
typedef struct SExtTableMeta {
  SExtColumnDef *pCols;
  int32_t        numOfCols;
  int8_t         tableType;
  SName          name;                             // dbname + tname
  char           sourceName[TSDB_EXT_SOURCE_NAME_LEN];
  char           schemaName[TSDB_EXT_SOURCE_SCHEMA_LEN];
  int64_t        fetched_at;                       // monotonic time of cache fill
  char           remoteTableName[TSDB_TABLE_NAME_LEN]; // actual table name on remote (preserves original case)
} SExtTableMeta;

// ---------------------------------------------------------------------------
// SExtSourceCfg  [DS §6.2.6.2]
// Built from SGetExtSourceRsp; passed to extConnectorOpen.
// ---------------------------------------------------------------------------
typedef struct SExtSourceCfg {
  char           source_name[TSDB_EXT_SOURCE_NAME_LEN];
  EExtSourceType source_type;
  char           host[TSDB_EXT_SOURCE_HOST_LEN];
  int32_t        port;
  char           user[TSDB_EXT_SOURCE_USER_LEN];
  char           password[TSDB_EXT_SOURCE_PASSWORD_LEN];
  char           default_database[TSDB_EXT_SOURCE_DATABASE_LEN];
  char           default_schema[TSDB_EXT_SOURCE_SCHEMA_LEN];
  char           options[TSDB_EXT_SOURCE_OPTIONS_LEN];   // JSON string (key-value pairs)
  int64_t        meta_version;    // source meta version (for connection pool invalidation)
  // Per-source timeout overrides (0 = use global SExtConnectorModuleCfg default).
  // Populated by extConnector from options JSON keys connect_timeout_ms / read_timeout_ms.
  int32_t        conn_timeout_ms;
  int32_t        query_timeout_ms;
} SExtSourceCfg;

// ---------------------------------------------------------------------------
// SExtConnectorModuleCfg  [DS §6.2.6.1]
// Passed once to extConnectorModuleInit during taosd startup.
// ---------------------------------------------------------------------------
typedef struct SExtConnectorModuleCfg {
  int32_t max_pool_size_per_source;
  int32_t conn_timeout_ms;
  int32_t query_timeout_ms;
  int32_t idle_conn_ttl_s;
  int32_t thread_pool_size;
  int32_t probe_timeout_ms;
} SExtConnectorModuleCfg;

// ---------------------------------------------------------------------------
// External Connector API  [DS §6.1.2]
// ---------------------------------------------------------------------------

// Module lifecycle (called once at taosd startup / shutdown)
int32_t extConnectorModuleInit(const SExtConnectorModuleCfg *cfg);
void    extConnectorModuleDestroy(void);

// Connection handle lifecycle
int32_t extConnectorOpen(const SExtSourceCfg *cfg, SExtConnectorHandle **ppHandle);
void    extConnectorClose(SExtConnectorHandle *pHandle);

// Metadata
int32_t extConnectorGetTableSchema(SExtConnectorHandle *pHandle,
                                   const SExtTableNode  *pTable,
                                   SExtTableMeta       **ppOut);
void             extConnectorFreeTableSchema(SExtTableMeta *pMeta);
SExtTableMeta*   extConnectorCloneTableSchema(const SExtTableMeta *pMeta);

int32_t extConnectorGetCapabilities(SExtConnectorHandle  *pHandle,
                                    const SExtTableNode  *pTable,
                                    SExtSourceCapability *pOut);

// Namespace check (FS §3.5.7): verify that dbName/schemaName exists on the remote.
// dbName: database (MySQL/Influx) or schema (PG 2-seg).
// schemaName: schema name for PG 3-seg form; NULL for others.
// Returns TSDB_CODE_SUCCESS, TSDB_CODE_EXT_DB_NOT_EXIST, or TSDB_CODE_OPS_NOT_SUPPORT.
int32_t extConnectorCheckNamespace(SExtConnectorHandle *pHandle,
                                   const char          *dbName,
                                   const char          *schemaName);

// Query execution
// pSQL is optional: when non-NULL the Connector uses it directly instead of
// regenerating via nodesRemotePlanToSQL (which has no subquery resolve context).
// The Executor MUST pass the pre-generated SQL when pNode->pConditions contains
// REMOTE_VALUE_LIST nodes (IN subquery pushdown path).
int32_t extConnectorExecQuery(SExtConnectorHandle              *pHandle,
                              const SFederatedScanPhysiNode    *pNode,
                              const char                       *pSQL,
                              SExtQueryHandle                 **ppQHandle,
                              SExtConnectorError               *pOutErr);

int32_t extConnectorFetchBlock(SExtQueryHandle          *pQHandle,
                               const SExtColTypeMapping *pColMappings,
                               int32_t                   numColMappings,
                               SSDataBlock             **ppOut,
                               SExtConnectorError       *pOutErr);

void extConnectorCloseQuery(SExtQueryHandle *pQHandle);

// Pool management
// Remove and destroy the connection pool for the given source name.
// Called when DROP EXTERNAL SOURCE succeeds to release idle connections immediately.
void extConnectorPoolRemove(const char *sourceName);

// Fault tolerance
bool extConnectorIsRetryable(int32_t errCode);

#ifdef __cplusplus
}
#endif

#endif  // _TD_EXT_CONNECTOR_H_
