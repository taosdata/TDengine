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

// extConnectorInt.h — module-internal types for the External Connector
//
// Location: source/libs/extconnector/inc/extConnectorInt.h
// Included by: enterprise connector source files only; NOT exported in the public API.

#ifndef _TD_EXT_CONNECTOR_INT_H_
#define _TD_EXT_CONNECTOR_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "extConnector.h"
#include "plannodes.h"
#include "taos.h"
#include "taoserror.h"
#include "tdef.h"
#include "thash.h"
#include "tlog.h"
#include "tmsg.h"
#include "tthread.h"
#include "ttime.h"

// ============================================================
// Provider SPI (Strategy pattern — DS §4.2 + §6.1.2)
// ============================================================

// Forward declarations for SExtProvider callback parameters
typedef struct SExtProvider SExtProvider;

typedef struct SExtProvider {
  EExtSourceType type;
  const char    *name;  // "mysql" / "postgresql" / "influxdb"

  // Connection management (DS §6.1.2.2)
  // password in cfg is AES-encrypted; providers must decrypt before connecting
  int32_t (*connect)(const SExtSourceCfg *cfg, void **ppConn);
  void    (*disconnect)(void *pConn);
  bool    (*isAlive)(void *pConn);  // probe conn before reuse; NULL = skip probe

  // Metadata (DS §6.1.2.3) — keep extTypeName as raw external type name
  int32_t (*getTableSchema)(void *pConn, const SExtTableNode *pTable, SExtTableMeta **ppOut);
  int32_t (*getCapabilities)(void *pConn, const SExtTableNode *pTable,
                             SExtSourceCapability *pOut);

  // Query execution (DS §6.1.2.4)
  int32_t (*execQuery)(void *pConn, const char *sql, void **ppResult);
  int32_t (*fetchBlock)(void *pResult, const SExtColTypeMapping *pColMappings,
                        int32_t numColMappings, SSDataBlock **ppOut);
  void    (*closeResult)(void *pResult);

  // Error mapping (DS §5.3.11): fills pOutErr from native driver state
  int32_t (*mapError)(void *pConn, SExtConnectorError *pOutErr);
} SExtProvider;

// EXT_SOURCE_TYPE_COUNT — must match the EExtSourceType enum in tmsg.h
#define EXT_SOURCE_TYPE_COUNT  4   // MYSQL=0, POSTGRESQL=1, INFLUXDB=2, TDENGINE=3(reserved)

// Global provider table (indexed by EExtSourceType)
extern SExtProvider gExtProviders[EXT_SOURCE_TYPE_COUNT];

// ============================================================
// Connection pool entry
// ============================================================

typedef struct SExtPoolEntry {
  void    *pConn;           // native connection handle (MYSQL* / PGconn* / SInfluxConn*)
  int64_t  lastActiveTime;  // timestamp (ms) of last use
  bool     inUse;           // true = currently held by a Handle
  bool     drainOnReturn;   // true = disconnect when returned (config changed)
} SExtPoolEntry;

// ============================================================
// Per-source connection pool
// ============================================================

typedef struct SExtConnPool {
  char           sourceName[TSDB_TABLE_NAME_LEN];
  SExtSourceCfg  cfg;           // deep copy of the source config (password = AES-encrypted)
  int64_t        cfgVersion;    // meta_version at last pool update
  SExtProvider  *pProvider;     // pointer into gExtProviders[]
  SExtPoolEntry *entries;       // connection array
  int32_t        poolSize;      // current number of entries
  int32_t        maxPoolSize;   // max pool size from module cfg
  TdThreadMutex  mutex;
} SExtConnPool;

// ============================================================
// Opaque handle types (declared in extConnector.h, defined here)
// ============================================================

struct SExtConnectorHandle {
  SExtConnPool  *pPool;
  SExtPoolEntry *pEntry;
};

struct SExtQueryHandle {
  SExtConnectorHandle *pConnHandle;
  void                *pResult;    // native result set handle
  SExtProvider        *pProvider;
  bool                 eof;
};

// ============================================================
// Internal helpers
// ============================================================

// Password decrypt (AES-128-CBC with fixed enterprise key)
void extDecryptPassword(const char *cipherBuf, char *outPlain, int32_t outLen);

// Helper: remove entry at index i (compact the array)
void extConnPoolRemoveEntry(SExtConnPool *pPool, int32_t idx);

// Helper: get entry index from pointer
int32_t extConnPoolEntryIndex(const SExtConnPool *pPool, const SExtPoolEntry *pEntry);

// Helper: append a new entry to pool (already initialised pConn)
SExtPoolEntry *extConnPoolAppendEntry(SExtConnPool *pPool, void *pConn);

// Helper: dialect from source type
EExtSQLDialect extDialectFromSourceType(EExtSourceType srcType);

// ============================================================
// Provider forward declarations (implemented per-provider)
// ============================================================

#ifdef TD_ENTERPRISE
extern SExtProvider mysqlProvider;
extern SExtProvider pgProvider;
extern SExtProvider influxProvider;
#endif

// ============================================================
// Value conversion (extConnectorQuery.c)
// ============================================================

int32_t extValueConvert(EExtSourceType srcType, int8_t tdType,
                        const void *srcVal, int32_t srcLen,
                        SColumnInfoData *pColData, int32_t rowIdx);

#ifdef __cplusplus
}
#endif

#endif  // _TD_EXT_CONNECTOR_INT_H_
