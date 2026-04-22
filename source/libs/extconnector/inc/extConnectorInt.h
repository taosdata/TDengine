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
// Connection pool entry state
// ============================================================

typedef enum {
  EXT_ENTRY_FREE   = 0,  // slot unoccupied; lives in freeList
  EXT_ENTRY_IDLE   = 1,  // connected, waiting to be reused; lives in idleList
  EXT_ENTRY_IN_USE = 2,  // borrowed by a caller; not in any stack
} EExtEntryState;

// ============================================================
// Connection pool entry
//
// An entry has two separate 'next' links so it can be safely
// on idleList while eviction modifies freeNext (or vice-versa).
// Each entry belongs to exactly one logical owner at a time
// (freeList, idleList, or a caller), enforced by CAS on state.
// ============================================================

typedef struct SExtPoolEntry {
  struct SExtPoolEntry *idleNext;       // idleList Treiber stack link
  struct SExtPoolEntry *freeNext;       // freeList Treiber stack link
  volatile int32_t      state;          // EExtEntryState — modified only via atomic CAS
  void                 *pConn;          // native connection handle
  int64_t               lastActiveTime; // ms timestamp of last use
  int64_t               generation;     // drainGeneration value at open() time
} SExtPoolEntry;

// ============================================================
// Memory slab — fixed array of entries
//
// Slabs are allocated on demand and freed only by destroyPool.
// The slab chain (slabHead) is append-only; once attached via CAS
// a slab's 'next' pointer never changes, making traversal safe
// without any extra locking.
// ============================================================

typedef struct SExtSlab {
  struct SExtSlab *next;      // next slab in pool's chain (stable after CAS attach)
  int32_t          capacity;  // number of entries in this slab
  SExtPoolEntry    entries[]; // flexible array member
} SExtSlab;

// ============================================================
// Per-source connection pool (fully lock-free, no mutex)
//
// Concurrency model:
//   idleHead   — Treiber stack (idleNext links).  May contain FREE zombies
//                after eviction.  open() discards zombies via CAS on state.
//   freeHead   — Treiber stack (freeNext links).  Always contains FREE entries.
//   slabHead   — append-only slab chain.  Traversed by eviction and destroyPool.
//   idleCount  — accurate: incremented on every IDLE←IN_USE transition,
//                decremented on every IDLE→IN_USE or IDLE→FREE transition.
//   inUseCount — accurate: incremented on successful open(), decremented on close().
//   cfgVersion — CAS in open() ensures only one thread calls checkAndDrainPool.
//   drainGeneration — bumped on conn-field change; entry.generation < this → drain.
//   destroying — set to 1 before destroyPool; causes open() to fail fast and
//                close() to recycle to freeList instead of idleList.
// ============================================================

typedef struct SExtConnPool {
  char             sourceName[TSDB_EXT_SOURCE_NAME_LEN];
  SExtSourceCfg    cfg;             // deep copy (password = AES-encrypted)
  volatile int64_t cfgVersion;      // meta_version at last pool update
  volatile int64_t drainGeneration; // bumped on conn-field change
  SExtProvider    *pProvider;       // pointer into gExtProviders[]
  SExtPoolEntry   *idleHead;        // Treiber stack — IDLE entries (may have FREE zombies)
  SExtPoolEntry   *freeHead;        // Treiber stack — guaranteed FREE entries
  SExtSlab        *slabHead;        // append-only slab chain
  volatile int32_t idleCount;       // accurate count of IDLE entries
  volatile int32_t inUseCount;      // accurate count of IN_USE entries
  int32_t          slabSize;        // entries per expansion slab (= maxPoolSize initially)
  int32_t          maxPoolSize;     // soft cap on (idleCount + inUseCount)
  volatile int32_t destroying;      // 1 = pool shutting down
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
