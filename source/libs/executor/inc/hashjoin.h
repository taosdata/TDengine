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

/*
 * Hash Join Operator - Type definitions, constants, and function declarations.
 *
 * This header defines the core data structures for the hash join operator.
 * The hash join implementation follows a classic build-probe model:
 *   1. Build phase: reads all rows from the build side, inserts them into an in-memory hash table.
 *   2. Probe phase: streams rows from the probe side, looks up matching keys in the hash table.
 *
 * Supported join types: INNER, LEFT/RIGHT OUTER, LEFT/RIGHT SEMI, LEFT/RIGHT ANTI, FULL OUTER.
 * For RIGHT joins, build and probe sides are swapped so LEFT join logic applies symmetrically.
 *
 * Key files:
 *   - hashjoin.h (this file): type definitions, constants, function declarations
 *   - hashjoin.c: per-join-type execution logic (hInnerJoinDo, hLeftJoinDo, etc.)
 *   - hashjoinoperator.c: operator framework (init, main loop, teardown, shared utilities)
 *   - join.h: shared primitives used by both hash join and merge join
 */
#ifndef TDENGINE_HASHJOIN_H
#define TDENGINE_HASHJOIN_H

#ifdef __cplusplus
extern "C" {
#endif

#include "executorInt.h"
#include "operator.h"
#include "join.h"

/* Default page size for the row buffer pool (10 MB). Pages are allocated on demand. */
#define HASH_JOIN_DEFAULT_PAGE_SIZE 10485760

/* Bitmap size for tracking matched rows in FULL OUTER JOIN (2 MB). */
#define HJOIN_ROW_BITMAP_SIZE (2 * 1048576)

/* Batch size used when emitting FULL JOIN unmatched build rows. */
#define HJOIN_NMATCH_BATCH_SIZE 64

/*
 * Threshold ratio for the output block. When finBlk reaches this fraction of its capacity,
 * the operator yields the block to the upstream caller instead of continuing to append rows.
 * This avoids frequent small returns while preventing excessive buffering.
 */
#define HJOIN_BLK_THRESHOLD_RATIO 0.9

/*
 * Output block row capacity and size limit.
 * The #if 1 branch uses small values (3 rows, no size limit) for unit testing to exercise
 * multi-batch output paths. For production, switch to #else branch (4096 rows, 10 MB limit).
 */
#if 1
#define HJOIN_DEFAULT_BLK_ROWS_NUM 3 //4096
#define HJOIN_BLK_SIZE_LIMIT 0 //10485760
#else
#define HJOIN_DEFAULT_BLK_ROWS_NUM 4096
#define HJOIN_BLK_SIZE_LIMIT 10485760
#endif

/*
 * Returns true if the join type requires non-matching row emission (LEFT/RIGHT OUTER, ANTI, FULL).
 * INNER and SEMI joins only produce matched rows, so they do not need the PRE/POST phases.
 */
#define IS_NEED_NMATCH_JOIN(_jtype, _stype) ((!IS_INNER_NONE_JOIN((_jtype), (_stype))) && (!IS_SEMI_JOIN((_stype))))

/* Function pointer type for join execution logic (hInnerJoinDo, hLeftJoinDo, etc.). */
typedef int32_t (*hJoinImplFp)(SOperatorInfo*);

/* Function pointer type for build phase logic (hJoinBuildHash, hFullJoinBuildHash). */
typedef int32_t (*hJoinBuildFp)(SOperatorInfo*, bool*);

/* Function pointer type for probe-end handling when downstream probe is exhausted. */
typedef int32_t (*hJoinProbeEndFp)(struct SHJoinOperatorInfo*, bool*, bool*);

/*
 * Row metadata stored at the head of each row buffer entry in the page pool.
 * Forms a singly-linked list per hash group so all rows with the same key can be traversed.
 * Packed to minimize memory overhead since one SBufRowInfo is allocated per build-side row.
 *
 * Constraint: the `rows` field in SGroupData/SFGroupData MUST be the first member so that
 * the hash table value can be cast to access the row list head directly.
 */
#pragma pack(push, 1)
typedef struct SBufRowInfo {
  void*    next;      /* pointer to the next SBufRowInfo in the same hash group (NULL if last) */
  uint16_t pageId;    /* index into pRowBufs array identifying which page holds the row data */
  int32_t  offset;    /* byte offset within the page where the row's value data begins */
  uint32_t grpRowIdx; /* row index inside one hash group, used by FULL JOIN match bitmap */
} SBufRowInfo;
#pragma pack(pop)

/*
 * Probe execution phase for LEFT/ANTI/FULL joins when hasTimeRange is true.
 * The probe block is divided into three phases based on the time window:
 *   PRE  - rows before the join time window, emitted as non-matching
 *   CUR  - rows within the time window, matched against the hash table
 *   POST - rows after the time window, emitted as non-matching
 *
 * Phase transitions: PRE -> CUR -> POST. The phase determines which code path
 * handles each probe row.
 */
typedef enum EHJoinPhase {
  E_JOIN_PHASE_PRE = 1,
  E_JOIN_PHASE_CUR,
  E_JOIN_PHASE_POST
} EHJoinPhase;

/*
 * Per-block execution context that tracks the current position within a probe block.
 * This context is preserved across multiple calls to hJoinMainProcess when the output
 * block fills up before all probe rows are processed (rowRemains/midRemains = true).
 */
typedef struct SHJoinCtx {
  int64_t      limit;          /* max result rows (from LIMIT clause), INT64_MAX if unlimited */
  bool         ascTs;          /* true if input is ordered by timestamp ascending */
  bool         grpSingleRow;   /* optimization: when true, each hash group has at most one row
                                  (SEMI/ANTI without ON condition) so we skip after first match */

  bool         rowRemains;     /* true if unprocessed probe rows remain in the current block */
  bool         midRemains;     /* true if midBlk has rows that haven't been merged into finBlk */
  SBufRowInfo* pBuildRow;      /* current position in the build-side row linked list being emitted */
  SSDataBlock* pProbeData;     /* current probe-side data block being processed */
  EHJoinPhase  probePhase;     /* current phase in the PRE/CUR/POST state machine */
  int32_t      probePreIdx;    /* start index for PRE phase emission (tracks progress) */
  int32_t      probeStartIdx;  /* start index of rows in CUR phase (within the time window) */
  int32_t      probeEndIdx;    /* end index of rows in CUR phase (within the time window) */
  int32_t      probePostIdx;   /* start index for POST phase emission (tracks progress) */
  bool         readMatch;      /* true if any build row matched the current probe row after filtering;
                                  used to decide whether to emit a NULL-padded non-match row */

  // FOR FULL JOIN - tracks build-side non-matching rows with NULL keys during build phase
  SSDataBlock* pBuildData;     /* current build block being processed (for NULL-key row emission) */
  int32_t      buildNMStartIdx; /* start index of consecutive NULL-key build rows, -1 if none */
  int32_t      buildNMEndIdx;   /* end index of consecutive NULL-key build rows */
  int32_t      buildStartIdx;   /* current row index in the build block being inserted to hash */
  int32_t      buildEndIdx;     /* last row index in the build block */
  bool         probeDone;       /* true once probe downstream returns NULL; do not call downstream again */

  // FOR FULL JOIN - tracks post-probe unmatched build row emission
  bool         buildNMatchDone;
  bool         buildNMatchInited;
  void*        pBuildNMatchGrp;
  int32_t      buildNMatchIter;
  SBufRowInfo* pBuildNMatchRow;

  // FOR FULL JOIN - lazy bitmap allocation from one global pool
  char*        fullGrpBitmapPool;       /* one shared bitmap pool for all FULL hash groups */
  uint64_t     fullGrpBitmapPoolSize;   /* total bytes lazily computed when pool is first required */
  uint64_t     fullGrpBitmapPoolOffset; /* next free byte offset in fullGrpBitmapPool */
} SHJoinCtx;

/*
 * Metadata for a single column involved in the join (key or value column).
 * Stores the mapping between source slot (in the input block) and destination slot
 * (in the output block), along with data access pointers for the current block.
 */
typedef struct SHJoinColInfo {
  int32_t          srcSlot;    /* slot index in the source data block */
  int32_t          dstSlot;    /* slot index in the output result block */
  int32_t          keyColIdx;  /* if this value col is also a key col, index into keyCols[]; -1 otherwise.
                                  When >= 0, the value is read from the probe key buffer to avoid
                                  redundant storage in the build-side row buffer. */
  bool             vardata;    /* true if this column is a variable-length type (VARCHAR/NCHAR/etc.) */
  int32_t          bytes;      /* fixed byte size of the column (max size for variable-length) */
  int32_t          bufOffset;  /* byte offset of this key column within the serialized key buffer
                                  (only used for multi-column keys) */
  char*            data;       /* pointer to column data (pCol->pData) for the current block */
  char*            bitMap;     /* pointer to null bitmap for fixed-length non-key value columns */
  SColumnInfoData* colData;    /* pointer to the SColumnInfoData for accessing var-length offsets */
} SHJoinColInfo;

/*
 * Metadata for a page in the row buffer pool.
 * The page pool stores build-side row data (value columns + SBufRowInfo header).
 * Pages are 10 MB each and allocated on demand. The offset tracks the write position
 * within the current page.
 */
typedef struct SBufPageInfo {
  int32_t pageSize;  /* total size of this page in bytes */
  int32_t offset;    /* current write offset; new rows are written at data + offset */
  char*   data;      /* page memory buffer */
} SBufPageInfo;


/*
 * Hash table value for non-FULL joins (INNER, LEFT, RIGHT, SEMI, ANTI).
 * Each hash entry stores a linked list of build-side rows sharing the same key.
 *
 * Constraint: `rows` MUST be the first field so SGroupData can be cast from the
 * hash table value pointer to access the row list directly.
 */
typedef struct SGroupData {
  SBufRowInfo*  rows;             // KEEP IT FIRST
} SGroupData;

/*
 * Hash table value for FULL OUTER JOIN. Extends SGroupData with a bitmap to track
 * which build-side rows have been matched during the probe phase.
 * After all probe rows are processed, unmatched build rows (bitmap bit = 0) are
 * emitted as non-matching with NULL-padded probe columns.
 *
 * Constraint: `rows` MUST be the first field (same layout requirement as SGroupData).
 */
typedef struct SFGroupData {
  SBufRowInfo*  rows;             // KEEP IT FIRST

  char*         bitmap;       /* per-row match bitmap; bit=1 means unmatched, bit=0 means matched */
  uint32_t      rowsNum;      /* total number of rows in this group */
  uint32_t      rowsMatchNum; /* number of rows matched so far (for quick skip when all matched) */
  int32_t*      keyOffsets;   /* cached key column offsets inside serialized group key */
  int32_t       keyOffsetNum; /* number of cached offsets (should equal build keyNum) */
} SFGroupData;


/*
 * Column mapping entry for result construction.
 * Maps a source slot in an input block to a destination slot in the output block.
 */
typedef struct SHJoinColMap {
  int32_t  srcSlot;   /* slot index in the source data block */
  int32_t  dstSlot;   /* slot index in the output result block */
  bool     vardata;   /* true if variable-length type */
  int32_t  bytes;     /* column byte width */
} SHJoinColMap;

/*
 * Context for evaluating TIMETRUNCATE expressions on primary key columns.
 * Used when the join equality condition involves timetruncate(ts, interval).
 * The truncated value is computed inline during the build/probe phases to avoid
 * materializing an extra expression evaluation step.
 */
// for now timetruncate only
typedef struct SHJoinPrimExprCtx {
  int64_t truncateUnit;   /* truncation interval in timestamp ticks */
  int64_t timezoneUnit;   /* timezone offset in timestamp ticks (0 if no timezone adjustment) */
  int32_t targetSlotId;   /* output slot for the truncated timestamp value */
} SHJoinPrimExprCtx;

/*
 * Per-table context holding all column metadata, key buffers, and expression contexts
 * for one side (build or probe) of the hash join.
 */
typedef struct SHJoinTableCtx {
  EJoinTableType type;          /* BUILD or PROBE */
  int32_t        downStreamIdx; /* index into the downstream operator array (0 or 1) */
  SOperatorInfo* downStream;    /* pointer to the downstream operator */
  int64_t        blkId;         /* data block ID used to identify which columns belong to this table */
  SQueryStat     inputStat;     /* input statistics (e.g. estimated row count for hash table sizing) */
  bool           hasTimeRange;  /* true if this table has a time range filter to apply */

  SHJoinColMap*      primCol;   /* primary key column mapping (for time range filtering) */
  SNode*             primExpr;  /* primary key expression node (e.g. TIMETRUNCATE), NULL if none */
  SHJoinPrimExprCtx  primCtx;   /* context for evaluating the primary key expression */
  SExprSupp          exprSup;   /* expression support for scalar expressions on key columns */

  int32_t        keyNum;        /* number of equality key columns */
  int32_t        keyNullSize;   /* size of the NULL indicator prefix in the serialized key buffer.
                                   1 byte if total key size > 1, 2 bytes otherwise (to ensure
                                   the NULL key representation differs from any valid key) */
  SHJoinColInfo* keyCols;       /* array of key column metadata (length: keyNum) */
  char*          keyBuf;        /* buffer for serializing multi-column composite keys */
  char*          keyData;       /* pointer to the current row's serialized key data.
                                   For single-column keys, points directly into the column data.
                                   For multi-column keys, points to keyBuf. */

  int32_t        valNum;        /* number of value (non-key) columns to include in output */
  SHJoinColInfo* valCols;       /* array of value column metadata (length: valNum) */
  char*          valData;       /* buffer for the current row's serialized value data */
  int32_t        valBitMapSize; /* size of the null bitmap for value columns */
  int32_t        valBufSize;    /* total buffer size for fixed-length value columns + bitmap */
  SArray*        valVarCols;    /* array of indices into valCols[] for variable-length columns;
                                   used to calculate per-row buffer size dynamically */
  bool           valColExist;   /* true if there are value columns that are not also key columns */
} SHJoinTableCtx;

/*
 * Execution statistics for the hash join operator.
 * Logged on operator destruction for debugging and performance analysis.
 */
typedef struct SHJoinExecInfo {
  int64_t buildBlkNum;   /* number of data blocks read from the build side */
  int64_t buildBlkRows;  /* total rows read from the build side */
  int64_t probeBlkNum;   /* number of data blocks read from the probe side */
  int64_t probeBlkRows;  /* total rows read from the probe side */
  int64_t resRows;       /* total result rows emitted */
  int64_t expectRows;    /* expected rows (debug/trace only) */
} SHJoinExecInfo;


/*
 * Main hash join operator info structure.
 * Contains all state needed for hash join execution including:
 *   - Build/probe table contexts
 *   - Hash table for key lookup
 *   - Output blocks (finBlk for final results, midBlk for intermediate pre-filter results)
 *   - Filter objects (pPreFilter for non-equi ON conditions, pFinFilter for WHERE conditions)
 *   - Function pointers for join-type-specific logic
 *
 * The two-block output strategy (midBlk/finBlk) is used when pPreFilter exists:
 *   1. Matched rows go to midBlk first
 *   2. pPreFilter is applied to midBlk
 *   3. Surviving rows are merged into finBlk
 *   4. If no rows survive and all build rows are exhausted, a NULL-padded non-match row is emitted
 */
typedef struct SHJoinOperatorInfo {
  SOperatorInfo*   pOperator;    /* back pointer to the parent SOperatorInfo */
  EJoinType        joinType;     /* INNER, LEFT, RIGHT, or FULL */
  EJoinSubType     subType;      /* OUTER, SEMI, or ANTI (for LEFT/RIGHT joins) */
  SHJoinTableCtx   tbs[2];       /* table contexts indexed by downstream position (0=left, 1=right) */
  SHJoinTableCtx*  pBuild;       /* pointer to the build-side table context */
  SHJoinTableCtx*  pProbe;       /* pointer to the probe-side table context */
  SFilterInfo*     pPreFilter;   /* non-equi ON condition filter (applied to midBlk before merging) */
  SFilterInfo*     pFinFilter;   /* WHERE clause filter (applied to finBlk before returning) */
  SSDataBlock*     finBlk;       /* final output block returned to the upstream operator */
  SSDataBlock*     midBlk;       /* intermediate block for pre-filter evaluation (NULL if no pPreFilter) */
  STimeWindow      tblTimeRange; /* time range for filtering rows during build/probe phases */
  int32_t          pResColNum;   /* number of columns in the result block */
  int8_t*          pResColMap;   /* per-column flag: 1 if column comes from build side, 0 if from probe */
  SArray*          pRowBufs;     /* page pool (array of SBufPageInfo) for storing build-side row data */
  SSHashObj*       pKeyHash;     /* hash table mapping serialized keys to SGroupData/SFGroupData */
  bool             keyHashBuilt; /* true after the build phase completes */
  SHJoinCtx        ctx;          /* per-block execution context */
  SHJoinExecInfo   execInfo;     /* execution statistics */
  int32_t          blkThreshold; /* row count threshold for yielding the output block (capacity * ratio) */
  hJoinImplFp      joinFp;       /* join execution function pointer (varies by join type) */
  hJoinBuildFp     buildFp;      /* build phase function pointer (standard or FULL) */
  hJoinProbeEndFp  probeEndFp;   /* probe-end hook (used by FULL JOIN unmatched-build emission) */
} SHJoinOperatorInfo;

/* Returns true if the column is also a key column (keyColIdx >= 0). */
#define IS_HASH_JOIN_KEY_COL(_keyIdx) ((_keyIdx) >= 0)

/*
 * Error handling macros for hash join.
 * HJ_ERR_RET: evaluates the expression, logs and returns the error code on failure.
 * HJ_ERR_JRET: evaluates the expression, logs and jumps to _return label on failure
 *              (for goto-based cleanup patterns).
 */
#define HJ_ERR_RET(c)                 \
  do {                                \
    int32_t _code = (c);              \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(_code)); \
      return _code;                   \
    }                                 \
  } while (0)

#define HJ_ERR_JRET(c)               \
  do {                               \
    code = (c);                      \
    if (code != TSDB_CODE_SUCCESS) { \
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code)); \
      terrno = code;                 \
      goto _return;                  \
    }                                \
  } while (0)

/*
 * Per-join-type execution functions.
 * Each function processes the current probe block against the hash table and emits
 * matched (and non-matched, where applicable) rows to the output block.
 *
 * @param pOperator  the hash join operator
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hInnerJoinDo(struct SOperatorInfo* pOperator);
int32_t hLeftJoinDo(struct SOperatorInfo* pOperator);
int32_t hSemiJoinDo(struct SOperatorInfo* pOperator);
int32_t hAntiJoinDo(struct SOperatorInfo* pOperator);
int32_t hFullJoinDo(struct SOperatorInfo* pOperator);

/*
 * Build phase for FULL OUTER JOIN. Unlike hJoinBuildHash, this function also emits
 * build-side rows with NULL keys as non-matching rows (with NULL-padded probe columns)
 * during the build phase itself.
 *
 * @param pOperator    the hash join operator
 * @param returnDirect set to true if finBlk has rows to return immediately
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hFullJoinBuildHash(struct SOperatorInfo* pOperator, bool* returnDirect);

/*
 * Marks the operator as done: frees the hash table, midBlk, and row buffers.
 * Called when all probe rows have been processed and all results emitted.
 * Side effect: sets operator status to OP_EXEC_DONE.
 *
 * @param pOperator  the hash join operator
 */
void    hJoinSetDone(struct SOperatorInfo* pOperator);

/*
 * Appends matched build-side rows to the result block by traversing the row linked list
 * starting from ctx.pBuildRow. Combines build-side value data with probe-side column data
 * for the current probe row.
 *
 * @param pOperator   the hash join operator
 * @param pRes        output block to append rows to
 * @param allFetched  set to true when all rows in the current group have been appended
 *
 * Side effect: advances ctx.pBuildRow through the linked list.
 * Limitation: on error, invokes T_LONG_JMP instead of returning an error code.
 */
void    hJoinAppendResToBlock(struct SOperatorInfo* pOperator, SSDataBlock* pRes, bool* allFetched);

/*
 * Serializes the key columns of a single row into the key buffer for hash lookup.
 * For single-column keys, keyData points directly to the column data (no copy).
 * For multi-column keys, values are concatenated into keyBuf.
 *
 * @param pTable   table context (provides key column metadata and buffers)
 * @param rowIdx   row index in the current data block
 * @param pBufLen  output: length of the serialized key in bytes
 * @return true if any key column is NULL (row should be skipped), false otherwise
 */
bool    hJoinCopyKeyColsDataToBuf(SHJoinTableCtx* pTable, int32_t rowIdx, size_t *pBufLen);

/*
 * Merges rows from midBlk into finBlk. If the total rows exceed finBlk capacity,
 * copies as many as possible and sets ctx.midRemains = true. The remaining rows
 * stay in midBlk and are handled on the next call via hJoinHandleMidRemains.
 *
 * @param pCtx  join context (midRemains flag is set here)
 * @param ppMid pointer to midBlk pointer
 * @param ppFin pointer to finBlk pointer
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hJoinCopyMergeMidBlk(SHJoinCtx* pCtx, SSDataBlock** ppMid, SSDataBlock** ppFin);

/*
 * Handles remaining midBlk rows by swapping midBlk and finBlk pointers.
 * Called when the previous iteration left unmerged rows in midBlk.
 *
 * @param pJoin  hash join operator info
 * @return TSDB_CODE_SUCCESS
 */
int32_t hJoinHandleMidRemains(SHJoinOperatorInfo* pJoin);

/*
 * Checks whether the output block has reached the row threshold for yielding.
 * Takes into account the LIMIT clause when no finFilter is present.
 *
 * @param pInfo    hash join operator info
 * @param blkRows current row count in the output block
 * @return true if the block should be yielded to the upstream caller
 */
bool    hJoinBlkReachThreshold(SHJoinOperatorInfo* pInfo, int64_t blkRows);

/*
 * Copies non-matching probe rows to the result block with NULL-padded build columns.
 * Used by LEFT/ANTI/FULL joins for probe rows that have no matching build-side row.
 *
 * @param pJoin    hash join operator info
 * @param pRes     output block to append rows to
 * @param startIdx starting row index in the probe block
 * @param rows     number of rows to copy
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hJoinCopyNMatchRowsToBlock(SHJoinOperatorInfo* pJoin, SSDataBlock* pRes, int32_t startIdx, int32_t rows);

/*
 * Copies one unmatched build-side row to result block for FULL OUTER JOIN.
 * Build columns come from row buffer + group key, probe columns are filled with NULL.
 */
int32_t hJoinCopyBuildNMatchRowToBlock(SHJoinOperatorInfo* pJoin, SSDataBlock* pRes, SBufRowInfo* pRow, const char* pKeyData,
                                       const int32_t* pKeyOffsets);

/*
 * Emits unmatched build-side rows after all probe blocks are consumed in FULL OUTER JOIN.
 * Uses SFGroupData bitmap to skip rows that have already matched.
 */
int32_t hJoinEmitBuildNMatchRows(SHJoinOperatorInfo* pJoin);

/*
 * Standard build phase: reads all blocks from the build-side downstream operator,
 * filters by time range, computes key expressions, and inserts rows into the hash table.
 * If the hash table is empty after building and the join type doesn't need non-match rows,
 * the operator is marked as done immediately.
 *
 * @param pOperator    the hash join operator
 * @param returnDirect set to true if the operator should return immediately (e.g. empty hash + INNER)
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hJoinBuildHash(struct SOperatorInfo* pOperator, bool* returnDirect);

/*
 * Adds a single build-side row to the hash table. Sets up value column data pointers,
 * looks up the existing group (or creates a new one), and copies value data to the page pool.
 *
 * @param pJoin   hash join operator info
 * @param pBlock  build-side data block
 * @param keyLen  length of the serialized key
 * @param rowIdx  row index in the data block
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hJoinAddRowToHash(SHJoinOperatorInfo* pJoin, SSDataBlock* pBlock, size_t keyLen, int32_t rowIdx);

/*
 * Filters a data block by the join time range using binary search on the primary key column.
 * Returns the start/end indices of rows within the time window.
 * Handles both ascending and descending timestamp order.
 *
 * @param pCtx      join context (provides ascTs flag)
 * @param pBlock    data block to filter
 * @param pRange    time window [skey, ekey]
 * @param primSlot  slot index of the primary key column
 * @param startIdx  output: first row index within the time range
 * @param endIdx    output: last row index within the time range
 * @return true if any rows fall within the time range, false if all rows are outside
 */
bool    hJoinFilterTimeRange(SHJoinCtx* pCtx, SSDataBlock* pBlock, STimeWindow* pRange, int32_t primSlot, int32_t* startIdx, int32_t* endIdx);

/*
 * Evaluates equality key expressions (e.g. TIMETRUNCATE) and scalar expressions
 * on the specified row range of a data block. Must be called before key serialization.
 *
 * @param pOperator  the hash join operator (for task info access)
 * @param pBlock     data block to evaluate expressions on
 * @param pTable     table context with expression metadata
 * @param startIdx   first row index to evaluate
 * @param endIdx     last row index to evaluate
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t hJoinLaunchEqualExpr(SOperatorInfo* pOperator, SSDataBlock* pBlock, SHJoinTableCtx* pTable, int32_t startIdx, int32_t endIdx);

/*
 * Sets up key column data pointers (data, colData, bitMap) from the current data block.
 * Must be called before hJoinCopyKeyColsDataToBuf for each new block.
 * Also validates column type and byte size consistency.
 *
 * @param pBlock  data block containing the key columns
 * @param pTable  table context with key column metadata
 * @return TSDB_CODE_SUCCESS on success, TSDB_CODE_INVALID_PARA on type/size mismatch
 */
int32_t hJoinSetKeyColsData(SSDataBlock* pBlock, SHJoinTableCtx* pTable);

/*
 * Filters a block and keeps only the first matching row (for SEMI join with ON condition).
 * After filtering, at most one row remains in the block.
 *
 * @param pBlock       data block to filter (modified in place)
 * @param pFilterInfo  filter to apply
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t mJoinFilterAndKeepSingleRow(SSDataBlock* pBlock, SFilterInfo* pFilterInfo);

/*
 * Filters a block and removes all matching rows (for ANTI join with ON condition).
 * If any row passes the filter, it means this probe row has a match and should NOT
 * be emitted as a non-match.
 *
 * @param pBlock       data block to filter (modified in place)
 * @param pFilterInfo  filter to apply
 * @return TSDB_CODE_SUCCESS on success, error code on failure
 */
int32_t mJoinFilterAndNoKeepRows(SSDataBlock* pBlock, SFilterInfo* pFilterInfo);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HASHJOIN_H
