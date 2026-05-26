# Hash Join Data Structures

## Core Type Definitions

All definitions are in `hashjoin.h` unless noted otherwise.

### EJoinTableType (join.h)

```c
typedef enum EJoinTableType {
  E_JOIN_TB_BUILD = 1,   // Table whose rows are hashed into memory
  E_JOIN_TB_PROBE        // Table that drives the probe loop
} EJoinTableType;
```

### EHJoinPhase

```c
typedef enum EHJoinPhase {
  E_JOIN_PHASE_PRE = 1,   // Probe rows before the time-range window (emit as non-matching)
  E_JOIN_PHASE_CUR,       // Probe rows within the time-range window (match against hash)
  E_JOIN_PHASE_POST       // Probe rows after the time-range window (emit as non-matching)
} EHJoinPhase;
```

Used by LEFT, ANTI, and FULL joins to manage three regions of a probe block relative to the join time range.

### SBufRowInfo (packed, 1-byte aligned)

```c
typedef struct SBufRowInfo {
  void*    next;     // Intrusive linked list pointer to next row with the same hash key
  uint16_t pageId;   // Index into pRowBufs page array
  int32_t  offset;   // Byte offset within that page where the value data begins
} SBufRowInfo;
```

Node in the per-key linked list stored in the hash table. Packed to reduce memory overhead.

### SBufPageInfo

```c
typedef struct SBufPageInfo {
  int32_t pageSize;   // Always HASH_JOIN_DEFAULT_PAGE_SIZE (10 MB)
  int32_t offset;     // Current write position; grows as rows are appended
  char*   data;       // Heap-allocated page buffer
} SBufPageInfo;
```

Pages are allocated on demand and stored in `pRowBufs` (SArray). Each page is a flat arena for SBufRowInfo headers and inline value data.

### SGroupData

```c
typedef struct SGroupData {
  SBufRowInfo* rows;   // Head of linked list of build rows for this key
} SGroupData;
```

Hash table value type for INNER, LEFT, RIGHT, SEMI, and ANTI joins.

### SFGroupData

```c
typedef struct SFGroupData {
  SBufRowInfo* rows;           // Head of linked list (same layout as SGroupData)
  char*        bitmap;         // Bitmap tracking which rows were matched during probe
  uint32_t     rowsNum;        // Total build rows for this key
  uint32_t     rowsMatchNum;   // Count of matched rows (for finding un-matched ones)
} SFGroupData;
```

Hash table value type for FULL OUTER join. The bitmap enables tracking unmatched build rows.

### SHJoinColInfo

```c
typedef struct SHJoinColInfo {
  int32_t          srcSlot;    // Source slot index in the input block
  int32_t          dstSlot;    // Destination slot index in the output block
  int32_t          keyColIdx;  // >= 0 means this column IS a key column (shared data)
  bool             vardata;    // True for variable-length types (BINARY, NCHAR)
  int32_t          bytes;      // Fixed byte size (max length for vardata)
  int32_t          bufOffset;  // Byte offset of this key in the composite key buffer
  char*            data;       // Pointer into the input block's raw column data
  char*            bitMap;     // Pointer to the null bitmap
  SColumnInfoData* colData;    // Full column descriptor
} SHJoinColInfo;
```

### SHJoinColMap

```c
typedef struct SHJoinColMap {
  int32_t  srcSlot;
  int32_t  dstSlot;
  bool     vardata;
  int32_t  bytes;
} SHJoinColMap;
```

Lightweight mapping for the primary timestamp column.

### SHJoinPrimExprCtx

```c
typedef struct SHJoinPrimExprCtx {
  int64_t truncateUnit;   // Truncation unit (e.g., 1s = 1000 ticks)
  int64_t timezoneUnit;   // Timezone offset in ticks; 0 if not needed
  int32_t targetSlotId;   // Slot where the truncated timestamp is stored
} SHJoinPrimExprCtx;
```

Used when the join equality condition on timestamp uses `TIMETRUNCATE()`.

### SHJoinTableCtx

```c
typedef struct SHJoinTableCtx {
  EJoinTableType type;         // BUILD or PROBE
  int32_t        downStreamIdx;
  SOperatorInfo* downStream;
  int64_t        blkId;
  SQueryStat     inputStat;
  bool           hasTimeRange;

  // Primary key
  SHJoinColMap*      primCol;
  SNode*             primExpr;
  SHJoinPrimExprCtx  primCtx;
  SExprSupp          exprSup;

  // Key columns (equality condition)
  int32_t        keyNum;
  int32_t        keyNullSize;
  SHJoinColInfo* keyCols;
  char*          keyBuf;       // Scratch buffer for composite key serialization
  char*          keyData;      // Points to keyBuf (multi-key) or column data (single key)

  // Value columns (output)
  int32_t        valNum;
  SHJoinColInfo* valCols;
  char*          valData;
  int32_t        valBitMapSize;
  int32_t        valBufSize;
  SArray*        valVarCols;   // Indices of variable-length value columns
  bool           valColExist;
} SHJoinTableCtx;
```

### SHJoinCtx

```c
typedef struct SHJoinCtx {
  int64_t      limit;          // Row limit; INT64_MAX if none
  bool         ascTs;          // Timestamp sort order
  bool         grpSingleRow;   // Optimization: each hash group has at most one row

  bool         rowRemains;     // Current probe block still has work
  bool         midRemains;     // midBlk has rows that didn't fit in finBlk
  SBufRowInfo* pBuildRow;      // Current position in build-side linked list
  SSDataBlock* pProbeData;     // Current probe-side block
  EHJoinPhase  probePhase;     // PRE / CUR / POST
  int32_t      probePreIdx;    // Row index for PRE phase
  int32_t      probeStartIdx;  // First row in active time-range window
  int32_t      probeEndIdx;    // Last row in active time-range window
  int32_t      probePostIdx;   // Row index for POST phase
  bool         readMatch;      // At least one matching build row found for current probe row

  // Full outer join only
  SSDataBlock* pBuildData;
  int32_t      buildNMStartIdx;
  int32_t      buildNMEndIdx;
  int32_t      buildStartIdx;
  int32_t      buildEndIdx;
} SHJoinCtx;
```

### SHJoinExecInfo

```c
typedef struct SHJoinExecInfo {
  int64_t buildBlkNum;
  int64_t buildBlkRows;
  int64_t probeBlkNum;
  int64_t probeBlkRows;
  int64_t resRows;
  int64_t expectRows;
} SHJoinExecInfo;
```

### SHJoinOperatorInfo (top-level operator state)

```c
typedef struct SHJoinOperatorInfo {
  SOperatorInfo*   pOperator;
  EJoinType        joinType;      // INNER / LEFT / RIGHT / FULL
  EJoinSubType     subType;       // OUTER / SEMI / ANTI
  SHJoinTableCtx   tbs[2];        // [0]=left, [1]=right
  SHJoinTableCtx*  pBuild;        // Points to build side
  SHJoinTableCtx*  pProbe;        // Points to probe side
  SFilterInfo*     pPreFilter;    // ON condition filter (LEFT/RIGHT/FULL)
  SFilterInfo*     pFinFilter;    // Post-join filter
  SSDataBlock*     finBlk;        // Output block
  SSDataBlock*     midBlk;        // Intermediate block for pre-filter evaluation
  STimeWindow      tblTimeRange;
  int32_t          pResColNum;
  int8_t*          pResColMap;    // pResColMap[i]=1 if column i comes from build side
  SArray*          pRowBufs;      // Page pool (SArray of SBufPageInfo)
  SSHashObj*       pKeyHash;      // Hash table
  bool             keyHashBuilt;
  SHJoinCtx        ctx;
  SHJoinExecInfo   execInfo;
  int32_t          blkThreshold;
  hJoinImplFp      joinFp;        // Per-join-type execution function
  hJoinBuildFp     buildFp;       // Build phase function
} SHJoinOperatorInfo;
```

## Constants

| Constant | Value | Purpose |
|----------|-------|---------|
| `HASH_JOIN_DEFAULT_PAGE_SIZE` | 10,485,760 (10 MB) | Size of each page in the row buffer pool |
| `HJOIN_ROW_BITMAP_SIZE` | 2,097,152 (2 MB) | Size of the bitmap for tracking matched rows |
| `HJOIN_BLK_THRESHOLD_RATIO` | 0.9 | Fraction of output block capacity triggering return |
| `HJOIN_DEFAULT_BLK_ROWS_NUM` | 3 (dev) / 4096 (prod) | Default rows per output block |
| `HJOIN_BLK_SIZE_LIMIT` | 0 (dev) / 10,485,760 (prod) | Maximum output block byte size |

Note: The `#if 1` guard in hashjoin.h currently selects debug-mode values (3 rows per block, 0 size limit) for easier testing of block boundary behavior.

## Memory Layout of a Build Row in Page Pool

```
[SBufRowInfo header: next ptr (8B) + pageId (2B) + offset (4B)]
[value bitmap: valBitMapSize bytes]
[fixed-width value columns: each valCols[i].bytes bytes]
[variable-width value columns: each prefixed with varDataLen]
```

Key columns are NOT stored in the page pool. They are re-read from the probe side during result construction since keys are equal by definition of the hash match.
