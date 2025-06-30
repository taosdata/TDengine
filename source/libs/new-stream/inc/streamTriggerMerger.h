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

#ifndef TDENGINE_STREAM_TRIGGER_MERGER_H
#define TDENGINE_STREAM_TRIGGER_MERGER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "filter.h"
#include "stream.h"
#include "tcommon.h"
#include "tlosertree.h"

struct SStreamTriggerTask;

typedef struct SSTriggerMetaData {
  int64_t skey;
  int64_t ekey;
  int64_t ver;
  int64_t nrows;
} SSTriggerMetaData;

typedef struct SSTriggerTableMeta {
  int64_t tbUid;
  int32_t vgId;
  int32_t metaIdx;
  SArray *pMetas;  // SArray<SSTriggerMetaData>
} SSTriggerTableMeta;

typedef enum ETriggerMetaDataMask {
  TRIGGER_META_MASK_SKEY_INACCURATE = BIT_FLAG_MASK(0),
  TRIGGER_META_MASK_EKEY_INACCURATE = BIT_FLAG_MASK(1),
  TRIGGER_META_MASK_DATA_EMPTY = BIT_FLAG_MASK(2),
} ETriggerMetaDataMask;

#define SET_TRIGGER_META_NROW_INACCURATE(pMeta) \
  do {                                          \
    if ((pMeta)->nrows >= 0) {                  \
      (pMeta)->nrows = INT64_MIN;               \
    }                                           \
  } while (0)
#define IS_TRIGGER_META_NROW_INACCURATE(pMeta) ((pMeta)->nrows < 0)

#define SET_TRIGGER_META_SKEY_INACCURATE(pMeta)          \
  do {                                                   \
    SET_TRIGGER_META_NROW_INACCURATE(pMeta);             \
    (pMeta)->nrows |= TRIGGER_META_MASK_SKEY_INACCURATE; \
  } while (0)
#define IS_TRIGGER_META_SKEY_INACCURATE(pMeta) \
  (IS_TRIGGER_META_NROW_INACCURATE(pMeta) && ((pMeta)->nrows & TRIGGER_META_MASK_SKEY_INACCURATE))

#define SET_TRIGGER_META_EKEY_INACCURATE(pMeta)          \
  do {                                                   \
    SET_TRIGGER_META_NROW_INACCURATE(pMeta);             \
    (pMeta)->nrows |= TRIGGER_META_MASK_EKEY_INACCURATE; \
  } while (0)
#define IS_TRIGGER_META_EKEY_INACCURATE(pMeta) \
  (IS_TRIGGER_META_NROW_INACCURATE(pMeta) && ((pMeta)->nrows & TRIGGER_META_MASK_EKEY_INACCURATE))

#define SET_TRIGGER_META_DATA_EMPTY(pMeta)          \
  do {                                              \
    SET_TRIGGER_META_NROW_INACCURATE(pMeta);        \
    (pMeta)->nrows |= TRIGGER_META_MASK_DATA_EMPTY; \
  } while (0)
#define IS_TRIGGER_META_DATA_EMPTY(pMeta) \
  (IS_TRIGGER_META_NROW_INACCURATE(pMeta) && ((pMeta)->nrows & TRIGGER_META_MASK_DATA_EMPTY))

typedef enum ETriggerTimestampSorterMask {
  TRIGGER_TS_SORTER_MASK_SORT_INFO_SET = BIT_FLAG_MASK(0),
  TRIGGER_TS_SORTER_MASK_DATA_META_SET = BIT_FLAG_MASK(1),
  TRIGGER_TS_SORTER_MASK_DATA_MERGER_BUILD = BIT_FLAG_MASK(2),
  TRIGGER_TS_SORTER_MASK_SESS_WIN_BUILD = BIT_FLAG_MASK(3),
} ETriggerTimestampSorterMask;

typedef struct SSTriggerTimestampSorter {
  struct SStreamTriggerTask *pTask;
  int64_t                    flags;  // bitmask of ETriggerTimestampSorterMask

  STimeWindow readRange;
  int64_t     tbUid;
  int32_t     tsSlotId;

  SArray *pMetaNodeBuf;
  SArray *pMetaLists;

  SMultiwayMergeTreeInfo *pDataMerger;
  SArray                 *pSessWins;
} SSTriggerTimestampSorter;

#define SET_TRIGGER_TIMESTAMP_SORTER_EMPTY(pSorter) pSorter->readRange.skey = pSorter->readRange.ekey + 1
#define IS_TRIGGER_TIMESTAMP_SORTER_EMPTY(pSorter)  (pSorter->readRange.skey > pSorter->readRange.ekey)

/**
 * @brief Initializes a timestamp sorter for merging and sorting time-series data.
 *
 * @param pSorter The SSTriggerTimestampSorter instance to be initialized
 * @param pTask The SStreamTriggerTask instance that contains the task information
 * @return int32_t Status code indicating success or error
 */
int32_t stTimestampSorterInit(SSTriggerTimestampSorter *pSorter, struct SStreamTriggerTask *pTask);

/**
 * @brief Destroys a timestamp sorter, releasing all allocated resources.
 *
 * @param ptr Pointer to the SSTriggerTimestampSorter instance to be destroyed
 */
void stTimestampSorterDestroy(void *ptr);

/**
 * @brief Resets the timestamp sorter, clearing its state and preparing it for reuse.
 *
 * @param pSorter The SSTriggerTimestampSorter instance to be reset
 */
void stTimestampSorterReset(SSTriggerTimestampSorter *pSorter);

/**
 * @brief Sets the sort information for the timestamp sorter, including table UID and timestamp slot ID.
 *
 * @param pSorter The SSTriggerTimestampSorter instance to set the sort info
 * @param pRange The time range for sorting, containing start and end timestamps
 * @param tbUid The UID of the table to be sorted
 * @param tsSlotId The index of the timestamp column in the data block, starting from 0
 * @return int32_t Status code indicating success or error
 */
int32_t stTimestampSorterSetSortInfo(SSTriggerTimestampSorter *pSorter, STimeWindow *pRange, int64_t tbUid,
                                     int32_t tsSlotId);

/**
 * @brief Sets metadata for data blocks in the timestamp sorter.
 *
 * @param pSorter The SSTriggerTimestampSorter instance responsible for merging
 * @param pTableMeta metadatas of current table
 * @return int32_t Status code indicating success or error
 */
int32_t stTimestampSorterSetMetaDatas(SSTriggerTimestampSorter *pSorter, SSTriggerTableMeta *pTableMeta);

/**
 * @brief Get next data block from the sorter.
 *
 * @param pSorter The SSTriggerTimestampSorter instance responsible for merging
 * @param ppDataBlock Pointer to store the current data block
 * @param pStartIdx Pointer to store the start row index in the data block
 * @param pEndIdx Pointer to store the end row index in the data block
 * @return int32_t Status code indicating success or error
 */
int32_t stTimestampSorterNextDataBlock(SSTriggerTimestampSorter *pSorter, SSDataBlock **ppDataBlock, int32_t *pStartIdx,
                                       int32_t *pEndIdx);

/**
 * @brief Attempts to forward a specified number of rows on existing data blocks in pSorter.
 * @note It always try to use nrows in metadata to accelerate the process.
 *
 * @param pSorter The SSTriggerTimestampSorter instance responsible for merging
 * @param nrowsToSkip Number of rows to skip
 * @param pSkipped Pointer to store the number of rows actually skipped, could be NULL if not needed
 * @param pLastTs Pointer to store the last timestamp of the skipped rows, could be NULL if not needed
 * @return int32_t Status code indicating success or error
 */
int32_t stTimestampSorterForwardNrows(SSTriggerTimestampSorter *pSorter, int64_t nrowsToSkip, int64_t *pSkipped,
                                      int64_t *pLastTs);

/**
 * @brief Attempts to forward as far as possible until time diff between two consecutive rows larger than gap.
 * @note It always try to use skey/ekey in metadata to accelerate the process.
 *
 * @param pSorter The SSTriggerTimestampSorter instance responsible for merging
 * @param ts The timestamp to start from
 * @param gap The maximum allowed gap between consecutive timestamps
 * @param pLastTs Pointer to store the last timestamp of the skipped rows, could be NULL if not needed
 * @param pNextTs Pointer to store the next timestamp after the extended range, could be NULL if not needed
 * @return int32_t Status code indicating success or error
 */
int32_t stTimestampSorterForwardTs(SSTriggerTimestampSorter *pSorter, int64_t ts, int64_t gap, int64_t *pLastTs,
                                   int64_t *pNextTs);

/**
 * @brief Get the metadata of the next data block to be fetched.
 *
 * @param pSorter The SSTriggerTimestampSorter instance responsible for merging
 * @param ppMeta Pointer to store the metadata of the next data block to be fetched or NULL if no need to fetch more
 * data blocks now
 * @return int32_t Status code indicating success or error
 */
int32_t stTimestampSorterGetMetaToFetch(SSTriggerTimestampSorter *pSorter, SSTriggerMetaData **ppMeta);

/**
 * @brief Binds a fetched data block to the sorter.
 *
 * @param pSorter The SSTriggerTimestampSorter instance responsible for merging
 * @param pDataBlock Pointer to the data block to be bound, set to NULL if successfully bound
 * @return int32_t Status code indicating success or error
 */
int32_t stTimestampSorterBindDataBlock(SSTriggerTimestampSorter *pSorter, SSDataBlock **ppDataBlock);

typedef struct SSTriggerColMatch {
  col_id_t otbColId;
  col_id_t vtbColId;
  col_id_t vtbSlotId;
} SSTriggerColMatch;

typedef struct SSTriggerTableColRef {
  int64_t otbSuid;
  int64_t otbUid;
  SArray *pColMatches;  // SArray<SSTriggerColMatch>
} SSTriggerTableColRef;

typedef enum ETriggerVtableMergerMask {
  TRIGGER_VTABLE_MERGER_MASK_MERGE_INFO_SET = BIT_FLAG_MASK(0),
  TRIGGER_VTABLE_MERGER_MASK_DATA_META_SET = BIT_FLAG_MASK(1),
} ETriggerVtableMergerMask;

typedef struct SSTriggerVtableMerger {
  struct SStreamTriggerTask *pTask;
  int64_t                    flags;  // bitmask of ETriggerVtableMergerMask
  SSDataBlock               *pDataBlock;
  SFilterInfo               *pFilter;

  STimeWindow readRange;
  SArray     *pReaderInfos;  // SArray<SVtableMergerReaderInfo>
  SArray     *pReaders;      // SArray<SSTriggerTimestampSorter *>

  SMultiwayMergeTreeInfo *pDataMerger;
} SSTriggerVtableMerger;

#define SET_TRIGGER_VTABLE_MERGER_EMPTY(pMerger) pMerger->readRange.skey = pMerger->readRange.ekey + 1
#define IS_TRIGGER_VTABLE_MERGER_EMPTY(pMerger)  (pMerger->readRange.skey > pMerger->readRange.ekey)

/**
 * @brief Initializes a vtable merger for merging data from original tables.
 *
 * @param pMerger The SSTriggerVtableMerger instance to be initialized
 * @param pTask The SStreamTriggerTask instance that contains the task information
 * @param ppDataBlock Pointer to the data block to store the merged data, each column's colid is incremental
 * @param pFilter Pointer to the filter for the virtual table, can be NULL if no filter is present
 * @return int32_t Status code indicating success or error
 */
int32_t stVtableMergerInit(SSTriggerVtableMerger *pMerger, struct SStreamTriggerTask *pTask, SSDataBlock **ppDataBlock,
                           SFilterInfo **ppFilter);

/**
 * @brief Destroys a vtable merger, releasing all allocated resources.
 *
 * @param ptr Pointer to the SSTriggerVtableMerger instance to be destroyed
 */
void stVtableMergerDestroy(void *ptr);

/**
 * @brief Resets the vtable merger, clearing its state and preparing it for reuse.
 *
 * @param pMerger The SSTriggerVtableMerger instance to be reset
 * @return int32_t Status code indicating success or error
 */
void stVtableMergerReset(SSTriggerVtableMerger *pMerger);

/**
 * @brief Sets the merge information for the vtable merger, including timestamp slot ID, column maps, and filter.
 *
 * @param pMerger The SSTriggerVtableMerger instance to be configured
 * @param pRange The time range for merging, containing start and end timestamps
 * @param pTableColRefs Array of table column references, each containing original and virtual table column mappings
 * @return int32_t Status code indicating success or error
 */
int32_t stVtableMergerSetMergeInfo(SSTriggerVtableMerger *pMerger, STimeWindow *pRange, SArray *pTableColRefs);

/**
 * @brief Sets metadata for data blocks in the vtable merger.
 *
 * @param pMerger The SSTriggerVtableMerger instance responsible for merging
 * @param pOrigTableMetas Hash<tbUid, SSTriggerTableMeta> metadatas of all original tables
 * @return int32_t Status code indicating success or error
 */
int32_t stVtableMergerSetMetaDatas(SSTriggerVtableMerger *pMerger, SSHashObj *pOrigTableMetas);

/**
 * @brief Gets next data block from the vtable merger.
 *
 * @param pMerger The SSTriggerVtableMerger instance responsible for merging
 * @param ppDataBlock Pointer to store the current data block
 * @param pStartIdx Pointer to store the start row index in the data block
 * @param pEndIdx Pointer to store the end row index in the data block
 * @return int32_t Status code indicating success or error
 */
int32_t stVtableMergerNextDataBlock(SSTriggerVtableMerger *pMerger, SSDataBlock **ppDataBlock);

/**
 * @brief Gets the metadata for the data blocks to be fetched.
 *
 * @param pMerger The SSTriggerVtableMerger instance responsible for merging
 * @param ppMeta Pointer to store the metadata of the next data block to be fetched or NULL if no need to fetch more
 * data blocks now
 * @param ppColRef Pointer to store the column reference for the next data block to be fetched
 * @return int32_t Status code indicating success or error
 */
int32_t stVtableMergerGetMetaToFetch(SSTriggerVtableMerger *pMerger, SSTriggerMetaData **ppMeta,
                                     SSTriggerTableColRef **ppColRef);

/**
 * @brief Binds a fetched data block to the vtable merger.
 *
 * @param pMerger The SSTriggerVtableMerger instance responsible for merging
 * @param ppDataBlock Pointer to the data block to be bound, set to NULL if successfully bound
 * @return int32_t Status code indicating success or error
 */
int32_t stVtableMergerBindDataBlock(SSTriggerVtableMerger *pMerger, SSDataBlock **ppDataBlock);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_TRIGGER_MERGER_H
