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

#ifndef _TD_TSDB_H_
#define _TD_TSDB_H_

#include "os.h"
#include "taosmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

// Types exported
typedef struct STsdb             STsdb;
typedef struct STsdbOptions      STsdbOptions;
typedef struct STsdbSMAOptions   STsdbSMAOptions;  // SMA stands for Small Materialized Aggregation
typedef struct STsdbReadOptions  STsdbReadOptions;
typedef struct STsdbSnapshot     STsdbSnapshot;
typedef struct STsdbQueryHandle  STsdbQueryHandle;

// DB operations
int    tsdbCreate(const char *path);
int    tsdbDestroy(const char *path);
STsdb *tsdbOpen(const STsdbOptions *options);
void   tsdbClose(STsdb *);
int    tsdbReset(STsdb *, const STsdbOptions *);
int    tsdbInsert(STsdb *, SSubmitReq *, SSubmitRsp *);
int    tsdbCommit(STsdb *);
int    tsdbCompact(STsdb *);

// Options
STsdbOptions *tsdbOptionsCreate();
void          tsdbOptionsDestroy(STsdbOptions *);
void          tsdbOptionsSetId(STsdbOptions *, int id);
void          tsdbOptionsSetHoursPerFile(STsdbOptions *, int hours);
void          tsdbOptionsSetRetention(STsdbOptions *, int keep, int keep1, int keep2);
void          tsdbOptionsSetMinAndMaxRows(STsdbOptions *, int minRows, int maxRows);
void          tsdbOptionsSetPrecision(STsdbOptions *, int);
void          tsdbOptionsSetCache(STsdbOptions *, int);
typedef enum { TSDB_NO_UPDATE = 0, TSDB_WHOLE_ROW_UPDATE = 1, TSDB_PARTIAL_ROW_UPDATE = 2 } ETsdbUpdateType;
void tsdbOptionsSetUpdate(STsdbOptions *, ETsdbUpdateType);
void tsdbOptionsSetSMA(STsdbOptions *, STsdbSMAOptions *);

// STsdbSMAOptions
STsdbSMAOptions *tsdbSMAOptionsCreate();
void             tsdbSMAOptionsDestroy(STsdbSMAOptions *);
// void             tsdbSMAOptionsSetFuncs(STsdbSMAOptions *, SArray * /*Array of function to perform on each block*/);
// void             tsdbSMAOptionsSetIntervals(STsdbSMAOptions *, SArray *);
// void             tsdbSMAOptionsSetColTypes(STsdbSMAOptions *, SArray *);

// STsdbQueryHandle
STsdbQueryHandle *tsdbQueryHandleCreate(STsdb *, STsdbReadOptions *);
void              tsdbQueryHandleDestroy(STsdbQueryHandle *);
void              tsdbResetQueryHandle(STsdbQueryHandle *, STsdbReadOptions *);
bool              tsdbNextDataBlock(STsdbQueryHandle *);
// void              tsdbGetDataBlockInfo(STsdbQueryHandle *, SDataBlockInfo *);
// void              tsdbGetDataBlockStatisInfo(STsdbQueryHandle *, SDataStatis **);

// STsdbReadOptions
STsdbReadOptions *tsdbReadOptionsCreate();
void              tsdbReadOptionsDestroy(STsdbReadOptions *);
void              tsdbReadOptionsSetSnapshot(STsdbReadOptions *, STsdbSnapshot *);

// STsdbSnapshot
STsdbSnapshot *tsdbSnapshotCreate(STsdb *);
void           tsdbSnapshotDestroy(STsdbSnapshot *);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_H_*/
