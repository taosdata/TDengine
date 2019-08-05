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

#ifndef TDENGINE_VNODE_H
#define TDENGINE_VNODE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <syslog.h>

#include "tglobalcfg.h"
#include "tidpool.h"
#include "tlog.h"
#include "tmempool.h"
#include "trpc.h"
#include "tsclient.h"
#include "tsdb.h"
#include "tsdb.h"
#include "tsocket.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "vnodeCache.h"
#include "vnodeFile.h"
#include "vnodeShell.h"

#define TSDB_FILE_HEADER_LEN          512
#define TSDB_FILE_HEADER_VERSION_SIZE 32
#define TSDB_CACHE_POS_BITS           13
#define TSDB_CACHE_POS_MASK           0x1FFF

#define TSDB_ACTION_INSERT 0
#define TSDB_ACTION_IMPORT 1
#define TSDB_ACTION_DELETE 2
#define TSDB_ACTION_UPDATE 3
#define TSDB_ACTION_MAX    4

enum _data_source {
  TSDB_DATA_SOURCE_METER,
  TSDB_DATA_SOURCE_VNODE,
  TSDB_DATA_SOURCE_SHELL,
  TSDB_DATA_SOURCE_QUEUE,
  TSDB_DATA_SOURCE_LOG,
};

enum _sync_cmd {
  TSDB_SYNC_CMD_FILE,
  TSDB_SYNC_CMD_CACHE,
  TSDB_SYNC_CMD_CREATE,
  TSDB_SYNC_CMD_REMOVE,
};

enum _meter_state {
  TSDB_METER_STATE_READY       = 0x00,
  TSDB_METER_STATE_INSERT      = 0x01,
  TSDB_METER_STATE_IMPORTING   = 0x02,
  TSDB_METER_STATE_UPDATING    = 0x04,
  TSDB_METER_STATE_DELETING    = 0x10,
  TSDB_METER_STATE_DELETED     = 0x18,
};

typedef struct {
  int64_t offset : 48;
  int64_t length : 16;
} SMeterObjHeader;

typedef struct {
  int64_t len;
  char    data[];
} SData;

typedef struct {
  int       vnode;
  SVnodeCfg cfg;
  // SDiskDesc   tierDisk[TSDB_MAX_TIER];
  SVPeerDesc          vpeers[TSDB_VNODES_SUPPORT];
  SVnodeStatisticInfo vnodeStatistic;
  char                selfIndex;
  char                status;
  char                accessState;  // Vnode access state, Readable/Writable
  char                syncStatus;
  char                commitInProcess;
  pthread_t           commitThread;
  TSKEY               firstKey;  // minimum key uncommitted, it may be smaller than
  // commitFirstKey
  TSKEY commitFirstKey;  // minimum key for a commit file, it shall be
  // xxxx00000, calculated from fileId
  TSKEY commitLastKey;  // maximum key for a commit file, it shall be xxxx99999,
  // calculated fromm fileId
  int   commitFileId;
  TSKEY lastCreate;
  TSKEY lastRemove;
  TSKEY lastKey;  // last key for the whole vnode, updated by every insert
  // operation
  uint64_t version;

  int   streamRole;
  int   numOfStreams;
  void *streamTimer;

  TSKEY           lastKeyOnFile;  // maximum key on the last file, is shall be xxxx99999
  int             fileId;
  int             badFileId;
  int             numOfFiles;
  int             maxFiles;
  int             maxFile1;
  int             maxFile2;
  int             nfd;  // temp head file FD
  int             hfd;  // head file FD
  int             lfd;  // last file FD
  int             tfd;  // temp last file FD
  int             dfd;  // data file FD
  int64_t         dfSize;
  int64_t         lfSize;
  uint64_t *      fmagic;  // hold magic number for each file
  char            cfn[TSDB_FILENAME_LEN];
  char            nfn[TSDB_FILENAME_LEN];
  char            lfn[TSDB_FILENAME_LEN];  // last file name
  char            tfn[TSDB_FILENAME_LEN];  // temp last file name
  pthread_mutex_t vmutex;

  int             logFd;
  char *          pMem;
  char *          pWrite;
  pthread_mutex_t logMutex;
  char            logFn[TSDB_FILENAME_LEN];
  char            logOFn[TSDB_FILENAME_LEN];
  int64_t         mappingSize;
  int64_t         mappingThreshold;

  void *         commitTimer;
  void **        meterList;
  void *         pCachePool;
  void *         pQueue;
  pthread_t      thread;
  int            peersOnline;
  int            shellConns;
  int            meterConns;
  struct _qinfo *pQInfoList;

  TAOS *           dbConn;
  SMeterObjHeader *meterIndex;
} SVnodeObj;

typedef struct SColumn {
  short colId;
  short bytes;
  char  type;
} SColumn;

typedef struct _meter_obj {
  uint64_t uid;
  char     meterId[TSDB_METER_ID_LEN];
  int      sid;
  short    vnode;
  short    numOfColumns;
  short    bytesPerPoint;
  short    maxBytes;
  int32_t  pointsPerBlock;
  int32_t  pointsPerFileBlock;
  int      freePoints;
  TSKEY    lastKey;        // updated by insert operation
  TSKEY    lastKeyOnFile;  // last key on file, updated by commit action
  TSKEY    timeStamp;      // delete or added time
  uint64_t commitCount;
  int32_t  sversion;
  short    sqlLen;
  char     searchAlgorithm : 4;
  char     compAlgorithm : 4;
  char     status;  // 0: ok, 1: stop stream computing

  char     reserved[16];
  int      state;
  int      numOfQueries;
  char *   pSql;
  void *   pStream;
  void *   pCache;
  SColumn *schema;
} SMeterObj;

typedef struct {
  char     type;
  char     pversion;  // protocol version
  char     action;    // insert, import, delete, update
  int32_t  sversion;  // only for insert
  int32_t  sid;
  int32_t  len;
  uint64_t lastVersion;  // latest version
  char     cont[];
} SVMsgHeader;

/*
 * the value of QInfo.signature is used to denote that a query is executing, it
 * isn't safe
 * to release QInfo yet.
 * The release operations will be blocked in a busy-waiting until the query
 * operation reach a safepoint.
 * Then it will reset the signature in a atomic operation, followed by release
 * operation.
 * Only the QInfo.signature == QInfo, this structure can be released safely.
 */
#define TSDB_QINFO_QUERY_FLAG 0x1
#define TSDB_QINFO_RESET_SIG(x) ((x)->signature = (uint64_t)(x))
#define TSDB_QINFO_SET_QUERY_FLAG(x) \
  __sync_val_compare_and_swap(&((x)->signature), (uint64_t)(x), TSDB_QINFO_QUERY_FLAG);

// live lock: wait for query reaching a safe-point, release all resources
// belongs to this query
#define TSDB_WAIT_TO_SAFE_DROP_QINFO(x)                                                       \
  {                                                                                           \
    while (__sync_val_compare_and_swap(&((x)->signature), (x), 0) == TSDB_QINFO_QUERY_FLAG) { \
      taosMsleep(1);                                                                          \
    }                                                                                         \
  }

struct tSQLBinaryExpr;

typedef struct SColumnFilter {
  SColumnFilterMsg data;
  int16_t          colIdx;
  int16_t          colIdxInBuf;

  /*
   * 0: denotes if its is required in the first round of scan of data block
   * 1: denotes if its is required in the secondary scan
   */
  int16_t req[2];
} SColumnFilter;

typedef bool (*__filter_func_t)(SColumnFilter *pFilter, char *val1, char *val2);

typedef struct SColumnFilterInfo {
  SColumnFilter   pFilter;
  int16_t         elemSize;  // element size in pData
  __filter_func_t fp;        // filter function
  char *          pData;     // raw data, as the input for filter function
} SColumnFilterInfo;

typedef struct {
  short       numOfCols;
  SOrderVal   order;
  char        keyIsMet;  // if key is met, it will be set
  char        over;
  int         fileId;  // only for query in file
  int         hfd;     // only for query in file, head file handle
  int         dfd;     // only for query in file, data file handle
  int         lfd;     // only for query in file, last file handle
  SCompBlock *pBlock;  // only for query in file
  SField **   pFields;
  int         numOfBlocks;      // only for query in file
  int         blockBufferSize;  // length of pBlock buffer
  int         currentSlot;
  int         firstSlot;
  int         slot;
  int         pos;
  TSKEY       key;
  int         compBlockLen;  // only for import
  int64_t     blockId;
  TSKEY       skey;
  TSKEY       ekey;
  int64_t     nAggTimeInterval;
  char        intervalTimeUnit;  // interval data type, used for daytime revise

  int16_t numOfOutputCols;
  int16_t interpoType;
  int16_t checkBufferInLoop;  // check if the buffer is full during scan each block

  SLimitVal limit;
  int32_t   rowSize;
  int32_t   dataRowSize;  // row size of each loaded data from disk, the value is
  // used for prepare buffer
  SSqlGroupbyExpr * pGroupbyExpr;
  SSqlFunctionExpr *pSelectExpr;

  SColumnFilter *colList;

  int32_t            numOfFilterCols;
  SColumnFilterInfo *pFilterInfo;

  int64_t *defaultVal;

  TSKEY lastKey;
  // buffer info
  int64_t pointsRead;    // the number of points returned
  int64_t pointsToRead;  // maximum number of points to read
  int64_t pointsOffset;  // the number of points offset to save read data
  SData **sdata;
  SData * tsData;  // timestamp column/primary key column
} SQuery;

typedef struct {
  char spi;
  char encrypt;
  char secret[TSDB_KEY_LEN];
  char cipheringKey[TSDB_KEY_LEN];
} SConnSec;

typedef struct {
  char *          buffer;
  char *          offset;
  int             trans;
  int             bufferSize;
  pthread_mutex_t qmutex;
} STranQueue;

// internal globals
extern int        tsMeterSizeOnFile;
extern uint32_t   tsRebootTime;
extern void *     rpcQhandle;
extern void *     dmQhandle;
extern void *     queryQhandle;
extern int        tsMaxVnode;
extern int        tsOpenVnodes;
extern SVnodeObj *vnodeList;
extern void *     vnodeTmrCtrl;

// read API
extern int (*vnodeSearchKeyFunc[])(char *pValue, int num, TSKEY key, int order);

void *vnodeQueryInTimeRange(SMeterObj **pMeterObj, SSqlGroupbyExpr *pGroupbyExpr, SSqlFunctionExpr *sqlExprs,
                            SQueryMeterMsg *pQueryMsg, int *code);

void *vnodeQueryOnMultiMeters(SMeterObj **pMeterObj, SSqlGroupbyExpr *pGroupbyExpr, SSqlFunctionExpr *pSqlExprs,
                              SQueryMeterMsg *pQueryMsg, int *code);

// assistant/tool functions
SSqlGroupbyExpr *vnodeCreateGroupbyExpr(SQueryMeterMsg *pQuery, int32_t *code);

SSqlFunctionExpr *vnodeCreateSqlFunctionExpr(SQueryMeterMsg *pQuery, int32_t *code);
bool vnodeValidateExprColumnInfo(SQueryMeterMsg* pQueryMsg, SSqlFuncExprMsg* pExprMsg);

bool vnodeIsValidVnodeCfg(SVnodeCfg *pCfg);

int32_t vnodeGetResultSize(void *handle, int32_t *numOfRows);

int32_t vnodeCopyQueryResultToMsg(void *handle, char *data, int32_t numOfRows);

int64_t vnodeGetOffsetVal(void *thandle);

bool vnodeHasRemainResults(void *handle);

int vnodeRetrieveQueryResult(void *handle, int *pNum, char *argv[]);

int vnodeSaveQueryResult(void *handle, char *data);

int vnodeRetrieveQueryInfo(void *handle, int *numOfRows, int *rowSize, int16_t *timePrec);

void vnodeFreeQInfo(void *, bool);

void vnodeFreeQInfoInQueue(void *param);

bool vnodeIsQInfoValid(void *param);

int32_t vnodeConvertQueryMeterMsg(SQueryMeterMsg *pQuery);

void vnodeQueryData(SSchedMsg *pMsg);

// meter API
int vnodeOpenMetersVnode(int vnode);

void vnodeCloseMetersVnode(int vnode);

int vnodeCreateMeterObj(SMeterObj *pNew, SConnSec *pSec);

int vnodeRemoveMeterObj(int vnode, int sid);

int vnodeInsertPoints(SMeterObj *pObj, char *cont, int contLen, char source, void *, int sversion, int *numOfPoints);

int vnodeImportPoints(SMeterObj *pObj, char *cont, int contLen, char source, void *, int sversion, int *numOfPoints);

int vnodeInsertBufferedPoints(int vnode);

int vnodeSaveAllMeterObjToFile(int vnode);

int vnodeSaveMeterObjToFile(SMeterObj *pObj);

int vnodeSaveVnodeCfg(int vnode, SVnodeCfg *pCfg, SVPeerDesc *pDesc);

int vnodeSaveVnodeInfo(int vnode);

// cache API
void *vnodeOpenCachePool(int vnode);

void vnodeCloseCachePool(int vnode);

void *vnodeAllocateCacheInfo(SMeterObj *pObj);

void vnodeFreeCacheInfo(SMeterObj *pObj);

void vnodeSetCommitQuery(SMeterObj *pObj, SQuery *pQuery);

int vnodeInsertPointToCache(SMeterObj *pObj, char *pData);

int vnodeQueryFromCache(SMeterObj *pObj, SQuery *pQuery);

uint64_t vnodeGetPoolCount(SVnodeObj *pVnode);

void vnodeUpdateCommitInfo(SMeterObj *pObj, int slot, int pos, uint64_t count);

void vnodeCommitOver(SVnodeObj *pVnode);

TSKEY vnodeGetFirstKey(int vnode);

pthread_t vnodeCreateCommitThread(SVnodeObj *pVnode);

void vnodeCancelCommit(SVnodeObj *pVnode);

void vnodeCloseStream(SVnodeObj *pVnode);

void vnodeProcessCommitTimer(void *param, void *tmrId);

void vnodeSearchPointInCache(SMeterObj *pObj, SQuery *pQuery);

int vnodeAllocateCacheBlock(SMeterObj *pObj);

int vnodeFreeCacheBlock(SCacheBlock *pCacheBlock);

int vnodeIsCacheCommitted(SMeterObj *pObj);

// file API
int vnodeInitFile(int vnode);

int vnodeQueryFromFile(SMeterObj *pObj, SQuery *pQuery);

void *vnodeCommitToFile(void *param);

void *vnodeCommitMultiToFile(SVnodeObj *pVnode, int ssid, int esid);

int vnodeWriteBlockToFile(SMeterObj *pObj, SCompBlock *pBlock, SData *data[], SData *cdata[], int pointsRead);

int vnodeSearchPointInFile(SMeterObj *pObj, SQuery *pQuery);

int vnodeReadCompBlockToMem(SMeterObj *pObj, SQuery *pQuery, SData *sdata[]);

int vnodeOpenCommitFiles(SVnodeObj *pVnode, int noTempLast);

void vnodeCloseCommitFiles(SVnodeObj *pVnode);

int vnodeReadLastBlockToMem(SMeterObj *pObj, SCompBlock *pBlock, SData *sdata[]);

// vnode API
int vnodeInitPeer(int numOfThreads);

void vnodeCleanUpPeer();

int vnodeOpenPeerVnode(int vnode);

void vnodeClosePeerVnode(int vnode);

void *vnodeGetMeterPeerConnection(SMeterObj *pObj, int index);

int vnodeForwardToPeer(SMeterObj *pObj, char *msg, int msgLen, char action, int sversion);

void vnodeConfigVPeers(int vnode, int numOfPeers, SVPeerDesc peerDesc[]);

void vnodeListPeerStatus(char *buffer);

void vnodeCheckOwnStatus(SVnodeObj *pVnode);

int vnodeSaveMeterObjToFile(SMeterObj *pObj);

int vnodeRecoverFromPeer(SVnodeObj *pVnode, int fileId);

// vnodes API
int vnodeInitVnodes();

int vnodeInitStore();

void vnodeCleanUpVnodes();

int vnodeRemoveVnode(int vnode);

int vnodeCreateVnode(int vnode, SVnodeCfg *pCfg, SVPeerDesc *pDesc);

void vnodeOpenStreams(void *param, void *tmrId);

void vnodeCreateStream(SMeterObj *pObj);

void vnodeRemoveStream(SMeterObj *pObj);

// shell API
int vnodeInitShell();

void vnodeCleanUpShell();

int vnodeOpenShellVnode(int vnode);

void vnodeCloseShellVnode(int vnode);

// mgmt
int vnodeInitMgmt();

void vnodeCleanUpMgmt();

int vnodeRetrieveMissedCreateMsg(int vnode, int fd, uint64_t stime);

int vnodeRestoreMissedCreateMsg(int vnode, int fd);

int vnodeRetrieveMissedRemoveMsg(int vid, int fd, uint64_t stime);

int vnodeRestoreMissedRemoveMsg(int vnode, int fd);

int vnodeProcessBufferedCreateMsgs(int vnode);

int vnodeSendVpeerCfgMsg(int vnode);

int vnodeSendMeterCfgMsg(int vnode, int sid);

int vnodeMgmtConns();

// commit
int vnodeInitCommit(int vnode);

void vnodeCleanUpCommit(int vnode);

int vnodeRenewCommitLog(int vnode);

void vnodeRemoveCommitLog(int vnode);

int vnodeWriteToCommitLog(SMeterObj *pObj, char action, char *cont, int contLen, int sversion);

extern int (*vnodeProcessAction[])(SMeterObj *, char *, int, char, void *, int, int *);

extern int (*pCompFunc[])(const char *const input, int inputSize, const int elements, char *const output,
                          int outputSize, char algorithm, char *const buffer, int bufferSize);

extern int (*pDecompFunc[])(const char *const input, int compressedSize, const int elements, char *const output,
                            int outputSize, char algorithm, char *const buffer, int bufferSize);

// global variable and APIs provided by mgmt
extern char          mgmtStatus;
extern char          mgmtDirectory[];
extern const int16_t vnodeFileVersion;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODE_H
