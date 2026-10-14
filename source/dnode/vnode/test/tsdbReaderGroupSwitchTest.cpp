/* Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include <gtest/gtest.h>
#include <algorithm>
#include <memory>
#include <set>
#include <tuple>
#include <vector>
#include "stub.h"
extern "C" {
#include "meta.h"
#include "tlrucache.h"
#include "tmsgcb.h"
#include "tsdb.h"
#include "tsdbDataFileRW.h"
#include "tsdbReadUtil.h"
#include "tsdbSttFileRW.h"
#include "vnode.h"
#include "vnodeInt.h"
int32_t tsdbReaderSuspend2(STsdbReader*);
int32_t sttRowInfoDeepCopy(SSttTableRowsInfo*, SSttTableRowsInfo*, int32_t);
int32_t tLDataIterOpen2(SLDataIter*, SSttFileReader*, int32_t, int8_t, SMergeTreeConf*, SSttBlockLoadInfo*,
                        SSttKeyRange*, int64_t*, const char*, bool);
int32_t tLDataIterNextRow(SLDataIter*, const char*, bool*);
}
SDmNotifyHandle dmNotifyHdl = {};
namespace {
using Row = std::tuple<uint64_t, int64_t, int64_t, int64_t>;

TEST(TsdbScanInfoResetTest, PerTableRangeDoesNotChangeDefaultTimestamp) {
  std::unique_ptr<SSHashObj, decltype(&tSimpleHashCleanup)> tables(
      tSimpleHashInit(2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT)), tSimpleHashCleanup);
  ASSERT_NE(nullptr, tables);
  STableBlockScanInfo infos[2] = {};
  for (uint64_t uid = 0; uid < 2; ++uid) {
    auto* info = &infos[uid];
    ASSERT_EQ(0, tSimpleHashPut(tables.get(), &uid, sizeof(uid), &info, sizeof(info)));
  }
  int32_t position = 0;
  auto**  first = static_cast<STableBlockScanInfo**>(tSimpleHashIterate(tables.get(), nullptr, &position));
  ASSERT_NE(nullptr, first);
  (*first)->hasTableRange = true;
  (*first)->window = {10, 20};
  auto** second = static_cast<STableBlockScanInfo**>(tSimpleHashIterate(tables.get(), first, &position));
  ASSERT_NE(nullptr, second);
  const SBlockLoadSuppInfo supp = {};
  for (int32_t step : {1, -1}) {
    ASSERT_EQ(0, resetAllDataBlockScanInfo(tables.get(), 100, step, &supp));
    EXPECT_EQ(step > 0 ? 9 : 21, (*first)->lastProcKey.ts);
    EXPECT_EQ(step > 0 ? 10 : 20, (*first)->sttKeyInfo.nextProcKey.ts);
    EXPECT_EQ(100, (*second)->lastProcKey.ts);
    EXPECT_EQ(100 + step, (*second)->sttKeyInfo.nextProcKey.ts);
  }
}

SSchema              schemas[3] = {};
STsdbReadSnap*       snapshot = nullptr;
_query_reseek_func_t snapshotReseek = nullptr;
std::vector<void*>   allocations;
int32_t              schemaMaybe(SMeta*, tb_uid_t, int32_t, int, STSchema** out) {
  *out = tBuildTSchema(schemas, 3, 1);
  return *out ? 0 : terrno;
}
int32_t schemaEx(SMeta*, tb_uid_t, tb_uid_t, int32_t, STSchema** out) { return schemaMaybe(nullptr, 0, 0, 0, out); }
int32_t colCmpr(SMeta*, tb_uid_t, SHashObj** out) {
  *out = nullptr;
  return 0;
}
int32_t tableInfo(SMeta*, int64_t uid, SMetaInfo* out, SMetaReader*) {
  *out = {};
  out->uid = uid;
  out->suid = 1;
  return 0;
}
int32_t cacheDel(STsdb*, tb_uid_t, tb_uid_t, TSKEY, TSKEY) { return 0; }
int64_t earliest(STsdb*) { return 0; }
int32_t takeSnap(STsdbReader*, _query_reseek_func_t reseek, STsdbReadSnap** out, const char*) {
  snapshotReseek = reseek;
  *out = snapshot;
  return 0;
}
void  releaseSnap(STsdbReader*, STsdbReadSnap*, bool) {}
void  poolRef(SVBufPool*) {}
void  poolUnref(SVBufPool*, bool) {}
void* poolAlloc(SVBufPool*, int size) {
  void* p = taosMemoryCalloc(1, size);
  allocations.push_back(p);
  return p;
}
int32_t allocateDisk(STsdb*, const char*, int32_t, SDiskID* id) {
  *id = {};
  return 0;
}
Stub*   hashStub = nullptr;
int     hashInsertCalls = 0;
int32_t failSecondHashPut(SSHashObj* hash, const void* key, size_t keyLen, const void* data, size_t dataLen) {
  if (++hashInsertCalls == 2) return TSDB_CODE_OUT_OF_MEMORY;
  hashStub->reset(tSimpleHashPut);
  const int32_t code = tSimpleHashPut(hash, key, keyLen, data, dataLen);
  hashStub->set(tSimpleHashPut, failSecondHashPut);
  return code;
}

Stub*                      ioStub = nullptr;
int                        opens = 0, reads = 0, decodes = 0, physicalOpens = 0;
bool                       failOpen = false, failRead = false;
int                        failDecodeCall = 0, brinFailureStage = 0;
std::set<SDataFileReader*> liveFiles;
size_t                     peakFiles = 0;
TdFilePtr                  countedPhysicalOpen(const char* path, int32_t flags) {
  ++physicalOpens;
  ioStub->reset(taosOpenFile);
  TdFilePtr fd = taosOpenFile(path, flags);
  ioStub->set(taosOpenFile, countedPhysicalOpen);
  return fd;
}
int32_t countedOpen(const char* names[], const SDataFileReaderConfig* cfg, SDataFileReader** out) {
  ++opens;
  if (failOpen) return TSDB_CODE_OUT_OF_MEMORY;
  ioStub->reset(tsdbDataFileReaderOpen);
  int32_t code = tsdbDataFileReaderOpen(names, cfg, out);
  ioStub->set(tsdbDataFileReaderOpen, countedOpen);
  if (code == 0) {
    liveFiles.insert(*out);
    peakFiles = (std::max)(peakFiles, liveFiles.size());
  }
  return code;
}
void countedClose(SDataFileReader** file) {
  liveFiles.erase(*file);
  ioStub->reset(tsdbDataFileReaderClose);
  tsdbDataFileReaderClose(file);
  ioStub->set(tsdbDataFileReaderClose, countedClose);
}
int     lowOpenCalls = 0;
int32_t failSecondFileOpen(const char* path, STsdb* tsdb, int32_t flags, STsdbFD** out, int32_t lcn) {
  if (++lowOpenCalls == 2) return TSDB_CODE_OUT_OF_MEMORY;
  ioStub->reset(tsdbOpenFile);
  int32_t code = tsdbOpenFile(path, tsdb, flags, out, lcn);
  ioStub->set(tsdbOpenFile, failSecondFileOpen);
  return code;
}
int32_t countedRead(STsdbFD* fd, int64_t offset, int64_t size, SBuffer* buffer, int64_t hint, SEncryptData* encrypt) {
  ++reads;
  if (failRead) return TSDB_CODE_OUT_OF_MEMORY;
  ioStub->reset(tsdbReadFileToBuffer);
  int32_t code = tsdbReadFileToBuffer(fd, offset, size, buffer, hint, encrypt);
  ioStub->set(tsdbReadFileToBuffer, countedRead);
  return code;
}
int32_t countedDecode(void* input, SCompressInfo* info, SBuffer* output, SBuffer* assist) {
  ++decodes;
  if (failDecodeCall > 0 && decodes == failDecodeCall) return TSDB_CODE_OUT_OF_MEMORY;
  ioStub->reset(tDecompressDataToBuffer);
  int32_t code = tDecompressDataToBuffer(input, info, output, assist);
  ioStub->set(tDecompressDataToBuffer, countedDecode);
  return code;
}

int     brinBlockReads = 0;
int32_t countedBrinBlock(SDataFileReader* file, const SBrinBlk* description, SBrinBlock* block) {
  ++brinBlockReads;
  ioStub->reset(tsdbDataFileReadBrinBlock);
  int32_t code = tsdbDataFileReadBrinBlock(file, description, block);
  ioStub->set(tsdbDataFileReadBrinBlock, countedBrinBlock);
  return code;
}

int32_t failBrinPayload(SDataFileReader* file, const SBrinBlk* description, SBrinBlock* block) {
  ioStub->reset(tsdbDataFileReadBrinBlock);
  failRead = brinFailureStage == 0;
  decodes = 0;
  failDecodeCall = brinFailureStage == 1 ? 8 : 0;
  int32_t code = tsdbDataFileReadBrinBlock(file, description, block);
  failRead = false;
  failDecodeCall = 0;
  ioStub->set(tsdbDataFileReadBrinBlock, failBrinPayload);
  return code;
}

int32_t truncateBrinTimeColumn(SDataFileReader* file, const SBrinBlk* description, SBrinBlock* block) {
  ioStub->reset(tsdbDataFileReadBrinBlock);
  int32_t code = tsdbDataFileReadBrinBlock(file, description, block);
  ioStub->set(tsdbDataFileReadBrinBlock, truncateBrinTimeColumn);
  if (code == 0) block->lastKeyTimestamps.size -= sizeof(int64_t);
  return code;
}

int                   summaryDirectorySize = 0;
std::vector<SBrinBlk> summaryDirectoryEntries;
TBrinBlkArray         summaryDirectory = {};
int32_t               paddedBrinDirectory(SDataFileReader* file, const TBrinBlkArray** out) {
  ioStub->reset(tsdbDataFileReadBrinBlk);
  int32_t code = tsdbDataFileReadBrinBlk(file, out);
  ioStub->set(tsdbDataFileReadBrinBlk, paddedBrinDirectory);
  if (code != 0) return code;
  summaryDirectoryEntries.assign((*out)->data, (*out)->data + (*out)->size);
  SBrinBlk unused = {};
  unused.minTbid.suid = unused.maxTbid.suid = 2;
  while (summaryDirectoryEntries.size() < static_cast<size_t>(summaryDirectorySize)) {
    unused.dp->offset = 1000000 + summaryDirectoryEntries.size();
    summaryDirectoryEntries.push_back(unused);
  }
  summaryDirectory = **out;
  summaryDirectory.data = summaryDirectoryEntries.data();
  summaryDirectory.size = summaryDirectoryEntries.size();
  *out = &summaryDirectory;
  return 0;
}
size_t summaryAllocationSize = 0;
int    summaryAllocationFailures = 0;
void*  failSummaryAllocation(int64_t size) {
  if (size == summaryAllocationSize) {
    ++summaryAllocationFailures;
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  ioStub->reset(taosMemMalloc);
  void* result = taosMemoryMalloc(size);
  ioStub->set(taosMemMalloc, failSummaryAllocation);
  return result;
}

Stub*                     sttStub = nullptr;
std::set<SSttFileReader*> liveSttFiles;
int                       sttOpens = 0, sttDirectories = 0, sttStatistics = 0, sttGroupTombs = 0;
size_t                    peakSttFiles = 0;
int32_t                   countedSttOpen(const char* name, const SSttFileReaderConfig* cfg, SSttFileReader** out) {
  ++sttOpens;
  sttStub->reset(tsdbSttFileReaderOpen);
  int32_t code = tsdbSttFileReaderOpen(name, cfg, out);
  sttStub->set(tsdbSttFileReaderOpen, countedSttOpen);
  if (code == 0) {
    liveSttFiles.insert(*out);
    peakSttFiles = (std::max)(peakSttFiles, liveSttFiles.size());
  }
  return code;
}
void countedSttClose(SSttFileReader** file) {
  liveSttFiles.erase(*file);
  sttStub->reset(tsdbSttFileReaderClose);
  tsdbSttFileReaderClose(file);
  sttStub->set(tsdbSttFileReaderClose, countedSttClose);
}
int     sttTombLoads = 0;
int32_t countedSttTombs(STsdbReader* reader, SSttFileReader* file, SSttBlockLoadInfo* info) {
  ++sttTombLoads;
  return loadSttTombDataForAll(reader, file, info);
}
int32_t countedSttDirectory(SSttFileReader* file, const TSttBlkArray** blocks) {
  ++sttDirectories;
  sttStub->reset(tsdbSttFileReadSttBlk);
  int32_t code = tsdbSttFileReadSttBlk(file, blocks);
  sttStub->set(tsdbSttFileReadSttBlk, countedSttDirectory);
  return code;
}
int32_t countedSttStatistics(SSttFileReader* file, const SStatisBlk* description, STbStatisBlock* block) {
  ++sttStatistics;
  sttStub->reset(tsdbSttFileReadStatisBlock);
  int32_t code = tsdbSttFileReadStatisBlock(file, description, block);
  sttStub->set(tsdbSttFileReadStatisBlock, countedSttStatistics);
  return code;
}
int32_t countedGroupSttTombs(STsdbReader* reader, SSttFileReader* file, SSttBlockLoadInfo* info) {
  ++sttGroupTombs;
  sttStub->reset(loadSttTombDataForAll);
  int32_t code = loadSttTombDataForAll(reader, file, info);
  sttStub->set(loadSttTombDataForAll, countedGroupSttTombs);
  return code;
}
int     sttDataReads = 0, sttPhysicalReads = 0;
bool    sttDataActive = false;
int32_t countedSttPhysicalRead(STsdbFD* fd, int64_t offset, int64_t size, SBuffer* buffer, int64_t hint,
                               SEncryptData* encrypt) {
  if (sttDataActive) ++sttPhysicalReads;
  sttStub->reset(tsdbReadFileToBuffer);
  int32_t code = tsdbReadFileToBuffer(fd, offset, size, buffer, hint, encrypt);
  sttStub->set(tsdbReadFileToBuffer, countedSttPhysicalRead);
  return code;
}
int32_t countedSttData(SSttFileReader* file, const SSttBlk* block, SBlockData* data, STSchema* schema, int16_t* cols,
                       int32_t numCols) {
  ++sttDataReads;
  sttStub->reset(tsdbSttFileReadBlockDataByColumn);
  sttDataActive = true;
  int32_t code = tsdbSttFileReadBlockDataByColumn(file, block, data, schema, cols, numCols);
  sttDataActive = false;
  sttStub->set(tsdbSttFileReadBlockDataByColumn, countedSttData);
  return code;
}
int     arrayDupCalls = 0, failArrayDupAt = 0;
SArray* failStatsArrayDup(const SArray* src, __array_item_dup_fn_t fn) {
  if (++arrayDupCalls == failArrayDupAt) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  sttStub->reset(taosArrayDup);
  SArray* out = taosArrayDup(src, fn);
  sttStub->set(taosArrayDup, failStatsArrayDup);
  return out;
}
int        sttCacheLookups = 0;
LRUHandle* countedSttCacheLookup(SLRUCache* cache, const void* key, size_t len) {
  ++sttCacheLookups;
  sttStub->reset(taosLRUCacheLookup);
  LRUHandle* out = taosLRUCacheLookup(cache, key, len);
  sttStub->set(taosLRUCacheLookup, countedSttCacheLookup);
  return out;
}
int                 lruInsertAllocations = 0, lruPublicationDestructions = 0;
_taos_lru_deleter_t publicationDeleter = nullptr;
void*               failLruEntryAllocation(int64_t, int64_t) {
  ++lruInsertAllocations;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}
void countedPublicationDelete(const void* key, size_t keyLen, void* value, void* ud) {
  ++lruPublicationDestructions;
  publicationDeleter(key, keyLen, value, ud);
}
LRUStatus failRealSttCacheInsertion(SLRUCache* cache, const void* key, size_t keyLen, void* value, size_t charge,
                                    _taos_lru_deleter_t deleter, _taos_lru_overwriter_t overwriter, LRUHandle** handle,
                                    LRUPriority priority, void* ud) {
  EXPECT_EQ(6, arrayDupCalls);  // The independent statistics copy was fully constructed before insertion.
  publicationDeleter = deleter;
  sttStub->reset(taosLRUCacheInsert);
  sttStub->set(taosMemCalloc, failLruEntryAllocation);
  LRUStatus result =
      taosLRUCacheInsert(cache, key, keyLen, value, charge, countedPublicationDelete, overwriter, handle, priority, ud);
  sttStub->reset(taosMemCalloc);
  sttStub->set(taosLRUCacheInsert, failRealSttCacheInsertion);
  EXPECT_EQ(TAOS_LRU_STATUS_FAIL, result);
  return result;
}
int   pkAllocCalls = 0, failPkAllocAt = 0;
void* failStatsPkAlloc(int64_t size) {
  if (size == 8 && ++pkAllocCalls == failPkAllocAt) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  sttStub->reset(taosMemMalloc);
  void* out = taosMemoryMalloc(size);
  sttStub->set(taosMemMalloc, failStatsPkAlloc);
  return out;
}
int32_t failStatsFileOpen(const char*, const SSttFileReaderConfig*, SSttFileReader** out) {
  *out = nullptr;
  return TSDB_CODE_OUT_OF_MEMORY;
}
int32_t failAfterSttStatsDecode(SSttFileReader* file, const SStatisBlk* description, STbStatisBlock* block) {
  sttStub->reset(tsdbSttFileReadStatisBlock);
  int32_t code = tsdbSttFileReadStatisBlock(file, description, block);
  sttStub->set(tsdbSttFileReadStatisBlock, failAfterSttStatsDecode);
  return code == 0 ? TSDB_CODE_OUT_OF_MEMORY : code;
}
int32_t failSttDirectory(SSttFileReader*, const TSttBlkArray**) { return TSDB_CODE_OUT_OF_MEMORY; }
int32_t failSttStatistics(SSttFileReader*, const TStatisBlkArray**) { return TSDB_CODE_OUT_OF_MEMORY; }
int32_t failSttTombs(SSttFileReader*, const TTombBlkArray**) { return TSDB_CODE_OUT_OF_MEMORY; }
int32_t failAfterSttTombDecode(SSttFileReader* file, const STombBlk* description, STombBlock* block) {
  sttStub->reset(tsdbSttFileReadTombBlock);
  int32_t code = tsdbSttFileReadTombBlock(file, description, block);
  sttStub->set(tsdbSttFileReadTombBlock, failAfterSttTombDecode);
  return code == 0 ? TSDB_CODE_OUT_OF_MEMORY : code;
}
int32_t failSttLoadInfo(STSchema*, int16_t*, int32_t, SSttBlockLoadInfo**) { return TSDB_CODE_OUT_OF_MEMORY; }

enum Path { Data, PartialData, Stt, Mem, IMem, Merged };
class TsdbReaderGroupSwitchTest : public ::testing::TestWithParam<std::tuple<Path, int>> {
 protected:
  // Metadata, snapshot acquisition, vnode arena allocation and last-cache invalidation
  // are isolated. Row/tomb insertion, file writers and all Reader data paths are real.
  void SetUp() override {
    failOpen = failRead = false;
    failDecodeCall = 0;
    liveFiles.clear();
    peakFiles = 0;
    char path[] = "/tmp/tsdb-reader-group-XXXXXX";
    ASSERT_NE(nullptr, mkdtemp(path));
    path_ = path;
    tsdb_.path = const_cast<char*>(path_.c_str());
    vnode_.pTsdb = &tsdb_;
    tsdb_.pVnode = &vnode_;
    vnode_.config.tsdbCfg.maxRows = 128;
    vnode_.config.tsdbCfg.minRows = 1;
    vnode_.config.tsdbCfg.slLevel = 5;
    vnode_.config.tsdbCfg.days = 1;
    vnode_.config.tsdbPageSize = 4096;
    tsdb_.keepCfg.days = 1;
    vnode_.state.applied = 10;
    for (int i = 0; i < 3; ++i) {
      schemas[i] = {};
      schemas[i].colId = i + 1;
      schemas[i].type = i == 0 ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_BIGINT;
      schemas[i].bytes = 8;
      columns_[i].colId = i + 1;
      columns_[i].type = schemas[i].type;
      columns_[i].bytes = 8;
      slots_[i] = i;
    }
    schemas[1].flags = COL_IS_KEY;
    columns_[1].pk = true;
    schema_ = tBuildTSchema(schemas, 3, 1);
    ASSERT_NE(nullptr, schema_);
    snap_.pfSetArray = &files_;
    snapshot = &snap_;
    stub_.set(metaGetTbTSchemaMaybeNull, schemaMaybe);
    stub_.set(metaGetTbTSchemaEx, schemaEx);
    stub_.set(metaGetColCmpr, colCmpr);
    stub_.set(metaGetInfo, tableInfo);
    stub_.set(tsdbCacheDel, cacheDel);
    stub_.set(tsdbGetEarliestTs, earliest);
    stub_.set(tsdbTakeReadSnap2, takeSnap);
    stub_.set(tsdbUntakeReadSnap2, releaseSnap);
    stub_.set(vnodeBufPoolRef, poolRef);
    stub_.set(vnodeBufPoolUnRef, poolUnref);
    stub_.set(vnodeBufPoolMalloc, poolAlloc);
    stub_.set(vnodeBufPoolMallocAligned, poolAlloc);
    stub_.set(tsdbAllocateDisk, allocateDisk);
  }
  void TearDown() override {
    tsdbReaderClose2(reader_);
    tsdbMemTableDestroy(snap_.pMem, false);
    tsdbMemTableDestroy(snap_.pIMem, false);
    for (void* p : allocations) taosMemoryFree(p);
    allocations.clear();
    for (int i = 0; i < files_.size; ++i) tsdbTFileSetClear(&files_.data[i]);
    TARRAY2_DESTROY(&files_, nullptr);
    taosMemoryFree(schema_);
    taosRemoveDir(path_.c_str());
    snapshot = nullptr;
  }
  SQueryTableDataCond cond(int order = TSDB_ORDER_ASC) {
    SQueryTableDataCond c = {};
    c.order = order;
    c.numOfCols = 3;
    c.colList = columns_;
    c.pSlotList = slots_;
    c.type = TIMEWINDOW_RANGE_CONTAINED;
    c.twindows = {0, 200};
    c.startVersion = -1;
    c.endVersion = 5;
    c.suid = 1;
    return c;
  }
  std::vector<STableScanRange> ranges() {
    std::vector<STableScanRange> r(2);
    r[0].table.uid = 7;
    r[0].window = {10, 19};
    r[0].valid = true;
    r[1].table.uid = 8;
    r[1].window = {100, 109};
    r[1].valid = true;
    return r;
  }
  SRow* makeRow(int64_t ts, int64_t pk, int64_t value) {
    SArray* vals = taosArrayInit(3, sizeof(SColVal));
    int64_t values[] = {ts, pk, value};
    char    key[32];
    snprintf(key, sizeof(key), "%08lld", (long long)pk);
    for (int i = 0; i < 3; ++i) {
      SColVal v = {};
      v.cid = i + 1;
      v.value.type = schemas[i].type;
      v.value.val = values[i];
      if (IS_VAR_DATA_TYPE(v.value.type)) {
        v.value.pData = reinterpret_cast<uint8_t*>(key);
        v.value.nData = strlen(key);
      }
      taosArrayPush(vals, &v);
    }
    SRow*             row = nullptr;
    SRowBuildScanInfo scan = {};
    EXPECT_EQ(0, tRowBuild(vals, schema_, &row, &scan));
    taosArrayDestroy(vals);
    return row;
  }
  void seed(Path path, bool history = false, int fid = 0, int64_t shift = 0, uint64_t secondUid = 8,
            bool withRows = true, STFileSet* targetFileset = nullptr, int64_t tombShift = 0) {
    if (path == Mem || path == Merged) ASSERT_EQ(0, tsdbMemTableCreate(&tsdb_, &snap_.pMem));
    if (path == IMem || path == Merged) ASSERT_EQ(0, tsdbMemTableCreate(&tsdb_, &snap_.pIMem));
    SDataFileWriter* data = nullptr;
    SSttFileWriter*  stt = nullptr;
    TFileOpArray     ops = {};
    if (path == Data || path == PartialData || path == Merged) {
      SDataFileWriterConfig cfg = {};
      cfg.tsdb = &tsdb_;
      cfg.maxRow = 128;
      cfg.szPage = 4096;
      cfg.cid = fid * 10 + 1;
      cfg.fid = fid;
      ASSERT_EQ(0, tsdbDataFileWriterOpen(&cfg, &data));
    }
    if (path == Stt || path == Merged) {
      SSttFileWriterConfig cfg = {};
      cfg.tsdb = &tsdb_;
      cfg.maxRow = 128;
      cfg.szPage = 4096;
      cfg.cid = fid * 10 + 2;
      cfg.fid = fid;
      ASSERT_EQ(0, tsdbSttFileWriterOpen(&cfg, &stt));
    }
    for (uint64_t uid : {uint64_t{7}, secondUid}) {
      if (!withRows) break;
      const int64_t base = (uid == 7 ? 10 : 100) + shift;
      SBlockData    block = {};
      TABLEID       id = {};
      id.suid = 1;
      id.uid = uid;
      ASSERT_EQ(0, tBlockDataInit(&block, &id, schema_, nullptr, 0));
      for (int64_t ts = base - 1; ts <= base + 10; ++ts) {
        for (int pk = 1; pk <= 2; ++pk) {
          for (int version : {1, 3, 9}) {
            if (version != 1 && (!history || ts != base + 2 || pk != 1)) continue;
            SRow* row = makeRow(ts, pk, ts * 10 + pk + (version == 1 ? 0 : version * 1000));
            ASSERT_NE(nullptr, row);
            TSDBROW input = {};
            input.type = TSDBROW_ROW_FMT;
            input.version = version;
            input.pTSRow = row;
            ASSERT_EQ(0, tBlockDataAppendRow(&block, &input, schema_, uid));
            for (SMemTable* mem : {snap_.pMem, snap_.pIMem}) {
              if (!mem) continue;
              tsdb_.mem = mem;
              SSubmitTbData submit = {};
              submit.suid = 1;
              submit.uid = uid;
              submit.sver = 1;
              submit.aRowP = taosArrayInit(1, sizeof(SRow*));
              const bool newerMem = path == Merged && mem == snap_.pMem && version == 3;
              SRow*      memRow = newerMem ? makeRow(ts, pk, ts * 10 + pk + 4000) : row;
              taosArrayPush(submit.aRowP, &memRow);
              int32_t affected = 0;
              ASSERT_EQ(0, tsdbInsertTableData(&tsdb_, newerMem ? 4 : version, &submit, &affected));
              taosArrayDestroy(submit.aRowP);
              if (newerMem) taosMemoryFree(memRow);
            }
            taosMemoryFree(row);
          }
          // Separate sentinel blocks exercise whole clean blocks as well as partial blocks.
          if (data && path == Data && (ts == base - 1 || ts == base + 9) && pk == 2) {
            ASSERT_EQ(0, tsdbDataFileWriteBlockData(data, &block));
            tBlockDataClear(&block);
            ASSERT_EQ(0, tBlockDataInit(&block, &id, schema_, nullptr, 0));
          }
        }
      }
      if (data) ASSERT_EQ(0, tsdbDataFileWriteBlockData(data, &block));
      if (stt) ASSERT_EQ(0, tsdbSttFileWriteBlockData(stt, &block));
      tBlockDataDestroy(&block);
    }
    if (history) {
      for (uint64_t uid : {uint64_t{7}, secondUid}) {
        const int64_t ts = (uid == 7 ? 15 : 105) + shift + tombShift;
        STombRecord   tomb = {};
        tomb.suid = 1;
        tomb.uid = uid;
        tomb.version = 4;
        tomb.skey = ts;
        tomb.ekey = ts;
        if (data) ASSERT_EQ(0, tsdbDataFileWriteTombRecord(data, &tomb));
        if (stt) ASSERT_EQ(0, tsdbSttFileWriteTombRecord(stt, &tomb));
        for (SMemTable* mem : {snap_.pMem, snap_.pIMem}) {
          if (!mem) continue;
          tsdb_.mem = mem;
          ASSERT_EQ(0, tsdbDeleteTableData(&tsdb_, 4, 1, uid, ts, ts, 0));
        }
      }
    }
    if (data) ASSERT_EQ(0, tsdbDataFileWriterClose(&data, 0, &ops));
    if (stt) ASSERT_EQ(0, tsdbSttFileWriterClose(&stt, 0, &ops));
    if (ops.size > 0) {
      STFileSet* fs = targetFileset;
      if (!fs) ASSERT_EQ(0, tsdbTFileSetInit(fid, &fs));
      for (int i = 0; i < ops.size; ++i) ASSERT_EQ(0, tsdbTFileSetEdit(&tsdb_, fs, &ops.data[i]));
      if (!targetFileset) ASSERT_EQ(0, TARRAY2_APPEND(&files_, fs));
    }
    TARRAY2_DESTROY(&ops, nullptr);
  }
  void seedIndexedFile(int fid, int64_t shift, int64_t cidOffset = 10, bool extremes = false) {
    SDataFileWriterConfig cfg = {};
    cfg.tsdb = &tsdb_;
    cfg.maxRow = 128;
    cfg.szPage = 4096;
    cfg.cid = fid + cidOffset;
    cfg.fid = fid;
    SDataFileWriter* writer = nullptr;
    ASSERT_EQ(0, tsdbDataFileWriterOpen(&cfg, &writer));
    for (int i = 0; i < (extremes ? 2 : 257); ++i) {
      TABLEID id = {};
      id.suid = 1;
      id.uid = i < (extremes ? 1 : 256) ? 7 : 8;
      SBlockData block = {};
      ASSERT_EQ(0, tBlockDataInit(&block, &id, schema_, nullptr, 0));
      const int64_t ts = extremes ? (i == 0 ? INT64_MIN : INT64_MAX) : shift + 10 + i;
      SRow*         row = makeRow(ts, i + 1, i);
      TSDBROW input = {};
      input.type = TSDBROW_ROW_FMT;
      input.version = 1;
      input.pTSRow = row;
      ASSERT_EQ(0, tBlockDataAppendRow(&block, &input, schema_, id.uid));
      ASSERT_EQ(0, tsdbDataFileWriteBlockData(writer, &block));
      taosMemoryFree(row);
      tBlockDataDestroy(&block);
    }
    TFileOpArray ops = {};
    ASSERT_EQ(0, tsdbDataFileWriterClose(&writer, false, &ops));
    STFileSet* fs = nullptr;
    ASSERT_EQ(0, tsdbTFileSetInit(fid, &fs));
    for (int i = 0; i < ops.size; ++i) ASSERT_EQ(0, tsdbTFileSetEdit(&tsdb_, fs, &ops.data[i]));
    ASSERT_EQ(0, TARRAY2_APPEND(&files_, fs));
    TARRAY2_DESTROY(&ops, nullptr);
  }
  std::vector<Row> read(bool oneBlock = false) {
    std::vector<Row> rows;
    for (int i = 0; i < 1000; ++i) {
      bool next = false;
      EXPECT_EQ(0, tsdbNextDataBlock2(reader_, &next));
      if (!next) return rows;
      if (static_cast<STsdbReader*>(reader_)->status.composedDataBlock)
        ++composedBlocks_;
      else
        ++directBlocks_;
      SSDataBlock* block = nullptr;
      EXPECT_EQ(0, tsdbRetrieveDataBlock2(reader_, &block));
      if (!block) return rows;
      for (int j = 0; j < block->info.rows; ++j) {
        int64_t values[3] = {};
        for (int k = 0; k < 3; ++k) {
          auto* col = static_cast<SColumnInfoData*>(taosArrayGet(block->pDataBlock, k));
          if (IS_VAR_DATA_TYPE(col->info.type)) {
            char* key = colDataGetData(col, j);
            values[k] = std::stoll(std::string(varDataVal(key), varDataLen(key)));
          } else {
            values[k] = reinterpret_cast<int64_t*>(col->pData)[j];
          }
        }
        rows.emplace_back(block->info.id.uid, values[0], values[1], values[2]);
      }
      if (oneBlock) return rows;
    }
    ADD_FAILURE() << "Reader did not reach EOF";
    return rows;
  }
  std::vector<Row> reference(int order) {
    std::vector<Row> rows;
    for (const auto& r : ranges()) {
      auto c = cond(order);
      c.twindows = r.window;
      STableKeyInfo table = r.table;
      if (reader_ == nullptr) {
        EXPECT_EQ(0, tsdbReaderOpen2(&vnode_, &c, &table, 1, nullptr, &reader_, "group-reference", nullptr));
      } else {
        EXPECT_EQ(0, tsdbSetTableList2(reader_, &table, 1));
        EXPECT_EQ(0, tsdbReaderReset2(reader_, &c));
      }
      auto part = read();
      rows.insert(rows.end(), part.begin(), part.end());
    }
    tsdbReaderClose2(reader_);
    reader_ = nullptr;
    return rows;
  }
  void expectRows(std::vector<Row> actual, std::vector<Row> expected) {
    // Storage can interleave tables by physical block position; each table's key order must be preserved.
    for (uint64_t uid : {7, 8}) {
      std::vector<Row> a, e;
      for (const auto& r : actual)
        if (std::get<0>(r) == uid) a.push_back(r);
      for (const auto& r : expected)
        if (std::get<0>(r) == uid) e.push_back(r);
      ASSERT_EQ(a.size(), e.size()) << "uid=" << uid;
      for (size_t row = 0; row < a.size(); ++row) {
        ASSERT_EQ(a[row], e[row]) << "uid=" << uid << " row=" << row;
      }
    }
    EXPECT_EQ(actual.size(), expected.size());
  }
  Stub          stub_;
  SVnode        vnode_ = {};
  STsdb         tsdb_ = {};
  STsdbReadSnap snap_ = {};
  TFileSetArray files_ = {};
  STSchema*     schema_ = nullptr;
  SColumnInfo   columns_[3] = {};
  int32_t       slots_[3] = {};
  std::string   path_;
  void*         reader_ = nullptr;
  int           directBlocks_ = 0;
  int           composedBlocks_ = 0;
};
class TsdbSttPreparationTest : public TsdbReaderGroupSwitchTest {
 protected:
  void TearDown() override {
    tMergeTreeClose(&tree_);
    destroySttBlockReader(iters_, nullptr);
    EXPECT_TRUE(liveSttFiles.empty());
    // Recover unowned files only after reporting the ownership failure in RED.
    while (!liveSttFiles.empty()) {
      SSttFileReader* file = *liveSttFiles.begin();
      countedSttClose(&file);
    }
    TsdbReaderGroupSwitchTest::TearDown();
    sttStub = nullptr;
  }
  void prepare(bool withMem = true, uint64_t secondUid = 8, bool withRows = true) {
    seed(Stt, true, 0, 0, secondUid, withRows);
    if (withMem) seed(Mem);  // Only the STT file owns tombs; mem rows have none.
    // Drive file preparation through the real merge-tree, then read only mem rows
    // through the Reader so it cannot repair a missing tomb installation itself.
    snap_.pfSetArray = &emptyFiles_;
    iters_ = taosArrayInit(1, POINTER_BYTES);
    ASSERT_NE(nullptr, iters_);
    auto c = cond();
    auto r = ranges();
    ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), r.size(), nullptr, &reader_, "stt-preparation", nullptr));
    sttTombLoads = 0;
    sttStub = &stub_;
    liveSttFiles.clear();
    stub_.set(tsdbSttFileReaderOpen, countedSttOpen);
    stub_.set(tsdbSttFileReaderClose, countedSttClose);
  }
  int32_t openMerge(uint64_t uid, STimeWindow window = {200, 210}, bool backward = false, bool cache = false,
                    SSttDataInfoForTable* dataInfo = nullptr, bool skipByStats = true) {
    auto*                reader = static_cast<STsdbReader*>(reader_);
    STableBlockScanInfo* scan = nullptr;
    int32_t              code = getTableBlockScanInfo(reader->status.pTableMap, uid, &scan, reader->idStr);
    if (code) return code;
    SMergeTreeConf c = {};
    c.pTsdb = &tsdb_;
    c.suid = 1;
    c.uid = uid;
    c.timewindow = window;
    c.backward = backward;
    c.cacheStatis = cache;
    c.skipByTableStats = skipByStats;
    c.verRange = reader->info.verRange;
    c.pSttFileBlockIterArray = iters_;
    c.pCurrentFileset = files_.data[0];
    c.pSchema = schema_;
    c.pCols = reader->suppInfo.colId;
    c.numOfCols = 3;
    mergeStart_ = scan->sttKeyInfo.nextProcKey;
    mergeStart_.ts = backward ? window.ekey : window.skey;
    if (!IS_VAR_DATA_TYPE(mergeStart_.pks[0].type) && mergeStart_.pks[0].type != TSDB_DATA_TYPE_DECIMAL) {
      mergeStart_.pks[0].val = backward ? INT64_MAX : INT64_MIN;
    }
    c.pCurRowKey = &mergeStart_;
    c.loadTombFn = countedSttTombs;
    c.pReader = reader;
    c.idstr = reader->idStr;
    tMergeTreeClose(&tree_);
    return tMergeTreeOpen2(&tree_, &c, dataInfo);
  }
  SLDataIter* sttIter() {
    return static_cast<SLDataIter*>(taosArrayGetP(static_cast<SArray*>(taosArrayGetP(iters_, 0)), 0));
  }
  std::vector<Row> freshFileRows(uint64_t uid, STimeWindow window, int64_t maxVersion = 5) {
    void* saved = reader_;
    reader_ = nullptr;
    snap_.pfSetArray = &files_;
    auto c = cond();
    c.twindows = window;
    c.endVersion = maxVersion;
    STableKeyInfo table = {};
    table.uid = uid;
    EXPECT_EQ(0, tsdbReaderOpen2(&vnode_, &c, &table, 1, nullptr, &reader_, "fresh-stt-reference", nullptr));
    auto rows = read();
    tsdbReaderClose2(reader_);
    reader_ = saved;
    snap_.pfSetArray = &emptyFiles_;
    return rows;
  }
  void expectMemRows(uint64_t uid, int64_t first, int64_t last) {
    std::vector<Row> expected;
    for (int64_t ts = first; ts <= last; ++ts) {
      if (ts == (uid == 7 ? 15 : 105)) continue;
      for (int64_t pk : {1, 2}) expected.emplace_back(uid, ts, pk, ts * 10 + pk);
    }
    expectRows(read(), expected);
  }
  SRowKey       mergeStart_ = {};
  SArray*       iters_ = nullptr;
  SMergeTree    tree_ = {};
  TFileSetArray emptyFiles_ = {};
};

class TsdbSttStatsTest : public TsdbSttPreparationTest {
 protected:
  void TearDown() override {
    tMergeTreeClose(&tree_);
    destroySttBlockReader(iters_, nullptr);
    iters_ = nullptr;
    tsdbReaderClose2(reader_);
    reader_ = nullptr;
    TsdbSttPreparationTest::TearDown();
  }
  void SetUp() override {
    TsdbSttPreparationTest::SetUp();
    static int32_t nextVgId = 1000;
    vnode_.config.vgId = nextVgId++;  // Distinct disk fixtures must not share a global cache identity.
  }
  void observe() {
    sttDataReads = sttPhysicalReads = sttStatistics = sttCacheLookups = 0;
    stub_.set(taosLRUCacheLookup, countedSttCacheLookup);
    stub_.set(tsdbReadFileToBuffer, countedSttPhysicalRead);
    stub_.set(tsdbSttFileReadBlockDataByColumn, countedSttData);
    stub_.set(tsdbSttFileReadStatisBlock, countedSttStatistics);
  }
  int rows() {
    int  count = 0;
    bool has = false;
    while (tMergeTreeNext(&tree_, &has) == 0 && has) ++count;
    return count;
  }
  void releaseIters() {
    tMergeTreeClose(&tree_);
    destroySttBlockReader(iters_, nullptr);
    iters_ = taosArrayInit(1, POINTER_BYTES);
  }
};

TEST_F(TsdbSttStatsTest, SttDataReadFailureReturnsError) {
  prepare(false, 9);
  ASSERT_EQ(0, openMerge(7, {9, 20}, false, true));
  EXPECT_EQ(25, rows());
  ASSERT_EQ(0, resetSttBlockReaderForGroup(iters_));
  ioStub = &stub_;
  failRead = true;
  stub_.set(tsdbReadFileToBuffer, countedRead);
  SBlockData block = {};
  ASSERT_EQ(0, tBlockDataCreate(&block));
  auto* description = static_cast<SSttBlk*>(taosArrayGet(sttIter()->pBlockLoadInfo->aSttBlk, 0));
  auto* reader = static_cast<STsdbReader*>(reader_);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, tsdbSttFileReadBlockDataByColumn(sttIter()->pReader, description, &block, schema_,
                                                                      reader->suppInfo.colId + 1, 2));
  tBlockDataDestroy(&block);
  stub_.reset(tsdbReadFileToBuffer);
  failRead = false;
}

TEST_F(TsdbSttStatsTest, SttStatsSkipMissingUid) {
  prepare(false, 9);
  observe();
  ASSERT_EQ(0, openMerge(8, {10, 109}));
  ASSERT_NE(nullptr, sttIter());
  const auto* blocks = sttIter()->pBlockLoadInfo->aSttBlk;
  ASSERT_GT(taosArrayGetSize(blocks), 0);
  const auto* block = static_cast<const SSttBlk*>(taosArrayGet(blocks, 0));
  EXPECT_LT(block->minUid, 8u);
  EXPECT_GT(block->maxUid, 8u);
  EXPECT_EQ(0, rows());
  EXPECT_EQ(0, sttDataReads);
  EXPECT_EQ(0, sttPhysicalReads);
  EXPECT_EQ(1, sttTombLoads);
}

TEST_F(TsdbSttStatsTest, SttStatsSkipDisjointTime) {
  prepare(false);
  observe();
  // The mixed-UID block spans both queries; per-table stats prove disjointness.
  for (bool descending : {false, true}) {
    for (auto window : {STimeWindow{21, 90}, STimeWindow{30, 98}}) {
      uint64_t uid = window.skey == 21 ? 7 : 8;
      ASSERT_EQ(0, resetSttBlockReaderForGroup(iters_));
      sttDataReads = sttPhysicalReads = 0;
      ASSERT_EQ(0, openMerge(uid, window, descending));
      EXPECT_EQ(0, rows());
      EXPECT_EQ(0, sttDataReads);
      EXPECT_EQ(0, sttPhysicalReads);
      EXPECT_EQ(descending && uid == 7, tMergeTreeIgnoreEarlierTs(&tree_));
    }
  }
}

TEST_F(TsdbSttStatsTest, SttStatsKeepEqualEndpoints) {
  prepare(false);
  observe();
  for (bool descending : {false, true}) {
    for (int64_t ts : {int64_t{9}, int64_t{20}}) {
      ASSERT_EQ(0, resetSttBlockReaderForGroup(iters_));
      sttDataReads = sttPhysicalReads = 0;
      SSttDataInfoForTable count = {};
      count.pKeyRangeList = taosArrayInit(1, sizeof(SSttKeyRange));
      ASSERT_EQ(0, openMerge(7, {ts, ts}, descending, false, &count));
      std::vector<int64_t> keys;
      bool                 has = false;
      while (tMergeTreeNext(&tree_, &has) == 0 && has) {
        STsdbRowKey key = {};
        tsdbRowGetKey(tMergeTreeGetRow(&tree_), &key);
        EXPECT_EQ(ts, key.key.ts);
        keys.push_back(key.key.pks[0].val);
      }
      EXPECT_EQ((descending ? std::vector<int64_t>{2, 1} : std::vector<int64_t>{1, 2}), keys);
      EXPECT_GT(sttDataReads, 0);
      EXPECT_GT(sttPhysicalReads, 0);
      EXPECT_EQ(24, count.numOfRows);  // Statistics preserve the writer's distinct-key count.
      EXPECT_EQ(1, taosArrayGetSize(count.pKeyRangeList));
      taosArrayDestroy(count.pKeyRangeList);
    }
  }
}

TEST_F(TsdbSttStatsTest, SttStatsRetainedAcrossCacheMissAndHit) {
  prepare(false);
  observe();
  ASSERT_EQ(0, openMerge(7, {9, 20}, false, true));
  EXPECT_EQ(25, rows());
  EXPECT_EQ(2, taosArrayGetSize(sttIter()->pBlockLoadInfo->info.pUid));
  const int loads = sttStatistics;
  EXPECT_EQ(1, sttCacheLookups);
  ASSERT_EQ(0, resetSttBlockReaderForGroup(iters_));
  ASSERT_EQ(0, openMerge(8, {99, 110}, false, true));
  EXPECT_EQ(25, rows());
  EXPECT_EQ(loads, sttStatistics);
  EXPECT_EQ(1, sttCacheLookups);  // Retained complete statistics avoid even a global lookup.
  releaseIters();
  ASSERT_EQ(0, openMerge(7, {9, 20}, false, true));
  EXPECT_EQ(25, rows());
  EXPECT_EQ(loads, sttStatistics);  // A fresh iterator gets the complete global entry.
  EXPECT_EQ(2, sttCacheLookups);
  EXPECT_EQ(2, taosArrayGetSize(sttIter()->pBlockLoadInfo->info.pUid));
}

TEST_F(TsdbSttStatsTest, SttUnknownStatsFallBackToData) {
  prepare(false);
  observe();
  ASSERT_EQ(0, openMerge(7, {9, 20}));
  ASSERT_EQ(0, resetSttBlockReaderForGroup(iters_));
  SLDataIter*       iter = sttIter();
  auto*             info = iter->pBlockLoadInfo;
  SSttTableRowsInfo saved = info->info;
  info->info = {};
  info->statisLoaded = false;
  auto*          reader = static_cast<STsdbReader*>(reader_);
  SRowKey        start = {};
  SMergeTreeConf c = {};
  c.uid = 7;
  c.suid = 1;
  c.timewindow = {9, 20};
  c.verRange = reader->info.verRange;
  c.pCurRowKey = &start;
  c.pReader = reader;
  c.pSchema = schema_;
  c.pCols = reader->suppInfo.colId;
  c.numOfCols = 3;
  c.loadTombFn = countedSttTombs;
  c.skipByTableStats = true;
  sttDataReads = sttPhysicalReads = 0;
  int64_t count = 0;
  ASSERT_EQ(0, tLDataIterOpen2(iter, iter->pReader, 2, 0, &c, info, nullptr, &count, reader->idStr, false));
  bool has = false;
  int  visible = 0;
  while (tLDataIterNextRow(iter, reader->idStr, &has) == 0 && has) ++visible;
  EXPECT_EQ(25, visible);
  EXPECT_GT(sttPhysicalReads, 0);
  EXPECT_FALSE(info->statisLoaded);
  EXPECT_EQ(0, count);
  info->info = saved;
  info->statisLoaded = true;
}

TEST_F(TsdbSttStatsTest, SttEmptyStatsAreValid) {
  prepare(false, 8, false);  // Real tomb-only file has a complete empty statistics directory.
  observe();
  ASSERT_EQ(0, openMerge(7, {9, 20}, false, true));
  EXPECT_TRUE(sttIter()->pBlockLoadInfo->statisLoaded);
  EXPECT_EQ(0, taosArrayGetSize(sttIter()->pBlockLoadInfo->info.pUid));
  EXPECT_EQ(0, rows());
  EXPECT_EQ(0, sttPhysicalReads);
  releaseIters();
  ASSERT_EQ(0, openMerge(8, {99, 110}, false, true));
  EXPECT_TRUE(sttIter()->pBlockLoadInfo->statisLoaded);
  EXPECT_EQ(0, rows());
  EXPECT_EQ(0, sttPhysicalReads);
  EXPECT_EQ(0, sttStatistics);
  EXPECT_EQ(2, sttTombLoads);
}

TEST_F(TsdbSttStatsTest, SttStatsCachePublicationFailureKeepsLocalStats) {
  prepare(false);
  observe();
  arrayDupCalls = 0;
  failArrayDupAt = 4;
  stub_.set(taosArrayDup, failStatsArrayDup);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, openMerge(7, {9, 20}, false, true));
  stub_.reset(taosArrayDup);
  ASSERT_TRUE(sttIter()->pBlockLoadInfo->statisLoaded);
  EXPECT_EQ(2, taosArrayGetSize(sttIter()->pBlockLoadInfo->info.pUid));
  int diskReads = sttStatistics;
  releaseIters();
  ASSERT_EQ(0, openMerge(8, {99, 110}, false, true));
  EXPECT_GT(sttStatistics, diskReads);  // Failed publication left no partial cache entry.
  EXPECT_EQ(25, rows());
}

TEST_F(TsdbSttStatsTest, SttStatsLruInsertionFailureOwnsPublishedCopyOnce) {
  prepare(false);
  observe();
  arrayDupCalls = failArrayDupAt = 0;
  lruInsertAllocations = lruPublicationDestructions = 0;
  stub_.set(taosArrayDup, failStatsArrayDup);
  stub_.set(taosLRUCacheInsert, failRealSttCacheInsertion);
  EXPECT_EQ(0, openMerge(7, {9, 20}, false, true));  // Cache insertion remains best effort.
  stub_.reset(taosLRUCacheInsert);
  stub_.reset(taosArrayDup);
  EXPECT_EQ(1, lruInsertAllocations);
  EXPECT_EQ(1, lruPublicationDestructions);
  ASSERT_TRUE(sttIter()->pBlockLoadInfo->statisLoaded);
  EXPECT_EQ(2, taosArrayGetSize(sttIter()->pBlockLoadInfo->info.pUid));
  EXPECT_EQ(25, rows());
  const int diskReads = sttStatistics;
  releaseIters();
  ASSERT_EQ(0, openMerge(8, {99, 110}, false, true));
  EXPECT_GT(sttStatistics, diskReads);  // Failed insertion did not leave a cache hit.
  EXPECT_EQ(25, rows());
}

TEST_F(TsdbSttStatsTest, SttUnknownStatsAreNotPublished) {
  prepare(false);
  observe();
  stub_.set(tsdbSttFileReaderOpen, failStatsFileOpen);
  // Preserve the existing file-open caller contract; unprepared stats must not be cached.
  ASSERT_EQ(0, openMerge(7, {9, 20}, false, true));
  EXPECT_FALSE(sttIter()->pBlockLoadInfo->statisLoaded);
  EXPECT_EQ(0, rows());
  releaseIters();
  stub_.set(tsdbSttFileReaderOpen, countedSttOpen);
  ASSERT_EQ(0, openMerge(7, {9, 20}, false, true));
  EXPECT_EQ(25, rows());
  EXPECT_GT(sttStatistics, 0);
  EXPECT_GT(sttPhysicalReads, 0);
}

TEST_F(TsdbSttStatsTest, SttStatsCacheCopyFailureReturnsError) {
  prepare(false);
  observe();
  ASSERT_EQ(0, openMerge(7, {9, 20}, false, true));
  const int diskReads = sttStatistics;
  releaseIters();
  arrayDupCalls = 0;
  failArrayDupAt = 2;
  stub_.set(taosArrayDup, failStatsArrayDup);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, openMerge(7, {9, 20}, false, true));
  stub_.reset(taosArrayDup);
  EXPECT_FALSE(sttIter()->pBlockLoadInfo->statisLoaded);
  EXPECT_EQ(nullptr, sttIter()->pBlockLoadInfo->info.pUid);
  EXPECT_EQ(diskReads, sttStatistics);
  releaseIters();
  ASSERT_EQ(0, openMerge(7, {9, 20}, false, true));
  EXPECT_EQ(25, rows());
  EXPECT_EQ(diskReads, sttStatistics);
}

TEST_F(TsdbSttStatsTest, SttStatsCopyPkFailurePreservesSource) {
  schemas[1].type = columns_[1].type = TSDB_DATA_TYPE_VARCHAR;
  schemas[1].bytes = columns_[1].bytes = 32;
  taosMemoryFree(schema_);
  schema_ = tBuildTSchema(schemas, 3, 1);
  prepare(false);
  ASSERT_EQ(0, openMerge(7, {0, 200}));
  auto* source = &sttIter()->pBlockLoadInfo->info;
  for (int failAt = 1; failAt <= 4; ++failAt) {
    SSttTableRowsInfo dst = {};
    pkAllocCalls = 0;
    failPkAllocAt = failAt;
    stub_.set(taosMemMalloc, failStatsPkAlloc);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, sttRowInfoDeepCopy(&dst, source, 1));
    stub_.reset(taosMemMalloc);
    EXPECT_EQ(nullptr, dst.pUid);
    EXPECT_EQ(nullptr, dst.pFirstKey);
    EXPECT_EQ(nullptr, dst.pLastKey);
    for (auto pair :
         {std::make_pair(dst.pFirstKey, source->pFirstKey), std::make_pair(dst.pLastKey, source->pLastKey)}) {
      for (int i = 0; i < taosArrayGetSize(pair.first); ++i) {
        auto* value = static_cast<SValue*>(taosArrayGet(pair.first, i));
        auto* original = static_cast<SValue*>(taosArrayGet(pair.second, i));
        if (value->pData != original->pData) taosMemoryFree(value->pData);
      }
    }
    taosArrayDestroy(dst.pUid);
    taosArrayDestroy(dst.pCount);
    taosArrayDestroy(dst.pFirstTs);
    taosArrayDestroy(dst.pLastTs);
    taosArrayDestroy(dst.pFirstKey);
    taosArrayDestroy(dst.pLastKey);
    for (int i = 0; i < 2; ++i) {
      auto* first = static_cast<SValue*>(taosArrayGet(source->pFirstKey, i));
      auto* last = static_cast<SValue*>(taosArrayGet(source->pLastKey, i));
      EXPECT_EQ("00000001", std::string(reinterpret_cast<char*>(first->pData), first->nData));
      EXPECT_EQ("00000002", std::string(reinterpret_cast<char*>(last->pData), last->nData));
    }
  }
}

TEST_F(TsdbSttStatsTest, SttLastCachePreservesDescendingEarlierSignal) {
  prepare(false);
  observe();
  ASSERT_EQ(0, openMerge(7, {21, 90}, true, false, nullptr, false));
  EXPECT_EQ(0, rows());
  EXPECT_EQ(1, sttDataReads);
  EXPECT_FALSE(tMergeTreeIgnoreEarlierTs(&tree_));  // Mixed-UID block max is newer than skey.
  ASSERT_EQ(0, resetSttBlockReaderForGroup(iters_));
  ASSERT_EQ(0, openMerge(7, {111, 120}, true, false, nullptr, false));
  EXPECT_EQ(0, rows());
  EXPECT_TRUE(tMergeTreeIgnoreEarlierTs(&tree_));
}

TEST_F(TsdbSttStatsTest, SttStatsFailureReturnsError) {
  prepare(false);
  observe();
  for (int stage : {0, 1}) {
    if (stage == 0) stub_.set(tsdbSttFileReadStatisBlk, failSttStatistics);
    if (stage == 1) stub_.set(tsdbSttFileReadStatisBlock, failAfterSttStatsDecode);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, openMerge(7));
    EXPECT_EQ(0, sttPhysicalReads);
    EXPECT_FALSE(sttIter()->pBlockLoadInfo->sttBlockLoaded);
    EXPECT_FALSE(sttIter()->pBlockLoadInfo->statisLoaded);
    EXPECT_EQ(nullptr, sttIter()->pBlockLoadInfo->info.pUid);
    stub_.reset(tsdbSttFileReadStatisBlk);
    stub_.set(tsdbSttFileReadStatisBlock, countedSttStatistics);
    releaseIters();
  }
}

TEST_F(TsdbSttStatsTest, SttStatsGroupCountKeepsEndpointsAndSkipsDisjoint) {
  prepare(false);
  observe();
  snap_.pfSetArray = &files_;
  for (int order : {TSDB_ORDER_ASC, TSDB_ORDER_DESC}) {
    tsdbReaderClose2(reader_);
    reader_ = nullptr;
    auto c = cond(order);
    c.notLoadData = true;
    auto r = ranges();
    r.resize(1);
    r[0].window = {9, 9};
    ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "stats-group-count", nullptr));
    int64_t count = 0;
    bool    has = false;
    while (tsdbNextDataBlock2(reader_, &has) == 0 && has) {
      SSDataBlock* block = nullptr;
      ASSERT_EQ(0, tsdbRetrieveDataBlock2(reader_, &block));
      count += block->info.rows;
      tsdbReleaseDataBlock2(reader_);
    }
    EXPECT_EQ(2, count);
    r[0].window = {21, 90};
    ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), 1));
    sttDataReads = sttPhysicalReads = 0;
    ASSERT_EQ(0, tsdbNextDataBlock2(reader_, &has));
    EXPECT_FALSE(has);
    EXPECT_EQ(0, sttPhysicalReads);
  }
}

TEST_F(TsdbSttStatsTest, SttStatsCopyFailureDoesNotBorrowArrays) {
  prepare(false);
  ASSERT_EQ(0, openMerge(7));
  const auto* source = &sttIter()->pBlockLoadInfo->info;
  // Real array copies succeed up to each injected allocation failure.
  for (int failAt = 1; failAt <= 6; ++failAt) {
    SSttTableRowsInfo dst = {};
    arrayDupCalls = 0;
    failArrayDupAt = failAt;
    stub_.set(taosArrayDup, failStatsArrayDup);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, sttRowInfoDeepCopy(&dst, const_cast<SSttTableRowsInfo*>(source), 1));
    stub_.reset(taosArrayDup);
    EXPECT_EQ(nullptr, dst.pCount);
    EXPECT_EQ(nullptr, dst.pFirstKey);
    EXPECT_EQ(nullptr, dst.pLastKey);
    EXPECT_EQ(nullptr, dst.pFirstTs);
    EXPECT_EQ(nullptr, dst.pLastTs);
    EXPECT_EQ(nullptr, dst.pUid);
    // Recover the baseline's partially allocated result for the RED run.
    taosArrayDestroy(dst.pCount);
    taosArrayDestroy(dst.pFirstKey);
    taosArrayDestroy(dst.pLastKey);
    taosArrayDestroy(dst.pFirstTs);
    taosArrayDestroy(dst.pLastTs);
    taosArrayDestroy(dst.pUid);
    EXPECT_EQ(2, taosArrayGetSize(source->pUid));
  }
}

TEST_F(TsdbSttStatsTest, SttSkippedDataStillDeletesMemoryRows) {
  prepare();
  seed(IMem);  // Both memory layers contain rows; only the STT file owns tombs.
  observe();
  for (int64_t maxVersion : {int64_t{3}, int64_t{5}}) {
    static_cast<STsdbReader*>(reader_)->info.verRange.maxVer = maxVersion;
    for (uint64_t uid : {7, 8}) {
      auto r = ranges();
      r.erase(r.begin() + (uid == 7 ? 1 : 0));
      r[0].window = uid == 7 ? STimeWindow{14, 16} : STimeWindow{104, 106};
      // Reference runs the normal file+mem+imem merge on a fresh Reader.
      auto expected = freshFileRows(uid, r[0].window, maxVersion);
      ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), r.size()));
      ASSERT_EQ(0, resetSttBlockReaderForGroup(iters_));
      sttDataReads = sttPhysicalReads = 0;
      ASSERT_EQ(0, openMerge(uid, uid == 7 ? STimeWindow{21, 90} : STimeWindow{30, 98}));
      EXPECT_EQ(0, rows());
      EXPECT_EQ(0, sttPhysicalReads);
      expectRows(read(), expected);
    }
  }
  EXPECT_EQ(4, sttTombLoads);
}

TEST_F(TsdbSttPreparationTest, SttMetadataReuseReloadsGroupTombs) {
  prepare();
  ASSERT_EQ(0, openMerge(7));
  bool hasRow = true;
  ASSERT_EQ(0, tMergeTreeNext(&tree_, &hasRow));
  EXPECT_FALSE(hasRow);  // No STT rows match, but tombs still delete mem.
  auto* file = sttIter()->pReader;
  auto* info = sttIter()->pBlockLoadInfo;
  ASSERT_TRUE(info->sttBlockLoaded);
  auto fresh = freshFileRows(8, {103, 109});
  ASSERT_EQ(12u, fresh.size());
  auto r = ranges();
  r[1].window = {103, 109};
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
  info->tombLoaded = false;  // S2 will own this reset in the Reader group transition.
  ASSERT_EQ(0, openMerge(8));
  EXPECT_EQ(file, sttIter()->pReader);
  EXPECT_EQ(info, sttIter()->pBlockLoadInfo);
  expectRows(read(), fresh);
  EXPECT_EQ(2, sttTombLoads);
}

TEST_F(TsdbSttPreparationTest, SttTombsLoadOncePerGroup) {
  prepare();
  ASSERT_EQ(0, openMerge(7));
  ASSERT_EQ(0, openMerge(8));
  EXPECT_EQ(1, sttTombLoads);
  std::vector<Row> expected;
  for (uint64_t uid : {7, 8}) {
    for (int64_t ts = uid == 7 ? 10 : 100; ts <= (uid == 7 ? 19 : 109); ++ts) {
      if (ts == (uid == 7 ? 15 : 105)) continue;
      for (int64_t pk : {1, 2}) expected.emplace_back(uid, ts, pk, ts * 10 + pk);
    }
  }
  expectRows(read(), expected);
  auto r = ranges();
  r[0].window = {14, 16};
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), 1));
  sttIter()->pBlockLoadInfo->tombLoaded = false;
  ASSERT_EQ(0, openMerge(7));
  expectMemRows(7, 14, 16);
  EXPECT_EQ(2, sttTombLoads);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
  sttIter()->pBlockLoadInfo->tombLoaded = false;
  ASSERT_EQ(0, openMerge(8));
  expectMemRows(8, 100, 109);
  EXPECT_EQ(3, sttTombLoads);
}

TEST_F(TsdbSttPreparationTest, SttPreparationFailureDoesNotComplete) {
  prepare(false);
  for (int stage = 0; stage < 4; ++stage) {
    SCOPED_TRACE(stage);
    if (stage == 0) stub_.set(tsdbSttFileReadSttBlk, failSttDirectory);
    if (stage == 1) stub_.set(tsdbSttFileReadStatisBlk, failSttStatistics);
    if (stage == 2) stub_.set(tsdbSttFileReadTombBlk, failSttTombs);
    if (stage == 3) stub_.set(tsdbSttFileReadTombBlock, failAfterSttTombDecode);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, openMerge(7, {10, 19}));
    auto* iter = sttIter();
    ASSERT_NE(nullptr, iter);
    ASSERT_NE(nullptr, iter->pReader);
    ASSERT_NE(nullptr, iter->pBlockLoadInfo);
    EXPECT_EQ(stage >= 2, iter->pBlockLoadInfo->sttBlockLoaded);
    EXPECT_FALSE(iter->pBlockLoadInfo->tombLoaded);
    EXPECT_EQ(nullptr, iter->pSttBlk);  // Failure must stop before row positioning.
    EXPECT_EQ(nullptr, tree_.pIter);
    stub_.reset(tsdbSttFileReadSttBlk);
    stub_.reset(tsdbSttFileReadStatisBlk);
    stub_.reset(tsdbSttFileReadTombBlk);
    stub_.reset(tsdbSttFileReadTombBlock);
    // The failed object can be destroyed, and a fresh attempt reads real files.
    destroySttBlockReader(iters_, nullptr);
    iters_ = taosArrayInit(1, POINTER_BYTES);
    ASSERT_NE(nullptr, iters_);
    ASSERT_EQ(0, openMerge(7, {10, 19}));
    bool hasRow = false;
    ASSERT_EQ(0, tMergeTreeNext(&tree_, &hasRow));
    EXPECT_TRUE(hasRow);
    tMergeTreeClose(&tree_);
    destroySttBlockReader(iters_, nullptr);
    iters_ = taosArrayInit(1, POINTER_BYTES);
    ASSERT_NE(nullptr, iters_);
  }
}

TEST_F(TsdbSttPreparationTest, SttLoadInfoFailureKeepsFileOwned) {
  prepare(false);
  stub_.set(tCreateSttBlockLoadInfo, failSttLoadInfo);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, openMerge(7));
  // The iterator must already own the real open file when LoadInfo allocation fails.
  EXPECT_NE(nullptr, sttIter()->pReader);
  EXPECT_EQ(nullptr, sttIter()->pBlockLoadInfo);
  stub_.reset(tCreateSttBlockLoadInfo);
}

class TsdbGroupSttReuseTest : public TsdbReaderGroupSwitchTest {
 protected:
  void TearDown() override {
    tsdbReaderClose2(reader_);
    reader_ = nullptr;
    EXPECT_TRUE(liveSttFiles.empty());
    TsdbReaderGroupSwitchTest::TearDown();
    sttStub = nullptr;
  }
  void countPreparation() {
    sttStub = &stub_;
    liveSttFiles.clear();
    sttOpens = sttDirectories = sttStatistics = sttGroupTombs = 0;
    peakSttFiles = 0;
    stub_.set(tsdbSttFileReaderOpen, countedSttOpen);
    stub_.set(tsdbSttFileReaderClose, countedSttClose);
    stub_.set(tsdbSttFileReadSttBlk, countedSttDirectory);
    stub_.set(tsdbSttFileReadStatisBlock, countedSttStatistics);
    stub_.set(loadSttTombDataForAll, countedGroupSttTombs);
  }
  STsdbReader* reader() { return static_cast<STsdbReader*>(reader_); }
  SLDataIter*  activeIter() {
    auto* array = reader()->status.pLDataIterArray;
    if (taosArrayGetSize(array) == 0) return nullptr;
    auto* list = static_cast<SArray*>(taosArrayGetP(array, 0));
    return static_cast<SLDataIter*>(taosArrayGetP(list, 0));
  }
  std::vector<Row> freshRows(STableScanRange range) {
    void* saved = reader_;
    reader_ = nullptr;
    auto c = cond();
    c.twindows = range.window;
    EXPECT_EQ(0, tsdbReaderOpen2(&vnode_, &c, &range.table, 1, nullptr, &reader_, "fresh-group-stt", nullptr));
    auto rows = read();
    tsdbReaderClose2(reader_);
    reader_ = saved;
    return rows;
  }
  void expectReset(SLDataIter* iter) {
    ASSERT_NE(nullptr, iter);
    EXPECT_EQ(nullptr, iter->pSttBlk);
    EXPECT_EQ(nullptr, iter->pStartRowKey);
    EXPECT_EQ(nullptr, iter->rInfo.row.pBlockData);
    auto* info = iter->pBlockLoadInfo;
    ASSERT_NE(nullptr, info);
    EXPECT_TRUE(info->sttBlockLoaded);
    EXPECT_FALSE(info->tombLoaded);
    EXPECT_EQ(nullptr, info->pSchema);
    EXPECT_EQ(nullptr, info->colIds);
    for (const auto& slot : info->blockData) {
      EXPECT_EQ(-1, slot.sttBlockIndex);
      EXPECT_FALSE(slot.pin);
      EXPECT_EQ(0, slot.data.nRow);
      EXPECT_EQ(0, slot.data.nColData);
      EXPECT_EQ(nullptr, slot.data.aColData);
      EXPECT_EQ(nullptr, slot.data.aUid);
      EXPECT_EQ(nullptr, slot.data.aVersion);
      EXPECT_EQ(nullptr, slot.data.aTSKEY);
    }
  }
};

TEST_F(TsdbGroupSttReuseTest, GroupsReuseCurrentSttMetadata) {
  seed(Stt, true);
  auto r = ranges();
  auto first = freshRows(r[0]);
  auto second = freshRows(r[1]);
  ASSERT_EQ(18u, first.size());
  ASSERT_EQ(18u, second.size());
  countPreparation();
  auto c = cond();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "reuse", nullptr));
  expectRows(read(), first);
  ASSERT_NE(nullptr, activeIter());
  auto* original = activeIter()->pReader;
  EXPECT_EQ(1, sttOpens);
  EXPECT_EQ(1, sttDirectories);
  EXPECT_EQ(1, sttStatistics);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, nullptr, 0));
  EXPECT_TRUE(read().empty());
  ASSERT_NE(nullptr, activeIter());
  expectReset(activeIter());
  EXPECT_EQ(original, activeIter()->pReader);
  EXPECT_EQ(files_.data[0], reader()->pGroupSttFileset);
  EXPECT_EQ(nullptr, reader()->status.pCurrentFileset);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
  expectRows(read(), second);
  r[0].window = {12, 16};
  std::vector<Row> narrowed;
  for (const auto& row : first)
    if (std::get<1>(row) >= 12 && std::get<1>(row) <= 16) narrowed.push_back(row);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), 1));
  expectRows(read(), narrowed);
  EXPECT_EQ(1, sttOpens);
  EXPECT_EQ(1, sttDirectories);
  EXPECT_EQ(1, sttStatistics);
  EXPECT_EQ(3, sttGroupTombs);
  EXPECT_EQ(1u, liveSttFiles.size());
}

TEST_F(TsdbGroupSttReuseTest, GroupSttReuseReplacesFileset) {
  const int64_t filesetSpan = 60000;
  seed(Stt, true);
  seed(Stt, true, 1, filesetSpan);
  auto r = ranges();
  auto first = freshRows(r[0]);
  auto far = r[0];
  far.window.skey += filesetSpan;
  far.window.ekey += filesetSpan;
  auto second = freshRows(far);
  countPreparation();
  auto c = cond();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "replace", nullptr));
  expectRows(read(), first);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, &far, 1));
  expectRows(read(), second);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), 1));
  expectRows(read(), first);
  EXPECT_EQ(3, sttOpens);
  EXPECT_EQ(3, sttDirectories);
  EXPECT_EQ(3, sttStatistics);
  EXPECT_EQ(1u, peakSttFiles);
  EXPECT_EQ(nullptr, reader()->pFilesetSttCache);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, nullptr, 0));
  EXPECT_EQ(1u, liveSttFiles.size());
  ASSERT_NE(nullptr, activeIter());
  expectReset(activeIter());
}

TEST_F(TsdbGroupSttReuseTest, GroupSttReuseInvalidatesOnSnapshotChange) {
  seed(Stt, true);
  auto r = ranges();
  auto expected = freshRows(r[0]);
  countPreparation();
  auto c = cond();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "invalidate", nullptr));
  reader()->resBlockInfo.capacity = 3;
  auto partial = read(true);
  ASSERT_FALSE(partial.empty());
  ASSERT_LT(partial.size(), expected.size());
  ASSERT_TRUE(reader()->status.loadFromFile);
  ASSERT_EQ(0, tsdbReaderSuspend2(reader()));
  EXPECT_TRUE(liveSttFiles.empty());
  EXPECT_EQ(nullptr, reader()->pGroupSttFileset);
  EXPECT_EQ(nullptr, reader()->status.pCurrentFileset);
  auto rest = read();
  partial.insert(partial.end(), rest.begin(), rest.end());
  expectRows(partial, expected);
  EXPECT_EQ(2, sttOpens);
  ASSERT_FALSE(reader()->status.loadFromFile);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, nullptr, 0));
  EXPECT_EQ(1u, liveSttFiles.size());
  // A different snapshot of the same fid must reopen, including after an empty group.
  ASSERT_EQ(0, tsdbReaderSuspend2(reader()));
  EXPECT_TRUE(liveSttFiles.empty());
  auto*      oldFileset = files_.data[0];
  STFileSet* newFileset = nullptr;
  ASSERT_EQ(0, tsdbTFileSetInitRef(&tsdb_, oldFileset, &newFileset));
  ASSERT_NE(oldFileset, newFileset);
  files_.data[0] = newFileset;
  tsdbTFileSetClear(&oldFileset);
  STsdbReadSnap replacement = snap_;
  snapshot = &replacement;
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), 1));
  expectRows(read(), expected);
  EXPECT_EQ(3, sttOpens);
  EXPECT_EQ(newFileset, reader()->pGroupSttFileset);
  ASSERT_EQ(0, tsdbReaderSuspend2(reader()));
  EXPECT_EQ(nullptr, reader()->pGroupSttFileset);
  EXPECT_TRUE(liveSttFiles.empty());
  snapshot = &snap_;
}

TEST_F(TsdbGroupSttReuseTest, GroupSttReuseInvalidatesDuringMemoryPhase) {
  seed(Stt, true);
  seed(Mem, false, 0, 200);
  auto r = ranges();
  r[0].window = {9, 219};
  auto expected = freshRows(r[0]);
  countPreparation();
  auto c = cond();
  vnode_.config.tsdbCfg.maxRows = 3;
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "invalidate-mem", nullptr));
  std::vector<Row> rows;
  while (reader()->status.loadFromFile) {
    auto part = read(true);
    rows.insert(rows.end(), part.begin(), part.end());
  }
  ASSERT_LT(rows.size(), expected.size());
  ASSERT_EQ(1u, liveSttFiles.size());
  ASSERT_EQ(0, tsdbReaderSuspend2(reader()));
  EXPECT_TRUE(liveSttFiles.empty());
  EXPECT_EQ(nullptr, reader()->pGroupSttFileset);
  EXPECT_EQ(nullptr, reader()->status.pCurrentFileset);
  auto rest = read();
  rows.insert(rows.end(), rest.begin(), rest.end());
  expectRows(rows, expected);
  EXPECT_EQ(2, sttOpens);
}

TEST_F(TsdbGroupSttReuseTest, GroupSttReuseRejectsActivationWhileCleanBlockIsHeld) {
  seed(Stt);
  seedIndexedFile(0, 200);
  // Combine disjoint real DATA and STT files into one snapshot fileset.
  for (int i = 0; i < TSDB_FTYPE_MAX; ++i) {
    files_.data[0]->farr[i] = files_.data[1]->farr[i];
    files_.data[1]->farr[i] = nullptr;
  }
  tsdbTFileSetClear(&files_.data[1]);
  --files_.size;
  countPreparation();
  auto c = cond();
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "reuse-held", nullptr));
  EXPECT_EQ(20u, read().size());
  ASSERT_NE(nullptr, activeIter());
  auto* original = activeIter()->pReader;
  r[0].window = {210, 465};
  r[1].window = {466, 466};
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), 1));
  bool next = false;
  ASSERT_EQ(0, tsdbNextDataBlock2(reader_, &next));
  ASSERT_TRUE(next);
  EXPECT_FALSE(reader()->status.composedDataBlock);
  EXPECT_NE(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
  EXPECT_EQ(original, activeIter()->pReader);
  EXPECT_EQ(1u, liveSttFiles.size());
  tsdbReleaseDataBlock2(reader_);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
  EXPECT_EQ(1u, read().size());
  EXPECT_EQ(1, sttOpens);
  EXPECT_EQ(original, activeIter()->pReader);
}

TEST_F(TsdbGroupSttReuseTest, CachedFilesetReloadsTombsForNewGroup) {
  const int64_t filesetSpan = 60000;
  seed(Stt, true);
  seed(Stt, true, 1, filesetSpan);
  auto r = ranges();
  r[0].window = {10, filesetSpan + 19};
  auto first = freshRows(r[0]);
  auto second = r[1];
  second.window = {100, filesetSpan + 109};
  auto expected = freshRows(second);
  ASSERT_EQ(40u, expected.size());
  countPreparation();
  auto c = cond();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "legacy-cache", nullptr));
  tsdbSetFilesetDelimited(reader());
  expectRows(read(), first);
  ASSERT_NE(nullptr, reader()->pFilesetSttCache);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, &second, 1));
  expectRows(read(), expected);
  EXPECT_EQ(2, sttOpens);
  EXPECT_EQ(4, sttGroupTombs);
}

TEST_F(TsdbGroupSttReuseTest, RetainedPreparationFailureIsDestructible) {
  seed(Stt, true);
  auto r = ranges();
  auto expected = freshRows(r[0]);
  countPreparation();
  auto c = cond();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "reuse-failure", nullptr));
  expectRows(read(), expected);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
  stub_.set(tsdbSttFileReadTombBlk, failSttTombs);
  bool next = false;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, tsdbNextDataBlock2(reader_, &next));
  ASSERT_NE(nullptr, activeIter());
  EXPECT_FALSE(activeIter()->pBlockLoadInfo->tombLoaded);
  stub_.reset(tsdbSttFileReadTombBlk);
}

TEST_P(TsdbReaderGroupSwitchTest, RangeAcrossStoragePaths) {
  seed(std::get<0>(GetParam()));
  int  order = std::get<1>(GetParam());
  auto expected = reference(order);
  ASSERT_EQ(40u, expected.size());
  auto c = cond(order);
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), r.size(), nullptr, &reader_, "group-range", nullptr));
  directBlocks_ = composedBlocks_ = 0;
  auto next = r;
  next.erase(next.begin());
  next[0].window = {102, 108};
  auto mixed = r;
  mixed[0].valid = false;
  mixed[1].window = {101, 104};
  std::vector<std::vector<STableScanRange>> groups = {r, next, mixed, r};
  STsdbReadSnap*                            originalSnapshot = nullptr;
  for (size_t g = 0; g < groups.size(); ++g) {
    if (g) ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, groups[g].data(), groups[g].size()));
    std::vector<Row> groupExpected;
    for (const auto& row : expected) {
      for (const auto& range : groups[g]) {
        if (range.valid && std::get<0>(row) == range.table.uid && std::get<1>(row) >= range.window.skey &&
            std::get<1>(row) <= range.window.ekey) {
          groupExpected.push_back(row);
        }
      }
    }
    // The caller can release/modify its descriptors as soon as activation returns.
    for (auto& range : groups[g]) range.window = {0, 200};
    expectRows(read(), groupExpected);

    auto* current = static_cast<STsdbReader*>(reader_);
    if (g == 0) originalSnapshot = current->pReadSnap;
    EXPECT_EQ(originalSnapshot, current->pReadSnap);
  }
  if (std::get<0>(GetParam()) == Data) EXPECT_GT(directBlocks_, 0);
  if (std::get<0>(GetParam()) == PartialData || std::get<0>(GetParam()) == Merged) EXPECT_GT(composedBlocks_, 0);
}
TEST_P(TsdbReaderGroupSwitchTest, VersionsDeletesAndPrimaryKeys) {
  seed(std::get<0>(GetParam()), true);
  const int        order = std::get<1>(GetParam());
  std::vector<Row> expected;
  for (uint64_t uid : {7, 8}) {
    const int64_t    base = uid == 7 ? 10 : 100;
    std::vector<Row> table;
    for (int64_t ts = base; ts < base + 10; ++ts) {
      if (ts == base + 5) continue;
      for (int pk = 1; pk <= 2; ++pk) {
        const int update = std::get<0>(GetParam()) == Merged ? 4000 : 3000;
        table.emplace_back(uid, ts, pk, ts * 10 + pk + (ts == base + 2 && pk == 1 ? update : 0));
      }
    }
    if (order == TSDB_ORDER_DESC) std::reverse(table.begin(), table.end());
    expected.insert(expected.end(), table.begin(), table.end());
  }
  expectRows(reference(order), expected);
  auto c = cond(order);
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), r.size(), nullptr, &reader_, "group-visibility", nullptr));
  for (int i = 0; i < 3; ++i) {
    if (i) ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), r.size()));
    expectRows(read(), expected);
  }
}
TEST_P(TsdbReaderGroupSwitchTest, ResumePartialGroup) {
  seed(std::get<0>(GetParam()));
  int  order = std::get<1>(GetParam());
  auto expected = reference(order);
  vnode_.config.tsdbCfg.maxRows = 3;
  auto c = cond(order);
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), r.size(), nullptr, &reader_, "group-resume", nullptr));
  auto rows = read(true);
  ASSERT_FALSE(rows.empty());
  ASSERT_LT(rows.size(), expected.size());
  ASSERT_EQ(0, tsdbReaderSuspend2(static_cast<STsdbReader*>(reader_)));
  EXPECT_EQ(nullptr, static_cast<STsdbReader*>(reader_)->pFileReader);
  auto rest = read();
  rows.insert(rows.end(), rest.begin(), rest.end());
  expectRows(rows, expected);
}
TEST_P(TsdbReaderGroupSwitchTest, RescansPreserveRangesAndActivationRestoresDirection) {
  seed(std::get<0>(GetParam()));
  auto asc = reference(TSDB_ORDER_ASC);
  auto desc = reference(TSDB_ORDER_DESC);
  auto c = cond();
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), r.size(), nullptr, &reader_, "group-reset", nullptr));
  expectRows(read(), asc);
  ASSERT_EQ(0, tsdbReaderReset2(reader_, &c));
  expectRows(read(), asc);
  c.order = TSDB_ORDER_DESC;
  ASSERT_EQ(0, tsdbReaderReset2(reader_, &c));
  expectRows(read(), desc);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), r.size()));
  expectRows(read(), asc);
}
TEST_P(TsdbReaderGroupSwitchTest, EmptyGroupThenNonemptyGroup) {
  seed(std::get<0>(GetParam()));
  auto expected = reference(TSDB_ORDER_ASC);
  auto c = cond();
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, nullptr, 0, nullptr, &reader_, "group-empty", nullptr));
  EXPECT_TRUE(read().empty());
  for (int i = 0; i < 2; ++i) {
    ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), r.size()));
    expectRows(read(), expected);
    auto empty = r;
    for (auto& e : empty) e.valid = false;
    ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, empty.data(), empty.size()));
    EXPECT_TRUE(read().empty());
  }
}
TEST_P(TsdbReaderGroupSwitchTest, CountUsesPerTableRanges) {
  seed(std::get<0>(GetParam()));
  auto c = cond(std::get<1>(GetParam()));
  c.notLoadData = true;
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), r.size(), nullptr, &reader_, "group-count", nullptr));
  for (int group = 0; group < 2; ++group) {
    if (group) {
      r[0].window = {12, 16};
      r[1].window = {103, 107};
      ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), r.size()));
    }
    int64_t counts[2] = {};
    while (true) {
      bool next = false;
      ASSERT_EQ(0, tsdbNextDataBlock2(reader_, &next));
      if (!next) break;
      SSDataBlock* block = nullptr;
      ASSERT_EQ(0, tsdbRetrieveDataBlock2(reader_, &block));
      ASSERT_NE(nullptr, block);
      ASSERT_TRUE(block->info.id.uid == 7 || block->info.id.uid == 8);
      counts[block->info.id.uid - 7] += block->info.rows;
      // Count-only clean blocks retain the mutex until explicit release.
      tsdbReleaseDataBlock2(reader_);
    }
    EXPECT_EQ(group == 0 ? 20 : 10, counts[0]);
    EXPECT_EQ(group == 0 ? 20 : 10, counts[1]);
  }
}
TEST_P(TsdbReaderGroupSwitchTest, RejectsActivationWhileCleanBlockIsHeld) {
  seed(Data);
  auto c = cond(std::get<1>(GetParam()));
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), r.size(), nullptr, &reader_, "group-held", nullptr));
  bool next = false;
  ASSERT_EQ(0, tsdbNextDataBlock2(reader_, &next));
  ASSERT_TRUE(next);
  ASSERT_FALSE(static_cast<STsdbReader*>(reader_)->status.composedDataBlock);
  EXPECT_NE(0, tsdbReaderActivateGroup2(reader_, r.data(), r.size()));
  tsdbReleaseDataBlock2(reader_);
}
TEST_P(TsdbReaderGroupSwitchTest, ActivationFailureIsDestructible) {
  auto c = cond();
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, nullptr, 0, nullptr, &reader_, "group-fail", nullptr));
  auto* reader = static_cast<STsdbReader*>(reader_);
  reader->suppInfo.pk.type = TSDB_DATA_TYPE_VARCHAR;
  reader->suppInfo.pk.bytes = 32;
  hashStub = &stub_;
  hashInsertCalls = 0;
  stub_.set(tSimpleHashPut, failSecondHashPut);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, tsdbReaderActivateGroup2(reader_, r.data(), r.size()));
  stub_.reset(tSimpleHashPut);
  hashStub = nullptr;
  EXPECT_EQ(0, tSimpleHashGetSize(reader->status.pTableMap));
}

class TsdbBrinTimeSummaryTest : public TsdbReaderGroupSwitchTest {
 protected:
  void SetUp() override {
    TsdbReaderGroupSwitchTest::SetUp();
    ioStub = &stub_;
    brinBlockReads = 0;
    stub_.set(tsdbDataFileReadBrinBlock, countedBrinBlock);
  }
  STsdbReader*     reader() { return static_cast<STsdbReader*>(reader_); }
  std::vector<Row> freshRows(const STableScanRange& range, int order = TSDB_ORDER_ASC) {
    void* saved = reader_;
    reader_ = nullptr;
    auto c = cond(order);
    c.twindows = range.window;
    STableKeyInfo table = range.table;
    EXPECT_EQ(0, tsdbReaderOpen2(&vnode_, &c, &table, 1, nullptr, &reader_, "brin-reference", nullptr));
    auto rows = read();
    tsdbReaderClose2(reader_);
    reader_ = saved;
    return rows;
  }
};

TEST_F(TsdbBrinTimeSummaryTest, CompleteBlockIncludesRecordsAfterUidEarlyBreakAndEqualEndpoints) {
  seed(Data, true);
  auto r = ranges();
  for (int order : {TSDB_ORDER_ASC, TSDB_ORDER_DESC}) {
    auto c = cond(order);
    ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "complete-brin", nullptr));
    EXPECT_EQ(18u, read().size());
    ASSERT_EQ(1, reader()->brinTimeSummary.numEntries);
    EXPECT_EQ(9, reader()->brinTimeSummary.pEntries[0].window.skey);
    EXPECT_EQ(110, reader()->brinTimeSummary.pEntries[0].window.ekey);
    for (const STimeWindow window : {STimeWindow{109, 110}, STimeWindow{9, 9}, STimeWindow{110, 110}}) {
      auto next = r[window.skey == 9 ? 0 : 1];
      next.window = window;
      auto expected = freshRows(next, order);
      ASSERT_EQ(window.skey == 109 ? 4u : 2u, expected.size());
      int before = brinBlockReads;
      ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, &next, 1));
      expectRows(read(), expected);
      EXPECT_EQ(1, brinBlockReads - before);
    }
    tsdbReaderClose2(reader_);
    reader_ = nullptr;
  }
}

TEST_F(TsdbBrinTimeSummaryTest, UnknownBlocksReadAndFilesetRevisitStartsCold) {
  int64_t begin = 0, end = 0;
  tsdbFidKeyRange(1, tsdb_.keepCfg.days, tsdb_.keepCfg.precision, &begin, &end);
  seedIndexedFile(0, 0);
  seedIndexedFile(1, begin);
  auto c = cond();
  auto r = ranges();
  r[0].window = {10, 265};
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "brin-filesets", nullptr));
  EXPECT_EQ(256u, read().size());
  ASSERT_EQ(2, reader()->brinTimeSummary.numEntries);
  EXPECT_GT(reader()->brinTimeSummary.pEntries[1].window.skey, reader()->brinTimeSummary.pEntries[1].window.ekey);
  int before = brinBlockReads;
  r[1].window = {266, 266};
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
  EXPECT_EQ(1u, read().size());
  EXPECT_EQ(1, brinBlockReads - before);
  auto next = r[0];
  for (int fid : {1, 0}) {
    // Query disjoint from F1's warmed block: returning to F1 must still read it.
    next.window = {fid == 1 ? begin + 10 : 500, fid == 1 ? begin + 265 : 510};
    before = brinBlockReads;
    ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, &next, 1));
    EXPECT_EQ(fid == 1 ? 256u : 0u, read().size());
    EXPECT_EQ(1, brinBlockReads - before);
    EXPECT_EQ(files_.data[fid], reader()->brinTimeSummary.pFileset);
  }
}

TEST_F(TsdbBrinTimeSummaryTest, EmptyGroupAndPartialFetchRetainButSuspendClearsSnapshotIdentity) {
  seed(Data);
  auto c = cond();
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "brin-suspend", nullptr));
  reader()->resBlockInfo.capacity = 3;
  auto expected = freshRows(r[0]);
  auto rows = read(true);
  ASSERT_FALSE(rows.empty());
  ASSERT_LT(rows.size(), expected.size());
  auto* entries = reader()->brinTimeSummary.pEntries;
  ASSERT_NE(nullptr, entries);
  auto rest = read();
  rows.insert(rows.end(), rest.begin(), rest.end());
  expectRows(rows, expected);
  EXPECT_EQ(entries, reader()->brinTimeSummary.pEntries);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, nullptr, 0));
  EXPECT_TRUE(read().empty());
  EXPECT_EQ(entries, reader()->brinTimeSummary.pEntries);
  ASSERT_EQ(0, tsdbReaderSuspend2(reader()));
  EXPECT_EQ(nullptr, reader()->brinTimeSummary.pEntries);
  EXPECT_EQ(nullptr, reader()->brinTimeSummary.pSnapshot);
  STFileSet* replacement = nullptr;
  ASSERT_EQ(0, tsdbTFileSetInitRef(&tsdb_, files_.data[0], &replacement));
  auto* old = files_.data[0];
  files_.data[0] = replacement;
  tsdbTFileSetClear(&old);
  STsdbReadSnap newSnapshot = snap_;
  snapshot = &newSnapshot;
  auto next = r[0];
  next.window = {500, 510};
  int before = brinBlockReads;
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, &next, 1));
  EXPECT_TRUE(read().empty());
  EXPECT_EQ(1, brinBlockReads - before);
  EXPECT_EQ(&newSnapshot, reader()->brinTimeSummary.pSnapshot);
  EXPECT_EQ(replacement, reader()->brinTimeSummary.pFileset);
  ASSERT_EQ(0, tsdbReaderSuspend2(reader()));
  EXPECT_EQ(nullptr, reader()->brinTimeSummary.pHead);
  snapshot = &snap_;
}

TEST_F(TsdbBrinTimeSummaryTest, BudgetBoundaryAndAllocationFailureUseOriginalReads) {
  seedIndexedFile(0, 0);
  auto c = cond();
  auto r = ranges();
  ioStub = &stub_;
  stub_.set(tsdbDataFileReadBrinBlk, paddedBrinDirectory);
  const int32_t capacity = (65536 - sizeof(SBrinTimeSummary)) / sizeof(SBrinTimeSummaryEntry);
  const int64_t boundaryBytes = sizeof(SBrinTimeSummary) + capacity * sizeof(SBrinTimeSummaryEntry);
  for (int mode = 0; mode < 3; ++mode) {
    summaryDirectorySize = capacity + (mode == 1 ? 1 : 0);
    summaryAllocationSize = capacity * sizeof(SBrinTimeSummaryEntry);
    summaryAllocationFailures = 0;
    if (mode == 2) stub_.set(taosMemMalloc, failSummaryAllocation);
    ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data() + 1, 1, nullptr, &reader_, "brin-budget", nullptr));
    EXPECT_TRUE(read().empty());
    if (mode == 0) {
      ASSERT_NE(nullptr, reader()->brinTimeSummary.pEntries);
      EXPECT_EQ(boundaryBytes,
                sizeof(SBrinTimeSummary) + reader()->brinTimeSummary.numEntries * sizeof(SBrinTimeSummaryEntry));
      EXPECT_LE(boundaryBytes, 65536);
      EXPECT_GT(boundaryBytes + sizeof(SBrinTimeSummaryEntry), 65536);
    } else {
      EXPECT_EQ(nullptr, reader()->brinTimeSummary.pEntries);
      EXPECT_EQ(0, reader()->brinTimeSummary.numEntries);
    }
    int before = brinBlockReads;
    ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
    EXPECT_TRUE(read().empty());
    EXPECT_EQ(mode == 0 ? 0 : 1, brinBlockReads - before);
    EXPECT_EQ(mode == 2 ? 1 : 0, summaryAllocationFailures);
    if (mode == 2) {
      clearBrinTimeSummary(reader());
      int32_t previous = terrno;
      terrno = TSDB_CODE_FILE_CORRUPTED;
      prepareBrinTimeSummary(reader(), &summaryDirectory);
      EXPECT_EQ(TSDB_CODE_FILE_CORRUPTED, terrno);
      EXPECT_EQ(nullptr, reader()->brinTimeSummary.pEntries);
      terrno = previous;
      stub_.reset(taosMemMalloc);
    }
    tsdbReaderClose2(reader_);
    reader_ = nullptr;
  }
  summaryDirectoryEntries.clear();
}

TEST_F(TsdbBrinTimeSummaryTest, SkippedBrinStillLoadsDataTombsAndSttMemRows) {
  seed(Data, true, 0, 0, 8, true, nullptr, 140);
  seed(Stt, false, 0, 140, 8, true, files_.data[0]);
  seed(Mem, false, 0, 140);
  seed(IMem, false, 0, 140);
  auto c = cond();
  auto r = ranges();
  r[0].window = {150, 159};
  // DATA contains 9..110, but its tomb at 155 must remove STT/mem/imem rows.
  auto expected = freshRows(r[0]);
  ASSERT_EQ(18u, expected.size());
  for (const auto& row : expected) EXPECT_NE(155, std::get<1>(row));
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "brin-mixed", nullptr));
  expectRows(read(), expected);
  int before = brinBlockReads;
  sttStub = &stub_;
  sttGroupTombs = 0;
  stub_.set(loadSttTombDataForAll, countedGroupSttTombs);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), 1));
  expectRows(read(), expected);
  EXPECT_EQ(0, brinBlockReads - before);
  EXPECT_GT(sttGroupTombs, 0);
}

TEST_F(TsdbBrinTimeSummaryTest, TimeExtremesKeepEqualBoundsOnRealBrinIterator) {
  seedIndexedFile(0, 0, 10, true);
  auto c = cond();
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "brin-extremes", nullptr));
  EXPECT_TRUE(read().empty());
  ASSERT_EQ(1, reader()->brinTimeSummary.numEntries);
  EXPECT_EQ(INT64_MIN, reader()->brinTimeSummary.pEntries[0].window.skey);
  EXPECT_EQ(INT64_MAX, reader()->brinTimeSummary.pEntries[0].window.ekey);
  const TBrinBlkArray* directory = nullptr;
  ASSERT_EQ(0, tsdbDataFileReadBrinBlk(reader()->pFileReader, &directory));
  SArray* blocks = taosArrayInit(1, sizeof(SBrinBlk));
  ASSERT_NE(nullptr, blocks);
  ASSERT_NE(nullptr, taosArrayPush(blocks, &directory->data[0]));
  for (const STimeWindow window : {STimeWindow{INT64_MIN, INT64_MIN}, STimeWindow{INT64_MAX, INT64_MAX}}) {
    reader()->info.window = window;
    int             before = brinBlockReads;
    SBrinRecordIter iter = {};
    initBrinRecordIter(&iter, reader()->pFileReader, blocks, reader());
    SBrinRecord* record = nullptr;
    EXPECT_EQ(0, getNextBrinRecord(&iter, &record));
    ASSERT_NE(nullptr, record);
    EXPECT_EQ(INT64_MIN, record->firstKey.key.ts);
    EXPECT_EQ(0, getNextBrinRecord(&iter, &record));
    ASSERT_NE(nullptr, record);
    EXPECT_EQ(INT64_MAX, record->lastKey.key.ts);
    EXPECT_EQ(0, getNextBrinRecord(&iter, &record));
    EXPECT_EQ(nullptr, record);
    clearBrinBlockIter(&iter);
    EXPECT_EQ(1, brinBlockReads - before);
  }
  taosArrayDestroy(blocks);
}

TEST_F(TsdbBrinTimeSummaryTest, UnreliableTimeColumnsRemainUnknownAndReadAgain) {
  seedIndexedFile(0, 0);
  auto c = cond();
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "brin-incomplete", nullptr));
  EXPECT_FALSE(read(true).empty());
  const TBrinBlkArray* directory = nullptr;
  ASSERT_EQ(0, tsdbDataFileReadBrinBlk(reader()->pFileReader, &directory));
  clearBrinTimeSummary(reader());
  prepareBrinTimeSummary(reader(), directory);
  SArray* blocks = taosArrayInit(1, sizeof(SBrinBlk));
  ASSERT_NE(nullptr, blocks);
  ASSERT_NE(nullptr, taosArrayPush(blocks, &directory->data[0]));
  ioStub = &stub_;
  stub_.set(tsdbDataFileReadBrinBlock, truncateBrinTimeColumn);
  SBrinRecordIter iter = {};
  initBrinRecordIter(&iter, reader()->pFileReader, blocks, reader());
  SBrinRecord* record = nullptr;
  EXPECT_EQ(0, getNextBrinRecord(&iter, &record));
  EXPECT_NE(nullptr, record);
  EXPECT_GT(reader()->brinTimeSummary.pEntries[0].window.skey, reader()->brinTimeSummary.pEntries[0].window.ekey);
  clearBrinBlockIter(&iter);
  stub_.set(tsdbDataFileReadBrinBlock, countedBrinBlock);
  reader()->info.window = {500, 510};
  int before = brinBlockReads;
  initBrinRecordIter(&iter, reader()->pFileReader, blocks, reader());
  EXPECT_EQ(0, getNextBrinRecord(&iter, &record));
  EXPECT_NE(nullptr, record);
  clearBrinBlockIter(&iter);
  EXPECT_EQ(1, brinBlockReads - before);
  EXPECT_EQ(10, reader()->brinTimeSummary.pEntries[0].window.skey);
  EXPECT_EQ(265, reader()->brinTimeSummary.pEntries[0].window.ekey);
  taosArrayDestroy(blocks);
}

TEST_F(TsdbBrinTimeSummaryTest, SameFidReplacementHeadStartsColdAfterSnapshotRelease) {
  seedIndexedFile(0, 0);
  seedIndexedFile(0, 100, 20);
  ASSERT_NE(files_.data[0]->farr[TSDB_FTYPE_HEAD], files_.data[1]->farr[TSDB_FTYPE_HEAD]);
  TFileSetArray selected = files_;
  selected.size = 1;
  snap_.pfSetArray = &selected;
  auto c = cond();
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data() + 1, 1, nullptr, &reader_, "brin-head", nullptr));
  EXPECT_TRUE(read().empty());
  ASSERT_NE(nullptr, reader()->brinTimeSummary.pEntries);
  ASSERT_EQ(0, tsdbReaderSuspend2(reader()));
  EXPECT_EQ(nullptr, reader()->brinTimeSummary.pEntries);
  selected.data = &files_.data[1];
  STsdbReadSnap replacement = snap_;
  snapshot = &replacement;
  r[1].window = {366, 366};
  int before = brinBlockReads;
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
  ASSERT_EQ(1u, read().size());
  EXPECT_EQ(1, brinBlockReads - before);
  EXPECT_EQ(files_.data[1]->farr[TSDB_FTYPE_HEAD], reader()->brinTimeSummary.pHead);
  ASSERT_EQ(0, tsdbReaderSuspend2(reader()));
  snapshot = &snap_;
  snap_.pfSetArray = &files_;
}

TEST_F(TsdbBrinTimeSummaryTest, ReadAndDecodeErrorsDoNotPublishPartialRanges) {
  seedIndexedFile(0, 0);
  auto c = cond();
  auto r = ranges();
  ioStub = &stub_;
  for (int stage = 0; stage < 2; ++stage) {
    ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, nullptr, 0, nullptr, &reader_, "brin-errors", nullptr));
    stub_.set(tsdbReadFileToBuffer, countedRead);
    stub_.set(tDecompressDataToBuffer, countedDecode);
    brinFailureStage = stage;
    stub_.set(tsdbDataFileReadBrinBlock, failBrinPayload);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
    failRead = false;
    stub_.set(tsdbDataFileReadBrinBlock, countedBrinBlock);
    stub_.reset(tsdbReadFileToBuffer);
    stub_.reset(tDecompressDataToBuffer);
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
    // Failed queries keep their original sticky error; slots must never publish a partial decode.
    for (int i = 0; i < reader()->brinTimeSummary.numEntries; ++i) {
      EXPECT_GT(reader()->brinTimeSummary.pEntries[i].window.skey, reader()->brinTimeSummary.pEntries[i].window.ekey);
    }
    tsdbReaderClose2(reader_);
    reader_ = nullptr;
    const int before = brinBlockReads;
    ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data() + 1, 1, nullptr, &reader_, "brin-error-reopen", nullptr));
    EXPECT_TRUE(read().empty());
    EXPECT_EQ(1, brinBlockReads - before);
    tsdbReaderClose2(reader_);
    reader_ = nullptr;
  }
}

TEST_P(TsdbReaderGroupSwitchTest, SkippedBrinStillReturnsMemoryRowsAcrossGroups) {
  seedIndexedFile(0, 0);
  seed(Mem);
  ioStub = &stub_;
  brinBlockReads = 0;
  stub_.set(tsdbDataFileReadBrinBlock, countedBrinBlock);
  auto c = cond(std::get<1>(GetParam()));
  auto r = ranges();
  // UID 8 has disk data only at 266, but memory data at 100..109.
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data() + 1, 1, nullptr, &reader_, "brin-memory", nullptr));
  EXPECT_EQ(20u, read().size());
  EXPECT_EQ(1, brinBlockReads);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
  EXPECT_EQ(20u, read().size());
  EXPECT_EQ(1, brinBlockReads);
  auto* reader = static_cast<STsdbReader*>(reader_);
  ASSERT_EQ(0, tsdbReaderSuspend2(reader));
  EXPECT_EQ(nullptr, reader->brinTimeSummary.pEntries);
}

TEST_P(TsdbReaderGroupSwitchTest, GroupsReleaseDataFileAndReloadIndex) {
  seed(Data);
  auto c = cond(std::get<1>(GetParam()));
  auto r = ranges();
  ioStub = &stub_;
  opens = reads = decodes = physicalOpens = 0;
  stub_.set(taosOpenFile, countedPhysicalOpen);
  stub_.set(tsdbDataFileReaderOpen, countedOpen);
  stub_.set(tsdbReadFileToBuffer, countedRead);
  stub_.set(tDecompressDataToBuffer, countedDecode);
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "no-file-cache", nullptr));
  EXPECT_EQ(20u, read().size());
  const int oldOpens = opens, oldReads = reads, oldDecodes = decodes, oldPhysical = physicalOpens;
  ASSERT_GT(oldPhysical, 0);
  ASSERT_GT(oldOpens, 0);
  ASSERT_GT(oldReads, 0);
  ASSERT_GT(oldDecodes, 0);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
  // Activation loads the index, before DATA decompression obscures the counters.
  EXPECT_GT(opens, oldOpens);
  EXPECT_GT(physicalOpens, oldPhysical);
  EXPECT_GT(reads, oldReads);
  EXPECT_GT(decodes, oldDecodes);
  EXPECT_EQ(20u, read().size());

  // An empty group must release the previous group's file resources too.
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, nullptr, 0));
  EXPECT_EQ(nullptr, static_cast<STsdbReader*>(reader_)->pFileReader);
  EXPECT_TRUE(read().empty());
}
TEST_P(TsdbReaderGroupSwitchTest, GroupsReopenFilesAndReadDifferentBrinBlocks) {
  int64_t begin = 0, end = 0;
  tsdbFidKeyRange(1, tsdb_.keepCfg.days, tsdb_.keepCfg.precision, &begin, &end);
  seedIndexedFile(0, 0);
  seedIndexedFile(1, begin);
  ioStub = &stub_;
  stub_.set(tsdbDataFileReaderOpen, countedOpen);
  stub_.set(tsdbDataFileReaderClose, countedClose);
  brinBlockReads = 0;
  stub_.set(tsdbDataFileReadBrinBlock, countedBrinBlock);
  auto c = cond(std::get<1>(GetParam()));
  auto r = ranges();
  r.resize(1);
  r[0].window = {10, 265};
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "slots", nullptr));
  EXPECT_EQ(256u, read().size());
  int previousOpens = opens, previousReads = brinBlockReads;
  for (int step = 0; step < 4; ++step) {
    r[0].table.uid = step == 0 ? 8 : 7;
    r[0].window =
        step == 0 ? STimeWindow{266, 266} : (step == 2 ? STimeWindow{begin + 10, begin + 265} : STimeWindow{10, 265});
    ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), 1));
    EXPECT_EQ(step == 0 ? 1u : 256u, read().size());
    EXPECT_EQ(1, opens - previousOpens);
    EXPECT_EQ(1, brinBlockReads - previousReads);
    EXPECT_EQ(1u, liveFiles.size());
    EXPECT_EQ(1u, peakFiles);
    previousOpens = opens;
    previousReads = brinBlockReads;
  }
}
TEST_P(TsdbReaderGroupSwitchTest, SuspendAfterFilePhase) {
  seed(Data);
  // Add disjoint memory data in the same snapshot after the DATA phase.
  SMemTable** memory = std::get<0>(GetParam()) == IMem ? &snap_.pIMem : &snap_.pMem;
  ASSERT_EQ(0, tsdbMemTableCreate(&tsdb_, memory));
  tsdb_.mem = *memory;
  for (int ts = 150; ts < 160; ++ts) {
    SRow*         row = makeRow(ts, 1, ts * 10 + 1);
    SSubmitTbData submit = {};
    submit.suid = 1;
    submit.uid = 7;
    submit.sver = 1;
    submit.aRowP = taosArrayInit(1, sizeof(SRow*));
    taosArrayPush(submit.aRowP, &row);
    int32_t affected = 0;
    ASSERT_EQ(0, tsdbInsertTableData(&tsdb_, 1, &submit, &affected));
    taosArrayDestroy(submit.aRowP);
    taosMemoryFree(row);
  }
  vnode_.config.tsdbCfg.maxRows = 3;
  auto c = cond();
  auto r = ranges();
  r.resize(1);
  r[0].window = {9, 159};
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "phase", nullptr));
  auto*            reader = static_cast<STsdbReader*>(reader_);
  std::vector<Row> rows;
  while (reader->status.loadFromFile) {
    auto part = read(true);
    rows.insert(rows.end(), part.begin(), part.end());
  }
  ASSERT_NE(nullptr, reader->pFileReader);
  ASSERT_NE(nullptr, reader->brinTimeSummary.pEntries);
  ASSERT_LT(rows.size(), 34u);
  ASSERT_EQ(0, tsdbReaderSuspend2(reader));
  EXPECT_EQ(nullptr, reader->pFileReader);
  EXPECT_EQ(nullptr, reader->brinTimeSummary.pEntries);
  EXPECT_EQ(nullptr, reader->brinTimeSummary.pSnapshot);
  auto rest = read();
  rows.insert(rows.end(), rest.begin(), rest.end());
  EXPECT_EQ(34u, rows.size());
  EXPECT_EQ(34u, std::vector<Row>(rows.begin(), std::unique(rows.begin(), rows.end())).size());
  ASSERT_EQ(0, tsdbReaderSuspend2(reader));
  EXPECT_EQ(nullptr, reader->pFileReader);
  EXPECT_EQ(nullptr, reader->brinTimeSummary.pEntries);
  EXPECT_EQ(nullptr, reader->brinTimeSummary.pSnapshot);
  EXPECT_TRUE(read().empty());
}
TEST_P(TsdbReaderGroupSwitchTest, BrinRecordsOwnKeysAcrossBlocks) {
  schemas[1].type = columns_[1].type = TSDB_DATA_TYPE_VARCHAR;
  schemas[1].bytes = columns_[1].bytes = 32;
  taosMemoryFree(schema_);
  schema_ = tBuildTSchema(schemas, 3, 1);
  seedIndexedFile(0, 0);
  auto c = cond(std::get<1>(GetParam()));
  auto r = ranges();
  r[0].window = {10, 265};
  r[1].window = {266, 266};
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 2, nullptr, &reader_, "pk-slots", nullptr));
  // Block descriptors must own their keys after the BRIN iterator advances.
  auto rows = read();
  ASSERT_EQ(257u, rows.size());
  for (const auto& row : rows) EXPECT_EQ(std::get<1>(row) - 9, std::get<2>(row));
  auto*                file = static_cast<STsdbReader*>(reader_)->pFileReader;
  const TBrinBlkArray* directory = nullptr;
  ASSERT_EQ(0, tsdbDataFileReadBrinBlk(file, &directory));
  ASSERT_EQ(2, directory->size);
  SBrinBlock block = {};
  ASSERT_EQ(0, tsdbDataFileReadBrinBlock(file, &directory->data[0], &block));
  SBrinRecord record = {};
  ASSERT_EQ(0, tBrinBlockGet(&block, 0, &record));
  SFileDataBlockInfo copy = {};
  ASSERT_EQ(0, recordToBlockInfo(&copy, &record));
  tBrinBlockClear(&block);
  ASSERT_EQ(0, tsdbDataFileReadBrinBlock(file, &directory->data[1], &block));
  EXPECT_EQ("00000001", std::string(varDataVal(copy.firstPk.pData), varDataLen(copy.firstPk.pData)));
  EXPECT_EQ("00000001", std::string(varDataVal(copy.lastPk.pData), varDataLen(copy.lastPk.pData)));
  tBrinBlockDestroy(&block);
  taosMemoryFree(copy.firstPk.pData);
  taosMemoryFree(copy.lastPk.pData);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data(), 1));
  EXPECT_EQ(256u, read().size());
}

TEST_P(TsdbReaderGroupSwitchTest, FileOpenFailureIsDestructible) {
  seed(Data);
  auto c = cond();
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "open-failure", nullptr));
  ioStub = &stub_;
  lowOpenCalls = 0;
  stub_.set(tsdbOpenFile, failSecondFileOpen);
  bool next = false;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, tsdbNextDataBlock2(reader_, &next));
  EXPECT_EQ(2, lowOpenCalls);
  EXPECT_EQ(nullptr, static_cast<STsdbReader*>(reader_)->pFileReader);
}
TEST_P(TsdbReaderGroupSwitchTest, PreservesConditionalSttCache) {
  seed(Stt);
  auto c = cond(std::get<1>(GetParam()));
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "stt-cache", nullptr));
  auto* reader = static_cast<STsdbReader*>(reader_);
  tsdbSetFilesetDelimited(reader);
  EXPECT_EQ(20u, read().size());
  ASSERT_EQ(1, taosArrayGetSize(reader->status.pLDataIterArray));
  auto* list = static_cast<SArray*>(taosArrayGetP(reader->status.pLDataIterArray, 0));
  auto* original = static_cast<SLDataIter*>(taosArrayGetP(list, 0))->pReader;
  ASSERT_NE(nullptr, original);
  ASSERT_EQ(0, tsdbReaderActivateGroup2(reader_, r.data() + 1, 1));
  EXPECT_EQ(20u, read().size());
  list = static_cast<SArray*>(taosArrayGetP(reader->status.pLDataIterArray, 0));
  EXPECT_EQ(original, static_cast<SLDataIter*>(taosArrayGetP(list, 0))->pReader);
  ASSERT_EQ(0, tsdbReaderSuspend2(reader));
  EXPECT_EQ(nullptr, reader->pFilesetSttCache);
  EXPECT_EQ(0, taosArrayGetSize(reader->status.pLDataIterArray));
  EXPECT_TRUE(read().empty());
}
TEST_P(TsdbReaderGroupSwitchTest, SnapshotReseekClosesDataFile) {
  seed(Data);
  auto c = cond();
  auto r = ranges();
  ASSERT_EQ(0, tsdbReaderOpenGroup2(&vnode_, &c, r.data(), 1, nullptr, &reader_, "snapshot-reseek", nullptr));
  EXPECT_EQ(20u, read().size());
  ASSERT_NE(nullptr, snapshotReseek);
  ASSERT_EQ(0, snapshotReseek(reader_));
  EXPECT_EQ(nullptr, static_cast<STsdbReader*>(reader_)->pFileReader);
  EXPECT_TRUE(read().empty());
}
INSTANTIATE_TEST_SUITE_P(Storage, TsdbReaderGroupSwitchTest,
                         ::testing::Combine(::testing::Values(Data, PartialData, Stt, Mem, IMem, Merged),
                                            ::testing::Values(TSDB_ORDER_ASC, TSDB_ORDER_DESC)));
}  // namespace
