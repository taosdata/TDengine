/**
 * @file monTest.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief monitor module tests
 * @version 1.0
 * @date 2022-03-05
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include "os.h"

#include "monitor.h"
#include "stub.h"
#include "taoserror.h"
#include "tglobal.h"
#include "thttp.h"
#include "tjson.h"
#include "tlog.h"

namespace {

struct CapturedHttpReport {
  int32_t     calls = 0;
  std::string uri;
  std::string body;
};

CapturedHttpReport gCapturedHttpReport;

int32_t captureHttpReport(const char *, const char *uri, uint16_t, char *pCont, int32_t contLen, EHttpCompFlag,
                          const char *) {
  ++gCapturedHttpReport.calls;
  gCapturedHttpReport.uri = uri;
  gCapturedHttpReport.body.assign(pCont, contLen);
  return TSDB_CODE_SUCCESS;
}

class HttpReportStubGuard {
 public:
  HttpReportStubGuard() {
    gCapturedHttpReport = {};
    stub_.set(taosSendHttpReportWithQID, captureHttpReport);
  }

  ~HttpReportStubGuard() { stub_.reset(taosSendHttpReportWithQID); }

 private:
  Stub stub_;
};

struct HttpResolveLogState {
  std::mutex              mutex;
  std::condition_variable changed;
  int32_t                 resolveFailures = 0;
  bool                    firstFailureBlocked = false;
  bool                    releaseFirstFailure = false;
};

HttpResolveLogState gHttpResolveLogState;

void captureBlockingHttpResolveLog(const char *, int32_t, int32_t, const char *format, ...) {
  if (strstr(format, "failed to resolving domain names") == nullptr) return;

  std::unique_lock<std::mutex> lock(gHttpResolveLogState.mutex);
  ++gHttpResolveLogState.resolveFailures;
  if (gHttpResolveLogState.resolveFailures == 1) {
    gHttpResolveLogState.firstFailureBlocked = true;
    gHttpResolveLogState.changed.notify_all();
    gHttpResolveLogState.changed.wait(lock, [] { return gHttpResolveLogState.releaseFirstFailure; });
  }
  gHttpResolveLogState.changed.notify_all();
}

class HttpBurstHarness {
 public:
  HttpBurstHarness() : savedRpcDebugFlag_(rpcDebugFlag) {
    {
      std::lock_guard<std::mutex> lock(gHttpResolveLogState.mutex);
      gHttpResolveLogState.resolveFailures = 0;
      gHttpResolveLogState.firstFailureBlocked = false;
      gHttpResolveLogState.releaseFirstFailure = false;
    }
    stub_.set(taosPrintLog, captureBlockingHttpResolveLog);
    rpcDebugFlag |= DEBUG_ERROR | DEBUG_FILE;
    channelId_ = taosInitHttpChan();
  }

  ~HttpBurstHarness() {
    releaseFirstFailure();
    taosDestroyHttpChan(channelId_);
    rpcDebugFlag = savedRpcDebugFlag_;
    stub_.reset(taosPrintLog);
  }

  int64_t channelId() const { return channelId_; }

  bool waitForFirstFailure() {
    std::unique_lock<std::mutex> lock(gHttpResolveLogState.mutex);
    return gHttpResolveLogState.changed.wait_for(lock, std::chrono::seconds(5),
                                                 [] { return gHttpResolveLogState.firstFailureBlocked; });
  }

  void releaseFirstFailure() {
    std::lock_guard<std::mutex> lock(gHttpResolveLogState.mutex);
    gHttpResolveLogState.releaseFirstFailure = true;
    gHttpResolveLogState.changed.notify_all();
  }

  bool waitForFailures(int32_t expected) {
    std::unique_lock<std::mutex> lock(gHttpResolveLogState.mutex);
    return gHttpResolveLogState.changed.wait_for(
        lock, std::chrono::seconds(5), [expected] { return gHttpResolveLogState.resolveFailures == expected; });
  }

  int32_t resolveFailures() const {
    std::lock_guard<std::mutex> lock(gHttpResolveLogState.mutex);
    return gHttpResolveLogState.resolveFailures;
  }

 private:
  Stub    stub_;
  int32_t savedRpcDebugFlag_ = 0;
  int64_t channelId_ = -1;
};

struct JsonDeleter {
  void operator()(SJson *pJson) const { tjsonDelete(pJson); }
};

using JsonPtr = std::unique_ptr<SJson, JsonDeleter>;

}  // namespace

class MonitorTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    SMonCfg cfg;
    cfg.maxLogs = 2;
    cfg.port = 80;
    cfg.server = "localhost";
    cfg.comp = 1;
    monInit(&cfg);
  }

  static void TearDownTestSuite() {
    monCleanup();
    taosMsleep(100);
  }

 public:
  void SetUp() override {
    savedEnableMonitor_ = tsEnableMonitor;
    savedMonitorPort_ = tsMonitorPort;
    tstrncpy(savedMonitorFqdn_, tsMonitorFqdn, TSDB_FQDN_LEN);

    tsEnableMonitor = true;
    tsMonitorPort = 80;
    tstrncpy(tsMonitorFqdn, "localhost", TSDB_FQDN_LEN);
  }

  void TearDown() override {
    tsEnableMonitor = savedEnableMonitor_;
    tsMonitorPort = savedMonitorPort_;
    tstrncpy(tsMonitorFqdn, savedMonitorFqdn_, TSDB_FQDN_LEN);
  }

  void GetBasicInfo(SMonBasicInfo *pInfo);
  void GetDnodeInfo(SMonDnodeInfo *pInfo);
  void GetSysInfo(SMonSysInfo *pInfo);

  void GetClusterInfo(SMonClusterInfo *pInfo);
  void GetVgroupInfo(SMonVgroupInfo *pInfo);
  void GetGrantInfo(SMonGrantInfo *pInfo);

  void GetVnodeStat(SVnodesStat *pStat);
  void GetDiskInfo(SMonDiskInfo *pInfo);

  void GetLogInfo(SMonLogs *logs);

  void AddLogInfo1();
  void AddLogInfo2();

  bool     savedEnableMonitor_ = false;
  uint16_t savedMonitorPort_ = 0;
  char     savedMonitorFqdn_[TSDB_FQDN_LEN] = {0};
};

TEST(HttpReportTest, RejectsInvalidMessageWithoutDereferencingNullMessage) {
  char content[] = "{}";

  EXPECT_EQ(taosSendHttpReportWithQID(NULL, "/report", 80, content, sizeof(content) - 1, HTTP_FLAT, NULL),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(taosSendRecvHttpReportWithQID(NULL, "/report", 80, content, sizeof(content) - 1, HTTP_FLAT, NULL, 0),
            TSDB_CODE_INVALID_PARA);
}

TEST(HttpReportTest, BurstLargerThanBatchDrainsWithoutAnotherSend) {
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of transport logging is unreliable in Windows release builds";
#endif
  constexpr int32_t kReportCount = 41;
  char              content[] = "{}";
  HttpBurstHarness  harness;
  ASSERT_GT(harness.channelId(), 0);

  ASSERT_EQ(taosSendHttpReportByChan("", "/report", 80, content, sizeof(content) - 1, HTTP_FLAT, harness.channelId(),
                                     nullptr),
            TSDB_CODE_SUCCESS);
  ASSERT_TRUE(harness.waitForFirstFailure());

  for (int32_t i = 1; i < kReportCount; ++i) {
    ASSERT_EQ(taosSendHttpReportByChan("", "/report", 80, content, sizeof(content) - 1, HTTP_FLAT, harness.channelId(),
                                       nullptr),
              TSDB_CODE_SUCCESS);
  }
  harness.releaseFirstFailure();

  EXPECT_TRUE(harness.waitForFailures(kReportCount));
  EXPECT_EQ(harness.resolveFailures(), kReportCount);
}

void MonitorTest::GetBasicInfo(SMonBasicInfo *pInfo) {
  pInfo->dnode_id = 1;
  strcpy(pInfo->dnode_ep, "localhost");
  pInfo->cluster_id = 6980428120398645172;
  pInfo->protocol = 1;
}

void MonitorTest::GetDnodeInfo(SMonDnodeInfo *pInfo) {
  pInfo->uptime = 1.2;
  pInfo->has_mnode = 1;

  strcpy(pInfo->logdir.name, "/log/dir/d");
  pInfo->logdir.size.avail = 41;
  pInfo->logdir.size.total = 42;
  pInfo->logdir.size.used = 43;

  strcpy(pInfo->tempdir.name, "/data/dir/d");
  pInfo->tempdir.size.avail = 51;
  pInfo->tempdir.size.total = 52;
  pInfo->tempdir.size.used = 53;
}

void MonitorTest::GetSysInfo(SMonSysInfo *pInfo) {
  pInfo->cpu_engine = 2.1;
  pInfo->cpu_system = 2.1;
  pInfo->cpu_cores = 2;
  pInfo->mem_engine = 3;
  pInfo->mem_system = 3;
  pInfo->mem_total = 3;
  pInfo->disk_engine = 4;
  pInfo->disk_used = 4;
  pInfo->disk_total = 4;
  pInfo->net_in = 5;
  pInfo->net_out = 5;
  pInfo->io_read = 6;
  pInfo->io_write = 6;
  pInfo->io_read_disk = 7;
  pInfo->io_write_disk = 7;
}

void MonitorTest::GetClusterInfo(SMonClusterInfo *pInfo) {
  strcpy(pInfo->first_ep, "localhost:6030");
  pInfo->first_ep_dnode_id = 1;
  strcpy(pInfo->version, "3.0.0.0");
  pInfo->master_uptime = 1;
  pInfo->monitor_interval = 2;
  pInfo->vgroups_total = 3;
  pInfo->vgroups_alive = 43;
  pInfo->vnodes_total = 5;
  pInfo->vnodes_alive = 6;
  pInfo->connections_total = 7;

  pInfo->dnodes = taosArrayInit(4, sizeof(SMonDnodeDesc));
  SMonDnodeDesc d1 = {0};
  d1.dnode_id = 1;
  strcpy(d1.dnode_ep, "localhost:6030");
  strcpy(d1.status, "ready");
  taosArrayPush(pInfo->dnodes, &d1);
  SMonDnodeDesc d2 = {0};
  d2.dnode_id = 2;
  strcpy(d2.dnode_ep, "localhost:7030");
  strcpy(d2.status, "offline");
  taosArrayPush(pInfo->dnodes, &d2);

  pInfo->mnodes = taosArrayInit(4, sizeof(SMonMnodeDesc));
  SMonMnodeDesc m1 = {0};
  m1.mnode_id = 1;
  strcpy(m1.mnode_ep, "localhost:6030");
  strcpy(m1.role, "master");
  taosArrayPush(pInfo->mnodes, &m1);
  SMonMnodeDesc m2 = {0};
  m2.mnode_id = 2;
  strcpy(m2.mnode_ep, "localhost:7030");
  strcpy(m2.role, "unsynced");
  taosArrayPush(pInfo->mnodes, &m2);
}

void MonitorTest::GetVgroupInfo(SMonVgroupInfo *pInfo) {
  pInfo->vgroups = taosArrayInit(4, sizeof(SMonVgroupDesc));

  SMonVgroupDesc vg1 = {0};
  vg1.vgroup_id = 1;
  strcpy(vg1.database_name, "d1");
  vg1.tables_num = 4;
  strcpy(vg1.status, "ready");
  vg1.vnodes[0].dnode_id = 1;
  strcpy(vg1.vnodes[0].vnode_role, "master");
  vg1.vnodes[1].dnode_id = 2;
  strcpy(vg1.vnodes[1].vnode_role, "slave");
  taosArrayPush(pInfo->vgroups, &vg1);

  SMonVgroupDesc vg2 = {0};
  vg2.vgroup_id = 2;
  strcpy(vg2.database_name, "d2");
  vg2.tables_num = 5;
  strcpy(vg2.status, "offline");
  vg2.vnodes[0].dnode_id = 1;
  strcpy(vg2.vnodes[0].vnode_role, "master");
  vg2.vnodes[1].dnode_id = 2;
  strcpy(vg2.vnodes[1].vnode_role, "unsynced");
  taosArrayPush(pInfo->vgroups, &vg2);

  SMonVgroupDesc vg3 = {0};
  vg3.vgroup_id = 3;
  strcpy(vg3.database_name, "d3");
  vg3.tables_num = 6;
  strcpy(vg3.status, "ready");
  vg3.vnodes[0].dnode_id = 1;
  strcpy(vg3.vnodes[0].vnode_role, "master");
  taosArrayPush(pInfo->vgroups, &vg3);
}

void MonitorTest::GetGrantInfo(SMonGrantInfo *pInfo) {
  pInfo->expire_time = 1234567;
  pInfo->timeseries_total = 234567;
  pInfo->timeseries_used = 34567;
}

void MonitorTest::GetVnodeStat(SVnodesStat *pInfo) {
  pInfo->numOfSelectReqs = 8;
  pInfo->numOfInsertReqs = 9;
  pInfo->numOfInsertSuccessReqs = 10;
  pInfo->numOfBatchInsertReqs = 11;
  pInfo->numOfBatchInsertSuccessReqs = 12;
  pInfo->errors = 4;
  pInfo->totalVnodes = 5;
  pInfo->masterNum = 6;
}

void MonitorTest::GetDiskInfo(SMonDiskInfo *pInfo) {
  pInfo->datadirs = taosArrayInit(2, sizeof(SMonDiskDesc));
  SMonDiskDesc d1 = {0};
  strcpy(d1.name, "/t1/d1/d");
  d1.level = 0;
  d1.size.avail = 11;
  d1.size.total = 12;
  d1.size.used = 13;
  taosArrayPush(pInfo->datadirs, &d1);

  SMonDiskDesc d2 = {0};
  strcpy(d2.name, "/t2d2/d");
  d2.level = 2;
  d2.size.avail = 21;
  d2.size.total = 22;
  d2.size.used = 23;
  taosArrayPush(pInfo->datadirs, &d2);

  SMonDiskDesc d3 = {0};
  strcpy(d3.name, "/t3/d3/d");
  d3.level = 3;
  d3.size.avail = 31;
  d3.size.total = 32;
  d3.size.used = 33;
  taosArrayPush(pInfo->datadirs, &d3);
}

void MonitorTest::GetLogInfo(SMonLogs *logs) {
  logs->logs = taosArrayInit(4, sizeof(SMonLogItem));

  SMonLogItem item1 = {0};
  item1.level = DEBUG_INFO;

  item1.ts = taosGetTimestampMs();
  strcpy(item1.content, "log test1");
  taosArrayPush(logs->logs, &item1);

  SMonLogItem item2 = {0};
  item2.level = DEBUG_ERROR;
  item2.ts = taosGetTimestampMs();
  strcpy(item2.content, "log test2");
  taosArrayPush(logs->logs, &item2);

  logs->numOfErrorLogs = 1;
  logs->numOfInfoLogs = 2;
  logs->numOfDebugLogs = 3;
  logs->numOfTraceLogs = 4;
}

void MonitorTest::AddLogInfo1() {
  monRecordLog(taosGetTimestampMs(), DEBUG_INFO, "1 -------------------------- a");
  monRecordLog(taosGetTimestampMs(), DEBUG_ERROR, "1 ------------------------ b");
  monRecordLog(taosGetTimestampMs(), DEBUG_DEBUG, "1 ------- c");
}

void MonitorTest::AddLogInfo2() {
  monRecordLog(taosGetTimestampMs(), DEBUG_ERROR, "2 ------- a");
  monRecordLog(taosGetTimestampMs(), DEBUG_ERROR, "2 ------- b");
}

TEST_F(MonitorTest, 01_Full) {
  AddLogInfo1();

  SMonDmInfo dmInfo = {0};
  GetBasicInfo(&dmInfo.basic);
  GetDnodeInfo(&dmInfo.dnode);
  GetSysInfo(&dmInfo.sys);

  SMonMmInfo mmInfo = {0};
  GetClusterInfo(&mmInfo.cluster);
  GetVgroupInfo(&mmInfo.vgroup);
  GetGrantInfo(&mmInfo.grant);
  GetSysInfo(&mmInfo.sys);
  GetLogInfo(&mmInfo.log);

  SMonVmInfo vmInfo = {0};
  GetDiskInfo(&vmInfo.tfs);
  GetVnodeStat(&vmInfo.vstat);
  GetSysInfo(&vmInfo.sys);
  GetLogInfo(&vmInfo.log);

  SMonQmInfo qmInfo = {0};
  GetSysInfo(&qmInfo.sys);
  GetLogInfo(&qmInfo.log);

  SMonSmInfo smInfo = {0};
  GetSysInfo(&smInfo.sys);
  GetLogInfo(&smInfo.log);

  SMonBmInfo bmInfo = {0};
  GetSysInfo(&bmInfo.sys);
  GetLogInfo(&bmInfo.log);

  monSetDmInfo(&dmInfo);
  monSetMmInfo(&mmInfo);
  monSetVmInfo(&vmInfo);
  monSetQmInfo(&qmInfo);
  monSetSmInfo(&smInfo);
  monSetBmInfo(&bmInfo);

  tFreeSMonMmInfo(&mmInfo);
  tFreeSMonVmInfo(&vmInfo);
  tFreeSMonSmInfo(&smInfo);
  tFreeSMonQmInfo(&qmInfo);
  tFreeSMonBmInfo(&bmInfo);
  monGenAndSendReport();
}

TEST_F(MonitorTest, 02_Log) {
  AddLogInfo2();
  monGenAndSendReport();
}

TEST_F(MonitorTest, StreamFailureUsesGeneralMetricProtocol) {
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of HTTP transport is unreliable in Windows release builds";
#endif
  constexpr int64_t kClusterId = 6980428120398645172;
  constexpr int64_t kStreamId = 9223372036854775000;
  constexpr int64_t kTimestamp = 1787068800123;
  constexpr int32_t kErrorCode = -2147454964;
  const char       *streamName = "1.db.stream_\"quoted\\name";

  SMonDmInfo dmInfo = {};
  dmInfo.basic.cluster_id = kClusterId;
  monSetDmInfo(&dmInfo);

  HttpReportStubGuard guard;
  monGenAndSendReport();
  ASSERT_GT(gCapturedHttpReport.calls, 0);
  gCapturedHttpReport = {};

  monReportStreamFailure(kTimestamp, kStreamId, streamName, kErrorCode);

  ASSERT_EQ(gCapturedHttpReport.calls, 1);
  EXPECT_EQ(gCapturedHttpReport.uri, "/general-metric");

  JsonPtr root(tjsonParse(gCapturedHttpReport.body.c_str()));
  ASSERT_NE(root.get(), nullptr);
  ASSERT_TRUE(tjsonIsArray(root.get()));
  ASSERT_EQ(tjsonGetArraySize(root.get()), 1);

  SJson *report = tjsonGetArrayItem(root.get(), 0);
  ASSERT_NE(report, nullptr);
  EXPECT_STREQ(tjsonGetStringPointer(report, "ts"), "1787068800123");

  double protocol = 0;
  ASSERT_EQ(tjsonGetDoubleValue(report, "protocol", &protocol), TSDB_CODE_SUCCESS);
  EXPECT_DOUBLE_EQ(protocol, 2);

  SJson *tables = tjsonGetObjectItem(report, "tables");
  ASSERT_TRUE(tjsonIsArray(tables));
  ASSERT_EQ(tjsonGetArraySize(tables), 1);
  SJson *table = tjsonGetArrayItem(tables, 0);
  ASSERT_NE(table, nullptr);
  EXPECT_STREQ(tjsonGetStringPointer(table, "name"), "taosd_stream_failure");

  SJson *groups = tjsonGetObjectItem(table, "metric_groups");
  ASSERT_TRUE(tjsonIsArray(groups));
  ASSERT_EQ(tjsonGetArraySize(groups), 1);
  SJson *group = tjsonGetArrayItem(groups, 0);
  ASSERT_NE(group, nullptr);

  SJson *tags = tjsonGetObjectItem(group, "tags");
  ASSERT_TRUE(tjsonIsArray(tags));
  ASSERT_EQ(tjsonGetArraySize(tags), 3);
  const char *expectedTagNames[] = {"cluster_id", "stream_id", "stream_name"};
  const char *expectedTagValues[] = {"6980428120398645172", "9223372036854775000", streamName};
  for (int32_t i = 0; i < 3; ++i) {
    SJson *tag = tjsonGetArrayItem(tags, i);
    ASSERT_NE(tag, nullptr);
    EXPECT_STREQ(tjsonGetStringPointer(tag, "name"), expectedTagNames[i]);
    EXPECT_STREQ(tjsonGetStringPointer(tag, "value"), expectedTagValues[i]);
  }

  SJson *metrics = tjsonGetObjectItem(group, "metrics");
  ASSERT_TRUE(tjsonIsArray(metrics));
  ASSERT_EQ(tjsonGetArraySize(metrics), 1);
  SJson *metric = tjsonGetArrayItem(metrics, 0);
  ASSERT_NE(metric, nullptr);
  EXPECT_STREQ(tjsonGetStringPointer(metric, "name"), "error_code");
  double errorCode = 0;
  ASSERT_EQ(tjsonGetDoubleValue(metric, "value", &errorCode), TSDB_CODE_SUCCESS);
  EXPECT_DOUBLE_EQ(errorCode, static_cast<double>(kErrorCode));
}

TEST_F(MonitorTest, StreamFailureSkipsUnavailableMonitorIdentity) {
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of HTTP transport is unreliable in Windows release builds";
#endif
  SMonDmInfo dmInfo = {};
  dmInfo.basic.cluster_id = 1;
  monSetDmInfo(&dmInfo);

  HttpReportStubGuard guard;
  tsEnableMonitor = false;
  monReportStreamFailure(1, 2, "1.db.stream", -1);
  EXPECT_EQ(gCapturedHttpReport.calls, 0);

  tsEnableMonitor = true;
  dmInfo = {};
  monSetDmInfo(&dmInfo);
  monReportStreamFailure(1, 2, "1.db.stream", -1);
  EXPECT_EQ(gCapturedHttpReport.calls, 0);
}
