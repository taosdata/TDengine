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
#include "os.h"

#include "monitor.h"
#include "tglobal.h"

class MonitorTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    SMonCfg cfg;
    cfg.maxLogs = 2;
    cfg.port = 80;
    cfg.server = "localhost";
    cfg.comp = 0;
    monInit(&cfg);
  }

  static void TearDownTestSuite() { monCleanup(); }

 public:
  void SetUp() override {}
  void TearDown() override {}

  void GetBasicInfo(SMonInfo *pMonitor, SMonBasicInfo *pInfo);
  void GetClusterInfo(SMonInfo *pMonitor, SMonClusterInfo *pInfo);
  void GetVgroupInfo(SMonInfo *pMonitor, SMonVgroupInfo *pInfo);
  void GetGrantInfo(SMonInfo *pMonitor, SMonGrantInfo *pInfo);
  void GetDnodeInfo(SMonInfo *pMonitor, SMonDnodeInfo *pInfo);
  void GetDiskInfo(SMonInfo *pMonitor, SMonDiskInfo *pInfo);
  void AddLogInfo1();
  void AddLogInfo2();
};

void MonitorTest::GetBasicInfo(SMonInfo *pMonitor, SMonBasicInfo *pInfo) {
  pInfo->dnode_id = 1;
  strcpy(pInfo->dnode_ep, "localhost");
  pInfo->cluster_id = 6980428120398645172;
  pInfo->protocol = 1;
}

void MonitorTest::GetClusterInfo(SMonInfo *pMonitor, SMonClusterInfo *pInfo) {
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

void MonitorTest::GetVgroupInfo(SMonInfo *pMonitor, SMonVgroupInfo *pInfo) {
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

void MonitorTest::GetGrantInfo(SMonInfo *pMonitor, SMonGrantInfo *pInfo) {
  pInfo->expire_time = 1234567;
  pInfo->timeseries_total = 234567;
  pInfo->timeseries_used = 34567;
}

void MonitorTest::GetDnodeInfo(SMonInfo *pMonitor, SMonDnodeInfo *pInfo) {
  pInfo->uptime = 1.2;
  pInfo->cpu_engine = 2.1;
  pInfo->cpu_system = 2.1;
  pInfo->cpu_cores = 2;
  pInfo->mem_engine = 3.1;
  pInfo->mem_system = 3.2;
  pInfo->mem_total = 3.3;
  pInfo->disk_engine = 4.1;
  pInfo->disk_used = 4.2;
  pInfo->disk_total = 4.3;
  pInfo->net_in = 5.1;
  pInfo->net_out = 5.2;
  pInfo->io_read = 6.1;
  pInfo->io_write = 6.2;
  pInfo->io_read_disk = 7.1;
  pInfo->io_write_disk = 7.2;
  pInfo->req_select = 8;
  pInfo->req_insert = 9;
  pInfo->req_insert_success = 10;
  pInfo->req_insert_batch = 11;
  pInfo->req_insert_batch_success = 12;
  pInfo->errors = 4;
  pInfo->vnodes_num = 5;
  pInfo->masters = 6;
  pInfo->has_mnode = 1;
}

void MonitorTest::GetDiskInfo(SMonInfo *pMonitor, SMonDiskInfo *pInfo) {
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

  strcpy(pInfo->logdir.name, "/log/dir/d");
  pInfo->logdir.size.avail = 41;
  pInfo->logdir.size.total = 42;
  pInfo->logdir.size.used = 43;

  strcpy(pInfo->tempdir.name, "/data/dir/d");
  pInfo->tempdir.size.avail = 51;
  pInfo->tempdir.size.total = 52;
  pInfo->tempdir.size.used = 53;
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

  SMonInfo *pMonitor = monCreateMonitorInfo();
  if (pMonitor == NULL) return;

  SMonBasicInfo basicInfo = {0};
  GetBasicInfo(pMonitor, &basicInfo);
  monSetBasicInfo(pMonitor, &basicInfo);

  SMonClusterInfo clusterInfo = {0};
  SMonVgroupInfo  vgroupInfo = {0};
  SMonGrantInfo   grantInfo = {0};
  GetClusterInfo(pMonitor, &clusterInfo);
  GetVgroupInfo(pMonitor, &vgroupInfo);
  GetGrantInfo(pMonitor, &grantInfo);
  monSetClusterInfo(pMonitor, &clusterInfo);
  monSetVgroupInfo(pMonitor, &vgroupInfo);
  monSetGrantInfo(pMonitor, &grantInfo);

  SMonDnodeInfo dnodeInfo = {0};
  GetDnodeInfo(pMonitor, &dnodeInfo);
  monSetDnodeInfo(pMonitor, &dnodeInfo);

  SMonDiskInfo diskInfo = {0};
  GetDiskInfo(pMonitor, &diskInfo);
  monSetDiskInfo(pMonitor, &diskInfo);

  monSendReport(pMonitor);
  monCleanupMonitorInfo(pMonitor);

  taosArrayDestroy(clusterInfo.dnodes);
  taosArrayDestroy(clusterInfo.mnodes);
  taosArrayDestroy(vgroupInfo.vgroups);
  taosArrayDestroy(diskInfo.datadirs);
}

TEST_F(MonitorTest, 02_Log) {
  AddLogInfo2();

  SMonInfo *pMonitor = monCreateMonitorInfo();
  if (pMonitor == NULL) return;

  monSendReport(pMonitor);
  monCleanupMonitorInfo(pMonitor);
}
