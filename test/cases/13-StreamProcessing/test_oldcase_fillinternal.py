import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_basic(self):
        """Stream basic test

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillIntervalDelete0.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillIntervalDelete1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillIntervalLinear.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillIntervalPartitionBy.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillIntervalPrevNext.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillIntervalPrevNext1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillIntervalRange.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillIntervalValue.sim
        """

        # self.fillIntervalDelete0()
        # self.fillIntervalDelete1()
        # self.fillIntervalLinear()
        # self.fillIntervalPartitionBy()
        # self.fillIntervalPrevNext()
        # self.fillIntervalPrevNext1()
        # self.fillIntervalRange()
        # self.fillIntervalValue()

    def fillIntervalDelete0(self):
        tdLog.info(f"fillIntervalDelete0")
        clusterComCheck.drop_all_streams_and_dbs()
        
    def fillIntervalDelete1(self):
        tdLog.info(f"fillIntervalDelete1")
        clusterComCheck.drop_all_streams_and_dbs()
        
    def fillIntervalLinear(self):
        tdLog.info(f"fillIntervalLinear")
        clusterComCheck.drop_all_streams_and_dbs()
        
    def fillIntervalPartitionBy(self):
        tdLog.info(f"fillIntervalPartitionBy")
        clusterComCheck.drop_all_streams_and_dbs()
        
    def fillIntervalPrevNext(self):
        tdLog.info(f"fillIntervalPrevNext")
        clusterComCheck.drop_all_streams_and_dbs()
        
    def fillIntervalPrevNext1(self):
        tdLog.info(f"fillIntervalPrevNext1")
        clusterComCheck.drop_all_streams_and_dbs()
        
    def fillIntervalRange(self):
        tdLog.info(f"fillIntervalRange")
        clusterComCheck.drop_all_streams_and_dbs()
        
    def fillIntervalValue(self):
        tdLog.info(f"fillIntervalValue")
        clusterComCheck.drop_all_streams_and_dbs()

