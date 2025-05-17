import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseFillHistory:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_fillhistory(self):
        """Stream fill history

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillHistoryBasic1.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillHistoryBasic2.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillHistoryBasic3.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillHistoryBasic4.sim
            - 2025-5-15 Simon Guan Migrated from tsim/stream/fillHistoryTransform.sim
        """

        # self.fillHistoryBasic1()
        # self.fillHistoryBasic2()
        # self.fillHistoryBasic3()
        # self.fillHistoryBasic4()
        # self.stream_basic_5()


    def fillHistoryBasic1(self):
        tdLog.info(f"fillHistoryBasic1")
        clusterComCheck.drop_all_streams_and_dbs()
    
    def fillHistoryBasic2(self):
        tdLog.info(f"fillHistoryBasic2")
        clusterComCheck.drop_all_streams_and_dbs()
        
    def fillHistoryBasic3(self):
        tdLog.info(f"fillHistoryBasic3")
        clusterComCheck.drop_all_streams_and_dbs()
        
    def fillHistoryBasic4(self):
        tdLog.info(f"fillHistoryBasic4")
        clusterComCheck.drop_all_streams_and_dbs()
        
    def fillHistoryTransform(self):
        tdLog.info(f"fillHistoryTransform")
        clusterComCheck.drop_all_streams_and_dbs()
