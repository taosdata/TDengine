from test_idmp_tobacco import TestIdmpScene


class TestIdmpPhotovoltaic:
    def test_pv(self):
        """
        Refer:
        Catalog:
            - Streams:UseCases
        Since: v3.3.6.14
        Labels: common,ci
        Jira:
            - https://jira.taosdata.com:18080/browse/TD-36783
        History:
            - 2025-7-18 zyyang90 Created
        """
        pv = TestIdmpScene()
        pv.init(
            "photovoltaic",
            "idmp_sample_pv",
            "idmp",
            "cases/13-StreamProcessing/20-UseCase/pv_data/idmp_sample_pv",
            "cases/13-StreamProcessing/20-UseCase/pv_data/idmp/vstb.sql",
            "cases/13-StreamProcessing/20-UseCase/pv_data/idmp/vtb.sql",
            "cases/13-StreamProcessing/20-UseCase/pv_data/idmp/stream.json",
        )
        # tobac.stream_ids = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        pv.run()
