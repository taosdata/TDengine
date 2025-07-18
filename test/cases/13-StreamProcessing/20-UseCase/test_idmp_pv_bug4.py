from test_idmp_tobacco import TestIdmpScene


class TestIdmpTobaccoBug3:
    """
    JIRA: https://jira.taosdata.com:18080/browse/TD-36699
    """

    def test_idmp_tobacco(self):
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
        pv.stream_ids = [4]
        pv.run()
