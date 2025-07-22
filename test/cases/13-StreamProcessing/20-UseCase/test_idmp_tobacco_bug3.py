from test_idmp_tobacco import TestIdmpScene


class TestIdmpTobaccoBug3:
    """
    JIRA: https://jira.taosdata.com:18080/browse/TD-36699
    """

    def test_idmp_tobacco(self):
        tobac = TestIdmpScene()
        tobac.init(
            "tobacco",
            "idmp_sample_tobacco",
            "idmp",
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp_sample_tobacco",
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp/vstb.sql",
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp/vtb.sql",
            "cases/13-StreamProcessing/20-UseCase/tobacco_data/idmp/stream.json",
        )
        tobac.stream_ids = [3]
        tobac.assert_retry = 60
        tobac.run()
