from test_idmp_tobacco import IdmpScene


class TestIdmpTobaccoBug3:

    def test_idmp_tobacco_bug3(self):
        """IDMP tobacco scene

        1. Initialize IDMP tobacco scene
        2. Run stream processing job
        3. Verify results
        

        Since: v3.3.7.0

        Labels: common,ci,skip

        Jira:
            - https://jira.taosdata.com:18080/browse/TD-36699

        History:
            - 2025-7-18 zyyang90 Created
        """
        tobac = IdmpScene()
        tobac.init(
            "tobacco",
            "idmp_sample_tobacco",
            "idmp",
            "cases/41-StreamProcessing/20-UseCase/tobacco_data/idmp_sample_tobacco",
            "cases/41-StreamProcessing/20-UseCase/tobacco_data/idmp/vstb.sql",
            "cases/41-StreamProcessing/20-UseCase/tobacco_data/idmp/vtb.sql",
            "cases/41-StreamProcessing/20-UseCase/tobacco_data/idmp/stream.json",
        )
        tobac.stream_ids = [3]
        tobac.assert_retry = 60
        tobac.run()
