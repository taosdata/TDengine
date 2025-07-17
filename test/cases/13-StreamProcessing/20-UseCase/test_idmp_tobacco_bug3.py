from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug3:
    """
    JIRA: https://jira.taosdata.com:18080/browse/TD-36699
    """

    def test_idmp_tobacco(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.init()
        tobac.stream_ids = [3]
        tobac.assert_retry = 60
        tobac.run()
