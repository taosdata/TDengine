from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug11:
    """
    JIRA: https://jira.taosdata.com:18080/browse/TD-36729
    """

    def test_idmp_tobacco(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.stream_ids = [10]
        tobac.run()
