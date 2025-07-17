from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug4:
    def test_idmp_tobacco(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.init()
        tobac.stream_ids = [3]
        tobac.assert_retry = 60
        tobac.run()
