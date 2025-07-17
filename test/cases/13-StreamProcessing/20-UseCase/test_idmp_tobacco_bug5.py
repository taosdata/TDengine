from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug5:
    def test_idmp_tobacco(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.init()
        tobac.stream_ids = [4]
        tobac.run()
