from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug11:
    def test_idmp_tobacco(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.stream_ids = [10]
        tobac.run()
