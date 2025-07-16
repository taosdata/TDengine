from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug8:
    def test_idmp_tobacco(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.stream_ids = [7]
        tobac.run()
