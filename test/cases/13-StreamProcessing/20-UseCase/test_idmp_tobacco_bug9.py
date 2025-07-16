from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug9:
    def test_idmp_tobacco(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.stream_ids = [8]
        tobac.run()
