from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug6:
    def test_idmp_tobacco(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.stream_ids = [5]
        tobac.run()
