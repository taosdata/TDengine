from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug7:
    def test_idmp_tobacco(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.stream_ids = [6]
        tobac.run()
