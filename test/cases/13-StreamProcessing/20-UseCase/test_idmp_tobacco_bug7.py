from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug7:
    def run(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.stream_ids = [6]
        tobac.run()
