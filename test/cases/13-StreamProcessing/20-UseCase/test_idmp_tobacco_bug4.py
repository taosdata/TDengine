from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug4:
    def run(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.stream_ids = [3]
        tobac.run()
