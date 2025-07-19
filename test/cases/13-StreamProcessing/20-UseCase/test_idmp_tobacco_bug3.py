from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug3:
    def run(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.stream_ids = [2]
        tobac.run()
