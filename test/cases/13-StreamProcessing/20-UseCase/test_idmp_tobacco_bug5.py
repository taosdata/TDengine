from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug5:
    def run(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.stream_ids = [4]
        tobac.run()
