from test_idmp_tobacco import TestIdmpTobaccoImpl


class TestIdmpTobaccoBug10:
    def run(self):
        tobac = TestIdmpTobaccoImpl()
        tobac.stream_ids = [9]
        tobac.run()
