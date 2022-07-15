from taostest import TDCase, T


class Telegraf(TDCase):

    def init(self):
        pass

    def run(self):
        # test run
        self.logger.info("aaa")
        self.logger.info("bbb")
        self.logger.info("bbb")
        self.logger.info("ccc")
        self.logger.info("dd")
        self.logger.info("aaa")
        self.logger.info("aaa")
        self.logger.info("aaa")
        self.logger.info("axa")

    def desc(self) -> str:
        return "Test connectivity between cloud and telegraf"

    def author(self) -> str:
        return "DingBo"

    def tags(self):
        return T.Cloud
