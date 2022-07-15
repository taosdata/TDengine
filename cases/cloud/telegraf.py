from taostest import TDCase, T


class Telegraf(TDCase):

    def init(self):
        pass

    def run(self):
        TDENGINE_CLOUD_URL = self.env_setting["env"]["TDENGINE_CLOUD_URL"]
        TDENGINE_CLOUD_TOKEN = self.env_setting["env"]["TDENGINE_CLOUD_TOKEN"]
        url = f"{TDENGINE_CLOUD_URL}/influxdb/v1/write?db=telegraf&token={TDENGINE_CLOUD_TOKEN}"
        print(url)

    def desc(self) -> str:
        return "Test connectivity between cloud and telegraf"

    def author(self) -> str:
        return "DingBo"

    def tags(self):
        return T.Cloud
