import os
import taosrest


class CloudSql:
    def __init__(self):
        self._conn = None

    def connect(self):
        if self._conn is None:
            url = os.environ["TDENGINE_CLOUD_URL"]
            token = os.environ["TDENGINE_CLOUD_TOKEN"]
            self._conn = taosrest.connect(url=url, token=token)
        return self._conn

    def count(self, database, table):
        conn = self.connect()
        res = conn.query(f"select count(*) from {database}.`{table}`")
        if res.rows > 0:
            return res.data[0][0]
