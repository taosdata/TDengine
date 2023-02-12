import time


class MockDataSource:
    samples = [
        "8.8,119,0.32,California.LosAngeles,0",
        "10.7,116,0.34,California.SanDiego,1",
        "9.9,111,0.33,California.SanJose,2",
        "8.9,113,0.329,California.Campbell,3",
        "9.4,118,0.141,California.SanFrancisco,4"
    ]

    def __init__(self, tb_name_prefix, table_count, infinity=True):
        self.table_name_prefix = tb_name_prefix + "_"
        self.table_count = table_count
        self.max_rows = 10000000
        self.current_ts = round(time.time() * 1000) - self.max_rows * 100
        # [(tableId, tableName, values),]
        self.data = self._init_data()
        self.infinity = infinity

    def _init_data(self):
        lines = self.samples * (self.table_count // 5 + 1)
        data = []
        for i in range(self.table_count):
            table_name = self.table_name_prefix + str(i)
            data.append((i, table_name, lines[i]))  # tableId, row
        return data

    def __iter__(self):
        self.row = 0
        if not self.infinity:
            return iter(self._iter_data())
        else:
            return self

    def __next__(self):
        """
        next 1000 rows for each table.
        return: {tableId:[row,...]}
        """
        return self._iter_data()

    def _iter_data(self):
        ts = []
        for _ in range(1000):
            self.current_ts += 100
            ts.append(str(self.current_ts))
        # add timestamp to each row
        # [(tableId, ["tableName,ts,current,voltage,phase,location,groupId"])]
        result = []
        for table_id, table_name, values in self.data:
            rows = [table_name + ',' + t + ',' + values for t in ts]
            result.append((table_id, rows))
        return result


if __name__ == '__main__':
    datasource = MockDataSource('t', 10, False)
    for data in datasource:
        print(data)
