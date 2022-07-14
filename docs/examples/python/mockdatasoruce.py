import time


class MockDataSource:
    samples = [
        "8.8,119,0.32,LosAngeles,0",
        "10.7,116,0.34,SanDiego,1",
        "9.9,111,0.33,Hollywood,2",
        "8.9,113,0.329,Compton,3",
        "9.4,118,0.141,San Francisco,4"
    ]

    def __init__(self, tb_name_prefix, table_count):
        self.table_name_prefix = tb_name_prefix + "_"
        self.table_count = table_count
        self.max_rows = 10000000
        self.start_ms = round(time.time() * 1000) - self.max_rows * 100
        self.data = self._init_data()

    def _init_data(self):
        lines = self.samples * (self.table_count // 5 + 1)
        data = []
        for i in range(self.table_count):
            table_name = self.table_name_prefix + str(i)
            data.append((i, table_name, lines[i]))  # tableId, row
        return data

    def __iter__(self):
        self.row = 0
        return self

    def __next__(self):
        """
        next row for each table.
        [(tableId, row),(tableId, row)]
        """
        self.row += 1
        ts = self.start_ms + 100 * self.row

        # just add timestamp to each row
        # (tableId, "tableName,ts,current,voltage,phase,location,groupId")
        return map(lambda t: (t[0], t[1] + ',' + str(ts) + "," + t[2]), self.data)


if __name__ == '__main__':
    """
    Test performance of MockDataSource
    """
    from threading import Thread

    count = 0


    def consume():
        global count
        for data in MockDataSource("1", 1000):
            for _ in data:
                count += 1


    Thread(target=consume).start()
    while True:
        time.sleep(1)
        print(count)
