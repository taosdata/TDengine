import logging
import taos


class SQLWriter:
    log = logging.getLogger("SQLWriter")

    def __init__(self, get_connection_func):
        self._tb_values = {}
        self._tb_tags = {}
        self._conn = get_connection_func()
        self._max_sql_length = self.get_max_sql_length()
        self._conn.execute("create database if not exists test keep 36500")
        self._conn.execute("USE test")

    def get_max_sql_length(self):
        rows = self._conn.query("SHOW variables").fetch_all()
        for r in rows:
            name = r[0]
            if name == "maxSQLLength":
                return int(r[1])
        return 1024 * 1024

    def process_lines(self, lines: [str]):
        """
        :param lines: [[tbName,ts,current,voltage,phase,location,groupId]]
        """
        for line in lines:
            ps = line.split(",")
            table_name = ps[0]
            value = '(' + ",".join(ps[1:-2]) + ') '
            if table_name in self._tb_values:
                self._tb_values[table_name] += value
            else:
                self._tb_values[table_name] = value

            if table_name not in self._tb_tags:
                location = ps[-2]
                group_id = ps[-1]
                tag_value = f"('{location}',{group_id})"
                self._tb_tags[table_name] = tag_value
        self.flush()

    def flush(self):
        """
        Assemble INSERT statement and execute it.
        When the sql length grows close to MAX_SQL_LENGTH, the sql will be executed immediately, and a new INSERT statement will be created.
        In case of "Table does not exit" exception, tables in the sql will be created and the sql will be re-executed.
        """
        sql = "INSERT INTO "
        sql_len = len(sql)
        buf = []
        for tb_name, values in self._tb_values.items():
            q = tb_name + " VALUES " + values
            if sql_len + len(q) >= self._max_sql_length:
                sql += " ".join(buf)
                self.execute_sql(sql)
                sql = "INSERT INTO "
                sql_len = len(sql)
                buf = []
            buf.append(q)
            sql_len += len(q)
        sql += " ".join(buf)
        self.create_tables()
        self.execute_sql(sql)
        self._tb_values.clear()

    def execute_sql(self, sql):
        try:
            self._conn.execute(sql)
        except taos.Error as e:
            error_code = e.errno & 0xffff
            # Table does not exit
            if error_code == 9731:
                self.create_tables()
            else:
                self.log.error("Execute SQL: %s", sql)
                raise e
        except BaseException as baseException:
            self.log.error("Execute SQL: %s", sql)
            raise baseException

    def create_tables(self):
        sql = "CREATE TABLE "
        for tb in self._tb_values.keys():
            tag_values = self._tb_tags[tb]
            sql += "IF NOT EXISTS " + tb + " USING meters TAGS " + tag_values + " "
        try:
            self._conn.execute(sql)
        except BaseException as e:
            self.log.error("Execute SQL: %s", sql)
            raise e

    def close(self):
        if self._conn:
            self._conn.close()


if __name__ == '__main__':
    def get_connection_func():
        conn = taos.connect()
        return conn


    writer = SQLWriter(get_connection_func=get_connection_func)
    writer.execute_sql(
        "create stable if not exists meters (ts timestamp, current float, voltage int, phase float) "
        "tags (location binary(64), groupId int)")
    writer.execute_sql(
        "INSERT INTO d21001 USING meters TAGS ('California.SanFrancisco', 2) "
        "VALUES ('2021-07-13 14:06:32.272', 10.2, 219, 0.32)")
