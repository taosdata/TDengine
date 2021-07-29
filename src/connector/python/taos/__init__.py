"""
# TDengine Connector for Python

[TDengine](https://github.com/taosdata/TDengine) connector for Python enables python programs to access TDengine,
 using an API which is compliant with the Python DB API 2.0 (PEP-249). It uses TDengine C client library for client server communications.

## Install

```sh
git clone --depth 1 https://github.com/taosdata/TDengine.git
pip install ./TDengine/src/connector/python
```

## Source Code

[TDengine](https://github.com/taosdata/TDengine) connector for Python source code is hosted on [GitHub](https://github.com/taosdata/TDengine/tree/develop/src/connector/python).

## Examples

### Query with PEP-249 API

```python
import taos

conn = taos.connect()
cursor = conn.cursor()

cursor.execute("show database")
results = cursor.fetchall()
for row in results
    print(row)
cursor.close()
conn.close()
```

### Query with objective API

```python
import taos

conn = taos.connect()
conn.exec("create database pytest")

result = conn.query("show database")
num_of_fields = result.field_count
for field in result.fields:
    print(field)
for row in result
    print(row)
result.close()
conn.close()
```

### Query with async API

```python
import taos
import time

def fetch_callback(p_param, p_result, num_of_rows):
    print("fetched ", num_of_rows, "rows")
    p = cast(p_param, POINTER(Counter))
    result = TaosResult(p_result)

    if num_of_rows == 0:
        print("fetching completed")
        p.contents.done = True
        result.close()
        return
    if num_of_rows < 0:
        p.contents.done = True
        result.check_error(num_of_rows)
        result.close()
        return None
    
    for row in result.rows_iter(num_of_rows):
        # print(row)
        None
    p.contents.count += result.row_count
    result.fetch_rows_a(fetch_callback, p_param)
    


def query_callback(p_param, p_result, code):
    # type: (c_void_p, c_void_p, c_int) -> None
    if p_result == None:
        return
    result = TaosResult(p_result)
    if code == 0:
        result.fetch_rows_a(fetch_callback, p_param)
    result.check_error(code)


class Counter(Structure):
    _fields_ = [("count", c_int), ("done", c_bool)]

    def __str__(self):
        return "{ count: %d, done: %s }" % (self.count, self.done)


def test_query(conn):
    # type: (TaosConnection) -> None
    counter = Counter(count=0)
    conn.query_a("select * from log.log", query_callback, byref(counter))

    while not counter.done:
        print("wait query callback")
        time.sleep(1)
    print(counter)
    conn.close()


if __name__ == "__main__":
    test_query(taos.connect())

```

### Statement API

```python
import taos

conn = taos.connect()

dbname = "pytest_taos_stmt"
conn.exec("drop database if exists %s" % dbname)
conn.exec("create database if not exists %s" % dbname)
conn.select_db(dbname)

conn.exec(
    "create table if not exists log(ts timestamp, bo bool, nil tinyint, \\
        ti tinyint, si smallint, ii int, bi bigint, tu tinyint unsigned, \\
        su smallint unsigned, iu int unsigned, bu bigint unsigned, \\
        ff float, dd double, bb binary(100), nn nchar(100), tt timestamp)",
)

stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
params = new_bind_params(14)
params[0].timestamp(1626861392589, PrecisionEnum.Milliseconds)
params[1].null()
params[2].tinyint(2)
params[3].smallint(3)
params[4].int(4)
params[5].bigint(5)
params[6].tinyint_unsigned(6)
params[7].smallint_unsigned(7)
params[8].int_unsigned(8)
params[9].bigint_unsigned(9)
params[10].float(10.1)
params[11].double(10.11)
params[12].binary("hello")
params[13].nchar("stmt")

stmt.bind_param(params)
stmt.execute()

result = stmt.use_result()
assert result.affected_rows == 1
result.close()
stmt.close()


stmt = conn.statement("insert into log values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

params = new_multi_binds(16)
params[0].timestamp((1626861392589, 1626861392590, 1626861392591))
params[1].bool((True, None, False))
params[2].tinyint([-128, -128, None]) # -128 is tinyint null
params[3].tinyint([0, 127, None])
params[4].smallint([3, None, 2])
params[5].int([3, 4, None])
params[6].bigint([3, 4, None])
params[7].tinyint_unsigned([3, 4, None])
params[8].smallint_unsigned([3, 4, None])
params[9].int_unsigned([3, 4, None])
params[10].bigint_unsigned([3, 4, None])
params[11].float([3, None, 1])
params[12].double([3, None, 1.2])
params[13].binary(["abc", "dddafadfadfadfadfa", None])
params[14].nchar(["涛思数据", None, "a long string with 中文字符"])
params[15].timestamp([None, None, 1626861392591])
stmt.bind_param_batch(params)
stmt.execute()

result = stmt.use_result()
assert result.affected_rows == 3

stmt.close()
conn.close()
```

## License - AGPL-3.0

Keep same with [TDengine](https://github.com/taosdata/TDengine).
"""
from .connection import TaosConnection

# For some reason, the following is needed for VS Code (through PyLance) to
# recognize that "error" is a valid module of the "taos" package.
from .error import *
from .bind import *
from .field import *
from .cursor import *
from .result import *
from .statement import *
from .subscription import *

try:
    import importlib.metadata

    __version__ = importlib.metadata.version("taos")
except:
    None

# Globals
threadsafety = 0
paramstyle = "pyformat"

__all__ = [
    # functions
    "connect",
    "new_bind_param",
    "new_bind_params",
    "new_multi_binds",
    "new_multi_bind",
    # objects
    "TaosBind",
    "TaosConnection",
    "TaosCursor",
    "TaosResult",
    "TaosRows",
    "TaosRow",
    "TaosStmt",
    "PrecisionEnum",
]

def connect(*args, **kwargs):
    """Function to return a TDengine connector object

    Current supporting keyword parameters:
    @dsn: Data source name as string
    @user: Username as string(optional)
    @password: Password as string(optional)
    @host: Hostname(optional)
    @database: Database name(optional)

    @rtype: TDengineConnector
    """
    return TaosConnection(*args, **kwargs)
