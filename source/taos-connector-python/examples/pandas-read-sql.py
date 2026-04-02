import pandas
from sqlalchemy import create_engine, text

protocol_and_port = {"taos": 6030, "taosrest": 6041, "taosws": 6041}

for protocol, port in protocol_and_port.items():
    engine = create_engine(f"{protocol}://root:taosdata@localhost:{port}")
    conn = engine.connect()
    res = pandas.read_sql(text("show databases"), conn)
    conn.close()
    print(res)
