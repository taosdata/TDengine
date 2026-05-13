import os
import pandas
from sqlalchemy import create_engine, text

url = os.environ["TDENGINE_CLOUD_URL"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]
url = url.replace("https", "")

engine = create_engine(f"taosws{url}?token={token}")
conn = engine.connect()
res = pandas.read_sql(text("show databases"), conn)
conn.close()
print(res)
