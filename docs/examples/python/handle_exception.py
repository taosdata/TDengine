import taosrest
import os

url = os.environ["TDENGINE_CLOUD_URL"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]

try:
    conn = taosrest.connect(url=url, token=token)
    conn.execute("CREATE TABLE 123")  # wrong sql
except taosrest.Error as e:
    print(e)
    print("exception class: ", e.__class__.__name__)
    print("error number:", e.errno)
    print("error message:", e.msg)
except BaseException as other:
    print("exception occur")
    print(other)

# output:
# [0x2600]: syntax error near "123"
# exception class:  ConnectError
# error number: 9728
# error message: syntax error near "123"
