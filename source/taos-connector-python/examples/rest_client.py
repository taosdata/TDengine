from taosrest import RestClient
import os

# connect to cloud
url = os.environ["TDENGINE_CLOUD_URL"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]

cloud_client = RestClient(url=url, token=token)
resp = cloud_client.sql("show databases")
print(resp)

try:
    resp = cloud_client.sql("describe test.noexits")
    print(resp)
except BaseException as e:
    print(e)
