import httplib, urllib
import sys
import json
import datetime
#params = urllib.urlencode({'@number': 12524, '@type': 'issue', '@action': 'show'})
tabName,pointSz,dbName = 'tab',5,'tsdb'
svr = "39.108.136.10:7388"

try:
  tabName = sys.argv[1]
  pointSz = int(sys.argv[2])
  dbName = sys.argv[3]
  svr = sys.argv[4]
except Exception as ex:
  print ex

print tabName, pointSz,dbName,svr

headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}


def ts_query(server,uri,sql,head):
  try:
    conn = httplib.HTTPConnection(svr)
    conn.request("POST",uri, sql , head)
    response = conn.getresponse()
    print response.status, response.reason
    data = response.read()
    hdrs = response.getheaders()
    print hdrs

  except Exception as ex:
    print ex
    pass
  finally:
    conn.close()
  return data

sql ='create database '+dbName
uri ='/sql' 
print uri , sql
ts_query(svr, uri , sql ,headers)

sql ='create table '+tabName+'(ts timestamp, speed bigint)'
uri ='/'+dbName+'/sql'
print uri , sql
ts_query(svr,uri,sql,headers)

sql ='insert into '+tabName+' values(now,0)'
uri ='/'+dbName +'/'+tabName+'/sql'
print uri , sql
data = ts_query(svr,uri,sql,headers)
try:
  respObj = json.loads(data);
  print respObj['desc']
  if respObj['status']=='succ':
    if not respObj['desc']=='':
      headers['Cookie']=respObj['desc']
      
  stime = datetime.datetime.now()
  uri = '/'+dbName +'/'+tabName+'/sql'
  for i in range(pointSz):
    sql = 'insert into '+tabName+' values(now,'+str(i+100)+')'
    res = ts_query(svr,uri,sql,headers)
    res = json.loads(data)
    if res['status']=='error':
      break
  print datetime.datetime.now()-stime
except Exeception as ex:
  print ex

sql = 'select * from '+tabName
uri = '/'+dbName +'/'+tabName+'/sql'
rs = ts_query(svr,uri,sql,headers)
rsObj = json.loads(rs);

print rsObj



