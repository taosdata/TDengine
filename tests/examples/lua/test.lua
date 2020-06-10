local driver = require "luaconnector"

local host="127.0.0.1"
local user="root"
local password="taosdata"
local db =nil
local port=6030
local conn

local res = driver.connect(host,user,password,db,port)
if res.code ~=0 then
   print(res.error)
   return
else
   conn = res.conn
end

local res = driver.query(conn,"drop database demo")

res = driver.query(conn,"create database demo")
if res.code ~=0 then
   print(res.error)
   return
end

res = driver.query(conn,"use demo")
if res.code ~=0 then
   print(res.error)
   return
end

res = driver.query(conn,"create table m1 (ts timestamp, speed int,owner binary(20))")
if res.code ~=0 then
   print(res.error)
   return
end

res = driver.query(conn,"insert into m1 values ('2019-09-01 00:00:00.001',0,'robotspace'), ('2019-09-01 00:00:00.002',1,'Hilink'),('2019-09-01 00:00:00.003',2,'Harmony')")
if res.code ~=0 then
   print(res.error)
   return
else
   print("insert successfully, affected:"..res.affected)
end

res = driver.query(conn,"select * from m1")

if res.code ~=0 then
   print("select error:"..res.error)
   return
else
   print("in lua, result:")
   for i = 1, #(res.item) do
      print("timestamp:"..res.item[i].ts)
      print("speed:"..res.item[i].speed)
      print("owner:"..res.item[i].owner)
   end
end

res = driver.query(conn,"CREATE TABLE thermometer (ts timestamp, degree double) TAGS(location binary(20), type int)")
if res.code ~=0 then
   print(res.error)
   return
end
res = driver.query(conn,"CREATE TABLE therm1 USING thermometer TAGS ('beijing', 1)")
if res.code ~=0 then
   print(res.error)
   return
end
res = driver.query(conn,"INSERT INTO therm1 VALUES ('2019-09-01 00:00:00.001', 20),('2019-09-01 00:00:00.002', 21)")

if res.code ~=0 then
   print(res.error)
   return
else
   print("insert successfully, affected:"..res.affected)
end

res = driver.query(conn,"SELECT COUNT(*) count, AVG(degree) AS av, MAX(degree), MIN(degree) FROM thermometer WHERE location='beijing' or location='tianjin' GROUP BY location, type")
if res.code ~=0 then
   print("select error:"..res.error)
   return
else
   print("in lua, result:")
   for i = 1, #(res.item) do
      print("res:"..res.item[i].count)
   end
end

function callback(t)
   print("continuous query result:")
   for key, value in pairs(t) do
      print("key:"..key..", value:"..value)
   end
end

local stream
res = driver.open_stream(conn,"SELECT COUNT(*) as count, AVG(degree) as avg, MAX(degree) as max, MIN(degree) as min FROM thermometer interval(2s) sliding(2s);)",0,callback)
if res.code ~=0 then
   print("open stream error:"..res.error)
   return
else
   print("openstream ok")
   stream = res.stream
end

--From now on we begin continous query in an definite (infinite if you want) loop.
local loop_index = 0
while loop_index < 20 do
   local t = os.time()*1000
   local v = loop_index
   res = driver.query(conn,string.format("INSERT INTO therm1 VALUES (%d, %d)",t,v))

   if res.code ~=0 then
      print(res.error)
      return
   else
      print("insert successfully, affected:"..res.affected)
   end
   os.execute("sleep " .. 1)
   loop_index = loop_index + 1
end

driver.close_stream(stream)
driver.close(conn)
