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

res = driver.query(conn,"insert into m1 values (1592222222222,0,'robotspace'), (1592222222223,1,'Hilink'),(1592222222224,2,'Harmony')")
if res.code ~=0 then
   print(res.error)
   return
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

driver.close(conn)
