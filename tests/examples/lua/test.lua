local mylib = require "luaconnector"

local host="127.0.0.1"
local user="root"
local password="taosdata"
local db =nil
local port=6030
local conn

local res = mylib.connect(host,user,password,db,port)
if res.code ~=0 then
   print(res.error)
   return
else
   conn = res.conn
end

local res = mylib.query(conn,"drop database demo")

res = mylib.query(conn,"create database demo")
if res.code ~=0 then
   print(res.error)
   return
end

res = mylib.query(conn,"use demo")
if res.code ~=0 then
   print(res.error)
   return
end

res = mylib.query(conn,"create table m1 (ts timestamp, speed int)")
if res.code ~=0 then
   print(res.error)
   return
end

res = mylib.query(conn,"insert into m1 values (1592222222223,3)")

res = mylib.query(conn,"select * from m1")

if res.code ==0 then
   print("in lua, result:")
   for i=1,#(res.item) do
      print(res.item[i])
   end
end
