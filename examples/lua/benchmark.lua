local driver = require "luaconnector"

local config = {
   password = "taosdata",
   host = "127.0.0.1",
   port = 6030,
   database = "",
   user = "root",

   max_packet_size = 1024 * 1024 
}

local conn
local res = driver.connect(config)
if res.code ~=0 then
   print("connect--- failed: "..res.error)
   return
else
   conn = res.conn
   print("connect--- pass.")
end

local res = driver.query(conn,"drop database if exists demo")

res = driver.query(conn,"create database demo")
if res.code ~=0 then
   print("create db--- failed: "..res.error)
   return
else
   print("create db--- pass.")
end

res = driver.query(conn,"use demo")
if res.code ~=0 then
   print("select db--- failed: "..res.error)
   return
else
   print("select db--- pass.")
end

res = driver.query(conn,"create table m1 (ts timestamp, speed int,owner binary(20))")
if res.code ~=0 then
   print("create table---failed: "..res.error)
   return
else
   print("create table--- pass.")
end

local base = 1617330000000
local index =0
local count = 100000
local t
while( index < count )
do
   t = base + index
  local q=string.format([[insert into m1 values (%d,0,'robotspace')]],t)
res = driver.query(conn,q)
if res.code ~=0 then
   print("insert records failed: "..res.error)
   return
else

end
   index = index+1
end
print(string.format([["Done. %d records has been stored."]],count))
driver.close(conn)
