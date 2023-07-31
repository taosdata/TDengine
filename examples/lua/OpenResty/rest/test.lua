local driver = require "luaconnector51"
local cjson = require "cjson"
local Pool = require "tdpool"
local config = require "config"
ngx.say("start time:"..os.time())

local pool = Pool.new(Pool, config)
local another_pool = Pool.new(Pool, config)
local conn, conn1, conn2
conn = pool:get_connection()
conn1 = pool:get_connection()
conn2 = pool:get_connection()
local temp_conn = another_pool:get_connection()
ngx.say("pool size:"..config.connection_pool_size)
ngx.say("pool watermark:"..pool:get_watermark())
ngx.say("pool current load:"..pool:get_current_load())
pool:release_connection(conn1)
pool:release_connection(conn2)
another_pool:release_connection(temp_conn)
ngx.say("pool watermark:"..pool:get_watermark())
ngx.say("pool current load:"..pool:get_current_load())

local res = driver.query(conn,"drop database if exists nginx")
if res.code ~=0 then
   ngx.say("drop db--- failed: "..res.error)
else
   ngx.say("drop db--- pass.")
end
res = driver.query(conn,"create database nginx")
if res.code ~=0 then
   ngx.say("create db--- failed: "..res.error)

else
   ngx.say("create db--- pass.")
end

res = driver.query(conn,"use nginx")
if res.code ~=0 then
   ngx.say("select db--- failed: "..res.error)
else
   ngx.say("select db--- pass.")
end

res = driver.query(conn,"create table m1 (ts timestamp, speed int,owner binary(20))")
if res.code ~=0 then
   ngx.say("create table---failed: "..res.error)
else
   ngx.say("create table--- pass.")
end

res = driver.query(conn,"insert into m1 values ('2019-09-01 00:00:00.001', 0, 'robotspace'), ('2019-09-01 00:00:00.002',1,'Hilink'),('2019-09-01 00:00:00.003',2,'Harmony')")
if res.code ~=0 then
   ngx.say("insert records failed: "..res.error)
   return
else
   if(res.affected == 3) then
      ngx.say("insert records--- pass")
   else
      ngx.say("insert records---failed: expect 3 affected records, actually affected "..res.affected)
   end   
end

res = driver.query(conn,"select * from m1")

if res.code ~=0 then
   ngx.say("select failed: "..res.error)
   return
else
   ngx.say(cjson.encode(res))
    if (#(res.item) == 3) then
	ngx.say("select--- pass")
    else
	ngx.say("select--- failed: expect 3 affected records, actually received "..#(res.item))
    end

end

local flag = false
function query_callback(res)
   if res.code ~=0 then
      ngx.say("async_query_callback--- failed:"..res.error)
   else
      if(res.affected == 3) then
	 ngx.say("async_query_callback, insert records--- pass")
      else
	 ngx.say("async_query_callback, insert records---failed: expect 3 affected records, actually affected "..res.affected)
      end
   end
   flag = true
end

driver.query_a(conn,"insert into m1 values ('2019-09-01 00:00:00.001', 3, 'robotspace'),('2019-09-01 00:00:00.006', 4, 'Hilink'),('2019-09-01 00:00:00.007', 6, 'Harmony')", query_callback)

while not flag do
--   ngx.say("i am here once...")
   ngx.sleep(0.001) -- time unit is second
end
pool:release_connection(conn)
ngx.say("end time:"..os.time())
