local driver = require "luaconnector51"
local cjson = require "cjson"
ngx.say("start:"..os.time())


local config = {
   host = "127.0.0.1",
   port = 6030,
   database = "",
   user = "root",
   password = "taosdata",
   max_packet_size = 1024 * 1024 
}

local conn
local res = driver.connect(config)
if res.code ~=0 then
   ngx.say("connect--- failed: "..res.error)
   return
else
   conn = res.conn
   ngx.say("connect--- pass.")
end

local res = driver.query(conn,"drop database if exists nginx")
if res.code ~=0 then
   ngx.say("create db--- failed: "..res.error)

else
   ngx.say("create db--- pass.")
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

res = driver.query(conn,"insert into m1 values ('2019-09-01 00:00:00.001',0,'robotspace'), ('2019-09-01 00:00:00.002',1,'Hilink'),('2019-09-01 00:00:00.003',2,'Harmony')")
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

ngx.say("end:"..os.time())
--ngx.log(ngx.ERR,"in test file.")

