local driver = require "luaconnector"

local config = {
   host = "127.0.0.1",
   port = 6030,
   database = "",
   user = "root",
   password = "taosdata",
   max_packet_size = 1024 * 1024 
}

function dump(obj)
    local getIndent, quoteStr, wrapKey, wrapVal, dumpObj
    getIndent = function(level)
        return string.rep("\t", level)
    end
    quoteStr = function(str)
        return '"' .. string.gsub(str, '"', '\\"') .. '"'
    end
    wrapKey = function(val)
        if type(val) == "number" then
            return "[" .. val .. "]"
        elseif type(val) == "string" then
            return "[" .. quoteStr(val) .. "]"
        else
            return "[" .. tostring(val) .. "]"
        end
    end
    wrapVal = function(val, level)
        if type(val) == "table" then
            return dumpObj(val, level)
        elseif type(val) == "number" then
            return val
        elseif type(val) == "string" then
            return quoteStr(val)
        else
            return tostring(val)
        end
    end
    dumpObj = function(obj, level)
        if type(obj) ~= "table" then
            return wrapVal(obj)
        end
        level = level + 1
        local tokens = {}
        tokens[#tokens + 1] = "{"
        for k, v in pairs(obj) do
            tokens[#tokens + 1] = getIndent(level) .. wrapKey(k) .. " = " .. wrapVal(v, level) .. ","
        end
        tokens[#tokens + 1] = getIndent(level - 1) .. "}"
        return table.concat(tokens, "\n")
    end
    return dumpObj(obj, 0)
end

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

res = driver.query(conn,"create table m1 (ts timestamp, speed int, owner binary(20), mark nchar(30))")
if res.code ~=0 then
   print("create table---failed: "..res.error)
   return
else
   print("create table--- pass.")
end

res = driver.query(conn,"insert into m1 values ('2019-09-01 00:00:00.001', 0, 'robotspace', '世界人民大团结万岁'), ('2019-09-01 00:00:00.002', 1, 'Hilink', '⾾⾿⿀⿁⿂⿃⿄⿅⿆⿇⿈⿉⿊⿋⿌⿍⿎⿏⿐⿑⿒⿓⿔⿕'),('2019-09-01 00:00:00.003', 2, 'Harmony', '₠₡₢₣₤₥₦₧₨₩₪₫€₭₮₯₰₱₲₳₴₵')")
if res.code ~=0 then
   print("insert records failed: "..res.error)
   return
else
   if(res.affected == 3) then
      print("insert records--- pass")
   else
      print("insert records---failed: expect 3 affected records, actually affected "..res.affected)
   end   
end

res = driver.query(conn,"select * from m1")

if res.code ~=0 then
   print("select failed: "..res.error)
   return
else
    if (#(res.item) == 3) then
        print("select--- pass")
	print(res.item[1].mark)
	print(res.item[2].mark)
	print(res.item[3].mark)

    else
	print("select--- failed: expect 3 affected records, actually received "..#(res.item))
    end

end

res = driver.query(conn,"create table thermometer (ts timestamp, degree double) tags(location binary(20), type int)")
if res.code ~=0 then
   print(res.error)
   return
else
   print("create super table--- pass")
end
res = driver.query(conn,"create table therm1 using thermometer tags ('beijing', 1)")
if res.code ~=0 then
   print(res.error)
   return
else
   print("create table--- pass")
end

res = driver.query(conn,"insert into therm1 values ('2019-09-01 00:00:00.001', 20),('2019-09-01 00:00:00.002', 21)")

if res.code ~=0 then
   print(res.error)
   return
else
   if(res.affected == 2) then
      print("insert records--- pass")
   else
      print("insert records---failed: expect 2 affected records, actually affected "..res.affected)
   end 
end

res = driver.query(conn,"select count(*) count, avg(degree) as av, max(degree), min(degree) from thermometer where location='beijing' or location='tianjin' group by location, type")
if res.code ~=0 then
   print("select from super table--- failed:"..res.error)
   return
else
   print("select from super table--- pass")
   for i = 1, #(res.item) do
      print("res:"..res.item[i].count)
   end
end

function async_query_callback(res)
   if res.code ~=0 then
      print("async_query_callback--- failed:"..res.error)
      return
   else

   if(res.affected == 3) then
      print("async_query_callback, insert records--- pass")
   else
      print("async_query_callback, insert records---failed: expect 3 affected records, actually affected "..res.affected)
   end 

   end
end

driver.query_a(conn,"insert into therm1 values ('2019-09-01 00:00:00.005', 100),('2019-09-01 00:00:00.006', 101),('2019-09-01 00:00:00.007', 102)", async_query_callback)

res = driver.query(conn, "create table avg_degree as select avg(degree) from thermometer where ts > now and ts <= now + 1m interval(5s) sliding(1s)")

print("From now on we start continous insertion in an definite (infinite if you want) loop.")
local loop_index = 0
while loop_index < 30 do
   local t = os.time()*1000
   local v = loop_index
   res = driver.query(conn,string.format("insert into therm1 values (%d, %d)",t,v))

   if res.code ~=0 then
      print("continous insertion--- failed:" .. res.error)
      return
   else
      --print("insert successfully, affected:"..res.affected)
   end
   local res1 = driver.query(conn, string.format("select last(*) from avg_degree"))
   if res1.code ~=0 then
      print("select failed: "..res1.error)
      return
   else
--      print(dump(res1))
      if(#res1.item > 0) then print("avg_degree: " .. res1.item[1]["last(avg_degree_)"]) end
   end

   os.execute("sleep " .. 1)
   loop_index = loop_index + 1
end

driver.close(conn)
