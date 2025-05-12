local _M = {}
local driver = require "luaconnector51"
td_pool_watermark = 0
td_pool_occupied = 0
td_connection_pool = {}

function _M.new(o, config)
   o = o or {}
   o.connection_pool = td_connection_pool
   o.watermark = td_pool_watermark
   o.occupied = td_pool_occupied
   if #td_connection_pool == 0 then
      for i = 1, config.connection_pool_size do
	 local res = driver.connect(config)
	 if res.code ~= 0 then
	    ngx.log(ngx.ERR, "connect--- failed:"..res.error)
	    return nil
	 else
	    local object = {obj = res.conn, state = 0}
	    table.insert(td_connection_pool, i, object)
	    ngx.log(ngx.INFO, "add connection, now pool size:"..#(td_connection_pool))
	 end
      end
      
   end
   
   return setmetatable(o, { __index = _M })
end

function _M:get_connection()

   local connection_obj

   for i = 1, #td_connection_pool do
      connection_obj = td_connection_pool[i]
      if connection_obj.state == 0 then
	 connection_obj.state = 1
	 td_pool_occupied = td_pool_occupied + 1
	 if td_pool_occupied > td_pool_watermark then
	    td_pool_watermark = td_pool_occupied
	 end
	 return connection_obj["obj"]	 
      end
   end
   
   ngx.log(ngx.ERR,"ALERT! NO FREE CONNECTION.")

   return nil
end

function _M:get_watermark()

   return td_pool_watermark
end


function _M:get_current_load()

    return td_pool_occupied
end

function _M:release_connection(conn)
 
   local connection_obj

   for i = 1, #td_connection_pool do
      connection_obj = td_connection_pool[i]
 
      if connection_obj["obj"] == conn then
	 connection_obj["state"] = 0
	 td_pool_occupied = td_pool_occupied -1
	 return
      end
   end
end

return _M
