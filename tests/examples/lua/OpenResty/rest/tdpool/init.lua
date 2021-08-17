local _M = {}
local driver = require "luaconnector51"
local water_mark = 0
local occupied = 0
local connection_pool = {}

function _M.new(o,config)
   o = o or {}
   o.connection_pool = connection_pool
   o.water_mark = water_mark
   o.occupied = occupied
   if #connection_pool == 0 then

      for i = 1, config.connection_pool_size do
	 local res = driver.connect(config)
	 if res.code ~= 0 then
	    ngx.log(ngx.ERR, "connect--- failed:"..res.error)
	    return nil
	 else
	    local object = {obj = res.conn, state = 0}
	    table.insert(o.connection_pool,i, object)
	    ngx.log(ngx.INFO, "add connection, now pool size:"..#(o.connection_pool))
	 end
      end
      
   end
   
   return setmetatable(o, { __index = _M })
end

function _M:get_connection()

   local connection_obj

   for i = 1, #connection_pool do
      connection_obj = connection_pool[i]
      if connection_obj.state == 0 then
	 connection_obj.state = 1
	 occupied = occupied +1
	 if occupied > water_mark then
	    water_mark = occupied
	 end
	 return connection_obj["obj"]	 
      end
   end
   
   ngx.log(ngx.ERR,"ALERT! NO FREE CONNECTION.")

   return nil
end

function _M:get_water_mark()

   return water_mark
end

function _M:release_connection(conn)
 
   local connection_obj

   for i = 1, #connection_pool do
      connection_obj = connection_pool[i]
 
      if connection_obj["obj"] == conn then
	 connection_obj["state"] = 0
	 occupied = occupied -1
	 return
      end
   end
end

return _M
