local ipconv={}
--for c bit
--local bit=require("bit")
--for lua module
local bit=require("bitopt")

function ipconv.Dig2Str(i)
   --for c bit
   --local ret = bit.band(bit.rshift(i,24),0xFF).."."..bit.band(bit.rshift(i,16),0xFF).."."..bit.band(bit.rshift(i,8),0xFF).."."..bit.band(i,0xFF) 
   --for lua module
   --for little endian
   --local ret = bit:band(bit:rshift(i,24),0xFF).."."..bit:band(bit:rshift(i,16),0xFF).."."..bit:band(bit:rshift(i,8),0xFF).."."..bit:band(i,0xFF) 
   --for bigendian 
   --begin modify by taosdata
   local ret = bit:band(i,0xFF).."."..bit:band(bit:rshift(i,8),0xFF).."."..bit:band(bit:rshift(i,16),0xFF).."."..bit:band(bit:rshift(i,24),0xFF)
   --end modify by taosdata
   return ret
end

function ipconv.Str2Dig(ipstr)
   local ret=0
   for d in string.gmatch(ipstr, "%d+") do
       ret = ret*256 + d 
   end
   return ret
end

return ipconv
