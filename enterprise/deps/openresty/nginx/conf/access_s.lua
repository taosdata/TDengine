local cjson = require "cjson"
local jwt = require "resty.jwt"
local ipconv = require "ipconv"
local headers = ngx.req.get_headers();
local checkups = require "resty.checkups.api"

ngx.log(ngx.WARN, "access s ")
if ngx.shared._ups:get("slave1") ~= nil then
    ngx.var.my_upstream = ngx.shared._ups:get("slave1")
    ngx.log(ngx.WARN, ngx.var.my_upstream)
end
ngx.log(ngx.WARN, ngx.var.my_upstream)
