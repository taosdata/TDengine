local cjson = require "cjson"
local jwt = require "resty.jwt"
local ipconv = require "ipconv"
local headers = ngx.req.get_headers();
local vpeerDescIp = {}
local cookieJson = cjson.encode(headers["Cookie"])
local metadata = ngx.re.match(cookieJson, ".*,*meta=(.*)\"", "o");
local decodemeta = ngx.decode_base64(tostring(metadata[1]));
local jsonmeta = cjson.decode(decodemeta);
local sid = jsonmeta["metadata"][1];
vpeerDescIp[1] = jsonmeta["metadata"][2];
vpeerDescIp[2] = jsonmeta["metadata"][4];
vpeerDescIp[3] = jsonmeta["metadata"][6];
vpeerDescIp[4] = jsonmeta["metadata"][8];
vpeerDescIp[5] = jsonmeta["metadata"][10];
local index = jsonmeta["metadata"][12];
local numofcolumn = jsonmeta["metadata"][14];
ngx.var.vnodeip = ipconv.Dig2Str(vpeerDescIp[index+1]);

ngx.log(ngx.WARN, "access 1 ")
if ngx.var.index == -1 then
ngx.log(ngx.WARN, "access 2 ")
    ngx.var.index = tonumber(index);
else
    ngx.var.index = 0
end
local checkups = require "resty.checkups.api"

ngx.log(ngx.WARN, "access 3 ")
local cb_ok = function(host, port)
    return true
end

while (tonumber(ngx.var.index)<5)
do
ngx.log(ngx.WARN, "access 4 ")
    local ok, err
    local peer, err = checkups.select_peer(ngx.var.vnodeip)
    if err then
ngx.log(ngx.WARN, "access 5 ")
        if peer == nil then
ngx.log(ngx.WARN, "access 6 ")
            ok, err = checkups.update_upstream(ngx.var.vnodeip, {
                    {
                        servers = {
                            {host=ngx.var.vnodeip, port=6290, weight=10, max_fails=1, fail_timeout=10},
                        }
                    },
                })
        end
        ngx.sleep(2)
        ok, err = checkups.ready_ok(ngx.var.vnodeip,cb_ok)
        if not ok then
ngx.log(ngx.WARN, "access 7 ")
            ngx.var.index = ngx.var.index + 1
            if vpeerDescIp[ngx.var.index+1] == 0 then
ngx.log(ngx.WARN, "access 8 ")
                break
            else
ngx.log(ngx.WARN, "access 9 ")
                ngx.var.vnodeip = ipconv.Dig2Str(vpeerDescIp[ngx.var.index+1]);
            end
        end
    else
ngx.log(ngx.WARN, "access 10 ")
        break
    end
end
