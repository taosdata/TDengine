local cjson = require "cjson"
local jwt = require "resty.jwt"
local ipconv = require "ipconv"
local headers = ngx.req.get_headers();
ngx.req.read_body()
local data = ngx.req.get_body_data();
local action = ngx.var.request_method;
local args = ngx.req.get_uri_args();

ngx.log(ngx.WARN, "capture 00 ")
local ups = ngx.shared._ups:get("master")  
if ups == nil then
    ups = ngx.shared._ups:get("firstip")
    ngx.shared._ups:set("master", ups);
    res0 = ngx.location.capture(
        "/sub1"..ngx.var.request_uri,
        {
            method = ngx.HTTP_GET,
            body = data
        }
        )
    if res0.status == ngx.HTTP_SEE_OTHER then
        ngx.log(ngx.WARN, "capture 02,revise upstream")
        local slaveip = {}
    	local cookiemeta = res0.header["Set-Cookie"]
        local metadata = ngx.re.match(cookiemeta, ".*meta=(.*)\"?", "o");
        local decodemeta = ngx.decode_base64(tostring(metadata[1]));
        local jsonmeta = cjson.decode(decodemeta);
        local masterip = jsonmeta["metadata"][1];
        slaveip[1] = jsonmeta["metadata"][2];
        slaveip[2] = jsonmeta["metadata"][3];
        slaveip[3] = jsonmeta["metadata"][4];
        slaveip[4] = jsonmeta["metadata"][5];
        ups = ipconv.Dig2Str(masterip);
    	ngx.shared._ups:set("master", ups);
    	ngx.shared._ups:set("slave1", slaveip[1]);
    	ngx.shared._ups:set("slave2", slaveip[2]);
    	ngx.shared._ups:set("slave3", slaveip[3]);
    	ngx.shared._ups:set("slave4", slaveip[4]);
    end
else
    ngx.log(ngx.WARN, "get [", ups, "] from ngx.shared._ups")
end

if headers["Cookie"] ~= nil then
    -- subrequest2
    ngx.log(ngx.WARN, "capture 7 ")
    res2 = ngx.location.capture(
        "/sub2"..ngx.var.request_uri,
        {
            method = ngx.HTTP_POST,
            body = data
        }
        )
    if res2.status == ngx.HTTP_OK then
    ngx.log(ngx.WARN, "capture 8 ")
        for k, v in pairs(res2.header) do
            ngx.header[k] = v
        end
        ngx.print(res2.body)
    end
else
    ngx.log(ngx.WARN, "capture 1 ")
    local uri = ngx.re.match(ngx.var.request_uri, "^/sql/.*/slave/.*", "o");
    if uri~=nil then
      res3 = ngx.location.capture(
          "/sub3"..ngx.var.request_uri,
          {
              method = ngx.HTTP_POST,
              body = data
          }
          )
      ngx.log(ngx.WARN, "slave capture ")
      if res3.status == ngx.HTTP_TEMPORARY_REDIRECT then
            local metadata = res3.header["Set-Cookie"]
            ngx.req.set_header("Cookie", metadata)
            res4 = ngx.location.capture(
                "/sub2"..ngx.var.request_uri,
                {
                    method = ngx.HTTP_POST,
                    body = data
                }
                )
            ngx.log(ngx.WARN, "slave capture 4 ")
            if res4.status == ngx.HTTP_OK then
            ngx.log(ngx.WARN, "slave capture 5 ")
                for k, v in pairs(res4.header) do
                    ngx.header[k] = v
                end
                ngx.header["Set-Cookie"] = metadata
                ngx.print(res4.body)
            end
      elseif res3.status == ngx.HTTP_OK then
            ngx.log(ngx.WARN, "slave show ")
            ngx.print(res3.body)
      elseif res3.status == ngx.HTTP_SEE_OTHER then
          ngx.log(ngx.WARN, "slave ip update")
          local slaveip = {}
      	local cookiemeta = res3.header["Set-Cookie"]
          local metadata = ngx.re.match(cookiemeta, ".*meta=(.*)\"?", "o");
          local decodemeta = ngx.decode_base64(tostring(metadata[1]));
          local jsonmeta = cjson.decode(decodemeta);
          local masterip = jsonmeta["metadata"][1];
          slaveip[1] = jsonmeta["metadata"][2];
          slaveip[2] = jsonmeta["metadata"][3];
          slaveip[3] = jsonmeta["metadata"][4];
          slaveip[4] = jsonmeta["metadata"][5];
          ups = ipconv.Dig2Str(masterip);
      	ngx.shared._ups:set("master", ups);
      	ngx.shared._ups:set("slave1", slaveip[1]);
      	ngx.shared._ups:set("slave2", slaveip[2]);
      	ngx.shared._ups:set("slave3", slaveip[3]);
      	ngx.shared._ups:set("slave4", slaveip[4]);
          --recapture again
      end
    end
      res1 = ngx.location.capture(
          "/sub1"..ngx.var.request_uri,
          {
              method = ngx.HTTP_POST,
              body = data
          }
          )
      ngx.log(ngx.WARN, "capture 2 ")
      if res1.status == ngx.HTTP_TEMPORARY_REDIRECT then
      ngx.log(ngx.WARN, "capture 3 ")
          local metadata = res1.header["Set-Cookie"]
          ngx.req.set_header("Cookie", metadata)
          res2 = ngx.location.capture(
              "/sub2"..ngx.var.request_uri,
              {
                  method = ngx.HTTP_POST,
                  body = data
              }
              )
      ngx.log(ngx.WARN, "capture 4 ")
          if res2.status == ngx.HTTP_OK then
      ngx.log(ngx.WARN, "capture 5 ")
              for k, v in pairs(res2.header) do
                  ngx.header[k] = v
              end
              ngx.header["Set-Cookie"] = metadata
              ngx.print(res2.body)
          end
      elseif res1.status == ngx.HTTP_OK then
        ngx.log(ngx.WARN, "show 6 ")
        ngx.print(res1.body)
      elseif res1.status == ngx.HTTP_SEE_OTHER then
        ngx.log(ngx.WARN, "get new master/slave ip ,update it ")
        local slaveip = {}
    	local cookiemeta = res1.header["Set-Cookie"]
        local metadata = ngx.re.match(cookiemeta, ".*meta=(.*)\"?", "o");
        local decodemeta = ngx.decode_base64(tostring(metadata[1]));
        local jsonmeta = cjson.decode(decodemeta);
        local masterip = jsonmeta["metadata"][1];
        slaveip[1] = jsonmeta["metadata"][2];
        slaveip[2] = jsonmeta["metadata"][3];
        slaveip[3] = jsonmeta["metadata"][4];
        slaveip[4] = jsonmeta["metadata"][5];
        ups = ipconv.Dig2Str(masterip);
    	ngx.shared._ups:set("master", ups);
    	ngx.shared._ups:set("slave1", slaveip[1]);
    	ngx.shared._ups:set("slave2", slaveip[2]);
    	ngx.shared._ups:set("slave3", slaveip[3]);
    	ngx.shared._ups:set("slave4", slaveip[4]);
        --recapture again
      else
        ngx.log(ngx.WARN, "first ip wrong, use second ")
        ups = ngx.shared._ups:get("secondip")  
    	local before=ngx.shared._ups:get("master");
        if ups~=before then
           ngx.shared._ups.set("master", ups);
           --recapture again
        else
           ngx.exit(ngx.HTTP_INTERNAL_SERVER_ERROR)
        end
      end
end
