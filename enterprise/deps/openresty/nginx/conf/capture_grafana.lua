
local cjson = require "cjson"
local action = ngx.var.request_method;
ngx.req.read_body()
local data = ngx.req.get_body_data();
local args = ngx.req.get_uri_args();
--local headers = {
--    ["Content-Encoding"] = "deflate",
--}

if action == "GET" then
    res3 = ngx.location.capture(
        "/sub3"..ngx.var.request_uri,
        {
            method = ngx.HTTP_GET,
            -- body = data
        }
        )
    if res3.status == ngx.HTTP_OK then
        ngx.print(res3.body)
    end
else
    res1 = ngx.location.capture(
        "/sub1"..ngx.var.request_uri,
        {
            method = ngx.HTTP_OPTIONS,
            body = data
        }
        )

    if res1.status == ngx.HTTP_TEMPORARY_REDIRECT then
        local metadata = res1.header["Set-Cookie"]
        ngx.req.set_header("Cookie", metadata)
        res2 = ngx.location.capture(
            "/sub2"..ngx.var.request_uri,
            {
                method = ngx.HTTP_POST,
                body = res1.body
            }
            )
        if res2.status == ngx.HTTP_OK then
            for k, v in pairs(res2.header) do
                ngx.header[k] = v
            end
            ngx.print(res2.body)
        end
    else
        ngx.print(res1.body)
    end
end
