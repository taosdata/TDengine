-- Copyright (C) 2017 Libo Huang (huangnauh), UPYUN Inc.

local ngx_subsystem = ngx.config.subsystem
local str_format    = string.format
local _M = {}

local function get_shm_key(key)
    if ngx_subsystem == "http" then
        return key
    else
        return str_format("%s_%s", ngx_subsystem, key)
    end
end

function _M.get_shm(key)
    local shm_key = get_shm_key(key)
    return ngx.shared[shm_key]
end

_M.get_shm_key = get_shm_key

return _M
