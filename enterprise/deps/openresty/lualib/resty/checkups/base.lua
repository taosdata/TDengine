-- Copyright (C) 2014-2016 UPYUN, Inc.

local cjson         = require "cjson.safe"

local lock          = require "resty.lock"
local subsystem     = require "resty.subsystem"

local str_format    = string.format
local str_sub       = string.sub
local str_find      = string.find
local str_match     = string.match
local tab_insert    = table.insert
local unpack        = unpack
local tostring      = tostring
local ipairs        = ipairs
local pairs         = pairs
local type          = type

local log           = ngx.log
local ERR           = ngx.ERR
local WARN          = ngx.WARN
local now           = ngx.now

local get_shm       = subsystem.get_shm
local get_shm_key   = subsystem.get_shm_key
local state         = get_shm("state")


local _M = {
    _VERSION = "0.20",
    STATUS_OK = 0, STATUS_UNSTABLE = 1, STATUS_ERR = 2
}

local ngx_upstream

local CHECKUP_TIMER_KEY = "checkups:timer"
_M.CHECKUP_TIMER_KEY = CHECKUP_TIMER_KEY
local CHECKUP_LAST_CHECK_TIME_KEY = "checkups:last_check_time"
_M.CHECKUP_LAST_CHECK_TIME_KEY = CHECKUP_LAST_CHECK_TIME_KEY
local CHECKUP_TIMER_ALIVE_KEY = "checkups:timer_alive"
_M.CHECKUP_TIMER_ALIVE_KEY = CHECKUP_TIMER_ALIVE_KEY
local PEER_STATUS_PREFIX = "checkups:peer_status:"
_M.PEER_STATUS_PREFIX = PEER_STATUS_PREFIX
local SHD_CONFIG_VERSION_KEY = "config_version"
_M.SHD_CONFIG_VERSION_KEY = SHD_CONFIG_VERSION_KEY
local SKEYS_KEY = "checkups:skeys"
_M.SKEYS_KEY = SKEYS_KEY
local SHD_CONFIG_PREFIX = "shd_config"
_M.SHD_CONFIG_PREFIX = SHD_CONFIG_PREFIX


local upstream = {}
_M.upstream = upstream
local peer_id_dict = {}

local ups_status_timer_created
_M.ups_status_timer_created = ups_status_timer_created
local cluster_status = {}
_M.cluster_status = cluster_status


_M.is_tab = function(t) return type(t) == "table" end
_M.is_str = function(t) return type(t) == "string" end
_M.is_num = function(t) return type(t) == "number" end
_M.is_nul = function(t) return t == nil or t == ngx.null end


local function _gen_key(skey, srv)
    return str_format("%s:%s:%d", skey, srv.host, srv.port)
end
_M._gen_key = _gen_key


local function extract_srv_host_port(name)
    local from, to = str_find(name, ":")
    if from then
        local host = str_sub(name, 1, from - 1)
        local port = str_sub(name, to + 1)
        host = str_match(host, "^%d+%.%d+%.%d+%.%d+$")
        port = str_match(port, "^%d+$")
        if host and port then
            return host, port
        end
    else
        local host = str_match(name, "^%d+%.%d+%.%d+%.%d+$")
        if host then
            return host, 80
        end
    end
end


function _M.get_srv_status(skey, srv)
    local server_status = cluster_status[skey]
    if not server_status then
        return _M.STATUS_OK
    end

    local srv_key = str_format("%s:%d", srv.host, srv.port)
    local srv_status = server_status[srv_key]
    local fail_timeout = srv.fail_timeout or 10

    if srv_status and srv_status.lastmodify + fail_timeout > now() then
        return srv_status.status
    end

    return _M.STATUS_OK
end


function _M.set_srv_status(skey, srv, failed)
    local server_status = cluster_status[skey]
    if not server_status then
        server_status = {}
        cluster_status[skey] = server_status
    end

    -- The default max_fails is 0, which differs from nginx upstream module(1).
    local max_fails = srv.max_fails or 0
    local fail_timeout = srv.fail_timeout or 10
    if max_fails == 0 then  -- disables the accounting of attempts
        return
    end

    local time_now = now()
    local srv_key = str_format("%s:%d", srv.host, srv.port)
    local srv_status = server_status[srv_key]
    if not srv_status then  -- first set
        srv_status = {
            status = _M.STATUS_OK,
            failed_count = 0,
            lastmodify = time_now
        }
        server_status[srv_key] = srv_status
    elseif srv_status.lastmodify + fail_timeout < time_now then -- srv_status expired
        srv_status.status = _M.STATUS_OK
        srv_status.failed_count = 0
        srv_status.lastmodify = time_now
    end

    if failed then
        srv_status.failed_count = srv_status.failed_count + 1

        if srv_status.failed_count >= max_fails then
            local ups = upstream.checkups[skey]
            for level, cls in pairs(ups.cluster) do
                for _, s in ipairs(cls.servers) do
                    local k = str_format("%s:%d", s.host, s.port)
                    local st = server_status[k]
                    -- not the last ok server
                    if not st or st.status == _M.STATUS_OK and k ~= srv_key then
                        srv_status.status = _M.STATUS_ERR
                        return
                    end
                end
            end
        end
    end
end


function _M.check_res(res, check_opts)
    if res then
        local typ = check_opts.typ

        if typ == "http" and type(res) == "table"
        and res.status then
            local status = tostring(res.status)
            local http_opts = check_opts.http_opts
            if http_opts and http_opts.statuses and
                http_opts.statuses[status] == false then
                return false
            end
        end
        return true
    end

    return false
end


function _M.try_server(skey, ups, srv, callback, args, try)
    try = try or 1
    local peer_key = _gen_key(skey, srv)
    local peer_status = cjson.decode(state:get(PEER_STATUS_PREFIX .. peer_key))
    local res, err

    if peer_status == nil or peer_status.status ~= _M.STATUS_ERR then
        for i = 1, try, 1 do
            res, err = callback(srv.host, srv.port, unpack(args))
            if _M.check_res(res, ups) then
                return res
            end
        end
    end

    return nil, err
end


function _M.get_lock(key, timeout)
    local lock = lock:new(get_shm_key("locks"), {timeout=timeout})
    local elapsed, err = lock:lock(key)
    if not elapsed then
        log(WARN, "failed to acquire the lock: ", key, ", ", err)
        return nil, err
    end

    return lock
end


function _M.release_lock(lock)
    local ok, err = lock:unlock()
    if not ok then
        log(WARN, "failed to unlock: ", err)
    end
end


function _M.get_peer_status(skey, srv)
    local peer_key = PEER_STATUS_PREFIX .. _gen_key(skey, srv)
    local peer_status = state:get(peer_key)
    return not _M.is_nul(peer_status) and cjson.decode(peer_status) or nil
end


function _M.get_upstream_status(skey)
    local ups = upstream.checkups[skey]
    if not ups then
        return
    end

    local ups_status = {}

    for level, cls in pairs(ups.cluster) do
        local servers = cls.servers
        ups_status[level] = {}
        if servers and type(servers) == "table" and #servers > 0 then
            for _, srv in ipairs(servers) do
                local peer_status = _M.get_peer_status(skey, srv) or {}
                peer_status.server = _gen_key(skey, srv)
                peer_status["weight"] = srv.weight
                peer_status["max_fails"] = srv.max_fails
                peer_status["fail_timeout"] = srv.fail_timeout
                if ups.enable == false or (ups.enable == nil and
                   upstream.default_heartbeat_enable == false) then
                    peer_status.status = "unchecked"
                else
                    if not peer_status.status or
                       peer_status.status == _M.STATUS_OK then
                        peer_status.status = "ok"
                    elseif peer_status.status == _M.STATUS_ERR then
                        peer_status.status = "err"
                    else
                        peer_status.status = "unstable"
                    end
                end
                tab_insert(ups_status[level], peer_status)
            end
        end
    end

    return ups_status
end


function _M.extract_servers_from_upstream(skey, cls)
    local up_key = cls.upstream
    if not up_key then
        return
    end

    cls.servers = cls.servers or {}

    if not ngx_upstream then
        local ok
        ok, ngx_upstream = pcall(require, "ngx.upstream")
        if not ok then
            log(ERR, "ngx_upstream_lua module required")
            return
        end
    end

    local ups_backup = cls.upstream_only_backup
    local srvs_getter = ngx_upstream.get_primary_peers
    if ups_backup then
        srvs_getter = ngx_upstream.get_backup_peers
    end
    local srvs, err = srvs_getter(up_key)
    if not srvs and err then
        log(ERR, "failed to get servers in upstream ", err)
        return
    end

    for _, srv in ipairs(srvs) do
        local host, port = extract_srv_host_port(srv.name)
        if not host then
            log(ERR, "invalid server name: ", srv.name)
            return
        end
        peer_id_dict[_gen_key(skey, { host = host, port = port })] = {
            id = srv.id, backup = ups_backup and true or false}
        tab_insert(cls.servers, {
            host = host,
            port = port,
            weight = srv.weight,
            max_fails = srv.max_fails,
            fail_timeout = srv.fail_timeout,
        })
    end
end


function _M.table_dup(ori_tab)
    if type(ori_tab) ~= "table" then
        return ori_tab
    end
    local new_tab = {}
    for k, v in pairs(ori_tab) do
        if type(v) == "table" then
            new_tab[k] = _M.table_dup(v)
        else
            new_tab[k] = v
        end
    end
    return new_tab
end


function _M.ups_status_checker(premature)
    if premature then
        return
    end

    if not ngx_upstream then
        local ok
        ok, ngx_upstream = pcall(require, "ngx.upstream")
        if not ok then
            log(ERR, "ngx_upstream_lua module required")
            return
        end
    end

    local ups_status = {}
    local names = ngx_upstream.get_upstreams()
    -- get current upstream down status
    for _, name in ipairs(names) do
        local srvs = ngx_upstream.get_primary_peers(name)
        for _, srv in ipairs(srvs) do
            ups_status[srv.name] = srv.down and _M.STATUS_ERR or _M.STATUS_OK
        end

        srvs = ngx_upstream.get_backup_peers(name)
        for _, srv in ipairs(srvs) do
            ups_status[srv.name] = srv.down and _M.STATUS_ERR or _M.STATUS_OK
        end
    end

    for skey, ups in pairs(upstream.checkups) do
        for level, cls in pairs(ups.cluster) do
            if not cls.upstream then
                break
            end

            for _, srv in pairs(cls.servers) do
                local peer_key = _gen_key(skey, srv)
                local status_key = PEER_STATUS_PREFIX .. peer_key

                local peer_status, err = state:get(status_key)
                if peer_status then
                    local st = cjson.decode(peer_status)
                    local up_st = ups_status[srv.host .. ':' .. srv.port]
                    local unstable = st.status == _M.STATUS_UNSTABLE
                    if (unstable and up_st == _M.STATUS_ERR) or
                        (not unstable and up_st and st.status ~= up_st) then
                        local up_id = peer_id_dict[peer_key]
                        local down = up_st == _M.STATUS_OK
                        local ok, err = ngx_upstream.set_peer_down(
                            cls.upstream, up_id.backup, up_id.id, down)
                        if not ok then
                            log(ERR, "failed to set peer down", err)
                        end
                    end
                elseif err then
                    log(WARN, "get peer status error ", status_key, " ", err)
                end
            end
        end
    end

    local interval = upstream.ups_status_timer_interval
    local ok, err = ngx.timer.at(interval, _M.ups_status_checker)
    if not ok then
        ups_status_timer_created = false
        log(WARN, "failed to create ups_status_checker: ", err)
    end
end


return _M
