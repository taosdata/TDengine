-- Copyright (C) 2014-2016, UPYUN Inc.

local cjson           = require "cjson.safe"
local round_robin     = require "resty.checkups.round_robin"
local consistent_hash = require "resty.checkups.consistent_hash"
local base            = require "resty.checkups.base"

local max        = math.max
local sqrt       = math.sqrt
local floor      = math.floor
local tab_insert = table.insert
local tostring   = tostring

local update_time     = ngx.update_time
local now             = ngx.now

local _M = { _VERSION = "0.11" }

local is_tab = base.is_tab

local NEED_RETRY       = 0
local REQUEST_SUCCESS  = 1
local EXCESS_TRY_LIMIT = 2


local function prepare_callbacks(skey, opts)
    local ups = base.upstream.checkups[skey]

    -- calculate count of cluster and server
    local cls_keys = {}  -- string key or number level
    local srvs_cnt = 0
    if is_tab(opts.cluster_key) then  -- specify try cluster
        for _, cls_key in ipairs(opts.cluster_key) do
            local cls = ups.cluster[cls_key]
            if is_tab(cls) then
                tab_insert(cls_keys, cls_key)
                srvs_cnt = srvs_cnt + #cls.servers
            end
        end
    else  -- default try cluster
        for cls_key, cls in pairs(ups.cluster) do
            tab_insert(cls_keys, cls_key)
            srvs_cnt = srvs_cnt + #cls.servers
        end
    end


    -- get next level cluster
    local cls_key
    local cls_index = 0
    local cls_cnt = #cls_keys
    local next_cluster_cb = function()
        cls_index = cls_index + 1
        if cls_index > cls_cnt then
            return
        end

        cls_key = cls_keys[cls_index]
        return ups.cluster[cls_key]
    end


    -- get next select server
    local mode = ups.mode
    local next_server_func = round_robin.next_round_robin_server
    local key
    if mode ~= nil then
        if mode == "hash" then
            key = opts.hash_key or ngx.var.uri
        elseif mode == "url_hash" then
            key = ngx.var.uri
        elseif mode == "ip_hash" then
            key = ngx.var.remote_addr
        elseif mode == "header_hash" then
            key = ngx.var.http_x_hash_key or ngx.var.uri
        end

        next_server_func = consistent_hash.next_consistent_hash_server
    end
    local next_server_cb = function(servers, peer_cb)
        return next_server_func(servers, peer_cb, key)
    end


    -- check whether ther server is available
    local bad_servers = {}
    local peer_cb = function(index, srv)
        local key = ("%s:%s:%s"):format(cls_key, srv.host, srv.port)
        if bad_servers[key] then
            return false
        end

        if ups.enable == false or (ups.enable == nil
            and base.upstream.default_heartbeat_enable == false) then
            return base.get_srv_status(skey, srv) == base.STATUS_OK
        end

        local peer_status = base.get_peer_status(skey, srv)
	-- begin modify by taosdata
        if (peer_status.status == base.STATUS_OK)
	-- end modify by taosdata
        and base.get_srv_status(skey, srv) == base.STATUS_OK then
            return true
        end
    end


    -- check whether need retry
    local statuses
    if ups.typ == "http" and is_tab(ups.http_opts) then
        statuses = ups.http_opts.statuses
    end
    local try_cnt = 0
    local try_limit = opts.try or ups.try or srvs_cnt
    local retry_cb = function(res)
        if is_tab(res) and res.status and is_tab(statuses) then
            if statuses[tostring(res.status)] ~= false then
                return REQUEST_SUCCESS
            end
        elseif res then
            return REQUEST_SUCCESS
        end

        try_cnt = try_cnt + 1
        if try_cnt >= try_limit then
            return EXCESS_TRY_LIMIT
        end

        return NEED_RETRY
    end


    -- check whether try_time has over amount_request_time
    local try_time = 0
    local try_time_limit = opts.try_timeout or ups.try_timeout or 0
    local try_time_cb = function(this_time_try_time)
        try_time = try_time + this_time_try_time
        if try_time_limit == 0 then
            return NEED_RETRY
        elseif try_time >= try_time_limit then
            return EXCESS_TRY_LIMIT
        end

        return NEED_RETRY
    end


    -- set some status
    local free_server_func = round_robin.free_round_robin_server
    if mode == "hash" then
        free_server_func = consistent_hash.free_consitent_hash_server
    end
    local set_status_cb = function(srv, failed)
        local key = ("%s:%s:%s"):format(cls_key, srv.host, srv.port)
        bad_servers[key] = failed
        base.set_srv_status(skey, srv, failed)
        free_server_func(srv, failed)
    end


    return {
        next_cluster_cb = next_cluster_cb,
        next_server_cb = next_server_cb,
        retry_cb = retry_cb,
        peer_cb = peer_cb,
        set_status_cb = set_status_cb,
        try_time_cb = try_time_cb,
    }
end



--[[
parameters:
    - (string) skey
    - (function) request_cb(host, port)
    - (table) opts
        - (number) try
        - (table) cluster_key
        - (string) hash_key
return:
    - (string) result
    - (string) error
--]]
function _M.try_cluster(skey, request_cb, opts)
    local callbacks = prepare_callbacks(skey, opts)

    local next_cluster_cb = callbacks.next_cluster_cb
    local next_server_cb  = callbacks.next_server_cb
    local peer_cb         = callbacks.peer_cb
    local retry_cb        = callbacks.retry_cb
    local set_status_cb   = callbacks.set_status_cb
    local try_time_cb     = callbacks.try_time_cb

    -- iter servers function
    local itersrvs = function(servers, peer_cb)
        return function() return next_server_cb(servers, peer_cb) end
    end

    local res, err = nil, "no servers available"
    repeat
        -- get next level/key cluster
        local cls = next_cluster_cb()
        if not cls then
            break
        end

        for srv, err in itersrvs(cls.servers, peer_cb) do
            -- exec request callback by server
            local start_time = now()
            res, err = request_cb(srv.host, srv.port)

            -- check whether need retry
            local end_time = now()
            local delta_time = end_time - start_time

            local feedback = retry_cb(res)
            set_status_cb(srv, feedback ~= REQUEST_SUCCESS) -- set some status
            if feedback ~= NEED_RETRY then
                return res, err
            end

            local feedback_try_time = try_time_cb(delta_time)
            if feedback_try_time ~= NEED_RETRY then
                return nil, "try_timeout excceed"
            end
       end
    until false

    return res, err
end


return _M
