-- Copyright (C) 2014-2016 UPYUN, Inc.

local cjson         = require "cjson.safe"

local base          = require "resty.checkups.base"
local subsystem     = require "resty.subsystem"

local str_sub       = string.sub
local lower         = string.lower
local tab_sort      = table.sort
local tab_concat    = table.concat
local tab_insert    = table.insert

local re_gmatch     = ngx.re.gmatch
local re_find       = ngx.re.find
local log           = ngx.log
local localtime     = ngx.localtime
local ERR           = ngx.ERR
local WARN          = ngx.WARN
local now           = ngx.now
local tcp           = ngx.socket.tcp
local update_time   = ngx.update_time

local get_shm       = subsystem.get_shm
local mutex         = get_shm("mutex")
local state         = get_shm("state")

local _M = {
    _VERSION = "0.11",
    STATUS_OK = base.STATUS_OK, STATUS_UNSTABLE = base.STATUS_UNSTABLE, STATUS_ERR = base.STATUS_ERR
}

local resty_redis, resty_mysql


local function update_peer_status(srv, sensibility)
    local peer_key = srv.peer_key
    local status_key = base.PEER_STATUS_PREFIX .. peer_key
    local status_str, err = state:get(status_key)

    if err then
        log(ERR, "get old status ", status_key, " ", err)
        return
    end

    local old_status, err
    if status_str then
        old_status, err = cjson.decode(status_str)
        if err then
            log(WARN, "decode old status error: ", err)
        end
    end

    if not old_status then
        old_status = {
            status = _M.STATUS_OK,
            fail_num = 0,
            lastmodified = localtime(),
        }
    end

    local status = srv.status
    if status == _M.STATUS_OK then
        if old_status.status ~= _M.STATUS_OK then
            old_status.lastmodified = localtime()
            old_status.status = _M.STATUS_OK
        end
        old_status.fail_num = 0
    else  -- status == _M.STATUS_ERR or _M.STATUS_UNSTABLE
        old_status.fail_num = old_status.fail_num + 1

        if old_status.status ~= status and
            old_status.fail_num >= sensibility then
            old_status.status = status
            old_status.lastmodified = localtime()
        end
    end

    for k, v in pairs(srv.statuses) do
        old_status[k] = v
    end

    local ok, err = state:set(status_key, cjson.encode(old_status))
    if not ok then
        log(ERR, "failed to set new status ", err)
    end
end


local function update_upstream_status(ups_status, sensibility)
    if not ups_status then
        return
    end

    for _, srv in ipairs(ups_status) do
        update_peer_status(srv, sensibility)
    end
end


local heartbeat = {
    general = function (host, port, ups)
        local id = host .. ':' .. port

        local sock = tcp()
        sock:settimeout(ups.timeout * 1000)
        local ok, err = sock:connect(host, port)
        if not ok then
            log(ERR, "failed to connect: ", id, ", ", err)
            return _M.STATUS_ERR, err
        end
        -- begin add by taosdata
        req = "GET /heartbeat HTTP/1.1\r\n\r\n"
        local bytes, err = sock:send(req)
        if not bytes then
            log(ERR, "failed to send request to: ", id, ", ", err)
            return _M.STATUS_ERR, err
        end

        local readline = sock:receiveuntil("\r\n")
        local status_line, err = readline()
        if not status_line then
            log(ERR, "failed to receive status line from: ", id, ", ", err)
            return _M.STATUS_ERR, err
        end
        log(ngx.DEBUG, "status_line: ", status_line)
        sock:close()
	-- end add by taosdata

        return _M.STATUS_OK
    end,

    redis = function (host, port, ups)
        local id = host .. ':' .. port

        if not resty_redis then
            local ok
            ok, resty_redis = pcall(require, "resty.redis")
            if not ok then
                log(ERR, "failed to require resty.redis")
                return _M.STATUS_ERR, "failed to require resty.redis"
            end
        end

        local red, err = resty_redis:new()
        if not red then
            log(WARN, "failed to new redis: ", err)
            return _M.STATUS_ERR, err
        end

        red:set_timeout(ups.timeout * 1000)

        local redis_err = { status = _M.STATUS_ERR, replication = cjson.null }
        local ok, err = red:connect(host, port)
        if not ok then
            log(ERR, "failed to connect redis: ", id, ", ", err)
            return redis_err, err
        end

        local res, err = red:ping()
        if not res then
            log(ERR, "failed to ping redis: ", id, ", ", err)
            return redis_err, err
        end

        local replication = {}
        local statuses = {
            status = _M.STATUS_OK ,
            replication = replication
        }

        local res, got_all_info = {}, false

        local info, err = red:info("replication")
        if not info then
            info, err = red:info()
            if not info then
                replication.err = err
                return statuses
            end

            got_all_info = true
        end

        tab_insert(res, info)

        if not got_all_info then
            local info, err = red:info("server")
            if info then
                tab_insert(res, info)
            end
        end

        res = tab_concat(res)

        red:set_keepalive(10000, 100)

        local iter, err = re_gmatch(res, [[([a-zA-Z_]+):(.+?)\r\n]], "jo")
        if not iter then
            replication.err = err
            return statuses
        end

        local replication_field = {
            role                           = true,
            master_host                    = true,
            master_port                    = true,
            master_link_status             = true,
            master_link_down_since_seconds = true,
            master_last_io_seconds_ago     = true,
        }

        local other_field = {
            redis_version = true,
        }

        while true do
            local m, err = iter()
            if err then
                replication.err = err
                return statuses
            end

            if not m then
                break
            end

            if replication_field[lower(m[1])] then
                replication[m[1]] = m[2]
            end

            if other_field[lower(m[1])] then
                statuses[m[1]] = m[2]
            end
        end

        if replication.master_link_status == "down" then
            statuses.status = _M.STATUS_UNSTABLE
            statuses.msg = "master link status: down"
        end

        return statuses
    end,

    mysql = function (host, port, ups)
        local id = host .. ':' .. port

        if not resty_mysql then
            local ok
            ok, resty_mysql = pcall(require, "resty.mysql")
            if not ok then
                log(ERR, "failed to require resty.mysql")
                return _M.STATUS_ERR, "failed to require resty.mysql"
            end
        end

        local db, err = resty_mysql:new()
        if not db then
            log(WARN, "failed to new mysql: ", err)
            return _M.STATUS_ERR, err
        end

        db:set_timeout(ups.timeout * 1000)

        local ok, err, errno, sqlstate = db:connect{
            host = host,
            port = port,
            database = ups.name,
            user = ups.user,
            password = ups.pass,
            max_packet_size = 1024*1024
        }

        if not ok then
            log(ERR, "failed to connect: ", id, ", ", err, ": ", errno, " ", sqlstate)
            return _M.STATUS_ERR, err
        end

        db:set_keepalive(10000, 100)

        return _M.STATUS_OK
    end,

    http = function(host, port, ups)
        local id = host .. ':' .. port

        local sock, err = tcp()
        if not sock then
            log(WARN, "failed to create sock: ", err)
            return _M.STATUS_ERR, err
        end

        sock:settimeout(ups.timeout * 1000)
        local ok, err = sock:connect(host, port)
        if not ok then
            log(ERR, "failed to connect: ", id, ", ", err)
            return _M.STATUS_ERR, err
        end

        local opts = ups.http_opts or {}

        local req = opts.query
        if not req then
            sock:setkeepalive()
            return _M.STATUS_OK
        end

        local bytes, err = sock:send(req)
        if not bytes then
            log(ERR, "failed to send request to: ", id, ", ", err)
            return _M.STATUS_ERR, err
        end

        local readline = sock:receiveuntil("\r\n")
        local status_line, err = readline()
        if not status_line then
            log(ERR, "failed to receive status line from: ", id, ", ", err)
            return _M.STATUS_ERR, err
        end

        local statuses = opts.statuses
        if statuses then
            local from, to, err = re_find(status_line,
                [[^HTTP/\d+\.\d+\s+(\d+)]], "joi", nil, 1)
            if not from then
                log(ERR, "bad status line from: ", id, ", ", err)
                return _M.STATUS_ERR, err
            end

            local status = str_sub(status_line, from, to)
            if statuses[status] == false then
                return _M.STATUS_ERR, "bad status code"
            end
        end

        sock:setkeepalive()

        return _M.STATUS_OK
    end,
}


local function cluster_heartbeat(skey)
    local ups = base.upstream.checkups[skey]
    if ups.enable == false or (ups.enable == nil and
        base.upstream.default_heartbeat_enable == false) then
        return
    end

    local ups_typ = ups.typ or "general"
    local ups_heartbeat = ups.heartbeat
    local ups_sensi = ups.sensibility or 1
    local ups_protected = true
    if ups.protected == false then
        ups_protected = false
    end

    ups.timeout = ups.timeout or 5

    local server_count = 0
    for level, cls in pairs(ups.cluster) do
        if cls.servers and #cls.servers > 0 then
            server_count = server_count + #cls.servers
        end
    end

    local error_count = 0
    local unstable_count = 0
    local srv_available = false
    local ups_status = {}
    for level, cls in pairs(ups.cluster) do
        for _, srv in ipairs(cls.servers) do
            local peer_key = base._gen_key(skey, srv)
            local cb_heartbeat = ups_heartbeat or heartbeat[ups_typ] or
                heartbeat["general"]
            local statuses, err = cb_heartbeat(srv.host, srv.port, ups)

            local status
            if type(statuses) == "table" then
                status = statuses.status
                statuses.status = nil
            else
                status = statuses
                statuses = {}
            end

            if not statuses.msg then
                statuses.msg = err or cjson.null
            end

            local srv_status = {
                peer_key = peer_key ,
                status   = status   ,
                statuses = statuses ,
            }

            if status == _M.STATUS_OK then
                update_peer_status(srv_status, ups_sensi)
                srv_status.updated = true
                srv_available = true
                if next(ups_status) then
                    for _, v in ipairs(ups_status) do
                        if v.status == _M.STATUS_UNSTABLE then
                            v.status = _M.STATUS_ERR
                        end
                        update_peer_status(v, ups_sensi)
                    end
                    ups_status = {}
                end
            end

            if status == _M.STATUS_ERR then
                error_count = error_count + 1
                if srv_available then
                    update_peer_status(srv_status, ups_sensi)
                    srv_status.updated = true
                end
            end

            if status == _M.STATUS_UNSTABLE then
                unstable_count = unstable_count + 1
                if srv_available then
                    srv_status.status = _M.STATUS_ERR
                    update_peer_status(srv_status, ups_sensi)
                    srv_status.updated = true
                end
            end

            if srv_status.updated ~= true then
                tab_insert(ups_status, srv_status)
            end
        end
    end

    if next(ups_status) then
        if error_count == server_count then
            if ups_protected then
                ups_status[1].status = _M.STATUS_UNSTABLE
            end
        elseif error_count + unstable_count == server_count then
            tab_sort(ups_status, function(a, b) return a.status < b.status end)
        end

        update_upstream_status(ups_status, ups_sensi)
    end
end


function _M.active_checkup(premature)
    local ckey = base.CHECKUP_TIMER_KEY

    update_time() -- flush cache time

    if premature then
        local ok, err = mutex:set(ckey, nil)
        if not ok then
            log(WARN, "failed to update shm: ", err)
        end
        return
    end

    for skey in pairs(base.upstream.checkups) do
        cluster_heartbeat(skey)
    end

    local interval = base.upstream.checkup_timer_interval
    local overtime = base.upstream.checkup_timer_overtime

    state:set(base.CHECKUP_LAST_CHECK_TIME_KEY, localtime())
    state:set(base.CHECKUP_TIMER_ALIVE_KEY, true, overtime)

    local ok, err = mutex:set(ckey, 1, overtime)
    if not ok then
        log(WARN, "failed to update shm: ", err)
    end

    local ok, err = ngx.timer.at(interval, _M.active_checkup)
    if not ok then
        log(WARN, "failed to create timer: ", err)
        local ok, err = mutex:set(ckey, nil)
        if not ok then
            log(WARN, "failed to update shm: ", err)
        end
        return
    end
end


return _M
