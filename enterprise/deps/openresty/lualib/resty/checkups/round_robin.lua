-- Copyright (C) 2014-2016, UPYUN Inc.

local ceil = math.ceil

local _M = { _VERSION = "0.11" }


--[[
parameters:
    - (table) servers
    - (function) peer_cb(index, server)
return:
    - (table) server
    - (string) error
--]]
function _M.next_round_robin_server(servers, peer_cb)
    local srvs_cnt = #servers

    if srvs_cnt == 1 then
        if peer_cb(1, servers[1]) then
            return servers[1], nil
        end

        return nil, "round robin: no servers available"
    end

    -- select round robin server
    local best
    local max_weight
    local weight_sum = 0
    for idx = 1, srvs_cnt do
        local srv = servers[idx]
        -- init round robin state
        srv.weight = srv.weight or 1
        srv.effective_weight = srv.effective_weight or srv.weight
        srv.current_weight = srv.current_weight or 0

        if peer_cb(idx, srv) then
            srv.current_weight = srv.current_weight + srv.effective_weight
            weight_sum = weight_sum + srv.effective_weight

            if srv.effective_weight < srv.weight then
                srv.effective_weight = srv.effective_weight + 1
            end

            if not max_weight or srv.current_weight > max_weight then
                max_weight = srv.current_weight
                best = srv
            end
        end
    end

    if not best then
        return nil, "round robin: no servers available"
    end

    best.current_weight = best.current_weight - weight_sum

    return best, nil
end



function _M.free_round_robin_server(srv, failed)
    if not failed then
        return
    end

    srv.effective_weight = ceil((srv.effective_weight or 1) / 2)
end


return _M
