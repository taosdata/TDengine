-- you can leave _M empty, the table will dynamicly added, or configure default below addresses
-- created by taosdata
_M = { 
    ["10.0.2.15"] = {
        cluster = {
            {
                servers = {
                    {host="10.0.2.15", port=6290, weight=10, max_fails=1, fail_timeout=10},
                }
            },
        },
    },
    ["10.0.4.15"] = {
        cluster = {
            {
                servers = {
                    {host="10.0.4.15", port=6290, weight=10, max_fails=1, fail_timeout=10},
                }
            },
        },
    }
}

_M.global = {
    checkup_timer_interval = 5,
    checkup_timer_overtime = 5,
    default_heartbeat_enable = true,
    checkup_shd_sync_enable = true,
    shd_config_timer_interval = 5,
    ups_status_sync_enable = true,
    ups_status_timer_interval = 10,
}

--_M.ups1 = {
--    cluster = {
--        {
--            servers = {
--                {host="10.0.2.15", port=6300, weight=10, max_fails=1, fail_timeout=10},
--            }
--        },
--    },
--}

return _M
