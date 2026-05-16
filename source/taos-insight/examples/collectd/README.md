# Example CollectD Grafana Dashboards using TDengine

This an example project to show the how CollectD + TDengine + Grafana works together.

The CollectD dashboard comes from the top 1 downloaded [dashboard](https://grafana.com/grafana/dashboards/24) - which downloaded 10k times, completely(mostly) ported to TDengine with its [data source plugin](https://github.com/taosdata/grafanaplugin).

![Dashboard](assets/collectd-dashboard.png)

## Start

Suppose you have latest [TDengine](http://taosdata.com/) **2.3+** installed and both `taosd` and `taosadapter` service started.

You could check if port `6041` is ready to be connected with `telnet` or `curl` or so.

Add these lines in `collectd.conf`.

```conf
LoadPlugin network
<Plugin network>
  Server "192.168.17.180" "6045"
</Plugin>
```

In `tdengine.env`, you should config the TDengine restful API as environment:

- `TDENGINE_API`: your TDengine RESTful API endpoint, like `http://tdengine:6041`
- `TDENGINE_USER`: TDengine user name, eg. `root`.
- `TDENGINE_PASS`: TDengine user password, eg. `taosdata`.

Then you could just start the example with `docker-compose`.

```bash
docker-compose up
```

## Services and Ports

### Grafana

- URL: http://localhost:3000
- User: admin
- Password: admin

### TDengine

- Port: 6041 (HTTP API)
- User: root
- Password: taosdata
- Database: collectd

Open grafana with dashboard name **CollectD Server Metrics with TDengine** and everything done.

## License

The AGPL-3.0 License. Please see [License File](LICENSE) for more information.
