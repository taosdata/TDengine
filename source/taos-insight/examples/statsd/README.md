# Example StatsD Grafana Dashboards using TDengine

This an example project to show the how StatsD + TDengine + Grafana works together.

![Dashboard](dashboards/statsd-with-tdengine-view.png)

## Start

Suppose you have latest [TDengine](http://taosdata.com/) **2.3+** installed and both `taosd` and `blm3` service started and `statsd` plugin enabled in `blm3`.

You could check if port `6041` is ready to be connected with `telenet` or `curl` or so.

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

- URL: <http://localhost:3000>
- User: admin
- Password: admin

### TDengine

- Port: 8125 (udp)

Start data simulation by `./sim.sh`.

Open grafana with dashboard name **Simulating Measurement for TDengine StatsD** and everything done.

## License

The AGPL-3.0 License. Please see [License File](LICENSE) for more information.
