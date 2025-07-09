{
  graphitePort: 2003
, graphiteHost: "127.0.0.1"
, port: 8125
, backends: [ "./backends/console", "./backends/graphite", "./backends/repeater" ]
, repeater: [ { host: '127.0.0.1', port: 8125 }, { host:'TaosadapterIp', port: TaosadapterPort } ]
}
