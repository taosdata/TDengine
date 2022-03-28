/*
Graphite Required Variable:

(Leave this unset to avoid sending stats to Graphite.
 Set debug flag and leave this unset to run in 'dry' debug mode -
 useful for testing statsd clients without a Graphite server.)

  graphiteHost:     hostname or IP of Graphite server

Optional Variables:

  graphitePort:     port for the graphite text collector [default: 2003]
  graphitePicklePort: port for the graphite pickle collector [default: 2004]
  graphiteProtocol: either 'text' or 'pickle' [default: 'text']
  backends:         an array of backends to load. Each backend must exist
                    by name in the directory backends/. If not specified,
                    the default graphite backend will be loaded.
                    * example for console and graphite:
                    [ "./backends/console", "./backends/graphite" ]

  servers:          an array of server configurations.
                    If not specified, the server, address,
                    address_ipv6, and port top-level configuration
                    options are used to configure a single server for
                    backwards-compatibility
                    Each server configuration supports the following keys:
    server:         the server to load. The server must exist by name in the directory
                    servers/. If not specified, the default udp server will be loaded.
                    * example for tcp server:
                    "./servers/tcp"
    address:        address to listen on [default: 0.0.0.0]
    address_ipv6:   defines if the address is an IPv4 or IPv6 address [true or false, default: false]
    port:           port to listen for messages on [default: 8125]
    socket:         (only for tcp servers) path to unix domain socket which will be used to receive
                    metrics [default: undefinded]
    socket_mod:     (only for tcp servers) file mode which should be applied to unix domain socket, relevant
                    only if socket option is used [default: undefined]

  debug:            debug flag [default: false]
  mgmt_address:     address to run the management TCP interface on
                    [default: 0.0.0.0]
  mgmt_port:        port to run the management TCP interface on [default: 8126]
  title:            Allows for overriding the process title. [default: statsd]
                    if set to false, will not override the process title and let the OS set it.
                    The length of the title has to be less than or equal to the binary name + cli arguments
                    NOTE: This does not work on Mac's with node versions prior to v0.10

  healthStatus:     default health status to be returned and statsd process starts ['up' or 'down', default: 'up']
  dumpMessages:     log all incoming messages
  flushInterval:    interval (in ms) to flush metrics to each backend
  percentThreshold: for time information, calculate the Nth percentile(s)
                    (can be a single value or list of floating-point values)
                    negative values mean to use "top" Nth percentile(s) values
                    [%, default: 90]
  flush_counts:     send stats_counts metrics [default: true]

  keyFlush:         log the most frequently sent keys [object, default: undefined]
    interval:       how often to log frequent keys [ms, default: 0]
    percent:        percentage of frequent keys to log [%, default: 100]
    log:            location of log file for frequent keys [default: STDOUT]
  deleteIdleStats:  don't send values to graphite for inactive counters, sets, gauges, or timers
                    as opposed to sending 0.  For gauges, this unsets the gauge (instead of sending
                    the previous value). Can be individually overridden. [default: false]
  deleteGauges:     don't send values to graphite for inactive gauges, as opposed to sending the previous value [default: false]
  gaugesMaxTTL:     number of flush cycles to wait before the gauge is marked as inactive, to use in combination with deleteGauges [default: 1]
  deleteTimers:     don't send values to graphite for inactive timers, as opposed to sending 0 [default: false]
  deleteSets:       don't send values to graphite for inactive sets, as opposed to sending 0 [default: false]
  deleteCounters:   don't send values to graphite for inactive counters, as opposed to sending 0 [default: false]
  prefixStats:      prefix to use for the statsd statistics data for this running instance of statsd [default: statsd]
                    applies to both legacy and new namespacing
  keyNameSanitize:  sanitize all stat names on ingress [default: true]
                    If disabled, it is up to the backends to sanitize keynames
                    as appropriate per their storage requirements.

  calculatedTimerMetrics: List of timer metrics that will be sent. Default will send all metrics.
                          To filter on percents and top percents: append '_percent' to the metric name.
                          Example: calculatedTimerMetrics: ['count', 'median', 'upper_percent', 'histogram']

  console:
    prettyprint:    whether to prettyprint the console backend
                    output [true or false, default: true]

  log:              log settings [object, default: undefined]
    backend:        where to log: stdout or syslog [string, default: stdout]
    application:    name of the application for syslog [string, default: statsd]
    level:          log level for [node-]syslog [string, default: LOG_INFO]

  graphite:
    legacyNamespace:  use the legacy namespace [default: true]
    globalPrefix:     global prefix to use for sending stats to graphite [default: "stats"]
    prefixCounter:    graphite prefix for counter metrics [default: "counters"]
    prefixTimer:      graphite prefix for timer metrics [default: "timers"]
    prefixGauge:      graphite prefix for gauge metrics [default: "gauges"]
    prefixSet:        graphite prefix for set metrics [default: "sets"]
    globalSuffix:     global suffix to use for sending stats to graphite [default: ""]
                      This is particularly useful for sending per host stats by
                      settings this value to: require('os').hostname().split('.')[0]

  repeater:         an array of hashes of the for host: and port:
                    that details other statsd servers to which the received
                    packets should be "repeated" (duplicated to).
                    e.g. [ { host: '10.10.10.10', port: 8125 },
                           { host: 'observer', port: 88125 } ]

  repeaterProtocol: whether to use udp4, udp6, or tcp for repeaters.
                    ["udp4," "udp6", or "tcp" default: "udp4"]

  histogram:        for timers, an array of mappings of strings (to match metrics) and
                    corresponding ordered non-inclusive upper limits of bins.
                    For all matching metrics, histograms are maintained over
                    time by writing the frequencies for all bins.
                    'inf' means infinity. A lower limit of 0 is assumed.
                    default: [], meaning no histograms for any timer.
                    First match wins.  examples:
                    * histogram to only track render durations, with unequal
                      class intervals and catchall for outliers:
                      [ { metric: 'render', bins: [ 0.01, 0.1, 1, 10, 'inf'] } ]
                    * histogram for all timers except 'foo' related,
                      equal class interval and catchall for outliers:
                     [ { metric: 'foo', bins: [] },
                       { metric: '', bins: [ 50, 100, 150, 200, 'inf'] } ]

  automaticConfigReload: whether to watch the config file and reload it when it
                         changes. The default is true. Set this to false to disable.
*/
{
  graphitePort: 2003
, graphiteHost: "127.0.0.1"
, port: 8125
, backends: [ "./backends/console", "./backends/graphite", "./backends/repeater" ]
, repeater: [ { host: '127.0.0.1', port: 8125 }, { host:'TaosadapterIp', port: TaosadapterPort } ]
}
