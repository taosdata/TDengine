package system

import (
	_ "github.com/taosdata/taosadapter/v3/plugin/collectd"       // import collectd plugin
	_ "github.com/taosdata/taosadapter/v3/plugin/influxdb"       // import influxdb plugin
	_ "github.com/taosdata/taosadapter/v3/plugin/inputjson"      // import inputjson plugin
	_ "github.com/taosdata/taosadapter/v3/plugin/nodeexporter"   // import nodeexporter plugin
	_ "github.com/taosdata/taosadapter/v3/plugin/openmetrics"    // import openmetrics plugin
	_ "github.com/taosdata/taosadapter/v3/plugin/opentsdb"       // import opentsdb plugin
	_ "github.com/taosdata/taosadapter/v3/plugin/opentsdbtelnet" // import opentsdbtelnet plugin
	_ "github.com/taosdata/taosadapter/v3/plugin/prometheus"     // import prometheus plugin
	_ "github.com/taosdata/taosadapter/v3/plugin/statsd"         // import statsd plugin
)
