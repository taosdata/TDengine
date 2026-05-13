package system

import (
	// http
	_ "github.com/taosdata/taosadapter/v3/controller/metrics" // metrics
	_ "github.com/taosdata/taosadapter/v3/controller/ping"    // http ping
	_ "github.com/taosdata/taosadapter/v3/controller/rest"    // http rest api

	// websocket
	_ "github.com/taosdata/taosadapter/v3/controller/ws/query"      // old query
	_ "github.com/taosdata/taosadapter/v3/controller/ws/schemaless" // old schemaless
	_ "github.com/taosdata/taosadapter/v3/controller/ws/stmt"       // old stmt
	_ "github.com/taosdata/taosadapter/v3/controller/ws/tmq"        // tmq
	_ "github.com/taosdata/taosadapter/v3/controller/ws/ws"         // ws(query, stmt, schemaless)
)
