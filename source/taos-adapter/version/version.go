package version

import "github.com/taosdata/taosadapter/v3/driver/wrapper"

var Version = "0.1.0"

var CommitID = "unknown"

var BuildInfo = "unknown"

var TaosClientVersion = wrapper.TaosGetClientInfo()

//revive:disable-next-line
var CUS_NAME = "TDengine"

//revive:disable-next-line
var CUS_PROMPT = "taos"
