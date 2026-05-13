package wstool

import (
	"database/sql/driver"
	"encoding/json"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/tools/innerjson"
	"github.com/taosdata/taosadapter/v3/tools/melody"
	"github.com/taosdata/taosadapter/v3/version"
)

type TDEngineRestfulResp struct {
	Code       int              `json:"code,omitempty"`
	Desc       string           `json:"desc,omitempty"`
	ColumnMeta [][]interface{}  `json:"column_meta,omitempty"`
	Data       [][]driver.Value `json:"data,omitempty"`
	Rows       int              `json:"rows,omitempty"`
}

func WSWriteJson(session *melody.Session, logger *logrus.Entry, data interface{}) {
	// use innerjson to marshal, because some string maybe contains HTML characters &, <, and >
	// such as "where ts >= 123" will be escaped to "where ts \u003e= 123" by encoding/json
	b, err := innerjson.Marshal(data)
	if err != nil {
		logger.Errorf("marshal json failed:%s, data:%#v", err, data)
		return
	}
	logger.Tracef("write json:%s", b)
	_ = session.Write(b)
	logger.Trace("write json done")
}

func WSWriteBinary(session *melody.Session, data []byte, logger *logrus.Entry) {
	logger.Tracef("write binary:%+v", data)
	_ = session.WriteBinary(data)
	logger.Trace("write binary done")
}

type WSVersionResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	Version string `json:"version"`
}

var VersionResp []byte

func WSWriteVersion(session *melody.Session, logger *logrus.Entry) {
	logger.Tracef("write version,%s", VersionResp)
	_ = session.Write(VersionResp)
	logger.Trace("write version done")
}

type WSAction struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args"`
}

func init() {
	resp := WSVersionResp{
		Action:  ClientVersion,
		Version: version.TaosClientVersion,
	}
	VersionResp, _ = innerjson.Marshal(resp)
}
