package prometheus

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/plugin/prometheus/prompb"
	prompbWrite "github.com/taosdata/taosadapter/v3/plugin/prometheus/proto/write"
	"github.com/taosdata/taosadapter/v3/tools/connectpool"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
	"github.com/taosdata/taosadapter/v3/tools/pool"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

var logger = log.GetLogger("PLG").WithField("mod", "prometheus")
var bufferPool pool.ByteBufferPool

type Plugin struct {
	conf Config
}

func (p *Plugin) Init(r gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Debug("opentsdb_telnet disabled")
		return nil
	}
	r.Use(plugin.Auth(func(c *gin.Context, code int, err error) {
		_ = c.AbortWithError(code, err)
	}))
	r.POST("remote_read/:db", func(c *gin.Context) {
		if monitor.QueryPaused() {
			c.Header("Retry-After", "120")
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "query memory exceeds threshold")
			return
		}
	}, p.Read)
	r.POST("remote_write/:db", func(c *gin.Context) {
		if monitor.AllPaused() {
			c.Header("Retry-After", "120")
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "memory exceeds threshold")
			return
		}
	}, p.Write)
	return nil
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) Stop() error {
	return nil
}

func (p *Plugin) String() string {
	return "prometheus"
}

func (p *Plugin) Version() string {
	return "v1"
}

func (p *Plugin) Read(c *gin.Context) {
	db := c.Param("db")
	user, password, token, err := plugin.GetAuth(c)
	if err != nil {
		c.String(http.StatusUnauthorized, err.Error())
		return
	}
	data, err := c.GetRawData()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	start := time.Now()
	buf, err := snappy.Decode(nil, data)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("read snappy decode cost:", time.Since(start))
	var req prompb.ReadRequest
	start = time.Now()
	err = proto.Unmarshal(buf, &req)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("read protobuf unmarshal cost:", time.Since(start))
	start = time.Now()
	taosConn, err := commonpool.GetConnection(user, password, token, iptool.GetRealIP(c.Request))
	if err != nil {
		logger.WithError(err).Error("connect server error")
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			c.String(http.StatusForbidden, err.Error())
			return
		}
		if errors.Is(err, connectpool.ErrTimeout) || errors.Is(err, connectpool.ErrMaxWait) {
			c.String(http.StatusServiceUnavailable, err.Error())
			return
		}
		c.String(http.StatusUnauthorized, err.Error())
		return
	}
	logger.Debug("read commonpool.GetConnection cost:", time.Since(start))
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("connect pool put error")
		}
	}()
	resp, err := processRead(taosConn.TaosConnection, &req, db)
	if err != nil {
		taosError, is := err.(*tErrors.TaosError)
		if is {
			web.SetTaosErrorCode(c, int(taosError.Code))
		}
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	start = time.Now()
	respData, err := proto.Marshal(resp)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("read protobuf marshal cost:", time.Since(start))
	start = time.Now()
	compressed := snappy.Encode(nil, respData)
	logger.Debug("read snappy encode cost:", time.Since(start))
	c.Header("Content-Encoding", "snappy")
	c.Data(http.StatusAccepted, "application/x-protobuf", compressed)
}

func (p *Plugin) Write(c *gin.Context) {
	db := c.Param("db")
	ttl := c.Query("ttl")
	ttlI := 0
	var err error
	if len(ttl) > 0 {
		ttlI, err = strconv.Atoi(ttl)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
		}
	}
	user, password, token, err := plugin.GetAuth(c)
	if err != nil {
		c.String(http.StatusUnauthorized, err.Error())
		return
	}
	c.Status(http.StatusAccepted)
	bp := bufferPool.Get()
	_, err = bp.ReadFrom(c.Request.Body)
	if err != nil {
		bufferPool.Put(bp)
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	start := time.Now()
	bb := bufferPool.Get()
	defer bufferPool.Put(bb)
	bb.B, err = snappy.Decode(bb.B[:cap(bb.B)], bp.B)
	bufferPool.Put(bp)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("snappy decode cost:", time.Since(start))
	req := prompbWrite.GetWriteRequest()
	defer prompbWrite.PutWriteRequest(req)
	start = time.Now()
	err = req.Unmarshal(bb.B)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("protobuf unmarshal cost:", time.Since(start))
	if len(req.Timeseries) == 0 {
		return
	}
	start = time.Now()
	taosConn, err := commonpool.GetConnection(user, password, token, iptool.GetRealIP(c.Request))
	if err != nil {
		logger.WithError(err).Error("connect server error")
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			c.String(http.StatusForbidden, err.Error())
			return
		}
		if errors.Is(err, connectpool.ErrTimeout) || errors.Is(err, connectpool.ErrMaxWait) {
			c.String(http.StatusServiceUnavailable, err.Error())
			return
		}
		c.String(http.StatusUnauthorized, err.Error())
		return
	}
	logger.Debug("commonpool.GetConnection cost:", time.Since(start))
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("connect pool put error")
		}
	}()
	err = processWrite(taosConn.TaosConnection, req, db, ttlI)
	if err != nil {
		taosError, is := err.(*tErrors.TaosError)
		if is {
			web.SetTaosErrorCode(c, int(taosError.Code))
		}
		logger.WithError(err).Error("connect server error")
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
}

func init() {
	plugin.Register(&Plugin{})
}
