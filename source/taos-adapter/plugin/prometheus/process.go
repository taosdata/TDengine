package prometheus

import (
	"bytes"
	"crypto/md5"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/driver/common"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/plugin/prometheus/prompb"
	prompbWrite "github.com/taosdata/taosadapter/v3/plugin/prometheus/proto/write"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/pool"
)

var jsonI = jsoniter.ConfigCompatibleWithStandardLibrary
var timeBufferPool pool.ByteBufferPool

func processWrite(taosConn unsafe.Pointer, req *prompbWrite.WriteRequest, db string, ttl int) error {
	reqID := generator.GetReqID()
	logger := logger.WithField(config.ReqIDKey, reqID)
	isDebug := log.IsDebug()
	start := time.Now()
	err := tool.SchemalessSelectDB(taosConn, logger, isDebug, db, 0)
	if err != nil {
		return err
	}
	logger.Debug("processWrite SchemalessSelectDB cost:", time.Since(start))
	start = time.Now()
	bp := pool.BytesPoolGet()
	defer pool.BytesPoolPut(bp)
	generateWriteSql(req.Timeseries, bp, ttl)
	logger.Debug("generateWriteSql cost:", time.Since(start))
	start = time.Now()
	sql := bytesutil.ToUnsafeString(bp.Bytes())

	logger.Debugf("execute sql:%s", sql)
	err = async.GlobalAsync.TaosExecWithoutResult(taosConn, logger, isDebug, sql, reqID)
	logger.Debug("processWrite TaosExecWithoutResult cost:", time.Since(start))
	if err != nil {
		logger.WithError(err).Error("processWrite error, retry processWrite")
		if tErr, is := err.(*tErrors.TaosError); is {
			start = time.Now()
			if tErr.Code == httperror.PAR_TABLE_NOT_EXIST || tErr.Code == httperror.MND_INVALID_TABLE_NAME || tErr.Code == httperror.TSC_INVALID_TABLE_NAME {
				err := async.GlobalAsync.TaosExecWithoutResult(taosConn, logger, isDebug, "create stable if not exists metrics(ts timestamp,v double) tags (labels json)", reqID)
				if err != nil {
					return err
				}
				// retry
				err = async.GlobalAsync.TaosExecWithoutResult(taosConn, logger, isDebug, sql, reqID)
				if err != nil {
					logger.WithError(err).Error(bp.String())
					return err
				}
				logger.Debug("retry processWrite cost", time.Since(start))
				return nil
			}
			logger.WithError(err).Error(bp.String())
			return err
		}
		logger.WithError(err).Error(bp.String())
		return err
	}
	return nil
}

func generateWriteSql(timeseries []prompbWrite.TimeSeries, sql *bytes.Buffer, ttl int) {
	sql.WriteString("insert into ")
	tmp := pool.BytesPoolGet()
	defer pool.BytesPoolPut(tmp)
	timeBuffer := timeBufferPool.Get()
	defer timeBufferPool.Put(timeBuffer)
	bb := bufferPool.Get()
	defer bufferPool.Put(bb)
	jsonBuilder := jsonI.BorrowStream(nil)
	defer jsonI.ReturnStream(jsonBuilder)
	for i := 0; i < len(timeseries); i++ {
		labelCount := len(timeseries[i].Labels)
		tmp.Reset()
		sort.Sort(timeseries[i].Labels)
		for labelIndex := 0; labelIndex < labelCount; labelIndex++ {
			tmp.Write(timeseries[i].Labels[labelIndex].Name)
			tmp.WriteByte('=')
			tmp.Write(timeseries[i].Labels[labelIndex].Value)
			if labelIndex != labelCount-1 {
				tmp.WriteByte(',')
			}
		}
		tableName := fmt.Sprintf("t_%x", md5.Sum(tmp.Bytes()))
		sql.WriteString(tableName)
		sql.WriteString(" using metrics tags('")
		bb.Reset()
		jsonBuilder.Reset(bb)
		jsonBuilder.WriteObjectStart()
		for labelIndex := 0; labelIndex < labelCount; labelIndex++ {
			if labelIndex != 0 {
				jsonBuilder.WriteMore()
			}
			jsonBuilder.WriteObjectField(bytesutil.ToUnsafeString(timeseries[i].Labels[labelIndex].Name))
			jsonBuilder.WriteString(bytesutil.ToUnsafeString(timeseries[i].Labels[labelIndex].Value))
		}
		jsonBuilder.WriteObjectEnd()
		_ = jsonBuilder.Flush()
		sql.Write(escapeBytes(bb.B))
		sql.WriteString("') ")
		if ttl > 0 {
			sql.WriteString("ttl ")
			sql.WriteString(strconv.Itoa(ttl))
			sql.WriteByte(' ')
		}
		sql.WriteString("values")
		for _, sample := range timeseries[i].Samples {
			sql.WriteString("('")
			timeBuffer.Reset()
			timeBuffer.B = time.Unix(sample.Timestamp/1e3, (sample.Timestamp%1e3)*1e6).UTC().AppendFormat(timeBuffer.B, time.RFC3339Nano)
			sql.WriteString(bytesutil.ToUnsafeString(timeBuffer.B))
			sql.WriteString("',")
			if math.IsNaN(sample.Value) {
				sql.WriteString("null")
			} else if math.IsInf(sample.Value, 0) {
				sql.WriteString("null")
			} else {
				_, _ = fmt.Fprintf(sql, "%v", sample.Value)
			}
			sql.WriteString(") ")
		}
	}
}

func processRead(taosConn unsafe.Pointer, req *prompb.ReadRequest, db string) (resp *prompb.ReadResponse, err error) {
	isDebug := log.IsDebug()
	reqID := generator.GetReqID()
	logger := logger.WithField(config.ReqIDKey, reqID)
	logger.Tracef("select db %s", db)
	code := syncinterface.TaosSelectDB(taosConn, db, logger, isDebug)
	if code != 0 {
		return nil, tErrors.NewError(code, syncinterface.TaosErrorStr(nil, logger, isDebug))
	}
	resp = &prompb.ReadResponse{}
	for i, query := range req.Queries {
		start := log.GetLogNow(isDebug)
		sql, err := generateReadSql(query)
		logger.Debug("processRead generateReadSql cost:", log.GetLogDuration(isDebug, start))
		if err != nil {
			return nil, err
		}
		start = time.Now()
		logger.Tracef("execute sql: %s", sql)
		data, err := async.GlobalAsync.TaosExec(taosConn, logger, isDebug, sql, func(ts int64, precision int) driver.Value {
			switch precision {
			case common.PrecisionMilliSecond:
				return ts
			case common.PrecisionMicroSecond:
				return ts / 1e3
			case common.PrecisionNanoSecond:
				return ts / 1e6
			default:
				return 0
			}
		}, reqID)
		if err != nil {
			logger.WithError(err).Error(sql)
			return nil, err
		}
		logger.Debug("processRead TaosExec cost:", time.Since(start))
		//ts value labels time.Time float64 []byte
		start = time.Now()
		group := map[string]*prompb.TimeSeries{}
		for _, d := range data.Data {
			if len(d) != 4 {
				continue
			}
			if d[0] == nil || d[1] == nil || d[2] == nil || d[3] == nil {
				continue
			}
			ts := d[0].(int64)
			value := d[1].(float64)
			var tags map[string]string
			err = json.Unmarshal(d[2].(json.RawMessage), &tags)
			if err != nil {
				return nil, err
			}
			tbName := d[3].(string)
			timeSeries, exist := group[tbName]
			if exist {
				timeSeries.Samples = append(timeSeries.Samples, prompb.Sample{
					Value:     value,
					Timestamp: ts,
				})
			} else {
				timeSeries = &prompb.TimeSeries{
					Samples: []prompb.Sample{
						{
							Value:     value,
							Timestamp: ts,
						},
					},
				}
				timeSeries.Labels = make([]prompb.Label, 0, len(tags))
				for name, tagValue := range tags {
					timeSeries.Labels = append(timeSeries.Labels, prompb.Label{
						Name:  name,
						Value: tagValue,
					})
				}
				group[tbName] = timeSeries
			}
		}
		if len(group) > 0 {
			resp.Results = append(resp.Results, &prompb.QueryResult{Timeseries: make([]*prompb.TimeSeries, 0, len(group))})
		}
		for _, series := range group {
			resp.Results[i].Timeseries = append(resp.Results[i].Timeseries, series)
		}
		logger.Debug("processRead process result cost:", time.Since(start))
	}
	return resp, err
}

func generateReadSql(query *prompb.Query) (string, error) {
	sql := pool.BytesPoolGet()
	defer pool.BytesPoolPut(sql)
	sql.WriteString("select metrics.*,tbname from metrics where ts >= '")
	sql.WriteString(ms2Time(query.GetStartTimestampMs()))
	sql.WriteString("' and ts <= '")
	sql.WriteString(ms2Time(query.GetEndTimestampMs()))
	sql.WriteByte('\'')
	for _, matcher := range query.GetMatchers() {
		sql.WriteString(" and ")
		k := escapeString(matcher.GetName())
		v := escapeString(matcher.GetValue())
		sql.WriteString("labels->'")
		sql.WriteString(k)
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			sql.WriteString("' = '")
		case prompb.LabelMatcher_NEQ:
			sql.WriteString("' != '")
		case prompb.LabelMatcher_RE:
			sql.WriteString("' match '")
		case prompb.LabelMatcher_NRE:
			sql.WriteString("' nmatch '")
		default:
			return "", errors.New("not support match type")
		}
		sql.WriteString(v)
		sql.WriteByte('\'')
	}
	if config.Conf.RestfulRowLimit > -1 {
		sql.WriteString(" limit ")
		_, _ = fmt.Fprintf(sql, "%d", config.Conf.RestfulRowLimit)
	}

	return sql.String(), nil
}

func ms2Time(ts int64) string {
	return time.Unix(ts/1e3, (ts%1e3)*1e6).UTC().Format(time.RFC3339Nano)
}

func escapeString(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `'`, `\'`)
}

var escapeSingleQuoteOld = []byte{'\''}
var escapeSingleQuoteNew = []byte{'\\', '\''}
var escapeBackslashOld = []byte{'\\'}
var escapeBackslashNew = []byte{'\\', '\\'}

func escapeBytes(s []byte) []byte {
	n := bytes.Count(s, escapeBackslashOld)
	if n > 0 {
		s = bytes.ReplaceAll(s, escapeBackslashOld, escapeBackslashNew)
	}
	n = bytes.Count(s, escapeSingleQuoteOld)
	if n > 0 {
		s = bytes.ReplaceAll(s, escapeSingleQuoteOld, escapeSingleQuoteNew)
	}
	return s
}
