package wrapper

import (
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/errors"
)

func prepareEnv() unsafe.Pointer {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}
	err = exec(conn, "create database if not exists test_schemaless_common")
	if err != nil {
		panic(err)
	}
	err = ensureDBCreated(conn, "test_schemaless_common")
	if err != nil {
		panic(err)
	}
	code := TaosSelectDB(conn, "test_schemaless_common")
	if code != 0 {
		panic("use db test_schemaless_common fail")
	}
	return conn
}

func cleanEnv(conn unsafe.Pointer) {
	err := exec(conn, "drop database if exists test_schemaless_common")
	if err != nil {
		panic(err)
	}
}

func BenchmarkTelnetSchemaless(b *testing.B) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	for i := 0; i < b.N; i++ {
		result := TaosSchemalessInsert(conn, []string{
			"sys_if_bytes_out 1636626444 1.3E3 host=web01 interface=eth0",
		}, OpenTSDBTelnetLineProtocol, "")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			b.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(result)
	}
}

// @author: xftan
// @date: 2022/1/27 17:26
// @description: test schemaless opentsdb telnet
func TestSchemalessTelnet(t *testing.T) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	result := TaosSchemalessInsert(conn, []string{
		"sys_if_bytes_out 1636626444 1.3E3 host=web01 interface=eth0",
	}, OpenTSDBTelnetLineProtocol, "")
	code := TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(result)
	s := time.Now()
	result = TaosSchemalessInsert(conn, []string{
		"sys_if_bytes_out 1636626444 1.3E3 host=web01 interface=eth0",
	}, OpenTSDBTelnetLineProtocol, "")
	code = TaosError(result)
	if code != 0 {
		errStr := TaosErrorStr(result)
		TaosFreeResult(result)
		t.Error(errors.NewError(code, errStr))
		return
	}
	TaosFreeResult(result)
	t.Log("finish ", time.Since(s))
}

// @author: xftan
// @date: 2022/1/27 17:26
// @description: test schemaless influxDB
func TestSchemalessInfluxDB(t *testing.T) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	{
		result := TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000",
		}, InfluxDBLineProtocol, "")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(result)
	}
	{
		result := TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000",
		}, InfluxDBLineProtocol, "ns")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(result)
	}
	{
		result := TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800000000",
		}, InfluxDBLineProtocol, "u")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(result)
	}
	{
		result := TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800000000",
		}, InfluxDBLineProtocol, "μ")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(result)
	}
	{
		result := TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800000",
		}, InfluxDBLineProtocol, "ms")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(result)
	}
	{
		result := TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 1577836800",
		}, InfluxDBLineProtocol, "s")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(result)
	}
	{
		result := TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 26297280",
		}, InfluxDBLineProtocol, "m")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(result)
	}
	{
		result := TaosSchemalessInsert(conn, []string{
			"measurement,host=host1 field1=2i,field2=2.0 438288",
		}, InfluxDBLineProtocol, "h")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Error(errors.NewError(code, errStr))
			return
		}
		TaosFreeResult(result)
	}
}

// @author: xftan
// @date: 2023/10/13 11:28
// @description: test schemaless insert with opentsdb telnet line protocol
func TestSchemalessRawTelnet(t *testing.T) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	type in struct {
		rows []string
	}
	data := []in{
		{
			rows: []string{"sys_if_bytes_out 1636626444 1.3E3 host=web01 interface=eth0"},
		},
		{
			rows: []string{"sys_if_bytes_out 1636626444 1.3E3 host=web01 interface=eth0"},
		},
	}
	for _, d := range data {
		row := strings.Join(d.rows, "\n")
		totalRows, result := TaosSchemalessInsertRaw(conn, row, OpenTSDBTelnetLineProtocol, "")
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Log(row)
			t.Error(errors.NewError(code, errStr))
			return
		}
		if int(totalRows) != len(d.rows) {
			t.Log(row)
			t.Errorf("expect rows %d got %d", len(d.rows), totalRows)
		}
		affected := TaosAffectedRows(result)
		if affected != len(d.rows) {
			t.Log(row)
			t.Errorf("expect affected %d got %d", len(d.rows), affected)
		}
		TaosFreeResult(result)
	}
}

// @author: xftan
// @date: 2023/10/13 11:29
// @description: test schemaless insert with opentsdb telnet line protocol
func TestSchemalessRawInfluxDB(t *testing.T) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	type in struct {
		rows      []string
		precision string
	}
	data := []in{
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"},
			precision: "",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"},
			precision: "ns",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000000"},
			precision: "u",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000000"},
			precision: "μ",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000"},
			precision: "ms",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800"},
			precision: "s",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 26297280"},
			precision: "m",
		},
		{
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 438288"},
			precision: "h",
		},
		{
			rows:      []string{"cpu_value,host=xyzzy,instance=0,type=cpu,type_instance=user value=63843347 1665212955372077566\n"},
			precision: "ns",
		},
	}
	for _, d := range data {
		row := strings.Join(d.rows, "\n")
		totalRows, result := TaosSchemalessInsertRaw(conn, row, InfluxDBLineProtocol, d.precision)
		code := TaosError(result)
		if code != 0 {
			errStr := TaosErrorStr(result)
			TaosFreeResult(result)
			t.Log(row)
			t.Error(errors.NewError(code, errStr))
			return
		}
		if int(totalRows) != len(d.rows) {
			t.Log(row)
			t.Errorf("expect rows %d got %d", len(d.rows), totalRows)
		}
		affected := TaosAffectedRows(result)
		if affected != len(d.rows) {
			t.Log(row)
			t.Errorf("expect affected %d got %d", len(d.rows), affected)
		}
		TaosFreeResult(result)
	}
}

// @author: xftan
// @date: 2023/10/13 11:29
// @description: test schemaless insert raw with reqid
func TestTaosSchemalessInsertRawWithReqID(t *testing.T) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	cases := []struct {
		name      string
		row       string
		rows      int32
		precision string
		reqID     int64
	}{
		{
			name:      "1",
			row:       "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000",
			rows:      1,
			precision: "",
			reqID:     1,
		},
		{
			name:      "2",
			row:       "measurement,host=host1 field1=2i,field2=2.0 1577836900000000000",
			rows:      1,
			precision: "ns",
			reqID:     2,
		},
		{
			name:      "3",
			row:       "measurement,host=host1 field1=2i,field2=2.0 1577837000000000",
			rows:      1,
			precision: "u",
			reqID:     3,
		},
		{
			name:      "4",
			row:       "measurement,host=host1 field1=2i,field2=2.0 1577837100000000",
			rows:      1,
			precision: "μ",
			reqID:     4,
		},
		{
			name: "5",
			row: "measurement,host=host1 field1=2i,field2=2.0 1577837200000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837300000",
			rows:      2,
			precision: "ms",
			reqID:     5,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows, result := TaosSchemalessInsertRawWithReqID(conn, c.row, InfluxDBLineProtocol, c.precision, c.reqID)
			if rows != c.rows {
				t.Fatal("rows miss")
			}
			code := TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				t.Fatal(errors.NewError(code, errStr))
			}
			TaosFreeResult(result)
		})
	}
}

// @author: xftan
// @date: 2023/10/13 11:29
// @description: test schemaless insert with reqid
func TestTaosSchemalessInsertWithReqID(t *testing.T) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	cases := []struct {
		name      string
		rows      []string
		precision string
		reqID     int64
	}{
		{
			name:      "1",
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"},
			precision: "",
			reqID:     1,
		},
		{
			name:      "2",
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836900000000000"},
			precision: "ns",
			reqID:     2,
		},
		{
			name:      "3",
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577837000000000"},
			precision: "u",
			reqID:     3,
		},
		{
			name:      "4",
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577837100000000"},
			precision: "μ",
			reqID:     4,
		},
		{
			name: "5",
			rows: []string{
				"measurement,host=host1 field1=2i,field2=2.0 1577837200000",
				"measurement,host=host1 field1=2i,field2=2.0 1577837300000",
			},
			precision: "ms",
			reqID:     5,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := TaosSchemalessInsertWithReqID(conn, c.rows, InfluxDBLineProtocol, c.precision, c.reqID)
			code := TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				t.Fatal(errors.NewError(code, errStr))
			}
			TaosFreeResult(result)
		})
	}
}

// @author: xftan
// @date: 2023/10/13 11:29
// @description: test schemaless insert with ttl
func TestTaosSchemalessInsertTTL(t *testing.T) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	cases := []struct {
		name      string
		rows      []string
		precision string
		ttl       int
	}{
		{
			name:      "1",
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"},
			precision: "",
			ttl:       1000,
		},
		{
			name:      "2",
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836900000000000"},
			precision: "ns",
			ttl:       1200,
		},
		{
			name:      "3",
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577837100000000"},
			precision: "μ",
			ttl:       1400,
		},
		{
			name: "4",
			rows: []string{
				"measurement,host=host1 field1=2i,field2=2.0 1577837200000",
				"measurement,host=host1 field1=2i,field2=2.0 1577837300000",
			},
			precision: "ms",
			ttl:       1600,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := TaosSchemalessInsertTTL(conn, c.rows, InfluxDBLineProtocol, c.precision, c.ttl)
			code := TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				t.Fatal(errors.NewError(code, errStr))
			}
			TaosFreeResult(result)
		})
	}
}

// @author: xftan
// @date: 2023/10/13 11:30
// @description: test schemaless insert with ttl and reqid
func TestTaosSchemalessInsertTTLWithReqID(t *testing.T) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	cases := []struct {
		name      string
		rows      []string
		precision string
		ttl       int
		reqId     int64
	}{
		{
			name:      "1",
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836800000000000"},
			precision: "",
			ttl:       1000,
			reqId:     1,
		},
		{
			name:      "2",
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577836900000000000"},
			precision: "ns",
			ttl:       1200,
			reqId:     2,
		},
		{
			name:      "3",
			rows:      []string{"measurement,host=host1 field1=2i,field2=2.0 1577837100000000"},
			precision: "μ",
			ttl:       1400,
			reqId:     3,
		},
		{
			name: "4",
			rows: []string{
				"measurement,host=host1 field1=2i,field2=2.0 1577837200000",
				"measurement,host=host1 field1=2i,field2=2.0 1577837300000",
			},
			precision: "ms",
			ttl:       1600,
			reqId:     4,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := TaosSchemalessInsertTTLWithReqID(conn, c.rows, InfluxDBLineProtocol, c.precision, c.ttl, c.reqId)
			code := TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				t.Fatal(errors.NewError(code, errStr))
			}
			TaosFreeResult(result)
		})
	}
}

// @author: xftan
// @date: 2023/10/13 11:30
// @description: test schemaless insert raw with ttl
func TestTaosSchemalessInsertRawTTL(t *testing.T) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	cases := []struct {
		name      string
		row       string
		rows      int32
		precision string
		ttl       int
	}{
		{
			name:      "1",
			row:       "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000",
			rows:      1,
			precision: "",
			ttl:       1000,
		},
		{
			name:      "2",
			row:       "measurement,host=host1 field1=2i,field2=2.0 1577836900000000000",
			rows:      1,
			precision: "ns",
			ttl:       1200,
		},
		{
			name: "3",
			row: "measurement,host=host1 field1=2i,field2=2.0 1577837200000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837300000",
			rows:      2,
			precision: "ms",
			ttl:       1400,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows, result := TaosSchemalessInsertRawTTL(conn, c.row, InfluxDBLineProtocol, c.precision, c.ttl)
			if rows != c.rows {
				t.Fatal("rows miss")
			}
			code := TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				t.Fatal(errors.NewError(code, errStr))
			}
			TaosFreeResult(result)
		})
	}
}

// @author: xftan
// @date: 2023/10/13 11:30
// @description: test schemaless insert raw with ttl and reqid
func TestTaosSchemalessInsertRawTTLWithReqID(t *testing.T) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	cases := []struct {
		name      string
		row       string
		rows      int32
		precision string
		ttl       int
		reqID     int64
	}{
		{
			name:      "1",
			row:       "measurement,host=host1 field1=2i,field2=2.0 1577836800000000000",
			rows:      1,
			precision: "",
			ttl:       1000,
			reqID:     1,
		},
		{
			name:      "2",
			row:       "measurement,host=host1 field1=2i,field2=2.0 1577836900000000000",
			rows:      1,
			precision: "ns",
			ttl:       1200,
			reqID:     2,
		},
		{
			name: "3",
			row: "measurement,host=host1 field1=2i,field2=2.0 1577837200000\n" +
				"measurement,host=host1 field1=2i,field2=2.0 1577837300000",
			rows:      2,
			precision: "ms",
			ttl:       1400,
			reqID:     3,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows, result := TaosSchemalessInsertRawTTLWithReqID(conn, c.row, InfluxDBLineProtocol, c.precision, c.ttl, c.reqID)
			if rows != c.rows {
				t.Fatal("rows miss")
			}
			code := TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				t.Fatal(errors.NewError(code, errStr))
			}
			TaosFreeResult(result)
		})
	}
}

func TestTaosSchemalessInsertRawTTLWithReqIDTBNameKey(t *testing.T) {
	conn := prepareEnv()
	defer TaosClose(conn)
	defer cleanEnv(conn)
	cases := []struct {
		name      string
		row       string
		rows      int32
		precision string
		ttl       int
		reqID     int64
		tbNameKey string
	}{
		{
			name:      "1",
			row:       "measurement,host=host1 field1=2i,field2=1.0 1577836800000000000",
			rows:      1,
			precision: "",
			ttl:       1000,
			reqID:     1,
			tbNameKey: "host",
		},
		{
			name:      "2",
			row:       "measurement,host=host1 field1=2i,field2=2.0 1577836900000000000",
			rows:      1,
			precision: "ns",
			ttl:       1200,
			reqID:     2,
			tbNameKey: "host",
		},
		{
			name: "3",
			row: "measurement,host=host1 field1=2i,field2=3.0 1577837200000\n" +
				"measurement,host=host1 field1=2i,field2=4.0 1577837300000",
			rows:      2,
			precision: "ms",
			ttl:       1400,
			reqID:     3,
			tbNameKey: "host",
		},
		{
			name:      "no table name key",
			row:       "measurement,host=host1 field1=2i,field2=2.0 1577836900000000000",
			rows:      1,
			precision: "ns",
			ttl:       1200,
			reqID:     2,
			tbNameKey: "",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rows, result := TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn, c.row, InfluxDBLineProtocol, c.precision, c.ttl, c.reqID, c.tbNameKey)
			if rows != c.rows {
				t.Fatal("rows miss")
			}
			code := TaosError(result)
			if code != 0 {
				errStr := TaosErrorStr(result)
				t.Fatal(errors.NewError(code, errStr))
			}
			TaosFreeResult(result)
		})
	}
}
