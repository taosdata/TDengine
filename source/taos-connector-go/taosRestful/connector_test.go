package taosRestful

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"database/sql/driver"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/common/testenv"
	taosError "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/types"
)

func generateCreateTableSql(db string, withJson bool) string {
	createSql := fmt.Sprintf("create table if not exists %s.alltype(ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20),"+
		"c14 varbinary(100),"+
		"c15 geometry(100),"+
		"c16 decimal(8,4),"+
		"c17 decimal(20,4)"+
		")",
		db)
	if withJson {
		createSql += " tags(t json)"
	}
	return createSql
}

func generateValues() (value []interface{}, scanValue []interface{}, insertSql string) {
	rand.Seed(time.Now().UnixNano())
	v1 := true
	v2 := int8(rand.Int())
	v3 := int16(rand.Int())
	v4 := rand.Int31()
	v5 := int64(rand.Int31())
	v6 := uint8(rand.Uint32())
	v7 := uint16(rand.Uint32())
	v8 := rand.Uint32()
	v9 := uint64(rand.Uint32())
	v10 := rand.Float32()
	v11 := rand.Float64()
	v12 := "test_binary"
	v13 := "test_nchar"
	v14 := []byte("test_varbinary")
	v15 := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}
	v16 := "123.4560"
	v17 := "-123456789.1234"
	ts := time.Now().Round(time.Millisecond).UTC()
	var (
		cts time.Time
		c1  bool
		c2  int8
		c3  int16
		c4  int32
		c5  int64
		c6  uint8
		c7  uint16
		c8  uint32
		c9  uint64
		c10 float32
		c11 float64
		c12 string
		c13 string
		c14 []byte
		c15 []byte
		c16 string
		c17 string
	)
	return []interface{}{
			ts, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17,
		}, []interface{}{cts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17},
		fmt.Sprintf(`values('%s',%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,'test_binary','test_nchar','test_varbinary','point(100 100)','123.456','-123456789.1234')`, ts.Format(time.RFC3339Nano), v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
}

// @author: xftan
// @date: 2021/12/21 10:59
// @description: test restful query of all type
func TestAllTypeQuery(t *testing.T) {
	database := "restful_test"
	db, err := sql.Open("taosRestful", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreateTableSql(database, true))
	if err != nil {
		t.Fatal(err)
	}
	colValues, scanValues, insertSql := generateValues()
	_, err = exec(db, fmt.Sprintf(`insert into %s.t1 using %s.alltype tags('{"a":"b"}') %s`, database, database, insertSql))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	var tt types.RawMessage
	dest := make([]interface{}, len(scanValues)+1)
	for i := range scanValues {
		dest[i] = reflect.ValueOf(&scanValues[i]).Interface()
	}
	dest[len(scanValues)] = &tt
	for rows.Next() {
		err := rows.Scan(dest...)
		assert.NoError(t, err)
	}
	for i, v := range colValues {
		assert.Equal(t, v, scanValues[i])
	}
	assert.Equal(t, types.RawMessage(`{"a":"b"}`), tt)
}

// @author: xftan
// @date: 2022/2/8 12:51
// @description: test query all null value
func TestAllTypeQueryNull(t *testing.T) {
	database := "restful_test_null"
	db, err := sql.Open("taosRestful", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreateTableSql(database, true))
	if err != nil {
		t.Fatal(err)
	}
	colValues, _, _ := generateValues()
	builder := &strings.Builder{}
	for i := 1; i < len(colValues); i++ {
		builder.WriteString(",null")
	}
	_, err = exec(db, fmt.Sprintf(`insert into %s.t1 using %s.alltype tags('{"a":"b"}') values('%s'%s)`, database, database, colValues[0].(time.Time).Format(time.RFC3339Nano), builder.String()))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	values := make([]interface{}, len(cTypes))
	values[0] = new(time.Time)
	for i := 1; i < len(colValues); i++ {
		var v interface{}
		values[i] = &v
	}
	var tt types.RawMessage
	values[len(colValues)] = &tt
	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			t.Fatal(err)
		}
	}
	assert.Equal(t, *values[0].(*time.Time), colValues[0].(time.Time))
	for i := 1; i < len(values)-1; i++ {
		assert.Nil(t, *values[i].(*interface{}))
	}
	assert.Equal(t, types.RawMessage(`{"a":"b"}`), *(values[len(values)-1]).(*types.RawMessage))
}

// @author: xftan
// @date: 2022/2/10 14:32
// @description: test restful query of all type with compression
func TestAllTypeQueryCompression(t *testing.T) {
	database := "restful_test_compression"
	db, err := sql.Open("taosRestful", dataSourceNameWithCompression)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreateTableSql(database, true))
	if err != nil {
		t.Fatal(err)
	}
	colValues, scanValues, insertSql := generateValues()
	_, err = exec(db, fmt.Sprintf(`insert into %s.t1 using %s.alltype tags('{"a":"b"}') %s`, database, database, insertSql))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	var tt types.RawMessage
	dest := make([]interface{}, len(scanValues)+1)
	for i := range scanValues {
		dest[i] = reflect.ValueOf(&scanValues[i]).Interface()
	}
	dest[len(scanValues)] = &tt
	for rows.Next() {
		err := rows.Scan(dest...)
		assert.NoError(t, err)
	}
	for i, v := range colValues {
		assert.Equal(t, v, scanValues[i])
	}
	assert.Equal(t, types.RawMessage(`{"a":"b"}`), tt)
}

// @author: xftan
// @date: 2022/5/19 15:22
// @description: test restful query of all type without json (httpd)
func TestAllTypeQueryWithoutJson(t *testing.T) {
	database := "restful_test_without_json"
	db, err := sql.Open("taosRestful", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreateTableSql(database, false))
	if err != nil {
		t.Fatal(err)
	}
	colValues, scanValues, insertSql := generateValues()
	_, err = exec(db, fmt.Sprintf(`insert into %s.alltype %s`, database, insertSql))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	dest := make([]interface{}, len(scanValues))
	for i := range scanValues {
		dest[i] = reflect.ValueOf(&scanValues[i]).Interface()
	}
	for rows.Next() {
		err := rows.Scan(dest...)
		assert.NoError(t, err)
	}
	for i, v := range colValues {
		assert.Equal(t, v, scanValues[i])
	}
}

// @author: xftan
// @date: 2022/5/19 15:22
// @description: test query all null value without json (httpd)
func TestAllTypeQueryNullWithoutJson(t *testing.T) {
	database := "restful_test_without_json_null"
	db, err := sql.Open("taosRestful", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreateTableSql(database, false))
	if err != nil {
		t.Fatal(err)
	}
	colValues, _, _ := generateValues()
	builder := &strings.Builder{}
	for i := 1; i < len(colValues); i++ {
		builder.WriteString(",null")
	}
	insertSql := fmt.Sprintf(`insert into %s.alltype values('%s'%s)`, database, colValues[0].(time.Time).Format(time.RFC3339Nano), builder.String())
	_, err = exec(db, insertSql)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	values := make([]interface{}, len(cTypes))
	values[0] = new(time.Time)
	for i := 1; i < len(colValues); i++ {
		var v interface{}
		values[i] = &v
	}
	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			t.Fatal(err)
		}
	}
	assert.Equal(t, *values[0].(*time.Time), colValues[0].(time.Time))
	for i := 1; i < len(values)-1; i++ {
		assert.Nil(t, *values[i].(*interface{}))
	}
}

func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P384(), crand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour)

	serialNumber, err := crand.Int(crand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Your Company"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(crand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	keyPEMBlock := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyPEM})

	return tls.X509KeyPair(certPEM, keyPEMBlock)
}

func startProxy() *http.Server {
	// Generate self-signed certificate
	cert, err := generateSelfSignedCert()
	if err != nil {
		log.Fatalf("Failed to generate self-signed certificate: %v", err)
	}

	target := "http://127.0.0.1:6041"
	proxyURL, err := url.Parse(target)
	if err != nil {
		log.Fatalf("Failed to parse target URL: %v", err)
	}

	proxy := httputil.NewSingleHostReverseProxy(proxyURL)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, e error) {
		http.Error(w, "Proxy error", http.StatusBadGateway)
	}
	mux := http.NewServeMux()
	mux.Handle("/", proxy)

	server := &http.Server{
		Addr:      ":34443",
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		// Setup server timeouts for better handling of idle connections and slowloris attacks
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	log.Println("Starting server on :34443")
	go func() {
		err = server.ListenAndServeTLS("", "")
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start HTTPS server: %v", err)
		}
	}()
	return server
}

func TestSSL(t *testing.T) {
	dataSourceNameWithSkipVerify := fmt.Sprintf("%s:%s@https(%s:%d)/?skipVerify=true", user, password, host, 34443)
	server := startProxy()
	defer func() {
		err := server.Shutdown(context.Background())
		assert.NoError(t, err)
	}()
	time.Sleep(1 * time.Second)
	database := "restful_test_ssl"
	db, err := sql.Open("taosRestful", dataSourceNameWithSkipVerify)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreateTableSql(database, true))
	if err != nil {
		t.Fatal(err)
	}
	colValues, scanValues, insertSql := generateValues()
	_, err = exec(db, fmt.Sprintf(`insert into %s.t1 using %s.alltype tags('{"a":"b"}') %s`, database, database, insertSql))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	var tt types.RawMessage
	dest := make([]interface{}, len(scanValues)+1)
	for i := range scanValues {
		dest[i] = reflect.ValueOf(&scanValues[i]).Interface()
	}
	dest[len(scanValues)] = &tt
	for rows.Next() {
		err := rows.Scan(dest...)
		assert.NoError(t, err)
	}
	for i, v := range colValues {
		assert.Equal(t, v, scanValues[i])
	}
	assert.Equal(t, types.RawMessage(`{"a":"b"}`), tt)
}

func TestConnect(t *testing.T) {
	conn := connector{
		cfg: &Config{},
	}
	db, err := conn.Connect(context.Background())
	assert.NoError(t, err)
	err = db.Close()
	assert.NoError(t, err)
	driver := conn.Driver()
	assert.Equal(t, &TDengineDriver{}, driver)
}

func TestTimezone(t *testing.T) {
	parisConn, err := sql.Open("taosRestful", dataSourceNameWithParisTimezone)
	if err != nil {
		t.Fatal(err)
	}
	shanghaiConn, err := sql.Open("taosRestful", dataSourceNameWithShanghaiTimezone)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = parisConn.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = shanghaiConn.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = parisConn.Ping()
	if err != nil {
		t.Fatal(err)
	}
	err = shanghaiConn.Ping()
	if err != nil {
		t.Fatal(err)
	}
	database := "rest_test_timezone"
	defer func() {
		_, err = exec(parisConn, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(parisConn, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(parisConn, fmt.Sprintf("create table if not exists %s.ctb(ts timestamp,v int)", database))
	require.NoError(t, err)
	shanghaiTimezone, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	parisTimezone, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	now := time.Now().Round(time.Millisecond)
	shanghaiNow := now.In(shanghaiTimezone)
	shanghaiTime := shanghaiNow.Format("2006-01-02 15:04:05.000")
	parisNow := now.In(parisTimezone)
	parisTime := parisNow.Format("2006-01-02 15:04:05.000")
	t.Log(shanghaiTime)
	t.Log(parisTime)
	t.Log(now)
	t.Log(shanghaiNow)
	t.Log(parisNow)
	// insert with shanghai timezone
	insertSql := fmt.Sprintf("insert into %s.ctb values ('%s',1)", database, shanghaiTime)
	t.Log(insertSql)
	_, err = exec(shanghaiConn, insertSql)
	require.NoError(t, err)
	// query with paris timezone
	querySql := fmt.Sprintf("select * from %s.ctb where ts = '%s'", database, parisTime)
	t.Log(querySql)
	rows, err := parisConn.Query(querySql)
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		var ts time.Time
		var v int
		err := rows.Scan(&ts, &v)
		require.NoError(t, err)
		t.Log(ts)
		assert.NotEqual(t, ts, now)
		assert.Equal(t, parisTimezone, ts.Location())
		assert.Equal(t, shanghaiNow.UnixNano()/1e6, ts.UnixNano()/1e6)
		assert.Equal(t, 1, v)
		count += 1
	}
	assert.Equal(t, 1, count)
}

func TestBearerToken(t *testing.T) {
	if !testenv.IsEnterpriseTest() {
		t.Skip("token feature is only available in enterprise edition")
	}
	db, err := sql.Open("taosRestful", dataSourceName)
	require.NoError(t, err)
	defer func() {
		err = db.Close()
		assert.NoError(t, err)
	}()
	rows, err := db.Query("create token go_rest_test_token from user root")
	require.NoError(t, err)
	defer func() {
		err = rows.Close()
		assert.NoError(t, err)
	}()
	defer func() {
		_, err = exec(db, "drop token go_rest_test_token")
		assert.NoError(t, err)
	}()
	var token string
	for rows.Next() {
		err := rows.Scan(&token)
		require.NoError(t, err)
	}
	assert.NotEmpty(t, token)

	dbWithToken, err := sql.Open("taosRestful", fmt.Sprintf("@http(%s:%d)/?bearerToken=%s", host, port, token))
	require.NoError(t, err)
	defer func() {
		err = dbWithToken.Close()
		assert.NoError(t, err)
	}()
	err = dbWithToken.Ping()
	require.NoError(t, err)

	var v int
	row := dbWithToken.QueryRow("select 1")
	err = row.Scan(&v)
	require.NoError(t, err)
	assert.Equal(t, 1, v)
}

func exec(db *sql.DB, query string, args ...interface{}) (driver.Result, error) {
	result, err := db.Exec(query, args...)
	if err != nil {
		var taosErr *taosError.TaosError
		if errors.As(err, &taosErr) && taosErr.Code == 0x3d3 {

			time.Sleep(100 * time.Millisecond)
			return exec(db, query, args...)
		}
		return nil, err
	}
	return result, nil
}
