package openmetrics

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql/driver"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"
	"time"
	"unsafe"

	"github.com/influxdata/telegraf/plugins/parsers/openmetrics"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
	"google.golang.org/protobuf/proto"
)

var prometheus004 = `
# HELP go_gc_duration_seconds A summary of the GC invocation durations.
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 0.00010425500000000001
go_gc_duration_seconds{quantile="0.25"} 0.000139108
go_gc_duration_seconds{quantile="0.5"} 0.00015749400000000002
go_gc_duration_seconds{quantile="0.75"} 0.000331463
go_gc_duration_seconds{quantile="1"} 0.000667154
go_gc_duration_seconds_sum 0.0018183950000000002
go_gc_duration_seconds_count 7
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 15
# HELP test_metric An untyped metric with a timestamp
# TYPE test_metric untyped
test_metric{label="value"} 1.0 1490802350000
`

var openMetricsV1 = `
# HELP go_gc_duration_seconds A summary of the GC invocation durations.
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 0.00010425500000000001
go_gc_duration_seconds{quantile="0.25"} 0.000139108
go_gc_duration_seconds{quantile="0.5"} 0.00015749400000000002
go_gc_duration_seconds{quantile="0.75"} 0.000331463
go_gc_duration_seconds{quantile="1"} 0.000667154
go_gc_duration_seconds_sum 0.0018183950000000002
go_gc_duration_seconds_count 7
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 15
# HELP test_metric An untyped metric with a timestamp
# TYPE test_metric gauge
test_metric{label="value"} 1.0 1490802350
# EOF
`

var testHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/metrics":
		w.Header().Set("Content-Type", "application/openmetrics-text")
		_, err := w.Write([]byte(openMetricsV1))
		if err != nil {
			return
		}
	case "/prometheus":
		_, err := w.Write([]byte(prometheus004))
		if err != nil {
			return
		}
	case "/wrongtype":
		w.Header().Set("Content-Type", "application/openmetrics-text")
		_, err := w.Write([]byte(prometheus004))
		if err != nil {
			return
		}
	case "/protobuf":
		var metricSet openmetrics.MetricSet
		metricSet.MetricFamilies = []*openmetrics.MetricFamily{
			{
				Name: "test_gauge",
				Type: openmetrics.MetricType_GAUGE,
				Help: "test gauge",
				Metrics: []*openmetrics.Metric{
					{
						MetricPoints: []*openmetrics.MetricPoint{
							{
								Value: &openmetrics.MetricPoint_GaugeValue{
									GaugeValue: &openmetrics.GaugeValue{
										Value: &openmetrics.GaugeValue_DoubleValue{
											DoubleValue: 1234567,
										},
									},
								},
							},
						},
					},
				},
			},
		}
		bs, err := proto.Marshal(&metricSet)
		if err != nil {
			panic(err)
		}
		w.Header().Set("Content-Type", "application/openmetrics-protobuf; version=1.0.0")
		_, err = w.Write([]byte(bs))
		if err != nil {
			return
		}
	case "/basicauth":
		username, password, ok := r.BasicAuth()
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
		}
		if username == "test_user" && password == "test_pass" {
			w.Header().Set("Content-Type", "application/openmetrics-text")
			_, err := w.Write([]byte(openMetricsV1))
			if err != nil {
				return
			}
		} else {
			panic("wrong username or password")
		}
	case "/bearertoken":
		bearerToken := r.Header.Get("Authorization")
		if bearerToken == "Bearer test_token" {
			w.Header().Set("Content-Type", "application/openmetrics-text")
			_, err := w.Write([]byte(openMetricsV1))
			if err != nil {
				return
			}
		} else {
			panic("wrong bearer token")
		}
	default:
		w.WriteHeader(http.StatusNotFound)
	}
})

func TestMain(m *testing.M) {
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	os.Exit(m.Run())
}

func TestOpenMetrics(t *testing.T) {
	ts := httptest.NewServer(testHandler)
	defer ts.Close()
	tlsServer := httptest.NewTLSServer(testHandler)
	defer tlsServer.Close()

	tsUrl := ts.URL
	tlsUrl := tlsServer.URL
	dbs := []string{
		"open_metrics",
		"open_metrics_prometheus",
		"open_metrics_wrong",
		"open_metrics_proto",
		"open_metrics_tls",
		"open_metrics_tls_prometheus",
		"open_metrics_tls_wrong",
		"open_metrics_tls_proto",
	}
	viper.Set("open_metrics.enable", true)
	viper.Set("open_metrics.urls", []string{
		tsUrl,
		tsUrl + "/prometheus",
		tsUrl + "/wrongtype",
		tsUrl + "/protobuf",
		tlsUrl,
		tlsUrl + "/prometheus",
		tlsUrl + "/wrongtype",
		tlsUrl + "/protobuf",
	})
	viper.Set("open_metrics.gatherDurationSeconds", []int{1, 1, 1, 1, 1, 1, 1, 1})
	viper.Set("open_metrics.ttl", []int{1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000})
	viper.Set("open_metrics.dbs", dbs)
	viper.Set("open_metrics.responseTimeoutSeconds", []int{5, 5, 5, 5, 5, 5, 5, 5})
	viper.Set("open_metrics.insecureSkipVerify", true) // skip verify for test
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer func() {
		wrapper.TaosClose(conn)
	}()
	for _, s := range dbs {
		err = exec(conn, fmt.Sprintf("create database if not exists %s precision 'ns'", s))
		assert.NoError(t, err)
		assert.NoError(t, testtools.EnsureDBCreated(s))
	}
	defer func() {
		for _, s := range dbs {
			err = exec(conn, fmt.Sprintf("drop database if exists %s", s))
			assert.NoError(t, err)
		}
	}()
	openMetrics := &OpenMetrics{}
	assert.Equal(t, "openmetrics", openMetrics.String())
	assert.Equal(t, "v1", openMetrics.Version())
	err = openMetrics.Init(nil)
	assert.NoError(t, err)
	err = openMetrics.Start()
	assert.NoError(t, err)
	var values [][]driver.Value
	// open_metrics
	testMetricsDBs := []string{
		"open_metrics",
		"open_metrics_tls",
	}
	for _, metricsDB := range testMetricsDBs {
		assert.Eventually(t, func() bool {
			values, err = query(conn, fmt.Sprintf("select * from information_schema.ins_tables where db_name='%s' and stable_name='test_metric'", metricsDB))
			return err == nil && len(values) == 1
		}, 10*time.Second, 500*time.Millisecond)
	}
	for i := 0; i < len(testMetricsDBs); i++ {
		dbName := testMetricsDBs[i]
		sql := fmt.Sprintf("select last(`gauge`) as `gauge` from %s.test_metric", dbName)
		values, err := query(conn, sql)
		assert.NoError(t, err)
		assert.Equal(t, float64(1), values[0][0])

		sql = fmt.Sprintf("select last(`gauge`) as `gauge` from %s.go_goroutines", dbName)
		values, err = query(conn, sql)
		assert.NoError(t, err)
		assert.Equal(t, float64(15), values[0][0])

		sql = fmt.Sprintf("select last(`0`) as `c_0`, last(`0.25`) as `c_0.25`,last(`0.5`) as `c_0.5`,last(`0.75`) as `c_0.75`, last(`1`) as `c_1`,last(`count`) as `count`,last(`sum`) as `sum` from %s.go_gc_duration_seconds", dbName)
		values, err = query(conn, sql)
		assert.NoError(t, err)
		assert.Equal(t, float64(0.00010425500000000001), values[0][0])
		assert.Equal(t, float64(0.000139108), values[0][1])
		assert.Equal(t, float64(0.00015749400000000002), values[0][2])
		assert.Equal(t, float64(0.000331463), values[0][3])
		assert.Equal(t, float64(0.000667154), values[0][4])
		assert.Equal(t, float64(7), values[0][5])
		assert.Equal(t, float64(0.0018183950000000002), values[0][6])
	}

	// prometheus
	testPrometheusDBs := []string{
		"open_metrics_prometheus",
		"open_metrics_tls_prometheus",
	}
	for _, prometheusDB := range testPrometheusDBs {
		assert.Eventually(t, func() bool {
			values, err = query(conn, fmt.Sprintf("select * from information_schema.ins_tables where db_name='%s' and stable_name='test_metric'", prometheusDB))
			return err == nil && len(values) == 1
		}, 10*time.Second, 500*time.Millisecond)
	}
	for i := 0; i < len(testPrometheusDBs); i++ {
		dbName := testPrometheusDBs[i]
		sql := fmt.Sprintf("select last(`value`) as `value` from %s.test_metric", dbName)
		values, err := query(conn, sql)
		assert.NoError(t, err)
		assert.Equal(t, float64(1), values[0][0])

		sql = fmt.Sprintf("select last(`gauge`) as `gauge` from %s.go_goroutines", dbName)
		values, err = query(conn, sql)
		assert.NoError(t, err)
		assert.Equal(t, float64(15), values[0][0])

		sql = fmt.Sprintf("select last(`0`) as `c_0`, last(`0.25`) as `c_0.25`,last(`0.5`) as `c_0.5`,last(`0.75`) as `c_0.75`, last(`1`) as `c_1`,last(`count`) as `count`,last(`sum`) as `sum` from %s.go_gc_duration_seconds", dbName)
		values, err = query(conn, sql)
		assert.NoError(t, err)
		assert.Equal(t, float64(0.00010425500000000001), values[0][0])
		assert.Equal(t, float64(0.000139108), values[0][1])
		assert.Equal(t, float64(0.00015749400000000002), values[0][2])
		assert.Equal(t, float64(0.000331463), values[0][3])
		assert.Equal(t, float64(0.000667154), values[0][4])
		assert.Equal(t, float64(7), values[0][5])
		assert.Equal(t, float64(0.0018183950000000002), values[0][6])
	}
	assert.Eventually(t, func() bool {
		values, err = query(conn, "select * from information_schema.ins_tables where db_name='open_metrics_proto' and stable_name='test_gauge'")
		return err == nil && len(values) == 1
	}, 10*time.Second, 500*time.Millisecond)
	// protobuf
	values, err = query(conn, "select last(`gauge`) as `gauge` from open_metrics_proto.test_gauge;")
	assert.NoError(t, err)
	assert.Equal(t, float64(1234567), values[0][0])

	values, err = query(conn, "select last(`gauge`) as `gauge` from open_metrics_tls_proto.test_gauge;")
	assert.NoError(t, err)
	assert.Equal(t, float64(1234567), values[0][0])
	err = openMetrics.Stop()
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
			" where db_name='open_metrics' and stable_name='test_metric'")
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	assert.NoError(t, err)
	if values[0][0].(int32) != 1000 {
		t.Fatal("ttl miss")
	}
}

func TestOpenMetricsMTls(t *testing.T) {
	tmpDir := t.TempDir()
	caCert, caKey := generateCertificate(true, nil, nil, "MyCA", []string{"MyCA Organization"})
	caCertFile := path.Join(tmpDir, "ca-cert.pem")
	caKeyFile := path.Join(tmpDir, "ca-key.pem")
	err := saveCertAndKey(caCertFile, caKeyFile, caCert, caKey)
	if err != nil {
		t.Fatal(err)
	}
	serverCert, serverKey := generateCertificate(false, caCert, caKey, "localhost", []string{"MyServer Organization"})
	serverCertFile := path.Join(tmpDir, "server-cert.pem")
	serverKeyFile := path.Join(tmpDir, "server-key.pem")
	err = saveCertAndKey(serverCertFile, serverKeyFile, serverCert, serverKey)
	if err != nil {
		t.Fatal(err)
	}
	clientCert, clientKey := generateCertificate(false, caCert, caKey, "Client", []string{"MyClient Organization"})
	clientCertFile := path.Join(tmpDir, "client-cert.pem")
	clientKeyFile := path.Join(tmpDir, "client-key.pem")
	err = saveCertAndKey(clientCertFile, clientKeyFile, clientCert, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AddCert(caCert)
	server := httptest.NewUnstartedServer(testHandler)
	serverTLSCert := tls.Certificate{
		Certificate: [][]byte{serverCert.Raw},
		PrivateKey:  serverKey,
		Leaf:        serverCert,
	}
	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverTLSCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caCertPool,
	}
	server.StartTLS()
	defer server.Close()
	tlsUrl := server.URL
	viper.Set("open_metrics.enable", true)
	viper.Set("open_metrics.urls", []string{tlsUrl + "/basicauth", tlsUrl + "/bearertoken"})
	viper.Set("open_metrics.gatherDurationSeconds", []int{1, 1})
	viper.Set("open_metrics.ttl", []int{1000, 1000})
	viper.Set("open_metrics.dbs", []string{"open_metrics_mtls_basicauth", "open_metrics_mtls_bearertoken"})
	viper.Set("open_metrics.responseTimeoutSeconds", []int{5, 5})
	viper.Set("open_metrics.httpUsernames", []string{"test_user", ""})
	viper.Set("open_metrics.httpPasswords", []string{"test_pass", ""})
	viper.Set("open_metrics.httpBearerTokenStrings", []string{"", "test_token"})
	viper.Set("open_metrics.caCertFiles", []string{caCertFile, caCertFile})
	viper.Set("open_metrics.certFiles", []string{clientCertFile, clientCertFile})
	viper.Set("open_metrics.keyFiles", []string{clientKeyFile, clientKeyFile})
	viper.Set("open_metrics.insecureSkipVerify", false)
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer func() {
		wrapper.TaosClose(conn)
	}()
	err = exec(conn, "create database if not exists open_metrics_mtls_basicauth precision 'ns'")
	assert.NoError(t, err)
	assert.NoError(t, testtools.EnsureDBCreated("open_metrics_mtls_basicauth"))
	err = exec(conn, "create database if not exists open_metrics_mtls_bearertoken precision 'ns'")
	assert.NoError(t, err)
	assert.NoError(t, testtools.EnsureDBCreated("open_metrics_mtls_bearertoken"))
	openMetrics := &OpenMetrics{}
	assert.Equal(t, "openmetrics", openMetrics.String())
	assert.Equal(t, "v1", openMetrics.Version())
	err = openMetrics.Init(nil)
	assert.NoError(t, err)
	err = openMetrics.Start()
	assert.NoError(t, err)
	var values [][]driver.Value
	assert.Eventually(t, func() bool {
		values, err = query(conn, "select * from information_schema.ins_tables where db_name='open_metrics_mtls_basicauth' and stable_name='test_metric'")
		return err == nil && len(values) == 1
	}, 10*time.Second, 500*time.Millisecond)
	assert.Eventually(t, func() bool {
		values, err = query(conn, "select * from information_schema.ins_tables where db_name='open_metrics_mtls_bearertoken' and stable_name='test_metric'")
		return err == nil && len(values) == 1
	}, 10*time.Second, 500*time.Millisecond)
	require.Equal(t, 1, len(values))
	values, err = query(conn, "select last(`gauge`) as `gauge` from open_metrics_mtls_basicauth.test_metric;")
	assert.NoError(t, err)
	assert.Equal(t, float64(1), values[0][0])
	values, err = query(conn, "select last(`gauge`) as `gauge` from open_metrics_mtls_bearertoken.test_metric;")
	assert.NoError(t, err)
	assert.Equal(t, float64(1), values[0][0])
	err = openMetrics.Stop()
	assert.NoError(t, err)
	for i := 0; i < 10; i++ {
		values, err = query(conn, "select `ttl` from information_schema.ins_tables "+
			" where db_name='open_metrics_mtls_basicauth' and stable_name='test_metric'")
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	assert.NoError(t, err)
	if values[0][0].(int32) != 1000 {
		t.Fatal("ttl miss")
	}
	err = exec(conn, "drop database if exists open_metrics_mtls_basicauth")
	assert.NoError(t, err)
	err = exec(conn, "drop database if exists open_metrics_mtls_bearertoken")
	assert.NoError(t, err)
}

func generateCertificate(isCA bool, parentCert *x509.Certificate, parentKey *rsa.PrivateKey,
	commonName string, organization []string) (*x509.Certificate, *rsa.PrivateKey) {

	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)

	serialNumber, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: organization,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	if isCA {
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
	}

	var certBytes []byte
	var err error
	if isCA {
		certBytes, err = x509.CreateCertificate(rand.Reader, &template, &template, &privKey.PublicKey, privKey)
	} else {
		certBytes, err = x509.CreateCertificate(rand.Reader, &template, parentCert, &privKey.PublicKey, parentKey)
	}
	if err != nil {
		panic(err)
	}

	cert, _ := x509.ParseCertificate(certBytes)
	return cert, privKey
}

func saveCertAndKey(certFile, keyFile string, cert *x509.Certificate, key *rsa.PrivateKey) error {
	certOut, _ := os.Create(certFile)
	err := pem.Encode(certOut, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Raw,
	})
	if err != nil {
		return err
	}
	err = certOut.Close()
	if err != nil {
		return err
	}

	keyOut, _ := os.Create(keyFile)
	err = pem.Encode(keyOut, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	if err != nil {
		return err
	}
	return keyOut.Close()
}

func TestOpenMetricsDisabled(t *testing.T) {
	viper.Set("open_metrics.enable", false)
	openMetrics := &OpenMetrics{}
	err := openMetrics.Init(nil)
	assert.NoError(t, err)
	err = openMetrics.Start()
	assert.NoError(t, err)
	err = openMetrics.Stop()
	assert.NoError(t, err)
}

func exec(conn unsafe.Pointer, sql string) error {
	logger := log.GetLogger("test")
	logger.Debugf("exec sql %s", sql)
	return testtools.Exec(conn, sql)
}

func query(conn unsafe.Pointer, sql string) ([][]driver.Value, error) {
	logger := log.GetLogger("test")
	logger.Debugf("query sql %s", sql)
	return testtools.Query(conn, sql)
}
