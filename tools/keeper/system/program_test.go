package system

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/kardianos/service"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/util"
)

func TestInit(t *testing.T) {
	server := Init()
	assert.NotNil(t, server)

	conn, err := db.NewConnectorWithDb(config.Conf.TDengine.Username, config.Conf.TDengine.Password, config.Conf.TDengine.Host, config.Conf.TDengine.Port, config.Conf.Metrics.Database.Name, config.Conf.TDengine.Usessl)
	assert.NoError(t, err)
	defer conn.Close()

	_, err = conn.Query(context.Background(), fmt.Sprintf("drop database if exists %s", config.Conf.Metrics.Database.Name), util.GetQidOwn(config.Conf.InstanceID))
	assert.NoError(t, err)
}

func Test_program(t *testing.T) {
	server := &http.Server{}
	prg := newProgram(server)
	svcConfig := &service.Config{
		Name:        "taoskeeper",
		DisplayName: "taoskeeper",
		Description: "taosKeeper is a tool for TDengine that exports monitoring metrics",
	}
	svc, err := service.New(prg, svcConfig)
	assert.NoError(t, err)

	err = prg.Start(svc)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	err = prg.Stop(svc)
	assert.NoError(t, err)
}

func Test_program_Start_HTTP(t *testing.T) {
	if config.Conf == nil {
		config.InitConfig()
	}

	server := &http.Server{
		Addr: "127.0.0.1:6662",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/check_health" {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}),
	}
	prg := newProgram(server)

	svcConfig := &service.Config{
		Name:        "taoskeeper",
		DisplayName: "taoskeeper",
		Description: "taosKeeper is a tool for TDengine that exports monitoring metrics",
	}
	svc, err := service.New(prg, svcConfig)
	assert.NoError(t, err)

	err = prg.Start(svc)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + server.Addr + "/check_health")
	assert.NoError(t, err)
	if err == nil {
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	server.Shutdown(context.Background())
}

func Test_program_Start_HTTPS(t *testing.T) {
	certFile := "test_cert.pem"
	keyFile := "test_key.pem"
	defer os.Remove(certFile)
	defer os.Remove(keyFile)

	err := generateTestCert(certFile, keyFile)
	assert.NoError(t, err)

	if config.Conf == nil {
		config.InitConfig()
	}

	origConf := config.Conf
	defer func() { config.Conf = origConf }()

	config.Conf.SSL.Enable = true
	config.Conf.SSL.CertFile = certFile
	config.Conf.SSL.KeyFile = keyFile

	server := &http.Server{
		Addr: "127.0.0.1:6663",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/check_health" {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}),
	}
	prg := newProgram(server)

	svcConfig := &service.Config{
		Name:        "taoskeeper",
		DisplayName: "taoskeeper",
		Description: "taosKeeper is a tool for TDengine that exports monitoring metrics",
	}
	svc, err := service.New(prg, svcConfig)
	assert.NoError(t, err)

	err = prg.Start(svc)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Get("https://" + server.Addr + "/check_health")
	assert.NoError(t, err)
	if err == nil {
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	server.Shutdown(context.Background())
}

func generateTestCert(certFile, keyFile string) error {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return err
	}

	certOut, err := os.Create(certFile)
	if err != nil {
		return err
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return err
	}

	keyOut, err := os.Create(keyFile)
	if err != nil {
		return err
	}
	defer keyOut.Close()
	privBytes := x509.MarshalPKCS1PrivateKey(priv)
	if err := pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: privBytes}); err != nil {
		return err
	}
	return nil
}
