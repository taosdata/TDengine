package opcua

import (
	"collector/common"
	"collector/config"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestUAClient_GetAllPoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.UaConnectConfig{
		Endpoint:       "opc.tcp://max:53530/OPCUA/SimulationServer",
		ConnectTimeout: 10,
		RequestTimeout: 10,
		SecurityPolicy: "None",
		SecurityMode:   "None",
		AuthMethod:     "anonymous",
	}

	client, err := NewUAClient(ctx, connectConfig, config.CollectConfig{}, 1, logrus.New().WithField("test", "test"), nil)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	pointsConf := config.PointsConfig{
		Limit: 0,
		Regex: ".*",
		Ua: config.UaPointsConfig{
			Root: "i=85",
		},
	}
	points, err := client.GetAllPoints(pointsConf)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(t, points)
	pointsConf = config.PointsConfig{
		Limit: 3,
		Regex: ".*",
		Ua: config.UaPointsConfig{
			Root: "i=85",
		},
	}
	points, err = client.GetAllPoints(pointsConf)
	assert.Equal(t, 3, len(points))
}

func TestUAClient_Collect_Observer(t *testing.T) {
	tmpDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.UaConnectConfig{
		Endpoint:       "opc.tcp://max:53530/OPCUA/SimulationServer",
		ConnectTimeout: 10,
		RequestTimeout: 10,
		SecurityPolicy: "None",
		SecurityMode:   "None",
		AuthMethod:     "anonymous",
	}
	collectConfig := config.CollectConfig{
		Interval:    1,
		ContainsBad: true,
		Dump: config.DumpConfig{
			Enable: true,
			Path:   tmpDir,
			Keep:   1,
		},
		Ua: config.UaCollectConfig{
			CollectMode: "observe",
			Nodes: []config.NodeConfig{
				{"ns=3;i=1001"},
				{"ns=3;i=1002"},
				{"ns=3;i=1003"},
			},
		},
	}

	gotMessage := false
	var onMessage = func(message []*common.NodeValue) {
		gotMessage = true
	}
	client, err := NewUAClient(ctx, connectConfig, collectConfig, 1, logrus.New().WithField("test", "test"), onMessage)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	err = client.Collect()
	assert.NoError(t, err)
	time.Sleep(time.Second * 2)
	files, err := findFilesWithPrefix(tmpDir, "opc_data.dump")
	assert.NoError(t, err)
	assert.Len(t, files, 1)
	data, err := os.ReadFile(files[0])
	assert.NoError(t, err)
	assert.NotEmpty(t, data)
	t.Log(string(data))
	assert.True(t, gotMessage)
}

func TestUAClient_Collect_Subscribe(t *testing.T) {
	tmpDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.UaConnectConfig{
		Endpoint:       "opc.tcp://max:53530/OPCUA/SimulationServer",
		ConnectTimeout: 10,
		RequestTimeout: 10,
		SecurityPolicy: "None",
		SecurityMode:   "None",
		AuthMethod:     "anonymous",
	}
	collectConfig := config.CollectConfig{
		Interval:    1,
		ContainsBad: true,
		Dump: config.DumpConfig{
			Enable: true,
			Path:   tmpDir,
			Keep:   1,
		},
		Ua: config.UaCollectConfig{
			CollectMode: "subscribe",
			Nodes: []config.NodeConfig{
				{"ns=3;i=1001"},
				{"ns=3;i=1002"},
				{"ns=3;i=1003"},
			},
		},
	}

	gotMessage := false
	var onMessage = func(message []*common.NodeValue) {
		gotMessage = true
	}
	client, err := NewUAClient(ctx, connectConfig, collectConfig, 1, logrus.New().WithField("test", "test"), onMessage)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	err = client.Collect()
	assert.NoError(t, err)
	time.Sleep(time.Second * 2)
	files, err := findFilesWithPrefix(tmpDir, "opc_data.dump")
	assert.NoError(t, err)
	assert.Len(t, files, 1)
	data, err := os.ReadFile(files[0])
	assert.NoError(t, err)
	assert.NotEmpty(t, data)
	t.Log(string(data))
	assert.True(t, gotMessage)
}

func findFilesWithPrefix(rootPath, prefix string) ([]string, error) {
	var matchingFiles []string

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			fileName := info.Name()
			if strings.HasPrefix(fileName, prefix) {
				matchingFiles = append(matchingFiles, path)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return matchingFiles, nil
}

func TestTLSOpts(t *testing.T) {
	tmp := t.TempDir()
	certFile := path.Join(tmp, "test_cert.pem")
	keyFile := path.Join(tmp, "test_key.pem")
	err := createTestCertAndKey(certFile, keyFile)
	if err != nil {
		t.Fatal("Failed to create test certificate and key:", err)
	}
	defer cleanupTestFiles(certFile, keyFile)

	certOpt, keyOpt, err := tlsOpts(certFile, keyFile)
	if err != nil {
		t.Fatalf("tlsOpts returned an error: %v", err)
	}

	t.Logf("Certificate and private key options: %v, %v", certOpt, keyOpt)
}

func createTestCertAndKey(certFile, keyFile string) error {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour)

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"Test"}},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return err
	}

	certFileContent := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyFileContent := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	err = ioutil.WriteFile(certFile, certFileContent, 0644)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(keyFile, keyFileContent, 0644)
	if err != nil {
		return err
	}

	return nil
}

func cleanupTestFiles(certFile, keyFile string) {
	os.Remove(certFile)
	os.Remove(keyFile)
}
