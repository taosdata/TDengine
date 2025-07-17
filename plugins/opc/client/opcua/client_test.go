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
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestUAClient_GetAllPoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.UaConnectConfig{
		Endpoint:       "opc.tcp://127.0.0.1:4840",
		ConnectTimeout: 10,
		RequestTimeout: 10,
		SecurityPolicy: "None",
		SecurityMode:   "None",
		AuthMethod:     "anonymous",
	}

	client, err := NewUAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
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
	t.Log(points)
	assert.Equal(t, 3, len(points))
}

func TestUAClient_GetAllPointsNamespaces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.UaConnectConfig{
		Endpoint:       "opc.tcp://127.0.0.1:4840",
		ConnectTimeout: 10,
		RequestTimeout: 10,
		SecurityPolicy: "None",
		SecurityMode:   "None",
		AuthMethod:     "anonymous",
	}

	client, err := NewUAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
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
			Root:       "i=85",
			Namespaces: []uint16{3},
		},
	}
	points, err := client.GetAllPoints(pointsConf)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(points))
	assert.Equal(t, "ns=3;i=1001", points[0].ID)
}

func TestUAClient_Collect_Observer(t *testing.T) {
	tmpDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.UaConnectConfig{
		Endpoint:       "opc.tcp://127.0.0.1:4840",
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
				{"ns=2;i=1001"},
				{"ns=2;i=1002"},
				{"ns=2;i=1003"},
			},
		},
	}

	gotMessage := false
	var onMessage = func(message []*common.NodeValue) {
		gotMessage = true
	}
	client, err := NewUAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	err = client.Collect(collectConfig, onMessage)
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
		Endpoint:       "opc.tcp://127.0.0.1:4840",
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
				{"ns=2;i=1001"},
				{"ns=2;i=1002"},
				{"ns=2;i=1003"},
			},
		},
	}

	gotMessage := false
	var onMessage = func(message []*common.NodeValue) {
		gotMessage = true
	}
	client, err := NewUAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	err = client.Collect(collectConfig, onMessage)
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

func TestTryGetCapabilities(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.UaConnectConfig{
		Endpoint:       "opc.tcp://127.0.0.1:50000",
		ConnectTimeout: 10,
		RequestTimeout: 10,
		SecurityPolicy: "None",
		SecurityMode:   "None",
		AuthMethod:     "anonymous",
	}

	client, err := NewUAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		t.Log("close client")
		client.Close()
		t.Log("close client finish")
	}()

	pointsConf := config.PointsConfig{
		Limit: 0,
		Regex: ".*",
		Ua: config.UaPointsConfig{
			Root: "i=85",
		},
	}
	points, err := client.GetAllPoints(pointsConf)
	t.Log(len(points))
	assert.Greater(t, len(points), 10000)

	nodes := make([]config.NodeConfig, len(points))
	for i := 0; i < len(points); i++ {
		nodes[i] = config.NodeConfig{ID: points[i].ID}
	}
	tmpDir := t.TempDir()
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
			Nodes:       nodes,
		},
	}

	gotMessage := false
	var onMessage = func(message []*common.NodeValue) {
		gotMessage = true
	}
	client2, err := NewUAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
	if err != nil {
		t.Fatal(err)
	}
	err = client2.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		t.Log("close client 2")
		client2.Close()
		t.Log("close client 2 finish")
	}()
	err = client2.Collect(collectConfig, onMessage)
	assert.NoError(t, err)
	time.Sleep(time.Second * 2)
	files, err := findFilesWithPrefix(tmpDir, "opc_data.dump")
	assert.NoError(t, err)
	assert.Len(t, files, 1)
	data, err := os.ReadFile(files[0])
	assert.NoError(t, err)
	assert.NotEmpty(t, data)
	assert.True(t, gotMessage)
}

func TestGetPointsInorder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.UaConnectConfig{
		Endpoint:       "opc.tcp://127.0.0.1:50000",
		ConnectTimeout: 10,
		RequestTimeout: 10,
		SecurityPolicy: "None",
		SecurityMode:   "None",
		AuthMethod:     "anonymous",
	}

	client, err := NewUAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		t.Log("close client")
		client.Close()
		t.Log("close client finish")
	}()

	pointsConf := config.PointsConfig{
		Limit: 500,
		Regex: ".*",
		Ua: config.UaPointsConfig{
			Root: "i=85",
		},
	}
	points, err := client.GetAllPoints(pointsConf)
	assert.NoError(t, err)
	assert.Equal(t, 500, len(points))

	nodes := make([]config.NodeConfig, len(points))
	for i := 0; i < len(points); i++ {
		nodes[i] = config.NodeConfig{ID: points[i].ID}
	}
	nextPoints, err := client.GetAllPoints(pointsConf)
	assert.NoError(t, err)
	assert.Equal(t, points, nextPoints)
}

func TestGetPointsRegexp(t *testing.T) {
	type args struct {
		conf config.PointsConfig
	}

	tests := []struct {
		name    string
		args    args
		want    []common.Point
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "wrong reg",
			args: args{
				conf: config.PointsConfig{
					Limit: 0,
					Regex: "(\\())",
				},
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "wrong name reg",
			args: args{
				conf: config.PointsConfig{
					Limit:     0,
					RegexName: "(\\())",
				},
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "wrong id reg",
			args: args{
				conf: config.PointsConfig{
					Limit:   0,
					RegexID: "(\\())",
				},
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "check with name and id",
			args: args{
				conf: config.PointsConfig{
					Limit: 0,
					Regex: ".*",

					RegexID:   `ns=2;.*`,
					RegexName: `.*int32`,
					Ua:        config.UaPointsConfig{Root: "i=85"},
				},
			},
			want: []common.Point{
				{
					ID:          "ns=2;i=1001",
					Name:        "int32",
					Description: "int32",
				},
			},
			wantErr: assert.NoError,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.UaConnectConfig{
		Endpoint:       "opc.tcp://127.0.0.1:4840",
		ConnectTimeout: 10,
		RequestTimeout: 10,
		SecurityPolicy: "None",
		SecurityMode:   "None",
		AuthMethod:     "anonymous",
	}

	client, err := NewUAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		t.Log("close client")
		client.Close()
		t.Log("close client finish")
	}()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.GetAllPoints(tt.args.conf)
			if !tt.wantErr(t, err, fmt.Sprintf("GetAllPoints(%v)", tt.args.conf)) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetAllPoints(%v)", tt.args.conf)
		})
	}
}

func TestChangeCollectConfigObs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.UaConnectConfig{
		Endpoint:       "opc.tcp://127.0.0.1:4840",
		ConnectTimeout: 10,
		RequestTimeout: 10,
		SecurityPolicy: "None",
		SecurityMode:   "None",
		AuthMethod:     "anonymous",
	}
	collectConfig := config.CollectConfig{
		Interval:    1,
		ContainsBad: true,
		Ua: config.UaCollectConfig{
			CollectMode: "observe",
			Nodes: []config.NodeConfig{
				{"ns=2;i=1001"},
				{"ns=2;i=1002"},
				{"ns=2;i=1003"},
			},
		},
	}
	expectNodes := map[string]bool{
		"ns=2;i=1001": true,
		"ns=2;i=1002": true,
		"ns=2;i=1003": true,
	}
	lock := sync.Mutex{}
	expectGotNodes := map[string]struct{}{
		"ns=2;i=1001": {},
		"ns=2;i=1002": {},
		"ns=2;i=1003": {},
	}
	var onMessage = func(message []*common.NodeValue) {
		for _, m := range message {
			t.Log(m.IDStr)
			lock.Lock()
			if !expectNodes[m.IDStr] {
				t.Fatal("unexpected node", m.IDStr)
			}
			delete(expectGotNodes, m.IDStr)
			lock.Unlock()
		}
	}
	client, err := NewUAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	err = client.Collect(collectConfig, onMessage)
	assert.NoError(t, err)
	time.Sleep(time.Second * 3)
	lock.Lock()
	if len(expectGotNodes) != 0 {
		t.Fatal("not all nodes got")
	}
	lock.Unlock()
	newCollectConfig := config.CollectConfig{
		Interval:    1,
		ContainsBad: true,
		Ua: config.UaCollectConfig{
			CollectMode: "observe",
			Nodes: []config.NodeConfig{
				{"ns=2;i=1001"},
				{"ns=2;i=1002"},
			},
		},
	}
	client.ChangeCollectConfig(newCollectConfig)
	lock.Lock()
	expectGotNodes = map[string]struct{}{
		"ns=2;i=1001": {},
		"ns=2;i=1002": {},
	}
	expectNodes["ns=2;i=1003"] = false
	lock.Unlock()
	time.Sleep(time.Second * 3)
	lock.Lock()
	if len(expectGotNodes) != 0 {
		t.Fatal("not all nodes got")
	}
	lock.Unlock()
	newCollectConfig = config.CollectConfig{
		Interval:    1,
		ContainsBad: true,
		Ua: config.UaCollectConfig{
			CollectMode: "observe",
			Nodes: []config.NodeConfig{
				{"ns=2;i=1001"},
			},
		},
	}
	client.ChangeCollectConfig(newCollectConfig)
	lock.Lock()
	expectGotNodes = map[string]struct{}{
		"ns=2;i=1001": {},
	}
	expectNodes["ns=2;i=1002"] = false
	lock.Unlock()
	time.Sleep(time.Second * 3)
	lock.Lock()
	if len(expectGotNodes) != 0 {
		t.Fatal("not all nodes got")
	}
	lock.Unlock()
	newCollectConfig = config.CollectConfig{
		Interval:    1,
		ContainsBad: true,
		Ua: config.UaCollectConfig{
			CollectMode: "observe",
			Nodes: []config.NodeConfig{
				{"ns=2;i=1001"},
				{"ns=2;i=1003"},
			},
		},
	}
	lock.Lock()
	expectGotNodes = map[string]struct{}{
		"ns=2;i=1001": {},
		"ns=2;i=1003": {},
	}
	expectNodes["ns=2;i=1003"] = true
	lock.Unlock()
	client.ChangeCollectConfig(newCollectConfig)
	time.Sleep(time.Second * 3)
	lock.Lock()
	if len(expectGotNodes) != 0 {
		t.Fatal("not all nodes got")
	}
	lock.Unlock()
	newCollectConfig = config.CollectConfig{
		Interval:    1,
		ContainsBad: true,
		Ua: config.UaCollectConfig{
			CollectMode: "observe",
			Nodes: []config.NodeConfig{
				{"ns=2;i=1001"},
				{"ns=2;i=1002"},
				{"ns=2;i=1003"},
			},
		},
	}
	lock.Lock()
	expectGotNodes = map[string]struct{}{
		"ns=2;i=1001": {},
		"ns=2;i=1002": {},
		"ns=2;i=1003": {},
	}
	expectNodes["ns=2;i=1002"] = true
	expectNodes["ns=2;i=1003"] = true
	lock.Unlock()
	client.ChangeCollectConfig(newCollectConfig)
	time.Sleep(time.Second * 3)
	lock.Lock()
	if len(expectGotNodes) != 0 {
		t.Fatal("not all nodes got")
	}
	lock.Unlock()
}

// func TestChangeCollectConfigSub(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
// 	connectConfig := config.UaConnectConfig{
// 		Endpoint:       "opc.tcp://127.0.0.1:4840",
// 		ConnectTimeout: 10,
// 		RequestTimeout: 10,
// 		SecurityPolicy: "None",
// 		SecurityMode:   "None",
// 		AuthMethod:     "anonymous",
// 	}
// 	collectConfig := config.CollectConfig{
// 		ContainsBad: true,
// 		Ua: config.UaCollectConfig{
// 			CollectMode: "subscribe",
// 			Nodes: []config.NodeConfig{
// 				{"ns=2;i=1001"},
// 				{"ns=2;i=1002"},
// 				{"ns=2;i=1003"},
// 			},
// 		},
// 	}
// 	expectNodes := map[string]bool{
// 		"ns=2;i=1001": true,
// 		"ns=2;i=1002": true,
// 		"ns=2;i=1003": true,
// 	}
// 	lock := sync.Mutex{}
// 	expectGotNodes := map[string]struct{}{
// 		"ns=2;i=1001": {},
// 		"ns=2;i=1002": {},
// 		"ns=2;i=1003": {},
// 	}
// 	var onMessage = func(message []*common.NodeValue) {
// 		for _, m := range message {
// 			t.Log(m.IDStr)
// 			lock.Lock()
// 			if !expectNodes[m.IDStr] {
// 				t.Fatal("unexpected node", m.IDStr)
// 			}
// 			delete(expectGotNodes, m.IDStr)
// 			lock.Unlock()
// 		}
// 	}
// 	client, err := NewUAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	err = client.Connect()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer client.Close()
// 	err = client.Collect(collectConfig, onMessage)
// 	assert.NoError(t, err)
// 	time.Sleep(time.Second * 3)
// 	lock.Lock()
// 	if len(expectGotNodes) != 0 {
// 		t.Fatal("not all nodes got")
// 	}
// 	lock.Unlock()
// 	newCollectConfig := config.CollectConfig{
// 		Interval:    1,
// 		ContainsBad: true,
// 		Ua: config.UaCollectConfig{
// 			CollectMode: "subscribe",
// 			Nodes: []config.NodeConfig{
// 				{"ns=2;i=1001"},
// 				{"ns=2;i=1002"},
// 			},
// 		},
// 	}
// 	client.ChangeCollectConfig(newCollectConfig)
// 	lock.Lock()
// 	expectGotNodes = map[string]struct{}{
// 		"ns=2;i=1001": {},
// 		"ns=2;i=1002": {},
// 	}
// 	expectNodes["ns=2;i=1003"] = false
// 	lock.Unlock()
// 	time.Sleep(time.Second * 3)
// 	lock.Lock()
// 	if len(expectGotNodes) != 0 {
// 		t.Fatal("not all nodes got")
// 	}
// 	lock.Unlock()
// 	newCollectConfig = config.CollectConfig{
// 		Interval:    1,
// 		ContainsBad: true,
// 		Ua: config.UaCollectConfig{
// 			CollectMode: "subscribe",
// 			Nodes: []config.NodeConfig{
// 				{"ns=2;i=1001"},
// 			},
// 		},
// 	}
// 	client.ChangeCollectConfig(newCollectConfig)
// 	lock.Lock()
// 	expectGotNodes = map[string]struct{}{
// 		"ns=2;i=1001": {},
// 	}
// 	expectNodes["ns=2;i=1002"] = false
// 	lock.Unlock()
// 	time.Sleep(time.Second * 3)
// 	lock.Lock()
// 	if len(expectGotNodes) != 0 {
// 		t.Fatal("not all nodes got")
// 	}
// 	lock.Unlock()
// 	newCollectConfig = config.CollectConfig{
// 		Interval:    1,
// 		ContainsBad: true,
// 		Ua: config.UaCollectConfig{
// 			CollectMode: "subscribe",
// 			Nodes: []config.NodeConfig{
// 				{"ns=2;i=1001"},
// 				{"ns=2;i=1003"},
// 			},
// 		},
// 	}
// 	lock.Lock()
// 	expectGotNodes = map[string]struct{}{
// 		"ns=2;i=1001": {},
// 		"ns=2;i=1003": {},
// 	}
// 	expectNodes["ns=2;i=1003"] = true
// 	lock.Unlock()
// 	client.ChangeCollectConfig(newCollectConfig)
// 	time.Sleep(time.Second * 3)
// 	lock.Lock()
// 	if len(expectGotNodes) != 0 {
// 		t.Fatal("not all nodes got")
// 	}
// 	lock.Unlock()
// 	newCollectConfig = config.CollectConfig{
// 		Interval:    1,
// 		ContainsBad: true,
// 		Ua: config.UaCollectConfig{
// 			CollectMode: "subscribe",
// 			Nodes: []config.NodeConfig{
// 				{"ns=2;i=1001"},
// 				{"ns=2;i=1002"},
// 				{"ns=2;i=1003"},
// 			},
// 		},
// 	}
// 	lock.Lock()
// 	expectGotNodes = map[string]struct{}{
// 		"ns=2;i=1001": {},
// 		"ns=2;i=1002": {},
// 		"ns=2;i=1003": {},
// 	}
// 	expectNodes["ns=2;i=1002"] = true
// 	expectNodes["ns=2;i=1003"] = true
// 	lock.Unlock()
// 	client.ChangeCollectConfig(newCollectConfig)
// 	time.Sleep(time.Second * 3)
// 	lock.Lock()
// 	if len(expectGotNodes) != 0 {
// 		t.Fatal("not all nodes got")
// 	}
// 	lock.Unlock()
// }

func TestReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.UaConnectConfig{
		Endpoint:       "opc.tcp://127.0.0.1:4840",
		ConnectTimeout: 10,
		RequestTimeout: 10,
		SecurityPolicy: "None",
		SecurityMode:   "None",
		AuthMethod:     "anonymous",
	}

	client, err := NewUAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	oldConn := client.conn
	client.reconnect(oldConn, nil)
	assert.Equal(t, oldConn, client.conn)
	client.reconnect(oldConn, fmt.Errorf("test"))
	assert.Equal(t, oldConn, client.conn)
	client.reconnect(oldConn, &net.OpError{Op: "wsasend", Net: "tcp", Source: nil, Addr: nil, Err: nil})
	assert.NotEqual(t, oldConn, client.conn)
	tmpDir := t.TempDir()
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
				{"ns=2;i=1001"},
				{"ns=2;i=1002"},
				{"ns=2;i=1003"},
			},
		},
	}
	gotMessage := false
	var onMessage = func(message []*common.NodeValue) {
		gotMessage = true
	}
	err = client.Collect(collectConfig, onMessage)
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
