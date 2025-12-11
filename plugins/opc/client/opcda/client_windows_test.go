//go:build windows
// +build windows

package opcda

import (
	"collector/client"
	"collector/common"
	"collector/config"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connConf := config.DaConnectConfig{
		Server: "Graybox.Simulator.1",
		Nodes:  []string{"localhost"},
	}

	c, err := NewDAClient(ctx, connConf, 0, logrus.New().WithField("test", "test"))
	assert.NoError(t, err)
	err = c.Connect()
	assert.NoError(t, err)
	err = c.Close()
	assert.NoError(t, err)
	connConf = config.DaConnectConfig{
		Server: "Graybox.Simulator.2",
		Nodes:  []string{"localhost"},
	}
	c2, err := NewDAClient(ctx, connConf, 0, logrus.New().WithField("test", "test"))
	assert.NoError(t, err)
	err = c2.Connect()
	assert.Error(t, err)
}

func TestDAClient_GetAllPoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connConf := config.DaConnectConfig{
		Server: "Graybox.Simulator.1",
		Nodes:  []string{"localhost"},
	}
	c, err := NewDAClient(ctx, connConf, 0, logrus.New().WithField("test", "test"))
	assert.NoError(t, err)
	err = c.Connect()
	assert.NoError(t, err)
	points, err := c.GetAllPoints(config.PointsConfig{})
	assert.NoError(t, err)
	assert.NotNil(t, points)
	err = c.Close()
	assert.NoError(t, err)
}

func TestDAClient_GetAllPoints1(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connConf := config.DaConnectConfig{
		Server: "Graybox.Simulator.1",
		Nodes:  []string{"localhost"},
	}
	c, err := NewDAClient(ctx, connConf, 0, logrus.New().WithField("test", "test"))
	assert.NoError(t, err)
	// not connect error
	_, err = c.GetAllPoints(config.PointsConfig{})
	assert.Error(t, err)
	err = c.Connect()
	assert.NoError(t, err)
	defer c.Close()
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
			name: "1",
			args: args{
				conf: config.PointsConfig{
					Limit: 1,
				},
			},
			want: []common.Point{
				{
					ID: "bandwidth", Name: "bandwidth",
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "all",
			args: args{
				conf: config.PointsConfig{
					Limit: 0,
				},
			},
			want: []common.Point{

				{
					ID:   "bandwidth",
					Name: "bandwidth",
				},
				{
					ID:   "options.sawfreq",
					Name: "sawfreq",
				},
				{
					ID:   "options.sinfreq",
					Name: "sinfreq",
				},
				{
					ID:   "options.trianglefreq",
					Name: "trianglefreq",
				},
				{
					ID:   "options.sqaurefreq",
					Name: "sqaurefreq",
				},
				{
					ID:   "textual.color",
					Name: "color",
				},
				{
					ID:   "textual.number",
					Name: "number",
				},
				{
					ID:   "textual.random",
					Name: "random",
				},
				{
					ID:   "textual.weekday",
					Name: "weekday",
				},
				{
					ID:   "time.current",
					Name: "current",
				},
				{
					ID:   "time.random",
					Name: "random",
				},
				{
					ID:   "enum.color",
					Name: "color",
				},
				{
					ID:   "enum.number",
					Name: "number",
				},
				{
					ID:   "enum.weekday",
					Name: "weekday",
				},
				{
					ID:   "numeric.saw.uint8",
					Name: "uint8",
				},
				{
					ID:   "numeric.saw.int8",
					Name: "int8",
				},
				{
					ID:   "numeric.saw.uint16",
					Name: "uint16",
				},
				{
					ID:   "numeric.saw.int16",
					Name: "int16",
				},
				{
					ID:   "numeric.saw.uint32",
					Name: "uint32",
				},
				{
					ID:   "numeric.saw.int32",
					Name: "int32",
				},
				{
					ID:   "numeric.saw.uint64",
					Name: "uint64",
				},
				{
					ID:   "numeric.saw.int64",
					Name: "int64",
				},
				{
					ID:   "numeric.saw.float",
					Name: "float",
				},
				{
					ID:   "numeric.saw.double",
					Name: "double",
				},
				{
					ID:   "numeric.sin.uint8",
					Name: "uint8",
				},
				{
					ID:   "numeric.sin.int8",
					Name: "int8",
				},
				{
					ID:   "numeric.sin.uint16",
					Name: "uint16",
				},
				{
					ID:   "numeric.sin.int16",
					Name: "int16",
				},
				{
					ID:   "numeric.sin.uint32",
					Name: "uint32",
				},
				{
					ID:   "numeric.sin.int32",
					Name: "int32",
				},
				{
					ID:   "numeric.sin.uint64",
					Name: "uint64",
				},
				{
					ID:   "numeric.sin.int64",
					Name: "int64",
				},
				{
					ID:   "numeric.sin.float",
					Name: "float",
				},
				{
					ID:   "numeric.sin.double",
					Name: "double",
				},
				{
					ID:   "numeric.triangle.uint8",
					Name: "uint8",
				},
				{
					ID:   "numeric.triangle.int8",
					Name: "int8",
				},
				{
					ID:   "numeric.triangle.uint16",
					Name: "uint16",
				},
				{
					ID:   "numeric.triangle.int16",
					Name: "int16",
				},
				{
					ID:   "numeric.triangle.uint32",
					Name: "uint32",
				},
				{
					ID:   "numeric.triangle.int32",
					Name: "int32",
				},
				{
					ID:   "numeric.triangle.uint64",
					Name: "uint64",
				},
				{
					ID:   "numeric.triangle.int64",
					Name: "int64",
				},
				{
					ID:   "numeric.triangle.float",
					Name: "float",
				},
				{
					ID:   "numeric.triangle.double",
					Name: "double",
				},
				{
					ID:   "numeric.square.uint8",
					Name: "uint8",
				},
				{
					ID:   "numeric.square.int8",
					Name: "int8",
				},
				{
					ID:   "numeric.square.uint16",
					Name: "uint16",
				},
				{
					ID:   "numeric.square.int16",
					Name: "int16",
				},
				{
					ID:   "numeric.square.uint32",
					Name: "uint32",
				},
				{
					ID:   "numeric.square.int32",
					Name: "int32",
				},
				{
					ID:   "numeric.square.uint64",
					Name: "uint64",
				},
				{
					ID:   "numeric.square.int64",
					Name: "int64",
				},
				{
					ID:   "numeric.square.float",
					Name: "float",
				},
				{
					ID:   "numeric.square.double",
					Name: "double",
				},
				{
					ID:   "numeric.square.bool",
					Name: "bool",
				},
				{
					ID:   "numeric.random.uint8",
					Name: "uint8",
				},
				{
					ID:   "numeric.random.int8",
					Name: "int8",
				},
				{
					ID:   "numeric.random.uint16",
					Name: "uint16",
				},
				{
					ID:   "numeric.random.int16",
					Name: "int16",
				},
				{
					ID:   "numeric.random.uint32",
					Name: "uint32",
				},
				{
					ID:   "numeric.random.int32",
					Name: "int32",
				},
				{
					ID:   "numeric.random.uint64",
					Name: "uint64",
				},
				{
					ID:   "numeric.random.int64",
					Name: "int64",
				},
				{
					ID:   "numeric.random.float",
					Name: "float",
				},
				{
					ID:   "numeric.random.double",
					Name: "double",
				},
				{
					ID:   "numeric.random.bool",
					Name: "bool",
				},
				{
					ID:   "storage.numeric.reg01",
					Name: "reg01",
				},
				{
					ID:   "storage.numeric.reg02",
					Name: "reg02",
				},
				{
					ID:   "storage.numeric.reg03",
					Name: "reg03",
				},
				{
					ID:   "storage.numeric.reg04",
					Name: "reg04",
				},
				{
					ID:   "storage.numeric.reg05",
					Name: "reg05",
				},
				{
					ID:   "storage.numeric.reg06",
					Name: "reg06",
				},
				{
					ID:   "storage.numeric.reg07",
					Name: "reg07",
				},
				{
					ID:   "storage.numeric.reg08",
					Name: "reg08",
				},
				{
					ID:   "storage.numeric.reg09",
					Name: "reg09",
				},
				{
					ID:   "storage.numeric.reg10",
					Name: "reg10",
				},
				{
					ID:   "storage.numeric.reg11",
					Name: "reg11",
				},
				{
					ID:   "storage.numeric.reg12",
					Name: "reg12",
				},
				{
					ID:   "storage.numeric.reg13",
					Name: "reg13",
				},
				{
					ID:   "storage.numeric.reg14",
					Name: "reg14",
				},
				{
					ID:   "storage.numeric.reg15",
					Name: "reg15",
				},
				{
					ID:   "storage.numeric.reg16",
					Name: "reg16",
				},
				{
					ID:   "storage.numeric.reg17",
					Name: "reg17",
				},
				{
					ID:   "storage.numeric.reg18",
					Name: "reg18",
				},
				{
					ID:   "storage.numeric.reg19",
					Name: "reg19",
				},
				{
					ID:   "storage.numeric.reg20",
					Name: "reg20",
				},
				{
					ID:   "storage.bool.reg01",
					Name: "reg01",
				},
				{
					ID:   "storage.bool.reg02",
					Name: "reg02",
				},
				{
					ID:   "storage.bool.reg03",
					Name: "reg03",
				},
				{
					ID:   "storage.bool.reg04",
					Name: "reg04",
				},
				{
					ID:   "storage.bool.reg05",
					Name: "reg05",
				},
				{
					ID:   "storage.bool.reg06",
					Name: "reg06",
				},
				{
					ID:   "storage.bool.reg07",
					Name: "reg07",
				},
				{
					ID:   "storage.bool.reg08",
					Name: "reg08",
				},
				{
					ID:   "storage.bool.reg09",
					Name: "reg09",
				},
				{
					ID:   "storage.bool.reg10",
					Name: "reg10",
				},
				{
					ID:   "storage.bool.reg11",
					Name: "reg11",
				},
				{
					ID:   "storage.bool.reg12",
					Name: "reg12",
				},
				{
					ID:   "storage.bool.reg13",
					Name: "reg13",
				},
				{
					ID:   "storage.bool.reg14",
					Name: "reg14",
				},
				{
					ID:   "storage.bool.reg15",
					Name: "reg15",
				},
				{
					ID:   "storage.bool.reg16",
					Name: "reg16",
				},
				{
					ID:   "storage.bool.reg17",
					Name: "reg17",
				},
				{
					ID:   "storage.bool.reg18",
					Name: "reg18",
				},
				{
					ID:   "storage.bool.reg19",
					Name: "reg19",
				},
				{
					ID:   "storage.bool.reg20",
					Name: "reg20",
				},
				{
					ID:   "storage.string.reg01",
					Name: "reg01",
				},
				{
					ID:   "storage.string.reg02",
					Name: "reg02",
				},
				{
					ID:   "storage.string.reg03",
					Name: "reg03",
				},
				{
					ID:   "storage.string.reg04",
					Name: "reg04",
				},
				{
					ID:   "storage.string.reg05",
					Name: "reg05",
				},
				{
					ID:   "storage.string.reg06",
					Name: "reg06",
				},
				{
					ID:   "storage.string.reg07",
					Name: "reg07",
				},
				{
					ID:   "storage.string.reg08",
					Name: "reg08",
				},
				{
					ID:   "storage.string.reg09",
					Name: "reg09",
				},
				{
					ID:   "storage.string.reg10",
					Name: "reg10",
				},
				{
					ID:   "storage.string.reg11",
					Name: "reg11",
				},
				{
					ID:   "storage.string.reg12",
					Name: "reg12",
				},
				{
					ID:   "storage.string.reg13",
					Name: "reg13",
				},
				{
					ID:   "storage.string.reg14",
					Name: "reg14",
				},
				{
					ID:   "storage.string.reg15",
					Name: "reg15",
				},
				{
					ID:   "storage.string.reg16",
					Name: "reg16",
				},
				{
					ID:   "storage.string.reg17",
					Name: "reg17",
				},
				{
					ID:   "storage.string.reg18",
					Name: "reg18",
				},
				{
					ID:   "storage.string.reg19",
					Name: "reg19",
				},
				{
					ID:   "storage.string.reg20",
					Name: "reg20",
				},
				{
					ID:   "storage.time.reg01",
					Name: "reg01",
				},
				{
					ID:   "storage.time.reg02",
					Name: "reg02",
				},
				{
					ID:   "storage.time.reg03",
					Name: "reg03",
				},
				{
					ID:   "storage.time.reg04",
					Name: "reg04",
				},
				{
					ID:   "storage.time.reg05",
					Name: "reg05",
				},
				{
					ID:   "storage.time.reg06",
					Name: "reg06",
				},
				{
					ID:   "storage.time.reg07",
					Name: "reg07",
				},
				{
					ID:   "storage.time.reg08",
					Name: "reg08",
				},
				{
					ID:   "storage.time.reg09",
					Name: "reg09",
				},
				{
					ID:   "storage.time.reg10",
					Name: "reg10",
				},
				{
					ID:   "storage.time.reg11",
					Name: "reg11",
				},
				{
					ID:   "storage.time.reg12",
					Name: "reg12",
				},
				{
					ID:   "storage.time.reg13",
					Name: "reg13",
				},
				{
					ID:   "storage.time.reg14",
					Name: "reg14",
				},
				{
					ID:   "storage.time.reg15",
					Name: "reg15",
				},
				{
					ID:   "storage.time.reg16",
					Name: "reg16",
				},
				{
					ID:   "storage.time.reg17",
					Name: "reg17",
				},
				{
					ID:   "storage.time.reg18",
					Name: "reg18",
				},
				{
					ID:   "storage.time.reg19",
					Name: "reg19",
				},
				{
					ID:   "storage.time.reg20",
					Name: "reg20",
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "reg",
			args: args{
				conf: config.PointsConfig{
					Limit: 0,
					Regex: "bandwidth",
				},
			},
			want: []common.Point{
				{
					ID: "bandwidth", Name: "bandwidth",
				},
			},
			wantErr: assert.NoError,
		},
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
					Limit:     0,
					Regex:     ".*",
					RegexID:   `storage\.time.*`,
					RegexName: `.*reg20`,
				},
			},
			want: []common.Point{
				{
					ID:   "storage.time.reg20",
					Name: "reg20",
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetAllPoints(tt.args.conf)
			if !tt.wantErr(t, err, fmt.Sprintf("GetAllPoints(%v)", tt.args.conf)) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetAllPoints(%v)", tt.args.conf)
		})
	}
}

func TestDAClient_GetAllPointsAccessPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connConf := config.DaConnectConfig{
		Server: "Graybox.Simulator.1",
		Nodes:  []string{"localhost"},
	}
	c, err := NewDAClient(ctx, connConf, 0, logrus.New().WithField("test", "test"))
	assert.NoError(t, err)
	err = c.Connect()
	assert.NoError(t, err)
	points, err := c.GetAllPoints(config.PointsConfig{Da: config.DaPointsConfig{
		AccessPath: []string{"numeric"},
	}})
	assert.NoError(t, err)
	assert.NotNil(t, points)
	err = c.Close()
	assert.NoError(t, err)
	expectIDs := []string{
		"numeric.saw.uint8",
		"numeric.saw.int8",
		"numeric.saw.uint16",
		"numeric.saw.int16",
		"numeric.saw.uint32",
		"numeric.saw.int32",
		"numeric.saw.uint64",
		"numeric.saw.int64",
		"numeric.saw.float",
		"numeric.saw.double",
		"numeric.sin.uint8",
		"numeric.sin.int8",
		"numeric.sin.uint16",
		"numeric.sin.int16",
		"numeric.sin.uint32",
		"numeric.sin.int32",
		"numeric.sin.uint64",
		"numeric.sin.int64",
		"numeric.sin.float",
		"numeric.sin.double",
		"numeric.triangle.uint8",
		"numeric.triangle.int8",
		"numeric.triangle.uint16",
		"numeric.triangle.int16",
		"numeric.triangle.uint32",
		"numeric.triangle.int32",
		"numeric.triangle.uint64",
		"numeric.triangle.int64",
		"numeric.triangle.float",
		"numeric.triangle.double",
		"numeric.square.uint8",
		"numeric.square.int8",
		"numeric.square.uint16",
		"numeric.square.int16",
		"numeric.square.uint32",
		"numeric.square.int32",
		"numeric.square.uint64",
		"numeric.square.int64",
		"numeric.square.float",
		"numeric.square.double",
		"numeric.square.bool",
		"numeric.random.uint8",
		"numeric.random.int8",
		"numeric.random.uint16",
		"numeric.random.int16",
		"numeric.random.uint32",
		"numeric.random.int32",
		"numeric.random.uint64",
		"numeric.random.int64",
		"numeric.random.float",
		"numeric.random.double",
		"numeric.random.bool",
	}
	assert.Equal(t, len(expectIDs), len(points))
	for i, point := range points {
		assert.Equal(t, expectIDs[i], point.ID)
	}
}

func TestDAClient_Collect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		time.Sleep(1 * time.Second)
	}()
	gotMessageCount := 0
	var onmessage client.OnMessage = func(message []*common.NodeValue) {
		gotMessageCount = len(message)
	}
	connConf := config.DaConnectConfig{
		Server:                      "Graybox.Simulator.1",
		Nodes:                       []string{"localhost"},
		ReconnectTimes:              100,
		ReconnectInterval:           1000,
		AddTagRetryTimes:            100,
		AddTagRetryInterval:         500,
		FailedReadsToForceReconnect: 100,
	}
	collectConf := config.CollectConfig{
		Da: config.DaCollectConfig{},
	}
	collectConfWithTag := config.CollectConfig{
		Interval: 1,
		Da: config.DaCollectConfig{
			Tags: []config.TagConfig{
				{
					Tag: "numeric.saw.int16",
				},
				{
					Tag: "storage.numeric.reg01",
				},
			},
		},
	}
	collectConfWithTagAndBad := config.CollectConfig{
		Interval: 1,
		Da: config.DaCollectConfig{
			Tags: []config.TagConfig{
				{
					Tag: "numeric.saw.int16",
				},
				{
					Tag: "storage.numeric.reg01",
				},
			},
		},
		ContainsBad: true,
	}
	notConnectClient, err := NewDAClient(ctx, connConf, 0, logrus.New().WithField("test", "test"))
	assert.NoError(t, err)
	defer notConnectClient.Close()
	noOnMessageClient, err := NewDAClient(ctx, connConf, 0, logrus.New().WithField("test", "test"))
	assert.NoError(t, err)
	defer noOnMessageClient.Close()
	err = noOnMessageClient.Connect()
	assert.NoError(t, err)
	noTagConnectClient, err := NewDAClient(ctx, connConf, 0, logrus.New().WithField("test", "test"))
	assert.NoError(t, err)
	defer noTagConnectClient.Close()
	err = noTagConnectClient.Connect()
	assert.NoError(t, err)
	normalConnectClient, err := NewDAClient(ctx, connConf, 0, logrus.New().WithField("test", "test"))
	assert.NoError(t, err)
	defer normalConnectClient.Close()
	err = normalConnectClient.Connect()
	assert.NoError(t, err)
	normalConnectClient2, err := NewDAClient(ctx, connConf, 0, logrus.New().WithField("test", "test"))
	assert.NoError(t, err)
	err = normalConnectClient2.Connect()
	assert.NoError(t, err)
	defer normalConnectClient2.Close()
	normalConnectClient3, err := NewDAClient(ctx, connConf, 3, logrus.New().WithField("test", "test"))
	assert.NoError(t, err)
	err = normalConnectClient3.Connect()
	assert.NoError(t, err)
	defer normalConnectClient3.Close()
	tests := []struct {
		name      string
		c         *DAClient
		conf      config.CollectConfig
		onMessage client.OnMessage
		wantErr   assert.ErrorAssertionFunc
		wait      time.Duration
		wantCount int
	}{
		{
			name:      "not connect",
			c:         notConnectClient,
			conf:      collectConf,
			onMessage: onmessage,
			wantErr:   assert.Error,
		},
		{
			name:      "no tag",
			c:         noTagConnectClient,
			conf:      collectConf,
			onMessage: onmessage,
			wantErr:   assert.Error,
		},
		{
			name:      "no onmessage",
			c:         noOnMessageClient,
			conf:      collectConfWithTag,
			onMessage: nil,
			wantErr:   assert.Error,
		},
		{
			name:      "normal",
			c:         normalConnectClient,
			conf:      collectConfWithTag,
			onMessage: onmessage,
			wantErr:   assert.NoError,
			wait:      2 * time.Second,
			wantCount: 1,
		},
		{
			name:      "normal2",
			c:         normalConnectClient2,
			conf:      collectConfWithTag,
			onMessage: onmessage,
			wantErr:   assert.NoError,
			wait:      2 * time.Second,
			wantCount: 1,
		},
		{
			name:      "containsBad",
			c:         normalConnectClient3,
			conf:      collectConfWithTagAndBad,
			onMessage: onmessage,
			wantErr:   assert.NoError,
			wait:      2 * time.Second,
			wantCount: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Log(test.conf)
			err = test.c.Collect(test.conf, test.onMessage)
			if !test.wantErr(t, err) {
				return
			}
			if test.wait > 0 {
				time.Sleep(test.wait)
				assert.Equal(t, test.wantCount, gotMessageCount)
			}
			err = test.c.Close()
			assert.NoError(t, err)
			gotMessageCount = 0
		})
	}
}

func TestChangeCollectConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectConfig := config.DaConnectConfig{
		Server: "Graybox.Simulator.1",
		Nodes:  []string{"localhost"},
	}
	collectConfig := config.CollectConfig{
		Interval:    1,
		ContainsBad: true,
		Da: config.DaCollectConfig{
			Tags: []config.TagConfig{
				{
					Tag: "numeric.saw.int16",
				},
				{
					Tag: "numeric.saw.int32",
				},
				{
					Tag: "numeric.saw.int64",
				},
			},
		},
	}
	expectNodes := map[string]bool{
		"numeric.saw.int16": true,
		"numeric.saw.int32": true,
		"numeric.saw.int64": true,
	}
	lock := sync.Mutex{}
	expectGotNodes := map[string]struct{}{
		"numeric.saw.int16": {},
		"numeric.saw.int32": {},
		"numeric.saw.int64": {},
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
	client, err := NewDAClient(ctx, connectConfig, 1, logrus.New().WithField("test", "test"))
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
	t.Log("expect 16 32 64")
	time.Sleep(time.Second * 3)
	lock.Lock()
	if len(expectGotNodes) != 0 {
		t.Fatal("not all nodes got")
	}
	lock.Unlock()
	newCollectConfig := config.CollectConfig{
		Interval:    1,
		ContainsBad: true,
		Da: config.DaCollectConfig{
			Tags: []config.TagConfig{
				{
					Tag: "numeric.saw.int16",
				},
				{
					Tag: "numeric.saw.int32",
				},
			},
		},
	}
	client.ChangeCollectConfig(newCollectConfig)
	t.Log("expect 16 32")
	lock.Lock()
	expectGotNodes = map[string]struct{}{
		"numeric.saw.int16": {},
		"numeric.saw.int32": {},
	}
	expectNodes["numeric.saw.int64"] = false
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
		Da: config.DaCollectConfig{
			Tags: []config.TagConfig{
				{
					Tag: "numeric.saw.int16",
				},
			},
		},
	}
	lock.Lock()
	expectGotNodes = map[string]struct{}{
		"numeric.saw.int16": {},
	}
	lock.Unlock()
	client.ChangeCollectConfig(newCollectConfig)
	t.Log("expect 16")
	expectNodes["numeric.saw.int32"] = false
	time.Sleep(time.Second * 3)
	lock.Lock()
	if len(expectGotNodes) != 0 {
		t.Fatal("not all nodes got")
	}
	lock.Unlock()
	newCollectConfig = config.CollectConfig{
		Interval:    1,
		ContainsBad: true,
		Da: config.DaCollectConfig{
			Tags: []config.TagConfig{
				{
					Tag: "numeric.saw.int16",
				},
				{
					Tag: "numeric.saw.int64",
				},
			},
		},
	}
	lock.Lock()
	expectGotNodes = map[string]struct{}{
		"numeric.saw.int16": {},
		"numeric.saw.int64": {},
	}
	expectNodes["numeric.saw.int64"] = true
	lock.Unlock()
	client.ChangeCollectConfig(newCollectConfig)
	t.Log("expect 16 64")
	time.Sleep(time.Second * 3)
	lock.Lock()
	if len(expectGotNodes) != 0 {
		t.Fatal("not all nodes got")
	}
	lock.Unlock()
	newCollectConfig = config.CollectConfig{
		Interval:    1,
		ContainsBad: true,
		Da: config.DaCollectConfig{
			Tags: []config.TagConfig{
				{
					Tag: "numeric.saw.int16",
				},
				{
					Tag: "numeric.saw.int32",
				},
				{
					Tag: "numeric.saw.int64",
				},
			},
		},
	}
	lock.Lock()
	expectGotNodes = map[string]struct{}{
		"numeric.saw.int16": {},
		"numeric.saw.int32": {},
		"numeric.saw.int64": {},
	}
	expectNodes["numeric.saw.int16"] = true
	expectNodes["numeric.saw.int32"] = true
	lock.Unlock()
	client.ChangeCollectConfig(newCollectConfig)
	t.Log("expect 16 32 64")
	time.Sleep(time.Second * 3)
	if len(expectGotNodes) != 0 {
		t.Fatal("not all nodes got")
	}
}

func Test_qualityGood(t *testing.T) {
	var expectGoods = []int16{192, 216, -16129, 4544}
	var expectBads = []int16{0, 4, 8, 12, 16, 20, 24, 28, 64, 68, 80, 84, 88}
	for _, g := range expectGoods {
		if !qualityGood(g) {
			t.Fatal("expect good", g)
		}
	}
	for _, b := range expectBads {
		if qualityGood(b) {
			t.Fatal("expect bad", b)
		}
	}
}
