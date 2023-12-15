//go:build windows
// +build windows

package opcda

import (
	"collector/client"
	"collector/common"
	"collector/config"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var onmessage client.OnMessage = func(message []*common.NodeValue) {
		t.Log(message)
	}
	connConf := config.DaConnectConfig{
		Server: "Graybox.Simulator.1",
		Nodes:  []string{"localhost"},
	}

	c, err := NewDAClient(ctx, connConf, config.CollectConfig{}, 0, logrus.New().WithField("test", "test"), onmessage)
	assert.NoError(t, err)
	err = c.Connect()
	assert.NoError(t, err)
	err = c.Close()
	assert.NoError(t, err)
	connConf = config.DaConnectConfig{
		Server: "Graybox.Simulator.2",
		Nodes:  []string{"localhost"},
	}
	c2, err := NewDAClient(ctx, connConf, config.CollectConfig{}, 0, logrus.New().WithField("test", "test"), onmessage)
	assert.NoError(t, err)
	err = c2.Connect()
	assert.Error(t, err)
}

func TestDAClient_GetAllPoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var onmessage client.OnMessage = func(message []*common.NodeValue) {
		t.Log(message)
	}
	connConf := config.DaConnectConfig{
		Server: "Graybox.Simulator.1",
		Nodes:  []string{"localhost"},
	}
	c, err := NewDAClient(ctx, connConf, config.CollectConfig{}, 0, logrus.New().WithField("test", "test"), onmessage)
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
	var onmessage client.OnMessage = func(message []*common.NodeValue) {
		t.Log(message)
	}
	connConf := config.DaConnectConfig{
		Server: "Graybox.Simulator.1",
		Nodes:  []string{"localhost"},
	}
	c, err := NewDAClient(ctx, connConf, config.CollectConfig{}, 0, logrus.New().WithField("test", "test"), onmessage)
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

func TestDAClient_Collect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		time.Sleep(1 * time.Second)
	}()
	gotMessage := false
	var onmessage client.OnMessage = func(message []*common.NodeValue) {
		gotMessage = true
	}
	connConf := config.DaConnectConfig{
		Server: "Graybox.Simulator.1",
		Nodes:  []string{"localhost"},
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
			},
		},
	}
	notConnectClient, err := NewDAClient(ctx, connConf, collectConf, 0, logrus.New().WithField("test", "test"), onmessage)
	assert.NoError(t, err)
	defer notConnectClient.Close()
	noOnMessageClient, err := NewDAClient(ctx, connConf, collectConfWithTag, 0, logrus.New().WithField("test", "test"), nil)
	assert.NoError(t, err)
	defer noOnMessageClient.Close()
	err = noOnMessageClient.Connect()
	assert.NoError(t, err)
	noTagConnectClient, err := NewDAClient(ctx, connConf, collectConf, 0, logrus.New().WithField("test", "test"), onmessage)
	assert.NoError(t, err)
	defer noTagConnectClient.Close()
	err = noTagConnectClient.Connect()
	assert.NoError(t, err)
	normalConnectClient, err := NewDAClient(ctx, connConf, collectConfWithTag, 0, logrus.New().WithField("test", "test"), onmessage)
	assert.NoError(t, err)
	defer normalConnectClient.Close()
	err = normalConnectClient.Connect()
	assert.NoError(t, err)
	normalConnectClient2, err := NewDAClient(ctx, connConf, collectConfWithTag, 0, logrus.New().WithField("test", "test"), onmessage)
	assert.NoError(t, err)
	err = normalConnectClient2.Connect()
	assert.NoError(t, err)
	tests := []struct {
		name    string
		c       *DAClient
		wantErr assert.ErrorAssertionFunc
		wait    time.Duration
	}{
		{
			name:    "not connect",
			c:       notConnectClient,
			wantErr: assert.Error,
		},
		{
			name:    "no tag",
			c:       noTagConnectClient,
			wantErr: assert.Error,
		},
		{
			name:    "no onmessage",
			c:       noOnMessageClient,
			wantErr: assert.Error,
		},
		{
			name:    "normal",
			c:       normalConnectClient,
			wantErr: assert.NoError,
			wait:    2 * time.Second,
		},
		{
			name:    "normal2",
			c:       normalConnectClient2,
			wantErr: assert.NoError,
			wait:    2 * time.Second,
		},
	}
	for _, test := range tests {
		err = test.c.Collect()
		if !test.wantErr(t, err) {
			return
		}
		if test.wait > 0 {
			time.Sleep(test.wait)
			assert.Equal(t, true, gotMessage)
		}
		gotMessage = false
	}
}
