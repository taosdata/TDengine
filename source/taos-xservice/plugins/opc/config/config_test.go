package config

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

const CheckDAConfig = `
opc_type = "opcda"
debug = false


[connect.da]  # opc da config.
server = "Matrikon.OPC.Simulation.1" # opc da server
nodes = ["localhost"] # opc da nodes
`

const CheckUAConfig = `
opc_type = "opcua"
debug = false

[connect.ua]
endpoint = "opc.tcp://127.0.0.1:4840"
connect_timeout = 10
request_timeout = 10
security_policy = "None"
security_mode = "None"
auth_method = "Anonymous"
`

const CollectDAConfig = `
opc_type = "opcda"
debug = false

[connect.da]  # opc da config.
server = "Matrikon.OPC.Simulation.1" # opc da server
nodes = ["localhost"] # opc da nodes

[report]
remote = "127.0.0.1:6051" # taosx's address ip:port or socket path
concurrent = 2
batch_size = 2
batch_timeout = 10

[collect]
interval = 1 # collect data interval in second for opc ua observe mode and opc da
contains_bad = true # collect bad node when collect data

[collect.dump]
enable = true # dump data to file
path = "./tmp/opc/" # dump file path
keep = 7 # keep dump file days

[[collect.da.tags]]
tag = "Random.Int8"
`

const CollectUAObserverConfig = `
opc_type = "opcua"
debug = false

[connect.ua]
endpoint = "opc.tcp://127.0.0.1:4840"
connect_timeout = 10
request_timeout = 10
security_policy = "None"
security_mode = "None"
auth_method = "Anonymous"

[collect]
interval = 1 # collect data interval in second for opc ua observe mode and opc da
contains_bad = true # collect bad node when collect data

[collect.dump]
enable = true # dump data to file
path = "./tmp/opc/" # dump file path
keep = 7 # keep dump file days

[report]
remote = "127.0.0.1:6051" # taosx's address ip:port or socket path
concurrent = 2
batch_size = 2
batch_timeout = 10

[collect.ua]
collect_mode = "observe" # observe or subscribe. default is observe

[[collect.ua.nodes]] # opc ua nodes. needs node id and value type. value type is same with data type in tdengine
id = "ns=2;i=1001"

[[collect.ua.nodes]]
id = "ns=2;i=1002"

[[collect.ua.nodes]]
id = "ns=2;i=1003"
`

const CollectUASubscribeConfig = `
opc_type = "opcua"
debug = false

[connect.ua]
endpoint = "opc.tcp://127.0.0.1:4840"
connect_timeout = 10
request_timeout = 10
security_policy = "None"
security_mode = "None"
auth_method = "Anonymous"

[collect]
interval = 1 # collect data interval in second for opc ua observe mode and opc da
contains_bad = true # collect bad node when collect data

[collect.dump]
enable = true # dump data to file
path = "./tmp/opc/" # dump file path
keep = 7 # keep dump file days

[report]
remote = "127.0.0.1:6051" # taosx's address ip:port or socket path
concurrent = 2
batch_size = 2
batch_timeout = 10

[collect.ua]
collect_mode = "subscribe" # observe or subscribe. default is observe

[[collect.ua.nodes]] # opc ua nodes. needs node id and value type. value type is same with data type in tdengine
id = "ns=2;i=1001"

[[collect.ua.nodes]]
id = "ns=2;i=1002"

[[collect.ua.nodes]]
id = "ns=2;i=1003"
`

const PointsDAConfig = `
opc_type = "opcda"
debug = false


[connect.da]  # opc da config.
server = "Matrikon.OPC.Simulation.1" # opc da server
nodes = ["localhost"] # opc da nodes

[points] # config for collect opc points.
limit = 200 # max points return in one request
regex = ".*" # regex for point name
`

const PointsUAConfig = `
opc_type = "opcua"
debug = false

[connect.ua]
endpoint = "opc.tcp://127.0.0.1:4840"
connect_timeout = 10
request_timeout = 10
security_policy = "None"
security_mode = "None"
auth_method = "Anonymous"


[points] # config for collect opc points.
limit = 200 # max points return in one request
regex = ".*" # regex for point name

[points.ua]
root = "i=85"
namespaces = [3,4,5]
`

func TestDaConnectConfig_Validate(t *testing.T) {
	tmp := t.TempDir()
	err := os.WriteFile(path.Join(tmp, "config.toml"), []byte(CheckDAConfig), 0644)
	assert.NoError(t, err)
	conf, err := ParseConfig(path.Join(tmp, "config.toml"))
	assert.NoError(t, err)
	err = conf.ValidateConnect()
	assert.NoError(t, err)
}

func TestUAConnectConfig_Validate(t *testing.T) {
	tmp := t.TempDir()
	err := os.WriteFile(path.Join(tmp, "config.toml"), []byte(CheckUAConfig), 0644)
	assert.NoError(t, err)
	conf, err := ParseConfig(path.Join(tmp, "config.toml"))
	assert.NoError(t, err)
	err = conf.ValidateConnect()
	assert.NoError(t, err)
}

func TestDACollectConfig_Validate(t *testing.T) {
	tmp := t.TempDir()
	err := os.WriteFile(path.Join(tmp, "config.toml"), []byte(CollectDAConfig), 0644)
	assert.NoError(t, err)
	conf, err := ParseConfig(path.Join(tmp, "config.toml"))
	assert.NoError(t, err)
	err = conf.ValidateCollect()
	assert.NoError(t, err)
}

func TestUAObserverCollectConfig_Validate(t *testing.T) {
	tmp := t.TempDir()
	err := os.WriteFile(path.Join(tmp, "config.toml"), []byte(CollectUAObserverConfig), 0644)
	assert.NoError(t, err)
	conf, err := ParseConfig(path.Join(tmp, "config.toml"))
	assert.NoError(t, err)
	err = conf.ValidateCollect()
	assert.NoError(t, err)
}

func TestUASubscribeCollectConfig_Validate(t *testing.T) {
	tmp := t.TempDir()
	err := os.WriteFile(path.Join(tmp, "config.toml"), []byte(CollectUASubscribeConfig), 0644)
	assert.NoError(t, err)
	conf, err := ParseConfig(path.Join(tmp, "config.toml"))
	assert.NoError(t, err)
	err = conf.ValidateCollect()
	assert.NoError(t, err)
}

func TestPointsDAConfig_Validate(t *testing.T) {
	tmp := t.TempDir()
	err := os.WriteFile(path.Join(tmp, "config.toml"), []byte(PointsDAConfig), 0644)
	assert.NoError(t, err)
	conf, err := ParseConfig(path.Join(tmp, "config.toml"))
	assert.NoError(t, err)
	err = conf.ValidateGetPoints()
	assert.NoError(t, err)
}

func TestPointsUAConfig_Validate(t *testing.T) {
	tmp := t.TempDir()
	err := os.WriteFile(path.Join(tmp, "config.toml"), []byte(PointsUAConfig), 0644)
	assert.NoError(t, err)
	conf, err := ParseConfig(path.Join(tmp, "config.toml"))
	assert.NoError(t, err)
	err = conf.ValidateGetPoints()
	assert.NoError(t, err)
}

func TestGetAutoReconnect(t *testing.T) {
	cfg := &ConnectConfig{
		Ua: UaConnectConfig{},
	}
	assert.Equal(t, true, cfg.Ua.GetAutoReconnect())
	vf := false
	vt := true
	pf := &vf
	pt := &vt
	cfg = &ConnectConfig{
		Ua: UaConnectConfig{
			AutoReconnect: pf,
		},
	}
	assert.Equal(t, false, cfg.Ua.GetAutoReconnect())

	cfg = &ConnectConfig{
		Ua: UaConnectConfig{
			AutoReconnect: pt,
		},
	}
	assert.Equal(t, true, cfg.Ua.GetAutoReconnect())
}
