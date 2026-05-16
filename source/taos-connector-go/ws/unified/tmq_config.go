package unified

import (
	"time"

	"github.com/taosdata/driver-go/v3/common"
)

type config struct {
	Url                  string
	Endpoints            []string
	ChanLength           uint
	MessageTimeout       time.Duration
	WriteWait            time.Duration
	Timezone             *time.Location
	User                 string
	Password             string
	GroupID              string
	ClientID             string
	OffsetRest           string
	AutoCommit           string
	AutoCommitIntervalMS string
	SnapshotEnable       string
	WithTableName        string
	EnableCompression    bool
	AutoReconnect        bool
	ReconnectIntervalMs  int
	ReconnectRetryCount  int
	SessionTimeoutMS     string
	MaxPollIntervalMS    string
	OtherOptions         map[string]string
}

func newConfig(url string, chanLength uint) *config {
	return &config{
		Url:          url,
		Endpoints:    []string{url},
		ChanLength:   chanLength,
		OtherOptions: make(map[string]string),
	}
}

func (c *config) setEndpoints(endpoints []string) {
	if len(endpoints) == 0 {
		c.Endpoints = nil
		return
	}
	c.Endpoints = make([]string, len(endpoints))
	copy(c.Endpoints, endpoints)
	if len(c.Endpoints) > 0 {
		c.Url = c.Endpoints[0]
	}
}

func (c *config) setConnectUser(user string) {
	c.User = user
}

func (c *config) setConnectPass(pass string) {
	c.Password = pass
}

func (c *config) setGroupID(groupID string) {
	c.GroupID = groupID
}

func (c *config) setClientID(clientID string) {
	c.ClientID = clientID
}

func (c *config) setAutoOffsetReset(offsetReset string) {
	c.OffsetRest = offsetReset
}

func (c *config) setMessageTimeout(timeout time.Duration) error {
	if timeout < time.Second {
		return newInvalidConfigErrorf("ws.message.timeout cannot be less than 1 second")
	}
	c.MessageTimeout = timeout
	return nil
}

func (c *config) setWriteWait(writeWait time.Duration) error {
	if writeWait < time.Second {
		return newInvalidConfigErrorf("ws.message.writeWait cannot be less than 1 second")
	}
	c.WriteWait = writeWait
	return nil
}

func (c *config) setAutoCommit(enable string) {
	c.AutoCommit = enable
}

func (c *config) setAutoCommitIntervalMS(autoCommitIntervalMS string) {
	c.AutoCommitIntervalMS = autoCommitIntervalMS
}

func (c *config) setSnapshotEnable(enableSnapshot string) {
	c.SnapshotEnable = enableSnapshot
}

func (c *config) setWithTableName(withTableName string) {
	c.WithTableName = withTableName
}

func (c *config) setEnableCompression(enableCompression bool) {
	c.EnableCompression = enableCompression
}

func (c *config) setAutoReconnect(autoReconnect bool) {
	c.AutoReconnect = autoReconnect
}

func (c *config) setReconnectIntervalMs(reconnectIntervalMs int) {
	c.ReconnectIntervalMs = reconnectIntervalMs
}

func (c *config) setReconnectRetryCount(reconnectRetryCount int) {
	c.ReconnectRetryCount = reconnectRetryCount
}

func (c *config) setSessionTimeoutMS(sessionTimeoutMS string) {
	c.SessionTimeoutMS = sessionTimeoutMS
}

func (c *config) setMaxPollIntervalMS(maxPollIntervalMS string) {
	c.MaxPollIntervalMS = maxPollIntervalMS
}

func (c *config) setTimezone(timezone string) error {
	if timezone == "" {
		return nil
	}
	tz, err := common.ParseTimezone(timezone)
	if err != nil {
		return err
	}
	c.Timezone = tz
	return nil
}
