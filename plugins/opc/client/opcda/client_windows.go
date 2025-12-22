//go:build windows
// +build windows

package opcda

import (
	"collector/client"
	"collector/common"
	"collector/config"
	"collector/log"
	"collector/regexp"
	"collector/types"
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/konimarti/opc"
	"github.com/sirupsen/logrus"
)

type DAClient struct {
	conn       opc.Connection
	tagInfo    map[string]*TagInfo
	onmessage  client.OnMessage
	ctx        context.Context
	index      int
	server     string
	nodes      []string
	tags       []string
	closeChan  chan struct{}
	finishChan chan struct{}

	logger *logrus.Entry

	interval   time.Duration
	once       sync.Once
	dumper     *log.DataDump
	changeChan chan *changeData

	containsBad bool

	ReconnectTimes              int
	AddTagRetryTimes            int
	ReconnectInterval           time.Duration
	AddTagRetryInterval         time.Duration
	FailedReadsToForceReconnect int
}

type TagInfo struct {
	name      string
	valueType types.ValueType
}

func NewDAClient(ctx context.Context, connectConfig config.DaConnectConfig, index int, logger *logrus.Entry) (*DAClient, error) {
	opcLogger := logger.WithField("opcType", "da").WithField("id", index)
	c := &DAClient{
		ctx:                         ctx,
		server:                      connectConfig.Server,
		nodes:                       connectConfig.Nodes,
		logger:                      opcLogger,
		ReconnectTimes:              connectConfig.ReconnectTimes,
		AddTagRetryTimes:            connectConfig.AddTagRetryTimes,
		ReconnectInterval:           time.Duration(connectConfig.ReconnectInterval) * time.Millisecond,
		AddTagRetryInterval:         time.Duration(connectConfig.AddTagRetryInterval) * time.Millisecond,
		FailedReadsToForceReconnect: connectConfig.FailedReadsToForceReconnect,
	}
	return c, nil
}

func (c *DAClient) Connect() error {
	c.logger.Info("opcda start to connect")
	var err error
	connConfig := opc.DefaultConnectionConfig()
	if c.ReconnectTimes > 0 {
		connConfig.ReconnectTimes = c.ReconnectTimes
	}
	if c.ReconnectInterval > 0 {
		connConfig.ReconnectInterval = c.ReconnectInterval
	}
	if c.AddTagRetryTimes > 0 {
		connConfig.AddTagRetryTimes = c.AddTagRetryTimes
	}
	if c.AddTagRetryInterval > 0 {
		connConfig.AddTagRetryInterval = c.AddTagRetryInterval
	}
	if c.FailedReadsToForceReconnect > 0 {
		connConfig.FailedReadsToForceReconnect = c.FailedReadsToForceReconnect
	}
	c.logger.Infof("opcda connect with reconnect_times:%d,reconnect_interval:%s,add_tag_retry_times:%d,add_tag_retry_interval:%s,failed_reads_to_force_reconnect:%d",
		connConfig.ReconnectTimes,
		connConfig.ReconnectInterval,
		connConfig.AddTagRetryTimes,
		connConfig.AddTagRetryInterval,
		connConfig.FailedReadsToForceReconnect,
	)
	c.conn, err = opc.NewConnection(c.server, c.nodes, []string{}, connConfig, c.logger)
	if err != nil {
		c.logger.WithError(err).Error("opcda connect error")
		return err
	}
	c.logger.Info("opcda connect success")
	return nil
}

func (c *DAClient) Collect(collectConfig config.CollectConfig, onMessage client.OnMessage) error {
	c.logger.Info("opcda start to collect")

	if c.conn == nil {
		c.logger.Error("opcda collect error: connection is nil")
		return errors.New("opcda collect error: connection is nil")
	}
	interval := collectConfig.Interval
	if interval <= 0 {
		interval = 10
	}
	tags := make([]string, 0, len(collectConfig.Da.Tags))
	for _, tag := range collectConfig.Da.Tags {
		tags = append(tags, tag.Tag)
	}
	var dataDumper *log.DataDump
	var err error
	if collectConfig.Dump.Enable {
		c.logger.Info("dump is enabled")
		dataDumper, err = log.NewDataDump(collectConfig.Dump.Path, collectConfig.Dump.Keep, false)
		if err != nil {
			c.logger.WithError(err).Error("new data dump error")
			return err
		}
	}
	c.containsBad = collectConfig.ContainsBad
	c.tags = tags
	c.onmessage = onMessage
	c.interval = time.Duration(interval) * time.Second
	c.dumper = dataDumper
	c.changeChan = make(chan *changeData)
	c.tagInfo = make(map[string]*TagInfo, len(tags))
	for _, tag := range tags {
		parts := strings.Split(tag, ".")
		lastPart := parts[len(parts)-1]
		c.tagInfo[tag] = &TagInfo{name: lastPart}
	}

	if c.onmessage == nil {
		c.logger.Error("opcda collect error: onmessage is nil")
		return errors.New("opcda collect error: onmessage is nil")
	}
	if len(c.tags) == 0 {
		c.logger.Error("opcda collect error: tags is empty")
		return errors.New("opcda collect error: tags is empty")
	}
	addedCount := 0
	for _, tag := range c.tags {
		err := c.conn.Add(tag)
		if err != nil {
			c.logger.WithError(err).WithField("tag", tag).Error("opcda add tag error")
		} else {
			addedCount += 1
		}
	}
	if addedCount == 0 {
		c.logger.Error("no tag added")
		return errors.New("no tag added")
	}
	c.closeChan = make(chan struct{})
	c.finishChan = make(chan struct{})
	ticker := time.NewTicker(c.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.read()
			case <-c.ctx.Done():
				c.logger.Info("opcda collect exit with ctx signal")
				c.Close()
				return
			case <-c.closeChan:
				c.logger.Info("opcda collect exit with close signal")
				close(c.finishChan)
				return
			case data := <-c.changeChan:
				c.logger.Info("opcda collect change collect points")
				for _, tag := range data.remove {
					c.logger.Info("opcda remove tag:", tag)
					c.conn.Remove(tag)
				}
				for _, tag := range data.add {
					c.logger.Info("opcda add tag:", tag)
					err := c.conn.Add(tag)
					if err != nil {
						c.logger.WithError(err).WithField("tag", tag).Error("opcda add tag error")
					}
				}
				c.tags = data.newTags
				for _, tag := range data.newTags {
					if _, exists := c.tagInfo[tag]; !exists {
						parts := strings.Split(tag, ".")
						lastPart := parts[len(parts)-1]
						c.tagInfo[tag] = &TagInfo{name: lastPart}
					}
				}
			}
		}
	}()
	c.logger.Infof("opcda collect success, total tags: %d, added tags: %d, add failed tags: %d", len(c.tags), addedCount, len(c.tags)-addedCount)
	return nil
}

const qualityMask byte = 0b11000000

func qualityGood(quality int16) bool {
	return (byte(quality) & qualityMask) == qualityMask
}

func (c *DAClient) read() {
	c.logger.Debug("opcda start to read")
	startRead := time.Now()
	items := c.conn.Read()
	finishTime := time.Now()
	cost := finishTime.Sub(startRead)
	if cost > c.interval {
		c.logger.WithField("cost", cost).WithField("interval", c.interval).Warn("opcda read cost too much time")
	}
	c.logger.WithField("cost", cost).Debug("opcda read success")
	values := make([]*common.NodeValue, 0, len(items))
	for tag, item := range items {
		info := c.tagInfo[tag]
		if info.valueType == 0 {
			if item.Value == nil {
				c.logger.Logger.WithField("tag", tag).Error("opcda tag value is nil, skip unknown type")
				continue
			}
			vt := types.GetValueType(item.Value)
			if !vt.IsValid() {
				c.logger.Logger.WithField("tag", tag).WithField("value", item.Value).WithField("valueType", vt).Error("opcda tag type is invalid")
				continue
			}
			info.valueType = vt
		}
		if !c.containsBad && !qualityGood(item.Quality) {
			c.logger.Logger.WithField("tag", tag).WithField("quality", item.Quality).WithField("value", item.Value).Warn("opcda tag quality is bad, skip")
			continue
		}
		v := &common.NodeValue{
			IDStr:      tag,
			Name:       info.name,
			Timestamp:  item.Timestamp,
			StartTime:  startRead,
			FinishTime: finishTime,
			Value:      item.Value,
			ValueType:  info.valueType,
			Status:     int64(item.Quality),
		}
		values = append(values, v)
	}
	if len(values) == 0 {
		c.logger.Warn("opcda read no values")
		return
	}
	if c.dumper != nil {
		c.logger.Debug("opcda start to dump")
		c.dumper.Dump(values)
		c.logger.Debug("opcda dump success")
	}
	c.logger.WithField("count", len(values)).Debug("read value success")
	c.onmessage(values)
	c.logger.Debug("handle message success")
}

func (c *DAClient) Close() error {
	c.once.Do(func() {
		if c.closeChan != nil {
			close(c.closeChan)
			select {
			case <-c.finishChan:
			case <-c.ctx.Done():
			}
		}
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		if c.dumper != nil {
			c.dumper.Close()
		}
	})
	return nil
}

func (c *DAClient) GetAllPoints(conf config.PointsConfig) ([]common.Point, error) {
	c.logger.Info("opcda start to get all points")
	if c.conn == nil {
		return nil, fmt.Errorf("opcda get all points error: connection is nil")
	}
	var reg regexp.Regexp
	var regName regexp.Regexp
	var regID regexp.Regexp
	var err error
	if len(conf.Regex) > 0 {
		reg, err = regexp.Compile(conf.Regex)
		if err != nil {
			return nil, fmt.Errorf("invalid points regex: %w", err)
		}
	}
	if len(conf.RegexName) > 0 {
		regName, err = regexp.Compile(conf.RegexName)
		if err != nil {
			return nil, fmt.Errorf("invalid regex_name: %w", err)
		}
	}

	if len(conf.RegexID) > 0 {
		regID, err = regexp.Compile(conf.RegexID)
		if err != nil {
			return nil, fmt.Errorf("invalid regex_id: %w", err)
		}
	}

	tree, err := opc.CreateBrowser(c.server, c.nodes, c.logger)
	if err != nil {
		return nil, fmt.Errorf("get all tags error. create browser error %v", err)
	}
	root := tree
	if len(conf.Da.AccessPath) > 0 {
		for _, s := range conf.Da.AccessPath {
			found := false
			for _, branch := range root.Branches {
				if branch.Name == s {
					root = branch
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("get all tags error. access path not found %s", s)
			}
		}
	}

	tags := c.browse(root, reg, regName, regID, conf.Limit)
	return tags, nil
}

func (c *DAClient) browse(tree *opc.Tree, pointRegex, nameRegex, idRegex regexp.Regexp, pointLimit int) (points []common.Point) {
	l := list.New()
	l.PushBack(tree)

	for {
		front := l.Front()
		if front == nil {
			break
		}

		t := l.Remove(front).(*opc.Tree)
		for _, leave := range t.Leaves {
			if (pointRegex != nil && !(pointRegex.MatchString(leave.Name) || pointRegex.MatchString(leave.Tag))) ||
				(nameRegex != nil && !nameRegex.MatchString(leave.Name)) ||
				(idRegex != nil && !idRegex.MatchString(leave.Tag)) {
				continue
			}

			points = append(points, common.Point{
				ID:   leave.Tag,
				Name: leave.Name,
			})
			if pointLimit > 0 && len(points) >= pointLimit {
				return
			}
		}
		for _, b := range t.Branches {
			l.PushBack(b)
		}
	}
	return
}

type changeData struct {
	add     []string
	remove  []string
	newTags []string
}

func (c *DAClient) ChangeCollectConfig(conf config.CollectConfig) {
	c.logger.Info("opcda start to change collect config")
	// compare tags
	tags := c.tags
	oldTagsMap := make(map[string]struct{}, len(c.tags))
	for _, tag := range tags {
		oldTagsMap[tag] = struct{}{}
	}
	addTags := make([]string, 0)
	removeTags := make([]string, 0)
	newTags := make([]string, len(conf.Da.Tags))
	for i := 0; i < len(conf.Da.Tags); i++ {
		newTags[i] = conf.Da.Tags[i].Tag
		tag := conf.Da.Tags[i].Tag
		_, ok := oldTagsMap[tag]
		if !ok {
			addTags = append(addTags, tag)
		} else {
			delete(oldTagsMap, tag)
		}
	}
	for tag := range oldTagsMap {
		removeTags = append(removeTags, tag)
	}
	c.changeChan <- &changeData{
		add:     addTags,
		remove:  removeTags,
		newTags: newTags,
	}
}
