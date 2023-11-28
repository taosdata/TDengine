//go:build windows
// +build windows

package opcda

import (
	"collector/client"
	"collector/common"
	"collector/config"
	"collector/log"
	"collector/types"
	"container/list"
	"context"
	"errors"
	"fmt"
	"regexp"
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

	interval time.Duration
	once     sync.Once
	dumper   *log.DataDump
}

type TagInfo struct {
	name      string
	valueType types.ValueType
}

func NewDAClient(ctx context.Context, connectConfig config.DaConnectConfig, collectConfig config.CollectConfig, index int, logger *logrus.Entry, onMessage client.OnMessage) (*DAClient, error) {
	interval := collectConfig.Interval
	if interval <= 0 {
		interval = 10
	}
	tags := make([]string, 0, len(collectConfig.Da.Tags))
	for _, tag := range collectConfig.Da.Tags {
		tags = append(tags, tag.Tag)
	}
	opcLogger := logger.WithField("opcType", "da").WithField("id", index)
	var dataDumper *log.DataDump
	var err error
	if collectConfig.Dump.Enable {
		opcLogger.Info("dump is enabled")
		dataDumper, err = log.NewDataDump(collectConfig.Dump.Path, collectConfig.Dump.Keep, false)
		if err != nil {
			opcLogger.WithError(err).Error("new data dump error")
			return nil, err
		}
	}
	c := &DAClient{
		ctx:       ctx,
		server:    connectConfig.Server,
		nodes:     connectConfig.Nodes,
		tags:      tags,
		logger:    opcLogger,
		onmessage: onMessage,
		interval:  time.Duration(interval) * time.Second,
		tagInfo:   make(map[string]*TagInfo, len(tags)),
		dumper:    dataDumper,
	}

	for _, tag := range tags {
		parts := strings.Split(tag, ".")
		lastPart := parts[len(parts)-1]
		c.tagInfo[tag] = &TagInfo{name: lastPart}
	}
	return c, nil
}

func (c *DAClient) Connect() error {
	c.logger.Info("opcda start to connect")
	var err error
	if len(c.tags) == 0 {
		c.conn, err = opc.NewConnection(c.server, c.nodes, []string{})
	} else {
		c.conn, err = opc.NewConnection(c.server, c.nodes, c.tags)
	}
	if err != nil {
		c.logger.WithError(err).Error("opcda connect error")
		return err
	}
	c.logger.Info("opcda connect success")
	return nil
}

func (c *DAClient) Collect() error {
	c.logger.Info("opcda start to collect")

	if c.conn == nil {
		c.logger.Error("opcda collect error: connection is nil")
		return errors.New("opcda collect error: connection is nil")
	}
	if c.onmessage == nil {
		c.logger.Error("opcda collect error: onmessage is nil")
		return errors.New("opcda collect error: onmessage is nil")
	}
	if len(c.tags) == 0 {
		c.logger.Error("opcda collect error: tags is empty")
		return errors.New("opcda collect error: tags is empty")
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
			}
		}
	}()
	c.logger.Info("opcda collect success")
	return nil
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
			vt := types.GetValueType(item.Value)
			if !vt.IsValid() {
				c.logger.Logger.Error("opcda tag type is invalid", "tag", tag, "value", item.Value, "valueType", vt)
				client.UnrecoverableError <- fmt.Errorf("opcda tag type is invalid, tag: %s, value: %v, valueType: %v", tag, item.Value, vt)
				return
			}
			info.valueType = vt
		}
		v := &common.NodeValue{
			Identifier: tag,
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
	if c.dumper != nil {
		c.logger.Debug("opcda start to dump")
		c.dumper.Dump(values)
		c.logger.Debug("opcda dump success")
	}
	c.logger.WithField("count", len(values)).Debug("prepare to send message")
	c.onmessage(values)
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
	var reg *regexp.Regexp
	var err error
	if len(conf.Regex) > 0 {
		reg, err = regexp.Compile(conf.Regex)
		if err != nil {
			return nil, fmt.Errorf("invalid points regex: %w", err)
		}
	}

	tree, err := opc.CreateBrowser(c.server, c.nodes)
	if err != nil {
		return nil, fmt.Errorf("get all tags error. create browser error %v", err)
	}
	tags := c.browse(tree, reg, conf.Limit)
	return tags, nil
}

func (c *DAClient) browse(tree *opc.Tree, pointRegex *regexp.Regexp, pointLimit int) (points []common.Point) {
	l := list.New()
	l.PushBack(tree)

	for {
		front := l.Front()
		if front == nil {
			break
		}

		t := l.Remove(front).(*opc.Tree)
		for _, leave := range t.Leaves {
			if pointRegex != nil && !(pointRegex.MatchString(leave.Name) || pointRegex.MatchString(leave.Tag)) {
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
