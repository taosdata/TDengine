//go:build windows
// +build windows

package opcda

import (
	"collector/common"
	"collector/connector"
	"container/list"
	"context"
	"errors"
	"fmt"
	"os/signal"
	"reflect"
	"regexp"
	"sync"
	"syscall"
	"time"

	"github.com/konimarti/opc"
	"github.com/sunpe/gobox/logger"
)

// state connection state
type state int

const (
	_ state = iota
	connected
	connecting
	disconnected
)

type daTag struct {
	tag       string
	name      string
	valueType common.ValueType
}

type reader struct {
	client      opc.Connection
	server      string
	nodes       []string
	tags        map[string]*daTag
	state       state
	interval    time.Duration
	pointLimit  int
	pointRegex  *regexp.Regexp
	mutex       sync.Mutex
	done        chan struct{}
	dumper      *connector.CsvDumper
	containsBad bool
	debug       bool
}

func newReader(config common.Config) (*reader, error) {
	if err := config.Connect.Da.Validate(); err != nil {
		return nil, fmt.Errorf("create opc da connector error %v", err)
	}

	if config.Collect.Interval <= 0 {
		config.Collect.Interval = 1
	}

	var pointRegex *regexp.Regexp
	if len(config.Points.Regex) > 0 {
		reg, err := regexp.Compile(config.Points.Regex)
		if err != nil {
			return nil, fmt.Errorf("invalid points regex: %w", err)
		}
		pointRegex = reg
	}

	r := reader{
		server:      config.Connect.Da.Server,
		nodes:       config.Connect.Da.Nodes,
		interval:    time.Duration(config.Collect.Interval) * time.Second,
		pointLimit:  config.Points.Limit,
		pointRegex:  pointRegex,
		done:        make(chan struct{}, 1),
		containsBad: config.Collect.ContainsBad,
		debug:       config.Debug,
	}

	ctx := context.Background()
	allTags, err := r.getAllTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("get all da node error %v", err)
	}
	tagName := make(map[string]string, len(allTags))
	for _, tag := range allTags {
		tagName[tag.ID] = tag.Name
	}

	tags := make(map[string]*daTag, len(config.Collect.Da.Tags))
	for _, tag := range config.Collect.Da.Tags {
		tags[tag.Tag] = &daTag{tag: tag.Tag, name: tagName[tag.Tag]}
	}
	r.tags = tags
	if config.Collect.Dump.Enable {
		dumper, err := connector.NewCsvDumper(config.Collect.Dump.Path, config.Collect.Dump.Keep)
		if err != nil {
			return nil, fmt.Errorf("failed to create dump file: %w", err)
		}

		r.dumper = dumper
	}

	return &r, nil
}

func (r *reader) connect(_ context.Context) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.state == connected {
		return nil
	}

	if r.debug {
		opc.Debug()
		logger.Debug("## Connecting to OPC DA server", "server", r.server, "nodes", r.nodes, "tags", r.tags)
	}

	r.state = connecting

	if r.client != nil {
		logger.Warn("## Closing connector due to Connection already instantiated")
		r.client.Close()
	}

	tags := make([]string, 0, len(r.tags))
	for _, t := range r.tags {
		tags = append(tags, t.tag)
	}
	conn, err := opc.NewConnection(r.server, r.nodes, tags)
	if err != nil {
		return fmt.Errorf("connect to opc da error. %v", err)
	}
	r.client = conn
	r.state = connected
	return nil
}

func (r *reader) disconnect() {
	if r.client != nil {
		r.client.Close()
	}
	r.client = nil
	r.state = disconnected
}

func (r *reader) stop(_ context.Context) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	defer close(r.done)

	r.disconnect()
	r.dumper.Close()
}

func (r *reader) ensureConnect(ctx context.Context) error {
	if r.state != connected || r.client == nil {
		if err := r.connect(ctx); err != nil {
			logger.Error("## ensureConnect error", "err", err)
			return err
		}
	}
	return nil
}

func (r *reader) read(ctx context.Context) (<-chan *common.NodeValue, error) {
	if len(r.nodes) == 0 {
		return nil, errors.New("config error. da nodes is null")
	}
	if len(r.tags) == 0 {
		return nil, errors.New("config error. tags is null")
	}
	if err := r.ensureConnect(ctx); err != nil {
		return nil, err
	}

	ch := make(chan *common.NodeValue, len(r.tags))

	go func() {
		defer close(ch)
		notifyCtx, _ := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)

		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			select {
			case <-r.done:
				return
			case <-notifyCtx.Done():
				return
			case <-ticker.C:
				values := r.readItems(r.tags)
				for _, val := range values {
					ch <- val
				}
			}
		}
	}()

	return ch, nil
}

func (r *reader) readItems(tags map[string]*daTag) (values []*common.NodeValue) {
	items := r.client.Read()
	values = make([]*common.NodeValue, 0, len(items))
	for id, item := range items {
		if !item.Good() && !r.containsBad {
			logger.WarnF("## read data for identifier %q status %v is not ok ", id, item)
			continue
		}

		value := item.Value
		valueType := tags[id].valueType
		if valueType == common.Invalid {
			t := reflect.TypeOf(value).Kind()
			var err error
			valueType, err = toValueType(t)
			if err != nil {
				logger.Warn("## read data for identifier %q. value type %T is not supported", id, value)
				continue
			}
			tags[id].valueType = valueType
		}

		nodeValue := &common.NodeValue{
			Identifier: id,
			Name:       tags[id].name,
			Timestamp:  item.Timestamp,
			Now:        time.Now(),
			Value:      value,
			ValueType:  valueType,
			Status:     int64(uint32(item.Quality)),
		}

		if err := r.dump(nodeValue); err != nil {
			logger.Error("## dump data error", "err", err)
			panic(fmt.Errorf("dump data error. %v", err))
		}

		values = append(values, nodeValue)
	}
	return values
}

func (r *reader) getAllTags(ctx context.Context) ([]common.Point, error) {
	if err := r.ensureConnect(ctx); err != nil {
		return nil, err
	}
	defer r.disconnect() // disconnect after get all tags

	tree, err := opc.CreateBrowser(r.server, r.nodes)
	if err != nil {
		return nil, fmt.Errorf("get all tags error. create browser error %v", err)
	}

	tags := r.browse(tree)

	return tags, nil
}

func (r *reader) browse(tree *opc.Tree) (points []common.Point) {
	l := list.New()
	l.PushBack(tree)

	for {
		front := l.Front()
		if front == nil {
			break
		}

		t := l.Remove(front).(*opc.Tree)
		for _, leave := range t.Leaves {
			if r.pointRegex != nil && !(r.pointRegex.MatchString(leave.Name) || r.pointRegex.MatchString(leave.Tag)) {
				continue
			}

			points = append(points, common.Point{
				ID:   leave.Tag,
				Name: leave.Name,
			})
			if r.pointLimit > 0 && len(points) >= r.pointLimit {
				return
			}
		}
		for _, b := range t.Branches {
			l.PushBack(b)
		}
	}
	return
}

func (r *reader) dump(value *common.NodeValue) error {
	if r.dumper == nil {
		return nil
	}

	return r.dumper.Dump(value)
}

func toValueType(k reflect.Kind) (common.ValueType, error) {
	switch k {
	case reflect.Bool:
		return common.BOOL, nil
	case reflect.Int:
		return common.INT, nil
	case reflect.Int8:
		return common.TINYINT, nil
	case reflect.Int16:
		return common.SMALLINT, nil
	case reflect.Int32:
		return common.INT, nil
	case reflect.Int64:
		return common.BIGINT, nil
	case reflect.Uint:
		return common.INTUNSIGNED, nil
	case reflect.Uint8:
		return common.TINYINTUNSIGNED, nil
	case reflect.Uint16:
		return common.SMALLINTUNSIGNED, nil
	case reflect.Uint32:
		return common.INTUNSIGNED, nil
	case reflect.Uint64:
		return common.BIGINTUNSIGNED, nil
	case reflect.Float32:
		return common.FLOAT, nil
	case reflect.Float64:
		return common.DOUBLE, nil
	case reflect.String:
		return common.VARCHAR, nil
	default:
		return common.Invalid, fmt.Errorf("unsupported type %s", k.String())
	}
}

func opcDaStatusString(status int64) string {
	switch status {
	case 0:
		return "Bad"
	case 192, 216:
		return "Good"
	case 64:
		return "Uncertain"
	case 6:
		return "Disconnected"
	case 2:
		return "Failed"
	case 3:
		return "Noconfig"
	case 1:
		return "Running"
	case 4:
		return "Suspended"
	case 5:
		return "Test"
	default:
		return "Unknown"
	}
}
