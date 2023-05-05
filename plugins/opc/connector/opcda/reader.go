//go:build windows
// +build windows

package opcda

import (
	"collector/common"
	"context"
	"errors"
	"fmt"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/konimarti/opc"
)

// state connection state
type state int

const (
	_ state = iota
	connected
	connecting
	disconnected
)

type reader struct {
	client     opc.Connection
	server     string
	nodes      []string
	tags       []string
	valueTypes map[string]common.ValueType
	state      state
	interval   time.Duration
	mutex      sync.Mutex
	done       chan struct{}
	debug      bool
}

func newReader(config common.Config) (*reader, error) {
	if err := config.Connect.Da.Validate(); err != nil {
		return nil, fmt.Errorf("create opc da connector error %v", err)
	}

	if config.Collect.Interval <= 0 {
		config.Collect.Interval = 1
	}

	tags := make([]string, 0, len(config.Collect.Da.Tags))
	valueTypes := make(map[string]common.ValueType, len(config.Collect.Da.Tags))
	for _, tag := range config.Collect.Da.Tags {
		tags = append(tags, tag.Tag)
		vt, err := common.ValueTypeFromString(tag.ValueType)
		if err != nil {
			return nil, fmt.Errorf("create opc da connector error %v", err)
		}
		valueTypes[tag.Tag] = vt
	}

	r := reader{
		server:     config.Connect.Da.Server,
		nodes:      config.Connect.Da.Nodes,
		valueTypes: valueTypes,
		tags:       tags,
		interval:   time.Duration(config.Collect.Interval) * time.Second,
		done:       make(chan struct{}, 1),
		debug:      config.Debug,
	}
	return &r, nil
}

func (r *reader) connect(_ context.Context) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.debug {
		opc.Debug()
		log.Println("## Connecting to OPC DA server", "server", r.server, "nodes", r.nodes, "tags", r.tags)
	}

	r.state = connecting

	if r.client != nil {
		log.Println("## Closing connector due to Connection already instantiated")
		r.client.Close()
	}

	conn, err := opc.NewConnection(r.server, r.nodes, r.tags)
	if err != nil {
		return fmt.Errorf("connect to opc da error. %v", err)
	}
	r.client = conn
	r.state = connected
	return nil
}

func (r *reader) stop(_ context.Context) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	defer close(r.done)

	if r.client != nil {
		r.client.Close()
	}
	r.client = nil
	r.state = disconnected
}

func (r *reader) ensureConnect(ctx context.Context) error {
	if r.state != connected || r.client == nil {
		if err := r.connect(ctx); err != nil {
			log.Println("## ensureConnect error", "err", err)
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
				for id, item := range r.client.Read() {
					if r.debug {
						log.Printf("## read data for identifier. id %s. item %v, value type %T", id, item, item.Value)
					}
					if !item.Good() {
						log.Printf("## read data for identifier %q status %v is not ok ", id, item)
						continue
					}
					ch <- &common.NodeValue{
						Identifier: id,
						Timestamp:  item.Timestamp,
						Now:        time.Now(),
						Value:      item.Value,
						ValueType:  r.valueTypes[id],
					}
				}
			}
		}
	}()

	return ch, nil
}

func (r *reader) getAllTags(ctx context.Context) ([]common.Point, error) {
	if err := r.ensureConnect(ctx); err != nil {
		return nil, err
	}
	tree, err := opc.CreateBrowser(r.server, r.nodes)
	if err != nil {
		return nil, fmt.Errorf("create browser error %v", err)
	}

	return r.browseRecursive(tree), nil
}

func (r *reader) browseRecursive(tree *opc.Tree) []common.Point {
	tags := make([]common.Point, 0, len(tree.Leaves))
	for _, l := range tree.Leaves {
		tags = append(tags, common.Point{
			ID:   l.Tag,
			Name: l.Name,
		})
	}

	for _, l := range tree.Branches {
		branchTags := r.browseRecursive(l)
		tags = append(tags, branchTags...)
	}

	return tags
}
