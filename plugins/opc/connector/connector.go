package connector

import (
	"collector/common"
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gopcua/opcua/ua"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sunpe/gobox/logger"
)

type Connector interface {
	Stop(ctx context.Context)
	Collect(ctx context.Context) (<-chan *common.NodeValue, error)
	GetAllPoints(ctx context.Context) ([]common.Point, error)
}

type CsvDumper struct {
	rotator *rotatelogs.RotateLogs
	writer  *csv.Writer
}

func NewCsvDumper(path string, keep int64) (*CsvDumper, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("invalid path")
	}
	rotate, err := rotatelogs.New(path+".%Y%m%d%H%M",
		rotatelogs.WithMaxAge(time.Duration(keep)*24*time.Hour),
		rotatelogs.WithRotationTime(time.Hour))
	if err != nil {
		return nil, fmt.Errorf("failed to create dump file: %w", err)
	}
	writer := csv.NewWriter(rotate)
	return &CsvDumper{rotator: rotate, writer: writer}, nil
}

func (c *CsvDumper) Dump(value *common.NodeValue) error {
	if value == nil {
		return nil
	}

	item := []string{
		value.Identifier,                                 // id
		value.Name,                                       // name
		value.Now.Format(time.RFC3339Nano),               // now
		value.Timestamp.Format(time.RFC3339Nano),         // timestamp
		fmt.Sprintf("%v", value.Value),                   // value
		value.ValueType.String(),                         // value type
		ua.StatusCodes[ua.StatusCode(value.Status)].Name, // status
	}

	return c.writer.Write(item)
}

func (c *CsvDumper) Close() {
	c.writer.Flush()
	_ = c.rotator.Close()
}

type FakeConnector struct {
	config common.CollectConfig
	done   chan struct{}
	once   sync.Once
}

func NewFakeConnector(config common.CollectConfig) *FakeConnector {
	return &FakeConnector{config: config, done: make(chan struct{}, 1)}
}

func (f *FakeConnector) Stop(_ context.Context) {
	f.once.Do(func() {
		close(f.done)
		logger.Warn("## fake connector stopped!")
	})
}

func (f *FakeConnector) Collect(ctx context.Context) (<-chan *common.NodeValue, error) {
	ch := make(chan *common.NodeValue, 1)
	points, err := f.getAllNodes()
	if err != nil {
		return nil, err
	}

	go func() {
		notifyCtx, _ := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
		defer close(ch)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-notifyCtx.Done():
				return
			case <-ticker.C:
				for _, point := range points {
					v := common.NodeValue{
						Identifier: point.id,
						Timestamp:  time.Now(),
						Now:        time.Now(),
						Value:      fakeValue(point.valueType),
						ValueType:  point.valueType,
					}

					ch <- &v
				}
			case <-f.done:
				return
			}
		}
	}()

	return ch, nil
}

type fakePoint struct {
	id        string
	valueType common.ValueType
}

func (f *FakeConnector) getAllNodes() ([]fakePoint, error) {
	points := make([]fakePoint, 0, len(f.config.Ua.Nodes)+len(f.config.Da.Tags))
	if len(f.config.Ua.Nodes) > 0 {
		for _, node := range f.config.Ua.Nodes {
			vt := f.randomValueType()
			points = append(points, fakePoint{
				id:        node.ID,
				valueType: vt,
			})
		}
	}

	if len(f.config.Da.Tags) > 0 {
		for _, tag := range f.config.Da.Tags {
			vt := f.randomValueType()
			points = append(points, fakePoint{
				id:        tag.Tag,
				valueType: vt,
			})
		}
	}

	return points, nil
}

func (f *FakeConnector) randomValueType() common.ValueType {
	return common.ValueType(rand.Int()%15 + 1)
}

func (f *FakeConnector) GetAllPoints(_ context.Context) ([]common.Point, error) {
	panic("implement me")
}

func fakeValue(valueType common.ValueType) any {
	switch valueType {
	case common.TIMESTAMP:
		return time.Now()
	case common.INT:
		return rand.Int()
	case common.INTUNSIGNED:
		return rand.Uint32()
	case common.BIGINT:
		return rand.Int63()
	case common.BIGINTUNSIGNED:
		return rand.Uint64()
	case common.FLOAT:
		return rand.Float32()
	case common.DOUBLE:
		return rand.Float64()
	case common.BINARY:
		return []byte("binary value")
	case common.SMALLINT:
		i := rand.Int()
		if i > math.MaxInt16 {
			i = 0
		}
		return int16(i)
	case common.SMALLINTUNSIGNED:
		i := rand.Uint32()
		if i > math.MaxUint16 {
			i = 0
		}
		return uint16(i)
	case common.TINYINT:
		i := rand.Int()
		if i > math.MaxInt8 {
			i = 0
		}
		return int8(i)
	case common.TINYINTUNSIGNED:
		i := rand.Uint32()
		if i > math.MaxUint8 {
			i = 0
		}
		return uint8(i)
	case common.BOOL:
		return rand.Int()%2 == 0
	case common.NCHAR:
		return "narchar value"
	case common.JSON:
		return `{"a":1,"b":2}`
	case common.VARCHAR:
		return "varchar value"
	}
	return nil
}

var _ Connector = (*FakeConnector)(nil)
