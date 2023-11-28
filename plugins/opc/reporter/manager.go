package reporter

import (
	"collector/config"
	"collector/types"
	"context"
	"hash/fnv"
	"sync"
	"time"
)

type Manager struct {
	tagReporterMap sync.Map // map[string]*ArrowReporter
	reporters      map[types.ValueType][]*ArrowReporter
	concurrent     int
	ctx            context.Context
	locker         sync.RWMutex
	conf           config.ReportConfig
}

func NewManager(ctx context.Context, conf config.ReportConfig) *Manager {
	concurrent := conf.Concurrent
	if concurrent == 0 {
		concurrent = 1
	}
	return &Manager{
		ctx:        ctx,
		concurrent: concurrent,
		reporters:  make(map[types.ValueType][]*ArrowReporter),
		conf:       conf,
	}
}

func (m *Manager) GetReporter(tag string, vt types.ValueType) (*ArrowReporter, error) {
	v, exist := m.tagReporterMap.Load(tag)
	if exist {
		return v.(*ArrowReporter), nil
	}
	reporter, err := m.getOrInitReporter(tag, vt)
	if err != nil {
		return nil, err
	}
	m.tagReporterMap.Store(tag, reporter)
	return reporter, nil
}

func (m *Manager) getOrInitReporter(tag string, vt types.ValueType) (*ArrowReporter, error) {
	h := hash(tag) % uint32(m.concurrent)
	m.locker.RLock()
	v, exist := m.reporters[vt]
	if exist {
		m.locker.RUnlock()
		return v[h], nil
	} else {
		m.locker.RUnlock()
		m.locker.Lock()
		defer m.locker.Unlock()
		v, exist = m.reporters[vt]
		if exist {
			return v[h], nil
		} else {
			v = make([]*ArrowReporter, m.concurrent)
			for i := 0; i < m.concurrent; i++ {
				reporter, err := NewArrowReporter(m.ctx, i, m.conf.Remote, vt, m.conf.BatchSize, time.Duration(m.conf.BatchTimeout)*time.Second)
				if err != nil {
					return nil, err
				}
				reporter.startReceiveMessage()
				v[i] = reporter
			}
			m.reporters[vt] = v
			return v[h], nil
		}
	}
}

func hash(str string) (hash uint32) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(str))
	return h.Sum32()
}

func (m *Manager) Close() {
	for _, reporters := range m.reporters {
		for _, reporter := range reporters {
			reporter.Close()
		}
	}
}
