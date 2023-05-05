package reporter

import (
	"collector/common"
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type writer interface {
	write(ctx context.Context) error
	close(ctx context.Context) error
}

func NewArrowWriter(address *net.TCPAddr, debug bool, batchSize int, batchTimeout time.Duration, schema *arrow.Schema, af appendFunc, ch chan *common.NodeValue) (*ArrowWriter, error) {
	conn, err := net.DialTCP("tcp", nil, address)
	if err != nil {
		return nil, fmt.Errorf("create arrow writer error %v", err)
	}
	return &ArrowWriter{
		debug:        debug,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		schema:       schema,
		ipcWriter:    ipc.NewWriter(conn, ipc.WithSchema(schema), ipc.WithAllocator(memory.NewGoAllocator())),
		af:           af,
		ch:           ch,
		done:         make(chan struct{}, 1),
		closeWriter:  make(chan struct{}, 1),
	}, nil
}

type ArrowWriter struct {
	debug        bool
	batchSize    int
	batchTimeout time.Duration
	schema       *arrow.Schema          // arrow schema
	ipcWriter    *ipc.Writer            // arrow ipc writer
	af           appendFunc             // append value to arrow field
	ch           chan *common.NodeValue // read node value from channel
	done         chan struct{}
	closeWriter  chan struct{}
}

var _ writer = (*ArrowWriter)(nil)

// write record to arrow ipc writer
// read data from channel
// and pack to arrow record
// and write to arrow ipc writer
// when batch size or batch timeout reached
func (w *ArrowWriter) write(ctx context.Context) (err error) {
	if w.debug {
		log.Printf("## start write arrow writer %p", w)
	}

	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()

	values := make([]*common.NodeValue, 0, w.batchSize)
	defer func() {
		defer close(w.closeWriter)
		_ = w.doWrite(ctx, values)
	}()

	for {
		select {
		case <-w.done:
			return
		case <-ctx.Done():
			return
		case value, ok := <-w.ch:
			if !ok {
				if w.debug {
					log.Printf("## value channel closed. exit!")
				}
				return
			}
			values = append(values, value)
			if len(values) < w.batchSize {
				continue
			}
			if err = w.doWrite(ctx, values); err != nil {
				log.Printf("## write record error %v", err)
				return
			}
			values = make([]*common.NodeValue, 0, w.batchSize)
		case <-ticker.C:
			if err = w.doWrite(ctx, values); err != nil {
				log.Printf("## write record error %v", err)
				return
			}
			values = make([]*common.NodeValue, 0, w.batchSize)
		}
	}
}

func (w *ArrowWriter) close(_ context.Context) error {
	close(w.done)
	<-w.closeWriter
	if w.debug {
		log.Printf("## close arrow writer %p", w)
		log.Printf("## close ipc writer instance %p", w)
	}
	return w.ipcWriter.Close()
}

func (w *ArrowWriter) doWrite(ctx context.Context, values []*common.NodeValue) error {
	if len(values) == 0 {
		return nil
	}
	record, err := w.pack(ctx, values)
	if err != nil {
		log.Printf("## pack record error %v", err)
		return err
	}

	if err := w.writeRecord(ctx, record); err != nil {
		log.Printf("## write record error %v", err)
		return err
	}
	return nil
}

func (w *ArrowWriter) pack(_ context.Context, values []*common.NodeValue) (record arrow.Record, err error) {
	return packData(values, w.schema, w.af)
}

func (w *ArrowWriter) writeRecord(_ context.Context, record arrow.Record) (err error) {
	if w.debug {
		j, _ := record.MarshalJSON()
		log.Printf("## report to taosx by writer [%p] values [%s]", w, string(j))
	}
	return w.ipcWriter.Write(record)
}
