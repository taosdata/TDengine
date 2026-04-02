package unified

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// BenchmarkFindPendingRequest measures benchmark performance for this scenario.
func BenchmarkFindPendingRequest(b *testing.B) {
	sizes := []int{1, 64, 512, 4096}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("hit_size_%d", size), func(b *testing.B) {
			c := &Client{
				pendingRequests: make(map[uint64]*pendingRequest, size),
			}
			for i := 1; i <= size; i++ {
				registerPendingRequestForTest(c, &pendingRequest{
					reqID:   uint64(i),
					channel: make(chan []byte, 1),
				})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reqID := uint64((i % size) + 1)
				if !pendingRequestExistsForTest(c, reqID) {
					b.Fatalf("pending req %d not found", reqID)
				}
			}
		})

		b.Run(fmt.Sprintf("miss_size_%d", size), func(b *testing.B) {
			c := &Client{
				pendingRequests: make(map[uint64]*pendingRequest, size),
			}
			for i := 1; i <= size; i++ {
				registerPendingRequestForTest(c, &pendingRequest{
					reqID:   uint64(i),
					channel: make(chan []byte, 1),
				})
			}
			missReqID := uint64(size + 1)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if pendingRequestExistsForTest(c, missReqID) {
					b.Fatalf("unexpected hit for req %d", missReqID)
				}
			}
		})
	}
}

// BenchmarkHandleMessageRoute measures benchmark performance for this scenario.
func BenchmarkHandleMessageRoute(b *testing.B) {
	c := &Client{
		pendingRequests: make(map[uint64]*pendingRequest, 1024),
	}
	message := []byte(`{"req_id":1,"code":0,"action":"query"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqID := uint64(i + 1)
		ch := make(chan []byte, 1)

		registerPendingRequestForTest(c, &pendingRequest{reqID: reqID, channel: ch})
		c.handleMessage(message, reqID)
		if got := <-ch; len(got) == 0 {
			b.Fatalf("empty routed message")
		}
	}
}

// BenchmarkExtractReqIDFromTextMessage measures benchmark performance for this scenario.
func BenchmarkExtractReqIDFromTextMessage(b *testing.B) {
	message := []byte(`{"code":0,"message":"","action":"query","req_id":123456789}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqID, err := extractReqIDFromTextMessage(message)
		if err != nil {
			b.Fatal(err)
		}
		if reqID != 123456789 {
			b.Fatalf("unexpected req_id %d", reqID)
		}
	}
}

// BenchmarkBuildBinaryQueryRequestToBuffer measures benchmark performance for this scenario.
func BenchmarkBuildBinaryQueryRequestToBuffer(b *testing.B) {
	sql := "select ts,v from meters where ts > now - 1h and tbname like 'd%';"
	var buf bytes.Buffer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildBinaryQueryRequestToBuffer(&buf, uint64(i+1), sql)
		if buf.Len() == 0 {
			b.Fatalf("empty payload")
		}
	}
}

var benchmarkResultSetSink *ResultSet

// BenchmarkBuildResultSetFromQueryResp measures benchmark performance for this scenario.
func BenchmarkBuildResultSetFromQueryResp(b *testing.B) {
	columnSizes := []int{8, 64, 256}
	for _, n := range columnSizes {
		b.Run(fmt.Sprintf("cols_%d", n), func(b *testing.B) {
			names := make([]string, n)
			types := make([]uint8, n)
			lengths := make([]int64, n)
			precisions := make([]int64, n)
			scales := make([]int64, n)
			for i := 0; i < n; i++ {
				names[i] = fmt.Sprintf("c%d", i)
				types[i] = common.TSDB_DATA_TYPE_INT
				lengths[i] = 4
				precisions[i] = 0
				scales[i] = 0
			}

			resp := &proto.WSQueryResp{
				ID:               88,
				FieldsCount:      n,
				FieldsNames:      names,
				FieldsTypes:      types,
				FieldsLengths:    lengths,
				FieldsPrecisions: precisions,
				FieldsScales:     scales,
				Precision:        0,
			}
			c := &Client{config: Config{}}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rs := buildResultSetFromQueryResp(c, nil, 1, resp)
				if rs == nil {
					b.Fatal("nil result set")
				}
				benchmarkResultSetSink = rs
			}
		})
	}
}
