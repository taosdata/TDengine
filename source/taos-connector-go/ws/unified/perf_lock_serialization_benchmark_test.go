package unified

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// BenchmarkPendingRegisterRemoveSerial measures benchmark performance for this scenario.
func BenchmarkPendingRegisterRemoveSerial(b *testing.B) {
	c := &Client{
		pendingRequests: make(map[uint64]*pendingRequest, 1024),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqID := uint64(i + 1)
		req := &pendingRequest{reqID: reqID, channel: make(chan []byte, 1)}
		registerPendingRequestForTest(c, req)
		c.removePendingRequest(reqID, req)
	}
}

// BenchmarkPendingRegisterRemoveParallel measures benchmark performance for this scenario.
func BenchmarkPendingRegisterRemoveParallel(b *testing.B) {
	c := &Client{
		pendingRequests: make(map[uint64]*pendingRequest, 1024),
	}
	var reqIDCounter uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reqID := atomic.AddUint64(&reqIDCounter, 1)
			req := &pendingRequest{reqID: reqID, channel: make(chan []byte, 1)}
			registerPendingRequestForTest(c, req)
			c.removePendingRequest(reqID, req)
		}
	})
}

// BenchmarkFindPendingRequestParallel measures benchmark performance for this scenario.
func BenchmarkFindPendingRequestParallel(b *testing.B) {
	sizes := []int{64, 512, 4096}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			c := &Client{
				pendingRequests: make(map[uint64]*pendingRequest, size),
			}
			for i := 1; i <= size; i++ {
				registerPendingRequestForTest(c, &pendingRequest{
					reqID:   uint64(i),
					channel: make(chan []byte, 1),
				})
			}
			var seq uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					idx := int(atomic.AddUint64(&seq, 1)%uint64(size)) + 1
					reqID := uint64(idx)
					if !pendingRequestExistsForTest(c, reqID) {
						b.Fatalf("pending req %d not found", reqID)
					}
				}
			})
		})
	}
}

// BenchmarkHandleMessageParallel measures benchmark performance for this scenario.
func BenchmarkHandleMessageParallel(b *testing.B) {
	c := &Client{
		pendingRequests: make(map[uint64]*pendingRequest, 1024),
	}
	message := []byte(`{"req_id":1,"code":0,"action":"query"}`)
	var reqIDCounter uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reqID := atomic.AddUint64(&reqIDCounter, 1)
			ch := make(chan []byte, 1)
			registerPendingRequestForTest(c, &pendingRequest{reqID: reqID, channel: ch})
			c.handleMessage(message, reqID)
			<-ch
		}
	})
}

// BenchmarkSwapRuntimePendingCleanup measures benchmark performance for this scenario.
func BenchmarkSwapRuntimePendingCleanup(b *testing.B) {
	sizes := []int{256, 1024, 4096, 16384}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("pending_%d", size), func(b *testing.B) {
			failoverState, err := newFailoverState([]string{"ws://127.0.0.1:6041/ws"})
			if err != nil {
				b.Fatal(err)
			}
			c := &Client{
				failover:        failoverState,
				pendingRequests: make(map[uint64]*pendingRequest, size),
				closeChan:       make(chan struct{}),
			}

			seedPending := func() {
				clearPendingRequestsForTest(c)
				for i := 0; i < size; i++ {
					registerPendingRequestForTest(c, &pendingRequest{
						reqID:   uint64(i + 1),
						channel: nil, // benchmark cleanup path without channel allocation cost
					})
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				seedPending()

				c.lock.Lock()
				c.runtimeGen = 0
				c.runtime = nil
				atomic.StoreUint32(&c.closedFlag, 0)
				c.lock.Unlock()

				nextRuntime := client.NewClient(nil, 1)
				if _, err = c.swapRuntime(nextRuntime, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMarshalWSConnectReq measures benchmark performance for this scenario.
func BenchmarkMarshalWSConnectReq(b *testing.B) {
	req := &proto.WSConnectReq{
		ReqID:       1,
		User:        "root",
		Password:    "taosdata",
		DB:          "benchmark_db",
		TZ:          "Asia/Shanghai",
		TOTPCode:    "123456",
		BearerToken: "token",
		App:         "bench",
		Connector:   "ws",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := client.JsonI.Marshal(req)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatalf("empty payload")
		}
	}
}

// BenchmarkMarshalWSActionWithArgs measures benchmark performance for this scenario.
func BenchmarkMarshalWSActionWithArgs(b *testing.B) {
	args, err := client.JsonI.Marshal(&proto.WSConnectReq{
		ReqID:       1,
		User:        "root",
		Password:    "taosdata",
		DB:          "benchmark_db",
		TZ:          "Asia/Shanghai",
		TOTPCode:    "123456",
		BearerToken: "token",
		App:         "bench",
		Connector:   "ws",
	})
	if err != nil {
		b.Fatal(err)
	}
	action := &client.WSAction{Action: "conn", Args: args}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := client.JsonI.Marshal(action)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatalf("empty payload")
		}
	}
}

// BenchmarkUnmarshalWSQueryResp measures benchmark performance for this scenario.
func BenchmarkUnmarshalWSQueryResp(b *testing.B) {
	payload := []byte(`{"code":0,"message":"","action":"query","req_id":100,"id":88,"is_update":false,"affected_rows":0,"fields_count":4,"fields_names":["ts","v","tag","city"],"fields_types":[9,4,8,10],"fields_lengths":[8,4,32,16],"precision":0,"fields_precisions":[0,0,0,0],"fields_scales":[0,0,0,0]}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var resp proto.WSQueryResp
		if err := client.JsonI.Unmarshal(payload, &resp); err != nil {
			b.Fatal(err)
		}
		if resp.ID == 0 {
			b.Fatalf("invalid response id")
		}
	}
}

// BenchmarkParseExecQueryResponseLite measures benchmark performance for this scenario.
func BenchmarkParseExecQueryResponseLite(b *testing.B) {
	payload := []byte(`{"code":0,"message":"","action":"query","req_id":100,"id":88,"is_update":false,"affected_rows":0,"fields_count":4,"fields_names":["ts","v","tag","city"],"fields_types":[9,4,8,10],"fields_lengths":[8,4,32,16],"precision":0,"fields_precisions":[0,0,0,0],"fields_scales":[0,0,0,0]}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := parseExecQueryResponse(payload)
		if err != nil {
			b.Fatal(err)
		}
		if resp == nil || resp.ID == 0 {
			b.Fatalf("invalid response")
		}
	}
}

// BenchmarkEncodeWSActionToEnvelopeBuffer measures benchmark performance for this scenario.
func BenchmarkEncodeWSActionToEnvelopeBuffer(b *testing.B) {
	req := &proto.SchemalessWriteRequest{
		ReqID:     1,
		Protocol:  1,
		Precision: "ns",
		TTL:       0,
		Data:      "measurement,host=host1 field1=2i 1577837300000",
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		b.Fatal(err)
	}
	action := &client.WSAction{Action: "insert", Args: args}
	env := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(env)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.Msg.Reset()
		if err := client.JsonI.NewEncoder(env.Msg).Encode(action); err != nil {
			b.Fatal(err)
		}
		if env.Msg.Len() == 0 {
			b.Fatalf("empty envelope")
		}
	}
}

// BenchmarkEncodeWSActionToEnvelopeBufferFast measures benchmark performance for this scenario.
func BenchmarkEncodeWSActionToEnvelopeBufferFast(b *testing.B) {
	req := &proto.SchemalessWriteRequest{
		ReqID:     1,
		Protocol:  1,
		Precision: "ns",
		TTL:       0,
		Data:      "measurement,host=host1 field1=2i 1577837300000",
	}
	args, err := client.JsonI.Marshal(req)
	if err != nil {
		b.Fatal(err)
	}
	env := client.GlobalEnvelopePool.Get()
	defer client.GlobalEnvelopePool.Put(env)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.Msg.Reset()
		if err := encodeWSActionToBuffer(env.Msg, "insert", args, true); err != nil {
			b.Fatal(err)
		}
		if env.Msg.Len() == 0 {
			b.Fatalf("empty envelope")
		}
	}
}
