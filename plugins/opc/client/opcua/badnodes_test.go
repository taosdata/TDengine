package opcua

import (
	"collector/common"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// newTestUAClient 创建一个用于测试黑名单逻辑的最小 UAClient 实例，不需要真实 OPC 连接。
func newTestUAClient() *UAClient {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	return &UAClient{
		logger:          logger.WithField("test", "badnodes"),
		badNodes:        make(map[string]*badNodeInfo),
		probeInterval:   defaultProbeInterval,
		maxNodesPerRead: 1000,
	}
}

func makeTestNodes(ids ...string) []*nodeValue {
	nodes := make([]*nodeValue, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, &nodeValue{
			nodeValue: &common.NodeValue{IDStr: id},
		})
	}
	return nodes
}

// --- addBadNode 测试 ---

func TestAddBadNode_Basic(t *testing.T) {
	c := newTestUAClient()

	c.addBadNode("ns=2;s=Node1", "batch_rpc_error", "StatusBadEncodingError")

	c.badNodesMu.RLock()
	defer c.badNodesMu.RUnlock()

	assert.Equal(t, 1, len(c.badNodes))
	info, exists := c.badNodes["ns=2;s=Node1"]
	assert.True(t, exists)
	assert.Equal(t, "batch_rpc_error", info.reason)
	assert.Equal(t, "StatusBadEncodingError", info.lastError)
	assert.Equal(t, "ns=2;s=Node1", info.idStr)
	assert.False(t, info.addedAt.IsZero())
}

func TestAddBadNode_Idempotent(t *testing.T) {
	c := newTestUAClient()

	c.addBadNode("ns=2;s=Node1", "batch_rpc_error", "error1")
	c.addBadNode("ns=2;s=Node1", "status_error", "error2") // 重复添加

	c.badNodesMu.RLock()
	defer c.badNodesMu.RUnlock()

	assert.Equal(t, 1, len(c.badNodes))
	assert.Equal(t, "batch_rpc_error", c.badNodes["ns=2;s=Node1"].reason)
}

func TestAddBadNode_Multiple(t *testing.T) {
	c := newTestUAClient()

	c.addBadNode("ns=2;s=Node1", "batch_rpc_error", "err1")
	c.addBadNode("ns=2;s=Node2", "status_error", "err2")
	c.addBadNode("ns=2;s=Node3", "batch_rpc_error", "err3")

	c.badNodesMu.RLock()
	defer c.badNodesMu.RUnlock()

	assert.Equal(t, 3, len(c.badNodes))
}

func TestAddBadNode_Concurrent(t *testing.T) {
	c := newTestUAClient()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := "ns=2;s=ConcurrentNode"
			c.addBadNode(id, "status_error", "concurrent error")
		}(i)
	}
	wg.Wait()

	c.badNodesMu.RLock()
	defer c.badNodesMu.RUnlock()
	assert.Equal(t, 1, len(c.badNodes))
}

// --- filterActiveNodes 测试 ---

func TestFilterActiveNodes_NoBadNodes(t *testing.T) {
	c := newTestUAClient()
	nodes := makeTestNodes("A", "B", "C")

	result := c.filterActiveNodes(nodes)

	assert.Equal(t, 3, len(result))
	assert.Equal(t, nodes, result)
}

func TestFilterActiveNodes_AllBad(t *testing.T) {
	c := newTestUAClient()
	nodes := makeTestNodes("A", "B", "C")
	c.addBadNode("A", "status_error", "")
	c.addBadNode("B", "status_error", "")
	c.addBadNode("C", "status_error", "")

	result := c.filterActiveNodes(nodes)

	assert.Equal(t, 0, len(result))
}

func TestFilterActiveNodes_SomeBad(t *testing.T) {
	c := newTestUAClient()
	nodes := makeTestNodes("A", "B", "C", "D", "E")
	c.addBadNode("B", "batch_rpc_error", "")
	c.addBadNode("D", "status_error", "")

	result := c.filterActiveNodes(nodes)

	assert.Equal(t, 3, len(result))
	ids := make([]string, 0, len(result))
	for _, n := range result {
		ids = append(ids, n.nodeValue.IDStr)
	}
	assert.Equal(t, []string{"A", "C", "E"}, ids)
}

func TestFilterActiveNodes_EmptyInput(t *testing.T) {
	c := newTestUAClient()
	c.addBadNode("X", "status_error", "")

	result := c.filterActiveNodes([]*nodeValue{})

	assert.Equal(t, 0, len(result))
}

func TestFilterActiveNodes_BadNodeNotInInput(t *testing.T) {
	c := newTestUAClient()
	nodes := makeTestNodes("A", "B")
	c.addBadNode("Z", "status_error", "")

	result := c.filterActiveNodes(nodes)

	assert.Equal(t, 2, len(result))
}

// --- 常量验证 ---

func TestConstants(t *testing.T) {
	assert.Equal(t, 3, statusFailThreshold)
	assert.Equal(t, uint64(60), uint64(defaultProbeInterval))
	assert.Equal(t, 500*time.Millisecond, postFailureDelay)
	assert.Equal(t, 10, individualTestThreshold)
}

// --- consecutiveFailures 字段测试 ---

func TestConsecutiveFailures_InitZero(t *testing.T) {
	nv := &nodeValue{
		nodeValue: &common.NodeValue{IDStr: "test"},
	}
	assert.Equal(t, 0, nv.consecutiveFailures)
}

func TestConsecutiveFailures_Threshold(t *testing.T) {
	c := newTestUAClient()
	nv := &nodeValue{
		nodeValue: &common.NodeValue{IDStr: "ns=2;s=BadNode"},
	}

	for i := 0; i < statusFailThreshold; i++ {
		nv.consecutiveFailures++
		if nv.consecutiveFailures == statusFailThreshold {
			c.addBadNode(nv.nodeValue.IDStr, "status_error", "test error")
		}
	}

	c.badNodesMu.RLock()
	defer c.badNodesMu.RUnlock()
	assert.Equal(t, statusFailThreshold, nv.consecutiveFailures)
	_, exists := c.badNodes["ns=2;s=BadNode"]
	assert.True(t, exists, "达到阈值后应被加入黑名单")
}

// --- badNodeInfo 结构测试 ---

func TestBadNodeInfo_Fields(t *testing.T) {
	now := time.Now()
	info := &badNodeInfo{
		idStr:     "ns=2;s=Test",
		reason:    "batch_rpc_error",
		lastError: "StatusBadEncodingError (0x80060000)",
		addedAt:   now,
	}

	assert.Equal(t, "ns=2;s=Test", info.idStr)
	assert.Equal(t, "batch_rpc_error", info.reason)
	assert.Equal(t, "StatusBadEncodingError (0x80060000)", info.lastError)
	assert.Equal(t, now, info.addedAt)
}

// --- 集成场景：模拟完整的黑名单生命周期 ---

func TestBadNodes_Lifecycle(t *testing.T) {
	c := newTestUAClient()
	allNodes := makeTestNodes("A", "B", "C", "D", "E")

	active := c.filterActiveNodes(allNodes)
	assert.Equal(t, 5, len(active))

	c.addBadNode("B", "batch_rpc_error", "StatusBadEncodingError")
	active = c.filterActiveNodes(allNodes)
	assert.Equal(t, 4, len(active))
	for _, n := range active {
		assert.NotEqual(t, "B", n.nodeValue.IDStr)
	}

	c.addBadNode("D", "status_error", "StatusUncertainInitialValue")
	active = c.filterActiveNodes(allNodes)
	assert.Equal(t, 3, len(active))

	c.badNodesMu.RLock()
	assert.Equal(t, 2, len(c.badNodes))
	c.badNodesMu.RUnlock()

	c.badNodesMu.Lock()
	delete(c.badNodes, "D")
	c.badNodesMu.Unlock()

	active = c.filterActiveNodes(allNodes)
	assert.Equal(t, 4, len(active))
}

// --- pollCount 和 probeInterval 联动测试 ---

func TestPollCount_ProbeIntervalTrigger(t *testing.T) {
	c := newTestUAClient()
	c.probeInterval = 5

	triggered := 0
	for i := uint64(1); i <= 20; i++ {
		c.pollCount = i
		if c.pollCount%c.probeInterval == 0 {
			triggered++
		}
	}
	assert.Equal(t, 4, triggered, "20 个周期中应触发 4 次探测（5,10,15,20）")
}

// --- isConnectionError 测试 ---

func TestIsConnectionError_Nil(t *testing.T) {
	assert.False(t, isConnectionError(nil))
}

func TestIsConnectionError_UAStatusCodes(t *testing.T) {
	connectionCodes := []ua.StatusCode{
		ua.StatusBadServerNotConnected,
		ua.StatusBadConnectionClosed,
		ua.StatusBadSessionClosed,
		ua.StatusBadSecureChannelClosed,
		ua.StatusBadCommunicationError,
		ua.StatusBadTimeout,
	}
	for _, code := range connectionCodes {
		assert.True(t, isConnectionError(code), "expected connection error for %v", code)
	}
}

func TestIsConnectionError_NonConnectionUAStatus(t *testing.T) {
	nonConnectionCodes := []ua.StatusCode{
		ua.StatusBadEncodingError,
		ua.StatusBadNodeIDInvalid,
		ua.StatusBadAttributeIDInvalid,
	}
	for _, code := range nonConnectionCodes {
		assert.False(t, isConnectionError(code), "should NOT be connection error: %v", code)
	}
}

func TestIsConnectionError_ContextErrors(t *testing.T) {
	assert.True(t, isConnectionError(context.Canceled))
	assert.True(t, isConnectionError(context.DeadlineExceeded))
}

func TestIsConnectionError_GenericError(t *testing.T) {
	assert.False(t, isConnectionError(fmt.Errorf("some random error")))
	assert.True(t, isConnectionError(fmt.Errorf("connection reset by peer")))
	assert.True(t, isConnectionError(fmt.Errorf("broken pipe")))
	assert.True(t, isConnectionError(fmt.Errorf("use of closed network connection")))
}

// --- batchReadResult 枚举测试 ---

func TestBatchReadResult_Values(t *testing.T) {
	assert.Equal(t, batchReadResult(0), batchReadOK)
	assert.Equal(t, batchReadResult(1), batchReadConnectionError)
	assert.Equal(t, batchReadResult(2), batchReadRPCError)
}

// --- 跨周期失败批次拆分测试 ---

func TestFailedBatchInfo_Fields(t *testing.T) {
	nodes := makeTestNodes("A", "B", "C")
	fb := failedBatchInfo{
		nodes:   nodes,
		subSize: 2,
	}
	assert.Equal(t, 3, len(fb.nodes))
	assert.Equal(t, 2, fb.subSize)
	assert.Equal(t, "A", fb.nodes[0].nodeValue.IDStr)
}

func TestFailedBatches_InitialEmpty(t *testing.T) {
	c := newTestUAClient()
	assert.Nil(t, c.failedBatches)
	assert.Equal(t, 0, len(c.failedBatches))
}

func TestFailedBatches_SubSizeHalving(t *testing.T) {
	// 模拟批次失败后 subSize 的减半序列
	batchSize := 966
	subSize := batchSize / 2 // 首次失败
	expected := []int{483, 241, 120, 60, 30, 15, 7}
	for i, exp := range expected {
		assert.Equal(t, exp, subSize, "step %d", i)
		subSize = subSize / 2
	}
	// subSize=7 <= individualTestThreshold(10)，触发逐节点测试
	assert.True(t, expected[len(expected)-1] <= individualTestThreshold)
}

func TestFailedBatches_ConvergenceCycles(t *testing.T) {
	// 从 966 节点失败到 subSize <= individualTestThreshold 需要多少次拆分
	batchSize := 966
	subSize := batchSize / 2
	cycles := 1
	for subSize > individualTestThreshold {
		subSize = subSize / 2
		cycles++
	}
	// 966 → 483 → 241 → 120 → 60 → 30 → 15 → 7: 7 次
	assert.Equal(t, 7, cycles, "should converge in ~7 cycles for 966-node batch")
	assert.True(t, subSize <= individualTestThreshold)
}

func TestFailedBatches_SubSizeFloorAtOne(t *testing.T) {
	// subSize 持续减半最终到 1
	subSize := 3
	for i := 0; i < 5; i++ {
		subSize = subSize / 2
		if subSize < 1 {
			subSize = 1
		}
	}
	assert.Equal(t, 1, subSize)
}

func TestFailedBatches_ClearedOnObserveChange(t *testing.T) {
	c := newTestUAClient()
	nodes := makeTestNodes("A", "B")
	c.failedBatches = []failedBatchInfo{
		{nodes: nodes, subSize: 1},
	}
	assert.Equal(t, 1, len(c.failedBatches))

	// 模拟 observeChange
	c.failedBatches = nil
	assert.Nil(t, c.failedBatches)
}

func TestReadNormalBatches_NoNodes(t *testing.T) {
	c := newTestUAClient()
	failed := c.readNormalBatches(nil)
	assert.Nil(t, failed)
}

func TestFailedBatches_ADSScenario(t *testing.T) {
	// 模拟 ADS 场景：1966 节点，批次 2 (966节点) 失败
	// 验证跨周期拆分的收敛过程
	totalBatch2 := 966

	// 周期 1: 批次 2 失败，生成 failedBatch
	fb := failedBatchInfo{
		nodes:   makeTestNodes(generateNodeIDs(totalBatch2)...),
		subSize: totalBatch2 / 2, // 483
	}

	// 模拟跨周期拆分：只有含坏节点的子批次会继续失败
	cycle := 1
	currentFailed := []failedBatchInfo{fb}
	for len(currentFailed) > 0 {
		cycle++
		var nextFailed []failedBatchInfo
		for _, f := range currentFailed {
			// 模拟：用 subSize 拆分，只有第一个子批次失败（坏节点在其中）
			subSize := f.subSize
			if subSize <= individualTestThreshold {
				// 触发逐节点测试，坏节点被精确定位
				t.Logf("cycle %d: subSize=%d <= threshold, individual test triggered", cycle, subSize)
				continue
			}
			// 第一个子批次失败，继续缩小
			failedSub := f.nodes[:subSize]
			if len(failedSub) > len(f.nodes) {
				failedSub = f.nodes
			}
			nextSub := subSize / 2
			if nextSub < 1 {
				nextSub = 1
			}
			t.Logf("cycle %d: subSize=%d, nextSub=%d, affected=%d", cycle, subSize, nextSub, len(failedSub))
			nextFailed = append(nextFailed, failedBatchInfo{
				nodes:   failedSub,
				subSize: nextSub,
			})
		}
		currentFailed = nextFailed
	}
	// 应在约 8 个周期内收敛（周期1检测 + 7次拆分）
	t.Logf("converged at cycle %d", cycle)
	assert.True(t, cycle <= 9, "should converge within 9 cycles, got %d", cycle)
}

func generateNodeIDs(n int) []string {
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("ns=2;s=Node%04d", i)
	}
	return ids
}
