package unified

import (
	"testing"

	"github.com/stretchr/testify/require"
	commontmq "github.com/taosdata/driver-go/v3/common/tmq"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// TestExtractTMQFetchRawPayloadBoundaries verifies short fetch-raw payloads are rejected.
func TestExtractTMQFetchRawPayloadBoundaries(t *testing.T) {
	_, err := extractTMQFetchRawPayload(nil)
	require.Error(t, err)

	_, err = extractTMQFetchRawPayload(make([]byte, tmqFetchRawPayloadOffset))
	require.Error(t, err)

	resp := append(make([]byte, tmqFetchRawPayloadOffset), byte(0x7f))
	payload, err := extractTMQFetchRawPayload(resp)
	require.NoError(t, err)
	require.Equal(t, []byte{0x7f}, payload)
}

// TestBuildTopicPartitionOffsetsLengthMismatch verifies response length mismatch returns error instead of panicking.
func TestBuildTopicPartitionOffsetsLengthMismatch(t *testing.T) {
	topic := "tp"
	partitions := []commontmq.TopicPartition{
		{Topic: &topic, Partition: 1},
	}

	_, err := buildTopicPartitionOffsets(partitions, []int64{10, 11}, proto.TMQActionCommitted)
	require.Error(t, err)

	_, err = buildTopicPartitionOffsets(partitions, nil, proto.TMQActionPosition)
	require.Error(t, err)
}

// TestBuildTopicPartitionOffsets verifies normal offset projection.
func TestBuildTopicPartitionOffsets(t *testing.T) {
	topicA := "tp_a"
	topicB := "tp_b"
	partitions := []commontmq.TopicPartition{
		{Topic: &topicA, Partition: 1},
		{Topic: &topicB, Partition: 2},
	}
	offsets, err := buildTopicPartitionOffsets(partitions, []int64{100, 200}, proto.TMQActionCommitted)
	require.NoError(t, err)
	require.Len(t, offsets, 2)
	require.Equal(t, partitions[0].Topic, offsets[0].Topic)
	require.Equal(t, partitions[0].Partition, offsets[0].Partition)
	require.Equal(t, commontmq.Offset(100), offsets[0].Offset)
	require.Equal(t, partitions[1].Topic, offsets[1].Topic)
	require.Equal(t, partitions[1].Partition, offsets[1].Partition)
	require.Equal(t, commontmq.Offset(200), offsets[1].Offset)
}
