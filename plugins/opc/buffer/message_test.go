package buffer

import (
	"collector/common"
	"collector/types"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMessageList(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messages := NewMessageList(ctx, 3, time.Second, logrus.New().WithField("test", "test"))

	messages.Add([]*common.NodeValue{
		{
			IDStr:      "test1",
			Name:       "test1",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(1),
			ValueType:  types.INT32,
			Status:     0,
		},
	})
	messages.Add([]*common.NodeValue{
		{
			IDStr:      "test2",
			Name:       "test2",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(2),
			ValueType:  types.INT32,
			Status:     0,
		},
		{
			IDStr:      "test3",
			Name:       "test3",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(3),
			ValueType:  types.INT32,
			Status:     0,
		},
		{
			IDStr:      "test4",
			Name:       "test4",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(4),
			ValueType:  types.INT32,
			Status:     0,
		},
	})
	messages.Add([]*common.NodeValue{
		{
			IDStr:      "test5",
			Name:       "test5",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(5),
			ValueType:  types.INT32,
			Status:     0,
		},
	})
	messages.Add([]*common.NodeValue{
		{
			IDStr:      "test6",
			Name:       "test6",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(6),
			ValueType:  types.INT32,
			Status:     0,
		},
	})
	messages.Add([]*common.NodeValue{
		{
			IDStr:      "test7",
			Name:       "test7",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(7),
			ValueType:  types.INT32,
			Status:     0,
		},
		{
			IDStr:      "test8",
			Name:       "test8",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(8),
			ValueType:  types.INT32,
			Status:     0,
		},
		{
			IDStr:      "test9",
			Name:       "test9",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(9),
			ValueType:  types.INT32,
			Status:     0,
		},
	})
	length := messages.Length()
	assert.Equal(t, 9, length)
	timer := time.NewTimer(time.Second * 2)
	select {
	case values := <-messages.C:
		assert.Equal(t, 6, messages.Length())
		assert.Equal(t, 3, len(values))
		for i := 0; i < 3; i++ {
			assert.Equal(t, "test"+strconv.FormatInt(int64(i+1), 10), values[i].IDStr)
			assert.Equal(t, int32(i+1), values[i].Value)
		}
	case <-timer.C:
		t.Fatal("get message channel timeout")
	}
	timer.Stop()
	timer = time.NewTimer(time.Second * 2)
	select {
	case values := <-messages.C:
		assert.Equal(t, 3, messages.Length())
		assert.Equal(t, 3, len(values))
		for i := 0; i < 3; i++ {
			assert.Equal(t, "test"+strconv.FormatInt(int64(i+4), 10), values[i].IDStr)
			assert.Equal(t, int32(i+4), values[i].Value)
		}
	case <-timer.C:
		t.Fatal("get message channel timeout")
	}
	time.Sleep(time.Second * 2)
	timer.Stop()
	timer = time.NewTimer(time.Second * 2)
	select {
	case values := <-messages.C:
		assert.Equal(t, 0, messages.Length())
		assert.Equal(t, 3, len(values))
		for i := 0; i < 3; i++ {
			assert.Equal(t, "test"+strconv.FormatInt(int64(i+7), 10), values[i].IDStr)
			assert.Equal(t, int32(i+7), values[i].Value)
		}
	case <-timer.C:
		t.Fatal("get message channel timeout")
	}

	messages.Add([]*common.NodeValue{
		{
			IDStr:      "test10",
			Name:       "test10",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(10),
			ValueType:  types.INT32,
			Status:     0,
		},
		{
			IDStr:      "test11",
			Name:       "test11",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(11),
			ValueType:  types.INT32,
			Status:     0,
		},
	})
	cancel()
	time.Sleep(time.Millisecond * 100)
}

func TestMessageListHungry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	messages := NewMessageList(ctx, 3, time.Second*3, logger.WithField("test", "test"))

	messages.Add([]*common.NodeValue{
		{
			IDStr:      "test1",
			Name:       "test1",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(1),
			ValueType:  types.INT32,
			Status:     0,
		},
	})
	messages.Add([]*common.NodeValue{
		{
			IDStr:      "test2",
			Name:       "test2",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(2),
			ValueType:  types.INT32,
			Status:     0,
		},
		{
			IDStr:      "test3",
			Name:       "test3",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(3),
			ValueType:  types.INT32,
			Status:     0,
		},
		{
			IDStr:      "test4",
			Name:       "test4",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(4),
			ValueType:  types.INT32,
			Status:     0,
		},
	})
	messages.Add([]*common.NodeValue{
		{
			IDStr:      "test5",
			Name:       "test5",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(5),
			ValueType:  types.INT32,
			Status:     0,
		},
	})
	messages.Add([]*common.NodeValue{
		{
			IDStr:      "test6",
			Name:       "test6",
			Timestamp:  time.Now(),
			StartTime:  time.Now(),
			FinishTime: time.Now(),
			Value:      int32(6),
			ValueType:  types.INT32,
			Status:     0,
		},
	})

	t.Log("1")
	length := messages.Length()
	assert.Equal(t, 6, length)
	//wait for first signal is ignored
	time.Sleep(time.Second)
	timer := time.NewTimer(time.Second * 1)
	addDone := make(chan struct{}, 1)
	go func() {
		messages.Add([]*common.NodeValue{
			{
				IDStr:      "test7",
				Name:       "test7",
				Timestamp:  time.Now(),
				StartTime:  time.Now(),
				FinishTime: time.Now(),
				Value:      int32(7),
				ValueType:  types.INT32,
				Status:     0,
			},
			{
				IDStr:      "test8",
				Name:       "test8",
				Timestamp:  time.Now(),
				StartTime:  time.Now(),
				FinishTime: time.Now(),
				Value:      int32(8),
				ValueType:  types.INT32,
				Status:     0,
			},
			{
				IDStr:      "test9",
				Name:       "test9",
				Timestamp:  time.Now(),
				StartTime:  time.Now(),
				FinishTime: time.Now(),
				Value:      int32(9),
				ValueType:  types.INT32,
				Status:     0,
			},
		})
		addDone <- struct{}{}
	}()
	// signal by full
	select {
	case values := <-messages.C:
		assert.Equal(t, 6, messages.Length())
		assert.Equal(t, 3, len(values))
		for i := 0; i < 3; i++ {
			assert.Equal(t, "test"+strconv.FormatInt(int64(i+1), 10), values[i].IDStr)
			assert.Equal(t, int32(i+1), values[i].Value)
		}
	case <-timer.C:
		t.Fatal("timeout")
	}
	<-addDone
	t.Log("2")
	//wait for full signal is ignored
	time.Sleep(time.Second)
	timer.Stop()
	//sig by timeout
	timer = time.NewTimer(time.Second * 5)
	select {
	case values := <-messages.C:
		assert.Equal(t, 3, messages.Length())
		assert.Equal(t, 3, len(values))
		for i := 0; i < 3; i++ {
			assert.Equal(t, "test"+strconv.FormatInt(int64(i+4), 10), values[i].IDStr)
			assert.Equal(t, int32(i+4), values[i].Value)
		}
	case <-timer.C:
		t.Fatal("timeout")
	}
	t.Log("3")
	timer.Stop()
	// no signal, expect timeout
	timer = time.NewTimer(time.Second * 1)
	select {
	case <-messages.C:
		t.Fatal("unexpected message")
	case <-timer.C:
	}
	t.Log("4")
	timer.Stop()
	timer = time.NewTimer(time.Second * 2)
	// signal by get
	messages.TryGet()
	select {
	case values := <-messages.C:
		assert.Equal(t, 0, messages.Length())
		assert.Equal(t, 3, len(values))
		for i := 0; i < 3; i++ {
			assert.Equal(t, "test"+strconv.FormatInt(int64(i+7), 10), values[i].IDStr)
			assert.Equal(t, int32(i+7), values[i].Value)
		}
	case <-timer.C:
		t.Fatal("get message channel timeout")
	}
	messages.TryGet()
	messages.TryGet()
	messages.TryGet()
	cancel()
	time.Sleep(time.Millisecond * 100)
}
