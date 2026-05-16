package statsd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventGather(t *testing.T) {
	now := time.Now()
	type expected struct {
		title  string
		tags   map[string]string
		fields map[string]interface{}
	}
	tests := []struct {
		name     string
		message  string
		hostname string
		now      time.Time
		err      bool
		expected expected
	}{{
		name:     "basic",
		message:  "_e{10,9}:test title|test text",
		hostname: "default-hostname",
		now:      now,
		err:      false,
		expected: expected{
			title: "test title",
			tags:  map[string]string{"source": "default-hostname"},
			fields: map[string]interface{}{
				"priority":   priorityNormal,
				"alert_type": "info",
				"text":       "test text",
			},
		},
	},
		{
			name:     "escape some stuff",
			message:  "_e{10,24}:test title|test\\line1\\nline2\\nline3",
			hostname: "default-hostname",
			now:      now.Add(1),
			err:      false,
			expected: expected{
				title: "test title",
				tags:  map[string]string{"source": "default-hostname"},
				fields: map[string]interface{}{
					"priority":   priorityNormal,
					"alert_type": "info",
					"text":       "test\\line1\nline2\nline3",
				},
			},
		},
		{
			name:     "custom time",
			message:  "_e{10,9}:test title|test text|d:21",
			hostname: "default-hostname",
			now:      now.Add(2),
			err:      false,
			expected: expected{
				title: "test title",
				tags:  map[string]string{"source": "default-hostname"},
				fields: map[string]interface{}{
					"priority":   priorityNormal,
					"alert_type": "info",
					"text":       "test text",
					"ts":         int64(21),
				},
			},
		},
	}
	acc := &Accumulator{}
	s := NewTestStatsd()
	require.NoError(t, s.Start(acc))
	defer s.Stop()

	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {
			err := s.parseEventMessage(tests[i].now, tests[i].message, tests[i].hostname)
			if tests[i].err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, uint64(i+1), acc.NMetrics())

			require.NoError(t, err)
			require.Equal(t, tests[i].expected.title, acc.Metrics[i].Measurement)
			require.Equal(t, tests[i].expected.tags, acc.Metrics[i].Tags)
			require.Equal(t, tests[i].expected.fields, acc.Metrics[i].Fields)
		})
	}
}

// These tests adapted from tests in
// https://github.com/DataDog/datadog-agent/blob/master/pkg/dogstatsd/parser_test.go
// to ensure compatibility with the datadog-agent parser

func TestEvents(t *testing.T) {
	now := time.Now()
	type args struct {
		now      time.Time
		message  string
		hostname string
	}
	type expected struct {
		title          string
		text           interface{}
		now            time.Time
		ts             interface{}
		priority       string
		source         string
		alertType      interface{}
		aggregationKey string
		sourceTypeName interface{}
		checkTags      map[string]string
	}

	tests := []struct {
		name     string
		args     args
		expected expected
	}{
		{
			name: "event minimal",
			args: args{
				now:      now,
				message:  "_e{10,9}:test title|test text",
				hostname: "default-hostname",
			},
			expected: expected{
				title:          "test title",
				text:           "test text",
				now:            now,
				priority:       priorityNormal,
				source:         "default-hostname",
				alertType:      eventInfo,
				aggregationKey: "",
			},
		},
		{
			name: "event multilines text",
			args: args{
				now:      now.Add(1),
				message:  "_e{10,24}:test title|test\\line1\\nline2\\nline3",
				hostname: "default-hostname",
			},
			expected: expected{
				title:          "test title",
				text:           "test\\line1\nline2\nline3",
				now:            now.Add(1),
				priority:       priorityNormal,
				source:         "default-hostname",
				alertType:      eventInfo,
				aggregationKey: "",
			},
		},
		{
			name: "event pipe in title",
			args: args{
				now:      now.Add(2),
				message:  "_e{10,24}:test|title|test\\line1\\nline2\\nline3",
				hostname: "default-hostname",
			},
			expected: expected{
				title:          "test|title",
				text:           "test\\line1\nline2\nline3",
				now:            now.Add(2),
				priority:       priorityNormal,
				source:         "default-hostname",
				alertType:      eventInfo,
				aggregationKey: "",
			},
		},
		{
			name: "event metadata timestamp",
			args: args{
				now:      now.Add(3),
				message:  "_e{10,9}:test title|test text|d:21",
				hostname: "default-hostname",
			},
			expected: expected{
				title:          "test title",
				text:           "test text",
				now:            now.Add(3),
				priority:       priorityNormal,
				source:         "default-hostname",
				alertType:      eventInfo,
				aggregationKey: "",
				ts:             int64(21),
			},
		},
		{
			name: "event metadata priority",
			args: args{
				now:      now.Add(4),
				message:  "_e{10,9}:test title|test text|p:low",
				hostname: "default-hostname",
			},
			expected: expected{
				title:     "test title",
				text:      "test text",
				now:       now.Add(4),
				priority:  priorityLow,
				source:    "default-hostname",
				alertType: eventInfo,
			},
		},
		{
			name: "event metadata hostname",
			args: args{
				now:      now.Add(5),
				message:  "_e{10,9}:test title|test text|h:localhost",
				hostname: "default-hostname",
			},
			expected: expected{
				title:     "test title",
				text:      "test text",
				now:       now.Add(5),
				priority:  priorityNormal,
				source:    "localhost",
				alertType: eventInfo,
			},
		},
		{
			name: "event metadata hostname in tag",
			args: args{
				now:      now.Add(6),
				message:  "_e{10,9}:test title|test text|#host:localhost",
				hostname: "default-hostname",
			},
			expected: expected{
				title:     "test title",
				text:      "test text",
				now:       now.Add(6),
				priority:  priorityNormal,
				source:    "localhost",
				alertType: eventInfo,
			},
		},
		{
			name: "event metadata empty host tag",
			args: args{
				now:      now.Add(7),
				message:  "_e{10,9}:test title|test text|#host:,other:tag",
				hostname: "default-hostname",
			},
			expected: expected{
				title:     "test title",
				text:      "test text",
				now:       now.Add(7),
				priority:  priorityNormal,
				source:    "true",
				alertType: eventInfo,
				checkTags: map[string]string{"other": "tag", "source": "true"},
			},
		},
		{
			name: "event metadata alert type",
			args: args{
				now:      now.Add(8),
				message:  "_e{10,9}:test title|test text|t:warning",
				hostname: "default-hostname",
			},
			expected: expected{
				title:     "test title",
				text:      "test text",
				now:       now.Add(8),
				priority:  priorityNormal,
				source:    "default-hostname",
				alertType: eventWarning,
			},
		},
		{
			name: "event metadata aggregation key",
			args: args{
				now:      now.Add(9),
				message:  "_e{10,9}:test title|test text|k:some aggregation key",
				hostname: "default-hostname",
			},
			expected: expected{
				title:          "test title",
				text:           "test text",
				now:            now.Add(9),
				priority:       priorityNormal,
				source:         "default-hostname",
				alertType:      eventInfo,
				aggregationKey: "some aggregation key",
			},
		},
		{
			name: "event metadata aggregation key",
			args: args{
				now:      now.Add(10),
				message:  "_e{10,9}:test title|test text|k:some aggregation key",
				hostname: "default-hostname",
			},
			expected: expected{
				title:          "test title",
				text:           "test text",
				now:            now.Add(10),
				priority:       priorityNormal,
				source:         "default-hostname",
				alertType:      eventInfo,
				aggregationKey: "some aggregation key",
			},
		},
		{
			name: "event metadata source type",
			args: args{
				now:      now.Add(11),
				message:  "_e{10,9}:test title|test text|s:this is the source",
				hostname: "default-hostname",
			},
			expected: expected{
				title:          "test title",
				text:           "test text",
				now:            now.Add(11),
				priority:       priorityNormal,
				source:         "default-hostname",
				sourceTypeName: "this is the source",
				alertType:      eventInfo,
			},
		},
		{
			name: "event metadata source type",
			args: args{
				now:      now.Add(11),
				message:  "_e{10,9}:test title|test text|s:this is the source",
				hostname: "default-hostname",
			},
			expected: expected{
				title:          "test title",
				text:           "test text",
				now:            now.Add(11),
				priority:       priorityNormal,
				source:         "default-hostname",
				sourceTypeName: "this is the source",
				alertType:      eventInfo,
			},
		},
		{
			name: "event metadata source tags",
			args: args{
				now:      now.Add(11),
				message:  "_e{10,9}:test title|test text|#tag1,tag2:test",
				hostname: "default-hostname",
			},
			expected: expected{
				title:     "test title",
				text:      "test text",
				now:       now.Add(11),
				priority:  priorityNormal,
				source:    "default-hostname",
				alertType: eventInfo,
				checkTags: map[string]string{"tag1": "true", "tag2": "test", "source": "default-hostname"},
			},
		},
		{
			name: "event metadata multiple",
			args: args{
				now:      now.Add(11),
				message:  "_e{10,9}:test title|test text|t:warning|d:12345|p:low|h:some.host|k:aggKey|s:source test|#tag1,tag2:test",
				hostname: "default-hostname",
			},
			expected: expected{
				title:          "test title",
				text:           "test text",
				now:            now.Add(11),
				priority:       priorityLow,
				source:         "some.host",
				ts:             int64(12345),
				alertType:      eventWarning,
				aggregationKey: "aggKey",
				sourceTypeName: "source test",
				checkTags:      map[string]string{"aggregation_key": "aggKey", "tag1": "true", "tag2": "test", "source": "some.host"},
			},
		},
	}
	s := NewTestStatsd()
	acc := &Accumulator{}
	require.NoError(t, s.Start(acc))
	defer s.Stop()
	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {
			acc.ClearMetrics()
			err := s.parseEventMessage(tests[i].args.now, tests[i].args.message, tests[i].args.hostname)
			require.NoError(t, err)
			m := acc.Metrics[0]
			require.Equal(t, tests[i].expected.title, m.Measurement)
			require.Equal(t, tests[i].expected.text, m.Fields["text"])
			require.Equal(t, tests[i].expected.now, m.Time)
			require.Equal(t, tests[i].expected.ts, m.Fields["ts"])
			require.Equal(t, tests[i].expected.priority, m.Fields["priority"])
			require.Equal(t, tests[i].expected.source, m.Tags["source"])
			require.Equal(t, tests[i].expected.alertType, m.Fields["alert_type"])
			require.Equal(t, tests[i].expected.aggregationKey, m.Tags["aggregation_key"])
			require.Equal(t, tests[i].expected.sourceTypeName, m.Fields["source_type_name"])
			if tests[i].expected.checkTags != nil {
				require.Equal(t, tests[i].expected.checkTags, m.Tags)
			}
		})
	}
}

func TestEventError(t *testing.T) {
	now := time.Now()
	s := NewTestStatsd()
	acc := &Accumulator{}
	require.NoError(t, s.Start(acc))
	defer s.Stop()

	// missing length header
	err := s.parseEventMessage(now, "_e:title|text", "default-hostname")
	require.Error(t, err)

	// greater length than packet
	err = s.parseEventMessage(now, "_e{10,10}:title|text", "default-hostname")
	require.Error(t, err)

	// zero length
	err = s.parseEventMessage(now, "_e{0,0}:a|a", "default-hostname")
	require.Error(t, err)

	// missing title or text length
	err = s.parseEventMessage(now, "_e{5555:title|text", "default-hostname")
	require.Error(t, err)

	// missing wrong len format
	err = s.parseEventMessage(now, "_e{a,1}:title|text", "default-hostname")
	require.Error(t, err)

	err = s.parseEventMessage(now, "_e{1,a}:title|text", "default-hostname")
	require.Error(t, err)

	// missing title or text length
	err = s.parseEventMessage(now, "_e{5,}:title|text", "default-hostname")
	require.Error(t, err)

	err = s.parseEventMessage(now, "_e{100,:title|text", "default-hostname")
	require.Error(t, err)

	err = s.parseEventMessage(now, "_e,100:title|text", "default-hostname")
	require.Error(t, err)

	err = s.parseEventMessage(now, "_e{,4}:title|text", "default-hostname")
	require.Error(t, err)

	err = s.parseEventMessage(now, "_e{}:title|text", "default-hostname")
	require.Error(t, err)

	err = s.parseEventMessage(now, "_e{,}:title|text", "default-hostname")
	require.Error(t, err)

	// not enough information
	err = s.parseEventMessage(now, "_e|text", "default-hostname")
	require.Error(t, err)

	err = s.parseEventMessage(now, "_e:|text", "default-hostname")
	require.Error(t, err)

	// invalid timestamp
	err = s.parseEventMessage(now, "_e{5,4}:title|text|d:abc", "default-hostname")
	require.NoError(t, err)

	// invalid priority
	err = s.parseEventMessage(now, "_e{5,4}:title|text|p:urgent", "default-hostname")
	require.NoError(t, err)

	// invalid priority
	err = s.parseEventMessage(now, "_e{5,4}:title|text|p:urgent", "default-hostname")
	require.NoError(t, err)

	// invalid alert type
	err = s.parseEventMessage(now, "_e{5,4}:title|text|t:test", "default-hostname")
	require.NoError(t, err)

	// unknown metadata
	err = s.parseEventMessage(now, "_e{5,4}:title|text|x:1234", "default-hostname")
	require.Error(t, err)
}
