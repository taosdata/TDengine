package prometheus

import (
	"testing"

	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/plugin/prometheus/prompb"
	prompbWrite "github.com/taosdata/taosadapter/v3/plugin/prometheus/proto/write"
	"github.com/taosdata/taosadapter/v3/tools/pool"
)

// @author: xftan
// @date: 2021/12/20 14:46
// @description: test generate remote_write insert sql
func Test_generateWriteSql(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	type args struct {
		timeseries []prompbWrite.TimeSeries
		ttl        int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "general",
			args: args{
				timeseries: []prompbWrite.TimeSeries{
					{
						Labels: []prompbWrite.Label{
							{
								Name:  []byte("k2"),
								Value: []byte("v2"),
							},
							{
								Name:  []byte("k1"),
								Value: []byte("v1"),
							},
						},
						Samples: []prompbWrite.Sample{
							{
								Value:     123.456,
								Timestamp: 1639979902000,
							}, {
								Value:     456.789,
								Timestamp: 1639979903000,
							},
						},
					},
				},
			},
			want: `insert into t_38afbaaeb3ee3f52623389a3af60f647 using metrics tags('{"k1":"v1","k2":"v2"}') values('2021-12-20T05:58:22Z',123.456) ('2021-12-20T05:58:23Z',456.789) `,
		},
		{
			name: "escape",
			args: args{
				timeseries: []prompbWrite.TimeSeries{
					{
						Labels: []prompbWrite.Label{
							{
								Name:  []byte("k'2"),
								Value: []byte("v'2"),
							},
							{
								Name:  []byte("k'1"),
								Value: []byte("v'1"),
							},
						},
						Samples: []prompbWrite.Sample{
							{
								Value:     123.456,
								Timestamp: 1639979902000,
							}, {
								Value:     456.789,
								Timestamp: 1639979903000,
							},
						},
					},
				},
			},
			want: `insert into t_98e95338dc63e470540a87386c1fea3f using metrics tags('{"k\'1":"v\'1","k\'2":"v\'2"}') values('2021-12-20T05:58:22Z',123.456) ('2021-12-20T05:58:23Z',456.789) `,
		},
		{
			name: "prometheus",
			args: args{
				timeseries: []prompbWrite.TimeSeries{
					{
						Labels: []prompbWrite.Label{
							{
								Name:  []byte("__name__"),
								Value: []byte("prometheus_tsdb_compaction_duration_seconds_sum"),
							},
							{
								Name:  []byte("instance"),
								Value: []byte("localhost:9090"),
							},
							{
								Name:  []byte("job"),
								Value: []byte("prometheus"),
							},
						},
						Samples: []prompbWrite.Sample{
							{
								Value:     0,
								Timestamp: 1655859635940,
							},
						},
					}, {
						Labels: []prompbWrite.Label{
							{
								Name:  []byte("__name__"),
								Value: []byte("prometheus_tsdb_compaction_duration_seconds_sum"),
							},
							{
								Name:  []byte("instance"),
								Value: []byte("localhost:9090"),
							},
							{
								Name:  []byte("job"),
								Value: []byte("prometheus"),
							},
						},
						Samples: []prompbWrite.Sample{
							{
								Value:     0,
								Timestamp: 1655859650940,
							},
						},
					},
				},
			},
			want: `insert into t_3897c17b8513dad48cf4354bf8cb3e13 using metrics tags('{"__name__":"prometheus_tsdb_compaction_duration_seconds_sum","instance":"localhost:9090","job":"prometheus"}') values('2022-06-22T01:00:35.94Z',0) t_3897c17b8513dad48cf4354bf8cb3e13 using metrics tags('{"__name__":"prometheus_tsdb_compaction_duration_seconds_sum","instance":"localhost:9090","job":"prometheus"}') values('2022-06-22T01:00:50.94Z',0) `,
		},
		{
			name: "escapeBackslash",
			args: args{
				timeseries: []prompbWrite.TimeSeries{
					{
						Labels: []prompbWrite.Label{
							{
								Name:  []byte(`key`),
								Value: []byte(`k\'v`),
							},
						},
						Samples: []prompbWrite.Sample{
							{
								Value:     123.456,
								Timestamp: 1639979902000,
							}, {
								Value:     456.789,
								Timestamp: 1639979903000,
							},
						},
					},
				},
			},
			want: `insert into t_376843ecdb3abb76454247aaf9bbe7b8 using metrics tags('{"key":"k\\\\\'v"}') values('2021-12-20T05:58:22Z',123.456) ('2021-12-20T05:58:23Z',456.789) `,
		},
		{
			name: "escapeBackslashWithTTL",
			args: args{
				timeseries: []prompbWrite.TimeSeries{
					{
						Labels: []prompbWrite.Label{
							{
								Name:  []byte(`key`),
								Value: []byte(`k\'v`),
							},
						},
						Samples: []prompbWrite.Sample{
							{
								Value:     123.456,
								Timestamp: 1639979902000,
							}, {
								Value:     456.789,
								Timestamp: 1639979903000,
							},
						},
					},
				},
				ttl: 12,
			},
			want: `insert into t_376843ecdb3abb76454247aaf9bbe7b8 using metrics tags('{"key":"k\\\\\'v"}') ttl 12 values('2021-12-20T05:58:22Z',123.456) ('2021-12-20T05:58:23Z',456.789) `,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bp := pool.BytesPoolGet()
			defer pool.BytesPoolPut(bp)
			generateWriteSql(tt.args.timeseries, bp, tt.args.ttl)
			got := bp.String()
			if got != tt.want {
				t.Errorf("generateWriteSql() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2021/12/20 14:46
// @description: test generate remote_read query sql
func Test_generateReadSql(t *testing.T) {
	config.Conf.RestfulRowLimit = -1
	type args struct {
		query *prompb.Query
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "general",
			args: args{
				query: &prompb.Query{
					StartTimestampMs: 1639979902000,
					EndTimestampMs:   1639979903000,
					Matchers: []*prompb.LabelMatcher{
						{
							Type:  prompb.LabelMatcher_EQ,
							Name:  "__name__",
							Value: "point1",
						},
						{
							Type:  prompb.LabelMatcher_NEQ,
							Name:  "type",
							Value: "info",
						},
						{
							Type:  prompb.LabelMatcher_RE,
							Name:  "server",
							Value: "server-1$",
						},
						{
							Type:  prompb.LabelMatcher_NRE,
							Name:  "group",
							Value: "group1.*",
						},
						{
							Type:  prompb.LabelMatcher_EQ,
							Name:  "key",
							Value: `k\'v`,
						},
					},
				},
			},
			want:    "select metrics.*,tbname from metrics where ts >= '2021-12-20T05:58:22Z' and ts <= '2021-12-20T05:58:23Z' and labels->'__name__' = 'point1' and labels->'type' != 'info' and labels->'server' match 'server-1$' and labels->'group' nmatch 'group1.*' and labels->'key' = 'k\\\\\\'v'",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := generateReadSql(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("generateReadSql() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("generateReadSql() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2022/1/18 17:57
// @description: test generate remote_read query sql with row limit
func Test_generateReadSqlWithLimit(t *testing.T) {
	type args struct {
		query *prompb.Query
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "general",
			args: args{
				query: &prompb.Query{
					StartTimestampMs: 1639979902000,
					EndTimestampMs:   1639979903000,
					Matchers: []*prompb.LabelMatcher{
						{
							Type:  prompb.LabelMatcher_EQ,
							Name:  "__name__",
							Value: "point1",
						},
						{
							Type:  prompb.LabelMatcher_NEQ,
							Name:  "type",
							Value: "info",
						},
						{
							Type:  prompb.LabelMatcher_RE,
							Name:  "server",
							Value: "server-1$",
						},
						{
							Type:  prompb.LabelMatcher_NRE,
							Name:  "group",
							Value: "group1.*",
						},
					},
				},
			},
			want:    "select metrics.*,tbname from metrics where ts >= '2021-12-20T05:58:22Z' and ts <= '2021-12-20T05:58:23Z' and labels->'__name__' = 'point1' and labels->'type' != 'info' and labels->'server' match 'server-1$' and labels->'group' nmatch 'group1.*' limit 2",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		config.Conf.RestfulRowLimit = 2
		t.Run(tt.name, func(t *testing.T) {
			got, err := generateReadSql(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("generateReadSql() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("generateReadSql() got = %v, want %v", got, tt.want)
			}
		})
		config.Conf.RestfulRowLimit = -1
	}
}
