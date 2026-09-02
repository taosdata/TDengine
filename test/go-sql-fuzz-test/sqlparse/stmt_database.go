package sqlparser

import (
	"fmt"
	"sqlparser/tool"
	"strconv"
	"strings"
	"time"
)

func normalizeByteList(value interface{}) ([][]byte, bool) {
	switch v := value.(type) {
	case [][]byte:
		return v, true
	case []byte:
		return [][]byte{v}, true
	default:
		return nil, false
	}
}

func parseCompactRangeValue(v []byte) (int32, error) {
	if tool.HasDurationUnit(v) {
		d, err := tool.ParseDuration(v)
		if err != nil {
			return 0, err
		}
		if d.Month != 0 {
			// Keep behavior deterministic for month-based inputs in absence of calendar context.
			return int32(d.Month * 30 * 24 * 60), nil
		}
		return int32(d.FixedDuration / time.Minute), nil
	}
	i, err := strconv.ParseInt(tool.BytesToString(v), 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(i), nil
}

func appendDatabaseOptions(buf *TrackedBuffer, opts *DatabaseOptions) {
	if buf == nil || opts == nil {
		return
	}
	if opts.Buffer != 0 {
		buf.Myprintf(" buffer %s", strconv.FormatInt(int64(opts.Buffer), 10))
	}
	if opts.CacheModelStr != "" {
		buf.Myprintf(" cachemodel '%s'", opts.CacheModelStr)
	}
	if opts.CacheLastSize != 0 {
		buf.Myprintf(" cachesize %s", strconv.FormatInt(int64(opts.CacheLastSize), 10))
	}
	if opts.CompressionLevel != 0 {
		buf.Myprintf(" comp %s", strconv.FormatInt(int64(opts.CompressionLevel), 10))
	}
	if len(opts.DaysPerFile) > 0 {
		buf.Myprintf(" duration %s", tdDurationToSQLLiteral(opts.DaysPerFile[0]))
	}
	if opts.MaxRowsPerBlock != 0 {
		buf.Myprintf(" maxrows %s", strconv.FormatInt(int64(opts.MaxRowsPerBlock), 10))
	}
	if opts.MinRowsPerBlock != 0 {
		buf.Myprintf(" minrows %s", strconv.FormatInt(int64(opts.MinRowsPerBlock), 10))
	}
	if opts.Pages != 0 {
		buf.Myprintf(" pages %s", strconv.FormatInt(int64(opts.Pages), 10))
	}
	if opts.Pagesize != 0 {
		buf.Myprintf(" pagesize %s", strconv.FormatInt(int64(opts.Pagesize), 10))
	}
	if opts.TsdbPageSize != 0 {
		buf.Myprintf(" tsdb_pagesize %s", strconv.FormatInt(int64(opts.TsdbPageSize), 10))
	}
	if opts.PrecisionStr != "" {
		buf.Myprintf(" precision '%s'", opts.PrecisionStr)
	}
	if opts.Replica != 0 {
		buf.Myprintf(" replica %s", strconv.FormatInt(int64(opts.Replica), 10))
	}
	if opts.WalLevel != 0 {
		buf.Myprintf(" wal_level %s", strconv.FormatInt(int64(opts.WalLevel), 10))
	}
	if opts.fsyncPeriod != 0 {
		buf.Myprintf(" wal_fsync_period %s", strconv.FormatInt(int64(opts.fsyncPeriod), 10))
	}
	if opts.WalRetentionPeriodIsSet {
		buf.Myprintf(" wal_retention_period %s", strconv.FormatInt(int64(opts.WalRetentionPeriod), 10))
	}
	if opts.WalRetentionSizeIsSet {
		buf.Myprintf(" wal_retention_size %s", strconv.FormatInt(int64(opts.WalRetentionSize), 10))
	}
	if opts.WalRollPeriodIsSet {
		buf.Myprintf(" wal_roll_period %s", strconv.FormatInt(int64(opts.WalRollPeriod), 10))
	}
	if opts.WalSegmentSizeIsSet {
		buf.Myprintf(" wal_segment_size %s", strconv.FormatInt(int64(opts.WalSegmentSize), 10))
	}
	if opts.SstTrigger != 0 {
		buf.Myprintf(" stt_trigger %s", strconv.FormatInt(int64(opts.SstTrigger), 10))
	}
	if opts.TablePrefix != 0 {
		buf.Myprintf(" table_prefix %s", strconv.FormatInt(int64(opts.TablePrefix), 10))
	}
	if opts.TableSuffix != 0 {
		buf.Myprintf(" table_suffix %s", strconv.FormatInt(int64(opts.TableSuffix), 10))
	}
	if opts.SsChunkSize != 0 {
		buf.Myprintf(" ss_chunkpages %s", strconv.FormatInt(int64(opts.SsChunkSize), 10))
	}
	if opts.SsKeepLocal != 0 {
		buf.Myprintf(" ss_keeplocal %s", strconv.FormatInt(int64(opts.SsKeepLocal), 10))
	}
	if opts.SsCompact != 0 {
		buf.Myprintf(" ss_compact %s", strconv.FormatInt(int64(opts.SsCompact), 10))
	}
	if opts.KeepTimeOffset != 0 {
		buf.Myprintf(" keep_time_offset %s", strconv.FormatInt(int64(opts.KeepTimeOffset), 10))
	}
	if len(opts.Keep) > 0 {
		buf.Myprintf(" keep ")
		for i, d := range opts.Keep {
			if i > 0 {
				buf.Myprintf(",")
			}
			buf.Myprintf("%s", tdDurationToSQLLiteral(d))
		}
	}
	if opts.DnodeListStr != "" {
		buf.Myprintf(" dnodes '%s'", opts.DnodeListStr)
	}
	if opts.EncryptAlgorithmStr != "" {
		buf.Myprintf(" encrypt_algorithm '%s'", opts.EncryptAlgorithmStr)
	}
	if opts.CompactInterval != 0 {
		buf.Myprintf(" compact_interval %s", strconv.FormatInt(int64(opts.CompactInterval), 10))
	}
	if opts.CompactStartTime != 0 || opts.CompactEndTime != 0 {
		buf.Myprintf(" compact_time_range %s,%s",
			strconv.FormatInt(int64(opts.CompactStartTime), 10),
			strconv.FormatInt(int64(opts.CompactEndTime), 10))
	}
	if opts.CompactTimeOffset != 0 {
		buf.Myprintf(" compact_time_offset %s", strconv.FormatInt(int64(opts.CompactTimeOffset), 10))
	}
	if opts.IsAudit != 0 {
		buf.Myprintf(" is_audit %s", strconv.FormatInt(int64(opts.IsAudit), 10))
	}
	if len(opts.Retentions) > 0 {
		buf.Myprintf(" retentions ")
		for i, d := range opts.Retentions {
			if i > 0 {
				buf.Myprintf(":")
			}
			buf.Myprintf("%s", tdDurationToSQLLiteral(d))
		}
	}
}

func tdDurationToSQLLiteral(d tool.TDDuration) string {
	if d.Month > 0 {
		if d.Month%12 == 0 {
			return strconv.FormatInt(d.Month/12, 10) + "y"
		}
		return strconv.FormatInt(d.Month, 10) + "n"
	}
	if d.FixedDuration%(24*time.Hour) == 0 {
		return strconv.FormatInt(int64(d.FixedDuration/(24*time.Hour)), 10) + "d"
	}
	if d.FixedDuration%time.Hour == 0 {
		return strconv.FormatInt(int64(d.FixedDuration/time.Hour), 10) + "h"
	}
	if d.FixedDuration%time.Minute == 0 {
		return strconv.FormatInt(int64(d.FixedDuration/time.Minute), 10) + "m"
	}
	if d.FixedDuration%time.Second == 0 {
		return strconv.FormatInt(int64(d.FixedDuration/time.Second), 10) + "s"
	}
	return strconv.FormatInt(int64(d.FixedDuration/time.Millisecond), 10) + "a"
}

func quoteIdentifierIfNeeded(name string) string {
	if name == "" {
		return name
	}
	for i := 0; i < len(name); i++ {
		ch := name[i]
		isLower := ch >= 'a' && ch <= 'z'
		isUpper := ch >= 'A' && ch <= 'Z'
		isDigit := ch >= '0' && ch <= '9'
		if !(isLower || isUpper || isDigit || ch == '_') {
			return "`" + name + "`"
		}
	}
	if strings.ToLower(name) != name {
		return "`" + name + "`"
	}
	return name
}

type CreateDatabaseStmt struct {
	DbName       string
	IgnoreExists bool
	Options      *DatabaseOptions
}

func (c *CreateDatabaseStmt) iStatement() {}

func (c *CreateDatabaseStmt) Format(buf *TrackedBuffer) {
	if c == nil {
		return
	}
	buf.Myprintf("create database ")
	if c.IgnoreExists {
		buf.Myprintf("if not exists ")
	}
	buf.Myprintf("%s", quoteIdentifierIfNeeded(c.DbName))
	appendDatabaseOptions(buf, c.Options)
}

func (c *CreateDatabaseStmt) walkSubtree(visit Visit) error {
	return nil
}

// NewCreateDatabaseStmt creates a new CreateDatabaseStmt
func NewCreateDatabaseStmt(lexer yyLexer, ignoreExists bool, dbName Token, options *DatabaseOptions) *CreateDatabaseStmt {
	return &CreateDatabaseStmt{
		DbName:       tool.BytesToString(dbName.Bytes),
		IgnoreExists: ignoreExists,
		Options:      options,
	}
}

type DatabaseOptionType int

const (
	DB_OPTION_BUFFER DatabaseOptionType = iota + 1
	DB_OPTION_CACHEMODEL
	DB_OPTION_CACHESIZE
	DB_OPTION_COMP
	DB_OPTION_DAYS
	DB_OPTION_FSYNC
	DB_OPTION_MAXROWS
	DB_OPTION_MINROWS
	DB_OPTION_KEEP
	DB_OPTION_PAGES
	DB_OPTION_PAGESIZE
	DB_OPTION_TSDB_PAGESIZE
	DB_OPTION_PRECISION
	DB_OPTION_REPLICA
	DB_OPTION_STRICT
	DB_OPTION_WAL
	DB_OPTION_VGROUPS
	DB_OPTION_SINGLE_STABLE
	DB_OPTION_RETENTIONS
	DB_OPTION_SCHEMALESS
	DB_OPTION_WAL_RETENTION_PERIOD
	DB_OPTION_WAL_RETENTION_SIZE
	DB_OPTION_WAL_ROLL_PERIOD
	DB_OPTION_WAL_SEGMENT_SIZE
	DB_OPTION_STT_TRIGGER
	DB_OPTION_TABLE_PREFIX
	DB_OPTION_TABLE_SUFFIX
	DB_OPTION_SS_CHUNKPAGES
	DB_OPTION_SS_KEEPLOCAL
	DB_OPTION_SS_COMPACT
	DB_OPTION_KEEP_TIME_OFFSET
	DB_OPTION_ENCRYPT_ALGORITHM
	DB_OPTION_DNODES
	DB_OPTION_COMPACT_INTERVAL
	DB_OPTION_COMPACT_TIME_RANGE
	DB_OPTION_COMPACT_TIME_OFFSET
	DB_OPTION_IS_AUDIT
)

type DatabaseOptionKV struct {
	Type  DatabaseOptionType
	Value interface{}
}

type AlterDatabaseOptions struct {
	Items []DatabaseOptionKV
}

func NewAlterDatabaseOptions(opt DatabaseOptionKV) *AlterDatabaseOptions {
	return &AlterDatabaseOptions{Items: []DatabaseOptionKV{opt}}
}

func AddAlterDatabaseOption(opts *AlterDatabaseOptions, opt DatabaseOptionKV) *AlterDatabaseOptions {
	if opts == nil {
		return NewAlterDatabaseOptions(opt)
	}
	opts.Items = append(opts.Items, opt)
	return opts
}

func ApplyAlterDatabaseOptions(lexer yyLexer, opts *AlterDatabaseOptions) *DatabaseOptions {
	var out *DatabaseOptions
	if opts == nil {
		return out
	}
	for _, it := range opts.Items {
		out = SetDatabaseOption(lexer, out, it.Type, it.Value)
		if out == nil {
			return nil
		}
	}
	return out
}

type DatabaseOptions struct {
	Buffer                  int32
	CacheModelStr           string
	CacheModel              int8
	CacheLastSize           int32
	CompressionLevel        int8
	EncryptAlgorithm        int8
	DaysPerFile             []tool.TDDuration
	DnodeListStr            string
	EncryptAlgorithmStr     string
	fsyncPeriod             int32
	MaxRowsPerBlock         int32
	MinRowsPerBlock         int32
	Keep                    []tool.TDDuration
	Pages                   int32
	Pagesize                int32
	TsdbPageSize            int32
	PrecisionStr            string
	Precision               int8
	Replica                 int8
	StrictStr               string
	Strict                  int8
	WalLevel                int8
	NumOfVgroups            int32
	SingleStable            int8
	Retentions              []tool.TDDuration
	Schemaless              int8
	WalRetentionPeriod      int32
	WalRetentionSize        int32
	WalRollPeriod           int32
	WalSegmentSize          int32
	WalRetentionPeriodIsSet bool
	WalRetentionSizeIsSet   bool
	WalRollPeriodIsSet      bool
	WalSegmentSizeIsSet     bool
	SstTrigger              int32
	TablePrefix             int32
	TableSuffix             int32
	SsChunkSize             int32
	SsKeepLocal             int32
	SsCompact               int8
	KeepTimeOffset          int32
	WithArbitrator          bool
	// for auto-compact
	CompactTimeOffset int32 // hours
	CompactInterval   int32 // minutes
	CompactStartTime  int32 // minutes
	CompactEndTime    int32 // minutes
	IsAudit           int8
}

func SetDatabaseOption(lexer yyLexer, options *DatabaseOptions, opt DatabaseOptionType, value interface{}) *DatabaseOptions {
	if options == nil {
		options = &DatabaseOptions{}
	}
	switch opt {
	case DB_OPTION_BUFFER:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid buffer value, cannot convert %s to int32", value))
			return nil
		}
		options.Buffer = int32(i)
	case DB_OPTION_CACHEMODEL:
		options.CacheModelStr = tool.BytesToString(value.([]byte))
	case DB_OPTION_CACHESIZE:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid cache size value, cannot convert %s to int32", value))
			return nil
		}
		options.CacheLastSize = int32(i)
	case DB_OPTION_COMP:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 8)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid compression level value, cannot convert %s to int8", value))
			return nil
		}
		options.CompressionLevel = int8(i)
	case DB_OPTION_DAYS:
		values, ok := normalizeByteList(value)
		if !ok {
			lexer.Error(fmt.Sprintf("invalid days per file value type: %T", value))
			return nil
		}
		for _, v := range values {
			if tool.HasDurationUnit(v) {
				duration, err := tool.ParseDuration(v)
				if err != nil {
					lexer.Error(fmt.Sprintf("invalid days per file value, cannot convert %s to duration", v))
					return nil
				}
				options.DaysPerFile = append(options.DaysPerFile, duration)
			} else {
				days, err := strconv.ParseInt(tool.BytesToString(v), 10, 32)
				if err != nil {
					lexer.Error(fmt.Sprintf("invalid days per file value, cannot convert %s to int32", v))
					return nil
				}
				options.DaysPerFile = append(options.DaysPerFile, tool.NewTDDuration(time.Duration(days)*24*time.Hour))
			}
		}
	case DB_OPTION_MAXROWS:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid max rows per block value, cannot convert %s to int32", value))
			return nil
		}
		options.MaxRowsPerBlock = int32(i)
	case DB_OPTION_MINROWS:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid min rows per block value, cannot convert %s to int32", value))
			return nil
		}
		options.MinRowsPerBlock = int32(i)
	case DB_OPTION_KEEP:
		values, ok := normalizeByteList(value)
		if !ok {
			lexer.Error(fmt.Sprintf("invalid keep value type: %T", value))
			return nil
		}
		for _, v := range values {
			if tool.HasDurationUnit(v) {
				duration, err := tool.ParseDuration(v)
				if err != nil {
					lexer.Error(fmt.Sprintf("invalid keep value, cannot convert %s to duration", v))
					return nil
				}
				options.Keep = append(options.Keep, duration)
			} else {
				days, err := strconv.ParseInt(tool.BytesToString(v), 10, 32)
				if err != nil {
					lexer.Error(fmt.Sprintf("invalid keep value, cannot convert %s to int32", v))
					return nil
				}
				options.Keep = append(options.Keep, tool.NewTDDuration(time.Duration(days)*24*time.Hour))
			}
		}
	case DB_OPTION_PAGES:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid pages value, cannot convert %s to int32", value))
			return nil
		}
		options.Pages = int32(i)
	case DB_OPTION_PAGESIZE:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid pagesize value, cannot convert %s to int32", value))
			return nil
		}
		options.Pagesize = int32(i)
	case DB_OPTION_TSDB_PAGESIZE:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid tsdb pagesize value, cannot convert %s to int32", value))
			return nil
		}
		options.TsdbPageSize = int32(i)
	case DB_OPTION_REPLICA:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 8)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid replica value, cannot convert %s to int8", value))
			return nil
		}
		options.Replica = int8(i)
		if i == 2 {
			options.WithArbitrator = true
		}
	case DB_OPTION_PRECISION:
		options.PrecisionStr = tool.BytesToString(value.([]byte))
	case DB_OPTION_WAL:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 8)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid wal level value, cannot convert %s to int8", value))
			return nil
		}
		options.WalLevel = int8(i)
	case DB_OPTION_STRICT:
		options.StrictStr = tool.BytesToString(value.([]byte))
	case DB_OPTION_VGROUPS:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid number of vgroups value, cannot convert %s to int32", value))
			return nil
		}
		options.NumOfVgroups = int32(i)
	case DB_OPTION_SINGLE_STABLE:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 8)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid single stable value, cannot convert %s to int8", value))
			return nil
		}
		options.SingleStable = int8(i)
	case DB_OPTION_RETENTIONS:
		values := value.([][]byte)
		for _, v := range values {
			parts := strings.Split(tool.BytesToString(v), ":")
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part == "" || part == "-" {
					continue
				}
				b := []byte(part)
				if tool.HasDurationUnit(b) {
					duration, err := tool.ParseDuration(b)
					if err != nil {
						lexer.Error(fmt.Sprintf("invalid retentions value, cannot convert %s to duration", b))
						return nil
					}
					options.Retentions = append(options.Retentions, duration)
				} else {
					days, err := strconv.ParseInt(part, 10, 32)
					if err != nil {
						lexer.Error(fmt.Sprintf("invalid retentions value, cannot convert %s to int32", b))
						return nil
					}
					options.Retentions = append(options.Retentions, tool.NewTDDuration(time.Duration(days)*24*time.Hour))
				}
			}
		}
	case DB_OPTION_SCHEMALESS:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 8)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid schemaless value, cannot convert %s to int8", value))
			return nil
		}
		options.Schemaless = int8(i)
	case DB_OPTION_FSYNC:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid fsync period value, cannot convert %s to int32", value))
			return nil
		}
		options.fsyncPeriod = int32(i)
	case DB_OPTION_WAL_RETENTION_PERIOD:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid wal retention period value, cannot convert %s to int32", value))
			return nil
		}
		options.WalRetentionPeriod = int32(i)
		options.WalRetentionPeriodIsSet = true
	case DB_OPTION_WAL_RETENTION_SIZE:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid wal retention size value, cannot convert %s to int32", value))
			return nil
		}
		options.WalRetentionSize = int32(i)
		options.WalRetentionSizeIsSet = true
	case DB_OPTION_WAL_ROLL_PERIOD:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid wal roll period value, cannot convert %s to int32", value))
			return nil
		}
		options.WalRollPeriod = int32(i)
		options.WalRollPeriodIsSet = true
	case DB_OPTION_WAL_SEGMENT_SIZE:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid wal segment size value, cannot convert %s to int32", value))
			return nil
		}
		options.WalSegmentSize = int32(i)
		options.WalSegmentSizeIsSet = true
	case DB_OPTION_STT_TRIGGER:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid stt trigger value, cannot convert %s to int32", value))
			return nil
		}
		options.SstTrigger = int32(i)
	case DB_OPTION_TABLE_PREFIX:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid table prefix value, cannot convert %s to int32", value))
			return nil
		}
		options.TablePrefix = int32(i)
	case DB_OPTION_TABLE_SUFFIX:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid table suffix value, cannot convert %s to int32", value))
			return nil
		}
		options.TableSuffix = int32(i)
	case DB_OPTION_SS_COMPACT:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 8)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid ss compact value, cannot convert %s to int8", value))
			return nil
		}
		options.SsCompact = int8(i)
	case DB_OPTION_SS_CHUNKPAGES:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid ss chunkpages value, cannot convert %s to int32", value))
			return nil
		}
		options.SsChunkSize = int32(i)
	case DB_OPTION_SS_KEEPLOCAL:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid ss keeplocal value, cannot convert %s to int32", value))
			return nil
		}
		options.SsKeepLocal = int32(i)
	case DB_OPTION_KEEP_TIME_OFFSET:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid keep time offset value, cannot convert %s to int32", value))
			return nil
		}
		options.KeepTimeOffset = int32(i)
	case DB_OPTION_DNODES:
		options.DnodeListStr = tool.BytesToString(value.([]byte))
	case DB_OPTION_COMPACT_INTERVAL:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid compact interval value, cannot convert %s to int32", value))
			return nil
		}
		options.CompactInterval = int32(i)
	case DB_OPTION_COMPACT_TIME_RANGE:
		values, ok := normalizeByteList(value)
		if !ok {
			lexer.Error(fmt.Sprintf("invalid compact time range value type: %T", value))
			return nil
		}
		if len(values) > 0 {
			start, err := parseCompactRangeValue(values[0])
			if err != nil {
				lexer.Error(fmt.Sprintf("invalid compact time range start value, cannot convert %s to int32", values[0]))
				return nil
			}
			options.CompactStartTime = start
		}
		if len(values) > 1 {
			end, err := parseCompactRangeValue(values[1])
			if err != nil {
				lexer.Error(fmt.Sprintf("invalid compact time range end value, cannot convert %s to int32", values[1]))
				return nil
			}
			options.CompactEndTime = end
		}
	case DB_OPTION_COMPACT_TIME_OFFSET:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid compact time offset value, cannot convert %s to int32", value))
			return nil
		}
		options.CompactTimeOffset = int32(i)
	case DB_OPTION_IS_AUDIT:
		i, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 8)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid is_audit value, cannot convert %s to int8", value))
			return nil
		}
		options.IsAudit = int8(i)
	case DB_OPTION_ENCRYPT_ALGORITHM:
		options.EncryptAlgorithmStr = tool.BytesToString(value.([]byte))
	}
	return options
}

// CreateDefaultDatabaseOptions creates default DatabaseOptions
func CreateDefaultDatabaseOptions() *DatabaseOptions {
	return &DatabaseOptions{}
}

// DropDatabaseStmt represents a DROP DATABASE statement
type DropDatabaseStmt struct {
	DbName          string
	IgnoreNotExists bool
	Force           bool
}

func (d *DropDatabaseStmt) iStatement() {}

func (d *DropDatabaseStmt) Format(buf *TrackedBuffer) {
	if d == nil {
		return
	}
	buf.Myprintf("drop database ")
	if d.IgnoreNotExists {
		buf.Myprintf("if exists ")
	}
	buf.Myprintf("%s", d.DbName)
	if d.Force {
		buf.Myprintf(" force")
	}
}

func (d *DropDatabaseStmt) walkSubtree(visit Visit) error {
	return nil
}

// NewDropDatabaseStmt creates a new DropDatabaseStmt
func NewDropDatabaseStmt(lexer yyLexer, ignoreNotExists bool, dbName Token, force bool) *DropDatabaseStmt {
	stmt := &DropDatabaseStmt{
		DbName:          tool.BytesToString(dbName.Bytes),
		IgnoreNotExists: ignoreNotExists,
		Force:           force,
	}
	return stmt
}

// UseDatabaseStmt represents a USE DATABASE statement
type UseDatabaseStmt struct {
	DbName string
}

func (u *UseDatabaseStmt) iStatement() {}

func (u *UseDatabaseStmt) Format(buf *TrackedBuffer) {
	if u == nil {
		return
	}
	buf.Myprintf("use %s", u.DbName)
}

func (u *UseDatabaseStmt) walkSubtree(visit Visit) error {
	return nil
}

// NewUseDatabaseStmt creates a new UseDatabaseStmt
func NewUseDatabaseStmt(lexer yyLexer, dbName Token) *UseDatabaseStmt {
	stmt := &UseDatabaseStmt{
		DbName: tool.BytesToString(dbName.Bytes),
	}
	return stmt
}

type FlushDatabaseStmt struct {
	DbName string
}

func (f *FlushDatabaseStmt) iStatement() {}

func (f *FlushDatabaseStmt) Format(buf *TrackedBuffer) {
	if f == nil {
		return
	}
	buf.Myprintf("flush database %s", f.DbName)
}

func (f *FlushDatabaseStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewFlushDatabaseStmt(lexer yyLexer, dbName Token) *FlushDatabaseStmt {
	return &FlushDatabaseStmt{
		DbName: tool.BytesToString(dbName.Bytes),
	}
}

type SsMigrateDatabaseStmt struct {
	DbName string
}

func (s *SsMigrateDatabaseStmt) iStatement() {}

func (s *SsMigrateDatabaseStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("ssmigrate database %s", s.DbName)
}

func (s *SsMigrateDatabaseStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewSsMigrateDatabaseStmt(lexer yyLexer, dbName Token) *SsMigrateDatabaseStmt {
	return &SsMigrateDatabaseStmt{
		DbName: tool.BytesToString(dbName.Bytes),
	}
}

type TrimDatabaseStmt struct {
	DbName  string
	BwLimit int32
}

func (t *TrimDatabaseStmt) iStatement() {}

func (t *TrimDatabaseStmt) Format(buf *TrackedBuffer) {
	if t == nil {
		return
	}
	buf.Myprintf("trim database %s", t.DbName)
	if t.BwLimit > 0 {
		buf.Myprintf(" bwlimit %s", strconv.FormatInt(int64(t.BwLimit), 10))
	}
}

func (t *TrimDatabaseStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewTrimDatabaseStmt(lexer yyLexer, dbName Token, bwLimit int32) *TrimDatabaseStmt {
	return &TrimDatabaseStmt{
		DbName:  tool.BytesToString(dbName.Bytes),
		BwLimit: bwLimit,
	}
}

type TrimDatabaseWalStmt struct {
	DbName string
}

func (t *TrimDatabaseWalStmt) iStatement() {}

func (t *TrimDatabaseWalStmt) Format(buf *TrackedBuffer) {
	if t == nil {
		return
	}
	buf.Myprintf("trim database %s wal", t.DbName)
}

func (t *TrimDatabaseWalStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewTrimDatabaseWalStmt(lexer yyLexer, dbName Token) *TrimDatabaseWalStmt {
	return &TrimDatabaseWalStmt{
		DbName: tool.BytesToString(dbName.Bytes),
	}
}
