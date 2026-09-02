package sqlparser

import (
	"fmt"
	"sqlparser/tool"
	"strconv"
	"strings"
	"time"
)

type CreateTableStmt struct {
	TableName    *TableName
	IgnoreExists bool
	Columns      []*ColumnDef
	Tags         []*ColumnDef
	Options      *TableOptions
	IsStable     bool
	IsVTable     bool
}

func (c *CreateTableStmt) iStatement() {}

func (c *CreateTableStmt) Format(buf *TrackedBuffer) {
	if c == nil {
		return
	}
	buf.Myprintf("create ")
	if c.IsStable {
		buf.Myprintf("stable ")
	} else if c.IsVTable {
		buf.Myprintf("vtable ")
	} else {
		buf.Myprintf("table ")
	}
	if c.IgnoreExists {
		buf.Myprintf("if not exists ")
	}
	if c.TableName != nil {
		buf.Myprintf("%v", *c.TableName)
	}
	if len(c.Columns) > 0 {
		formatColumnDefList(buf, c.Columns)
	}
	if len(c.Tags) > 0 {
		buf.Myprintf(" tags")
		formatColumnDefList(buf, c.Tags)
	}
	appendTableOptions(buf, c.Options)
}

func formatColumnDefList(buf *TrackedBuffer, cols []*ColumnDef) {
	buf.Myprintf(" (")
	for i, col := range cols {
		if i > 0 {
			buf.Myprintf(", ")
		}
		name := col.ColName
		if name != "" {
			name = quoteIdentifierIfNeeded(name)
		}
		buf.Myprintf("%s %s", name, formatDataType(col.DataType))
		if col.Options != nil {
			buf.Myprintf("%s", formatColumnOptionsForAlter(col.Options))
		}
	}
	buf.Myprintf(")")
}

func formatTDDurationList(buf *TrackedBuffer, durations []tool.TDDuration) {
	for i, d := range durations {
		if i > 0 {
			buf.Myprintf(",")
		}
		buf.Myprintf("%s", tdDurationToSQLLiteral(d))
	}
}

func appendTableOptions(buf *TrackedBuffer, opts *TableOptions) {
	if buf == nil || opts == nil {
		return
	}
	if opts.Comment != "" {
		buf.Myprintf(" comment '%s'", opts.Comment)
	}
	if len(opts.MaxDelay) > 0 {
		buf.Myprintf(" max_delay ")
		formatTDDurationList(buf, opts.MaxDelay)
	}
	if len(opts.Watermark) > 0 {
		buf.Myprintf(" watermark ")
		formatTDDurationList(buf, opts.Watermark)
	}
	if len(opts.RollupFuncs) > 0 {
		buf.Myprintf(" rollup(")
		for i, fn := range opts.RollupFuncs {
			if i > 0 {
				buf.Myprintf(",")
			}
			buf.Myprintf("%s", fn)
		}
		buf.Myprintf(")")
	}
	if opts.TTL > 0 {
		buf.Myprintf(" ttl %s", strconv.FormatInt(int64(opts.TTL), 10))
	}
	if len(opts.SMA) > 0 {
		buf.Myprintf(" sma(")
		for i, col := range opts.SMA {
			if i > 0 {
				buf.Myprintf(",")
			}
			buf.Myprintf("%s", quoteIdentifierIfNeeded(col))
		}
		buf.Myprintf(")")
	}
	if len(opts.DeleteMark) > 0 {
		buf.Myprintf(" delete_mark ")
		formatTDDurationList(buf, opts.DeleteMark)
	}
	if len(opts.Keep) > 0 {
		buf.Myprintf(" keep ")
		formatTDDurationList(buf, opts.Keep)
	}
	if opts.VirtualStb {
		buf.Myprintf(" virtual 1")
	}
}

func formatDataType(dt DataType) string {
	if int(dt.Type) < len(DataTypes) && dt.Type > 0 {
		name := strings.ToLower(DataTypes[dt.Type].Name)
		switch dt.Type {
		case TSDB_DATA_TYPE_VARCHAR, TSDB_DATA_TYPE_NCHAR, TSDB_DATA_TYPE_VARBINARY, TSDB_DATA_TYPE_GEOMETRY:
			if dt.Bytes > 0 {
				return fmt.Sprintf("%s(%d)", name, dt.Bytes)
			}
		case TSDB_DATA_TYPE_DECIMAL, TSDB_DATA_TYPE_DECIMAL64:
			if dt.Precision > 0 {
				if dt.Scale > 0 {
					return fmt.Sprintf("%s(%d,%d)", name, dt.Precision, dt.Scale)
				}
				return fmt.Sprintf("%s(%d)", name, dt.Precision)
			}
		}
		return name
	}
	return "int"
}

func (c *CreateTableStmt) walkSubtree(visit Visit) error {
	if c == nil {
		return nil
	}
	if c.TableName == nil {
		return nil
	}
	return Walk(visit, *c.TableName)
}

type TableOptions struct {
	VirtualStb  bool
	CommentNull bool
	Comment     string
	MaxDelay    []tool.TDDuration
	Watermark   []tool.TDDuration
	DeleteMark  []tool.TDDuration
	RollupFuncs []string
	TTL         int32
	SMA         []string
	Keep        []tool.TDDuration
}

type TableOptionType int

const (
	TABLE_OPTION_COMMENT TableOptionType = iota + 1
	TABLE_OPTION_MAXDELAY
	TABLE_OPTION_WATERMARK
	TABLE_OPTION_ROLLUP
	TABLE_OPTION_TTL
	TABLE_OPTION_SMA
	TABLE_OPTION_DELETE_MARK
	TABLE_OPTION_KEEP
	TABLE_OPTION_VIRTUAL
)

func SetTableOption(lexer yyLexer, options *TableOptions, opt TableOptionType, value interface{}) *TableOptions {
	if options == nil {
		options = &TableOptions{}
	}
	switch opt {
	case TABLE_OPTION_COMMENT:
		options.Comment = tool.BytesToString(value.([]byte))
		options.CommentNull = false
		return options
	case TABLE_OPTION_MAXDELAY:
		switch v := value.(type) {
		case []byte:
			duration, err := tool.ParseDuration(v)
			if err != nil {
				lexer.Error(fmt.Sprintf("invalid maxdelay duration %s", v))
				return nil
			}
			options.MaxDelay = append(options.MaxDelay, duration)
		case [][]byte:
			for _, dur := range v {
				duration, err := tool.ParseDuration(dur)
				if err != nil {
					lexer.Error(fmt.Sprintf("invalid maxdelay duration %s", dur))
					return nil
				}
				options.MaxDelay = append(options.MaxDelay, duration)
			}
		default:
			lexer.Error(fmt.Sprintf("invalid maxdelay duration type %T", value))
			return nil
		}
	case TABLE_OPTION_WATERMARK:
		durations := value.([][]byte)
		for _, dur := range durations {
			duration, err := tool.ParseDuration(dur)
			if err != nil {
				lexer.Error(fmt.Sprintf("invalid watermark duration %s", dur))
				return nil
			}
			options.Watermark = append(options.Watermark, duration)
		}
	case TABLE_OPTION_ROLLUP:
		values := value.([][]byte)
		for _, val := range values {
			options.RollupFuncs = append(options.RollupFuncs, tool.BytesToString(val))
		}
	case TABLE_OPTION_TTL:
		ttl, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid ttl value %s", value.([]byte)))
			return nil
		}
		options.TTL = int32(ttl)
	case TABLE_OPTION_SMA:
		switch v := value.(type) {
		case [][]byte:
			for _, val := range v {
				options.SMA = append(options.SMA, tool.BytesToString(val))
			}
		case []string:
			options.SMA = append(options.SMA, v...)
		default:
			lexer.Error(fmt.Sprintf("invalid sma value type %T", value))
			return nil
		}
	case TABLE_OPTION_DELETE_MARK:
		durations := value.([][]byte)
		for _, dur := range durations {
			duration, err := tool.ParseDuration(dur)
			if err != nil {
				lexer.Error(fmt.Sprintf("invalid delete mark duration %s", dur))
				return nil
			}
			options.DeleteMark = append(options.DeleteMark, duration)
		}
	case TABLE_OPTION_KEEP:
		switch v := value.(type) {
		case [][]byte:
			for _, dur := range v {
				duration, err := tool.ParseDuration(dur)
				if err != nil {
					lexer.Error(fmt.Sprintf("invalid keep duration %s", dur))
					return nil
				}
				options.Keep = append(options.Keep, duration)
			}
		case []byte:
			if tool.HasDurationUnit(v) {
				duration, err := tool.ParseDuration(v)
				if err != nil {
					lexer.Error(fmt.Sprintf("invalid keep duration %s", v))
					return nil
				}
				options.Keep = []tool.TDDuration{duration}
				return options
			}
			// fallback to days for plain integer inputs
			val, err := strconv.ParseInt(tool.BytesToString(v), 10, 64)
			if err != nil {
				lexer.Error(fmt.Sprintf("invalid keep duration %s", v))
				return nil
			}
			options.Keep = []tool.TDDuration{tool.NewTDDuration(time.Duration(val) * time.Hour * 24)}
		default:
			lexer.Error(fmt.Sprintf("invalid keep duration %s, type %T", value, value))
		}
	case TABLE_OPTION_VIRTUAL:
		val, err := strconv.ParseInt(tool.BytesToString(value.([]byte)), 10, 32)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid virtual stb value %s", value.([]byte)))
			return nil
		}
		switch val {
		case 0:
			options.VirtualStb = false
		case 1:
			options.VirtualStb = true
		default:
			lexer.Error(fmt.Sprintf("invalid virtual stb value %s", value.([]byte)))
			return nil
		}
	default:
		lexer.Error(fmt.Sprintf("unknown table option %d", opt))
		return nil
	}
	return options
}

type ColumnDef struct {
	ColName  string
	DataType DataType
	Options  *ColumnOption
	SMA      bool
}

type DataType struct {
	Type      uint8
	Precision uint8
	Scale     uint8
	Bytes     int32
}

type ColumnOption struct {
	CommentNull   bool
	Comment       string
	Encode        string
	Compress      string
	CompressLevel string
	PrimaryKey    bool
	HasRef        bool
	RefDB         string
	RefTable      string
	RefColumn     string
}

func CreateDataType(colType uint8) DataType {
	return DataType{
		Type:  colType,
		Bytes: DataTypes[colType].Bytes,
	}
}

func CreateVarLenDataType(lexer yyLexer, colType uint8, lengthVal []byte) DataType {
	length, err := strconv.ParseInt(tool.BytesToString(lengthVal), 10, 32)
	if err != nil {
		lexer.Error(fmt.Sprintf("invalid var length, cannot convert %s to int32", lengthVal))
	}
	typeLength := int32(TSDB_MAX_BINARY_LEN - VARSTR_HEADER_SIZE)
	if colType == TSDB_DATA_TYPE_NCHAR {
		typeLength /= TSDB_NCHAR_SIZE
	}
	if length > 0 {
		typeLength = int32(length)
	}
	return DataType{
		Type:  colType,
		Bytes: typeLength,
	}
}

func CreateDecimalDataType(lexer yyLexer, colType uint8, precisionVal, scaleVal []byte) DataType {
	u64precision, err := strconv.ParseUint(tool.BytesToString(precisionVal), 10, 8)
	if err != nil {
		lexer.Error(fmt.Sprintf("invalid decimal precision, cannot convert %s to uint8", precisionVal))
	}
	precision := uint8(u64precision)
	scale := uint8(0)
	if scaleVal != nil {
		u64Scale, err := strconv.ParseUint(tool.BytesToString(scaleVal), 10, 8)
		if err != nil {
			lexer.Error(fmt.Sprintf("invalid decimal scale, cannot convert %s to uint8", scaleVal))
		}
		scale = uint8(u64Scale)
	}
	return DataType{
		Type:      decimalTypeFromPrecision(colType),
		Precision: precision,
		Scale:     scale,
	}
}

func decimalTypeFromPrecision(precision uint8) uint8 {
	if precision > TSDB_DECIMAL64_MAX_PRECISION {
		return TSDB_DATA_TYPE_DECIMAL
	}
	return TSDB_DATA_TYPE_DECIMAL64
}

func SetColumnOptionsPK(colOption *ColumnOption) *ColumnOption {
	if colOption == nil {
		colOption = &ColumnOption{}
	}
	colOption.PrimaryKey = true
	return colOption
}

func SetColumnOptions(lexer yyLexer, colOption *ColumnOption, key []byte, value []byte) *ColumnOption {
	if colOption == nil {
		colOption = &ColumnOption{}
	}
	switch tool.BytesToString(key) {
	case "encode":
		colOption.Encode = tool.BytesToString(value)
	case "compress":
		colOption.Compress = tool.BytesToString(value)
	case "level":
		colOption.CompressLevel = tool.BytesToString(value)
	default:
		lexer.Error(fmt.Sprintf("unknown column option %s", key))
		return nil
	}
	return colOption
}

func SetColumnReference(colOption *ColumnOption, ref string) *ColumnOption {
	if colOption == nil {
		colOption = &ColumnOption{}
	}
	colOption.HasRef = true
	parts := strings.Split(ref, ".")
	switch len(parts) {
	case 1:
		colOption.RefColumn = parts[0]
	case 2:
		colOption.RefTable = parts[0]
		colOption.RefColumn = parts[1]
	case 3:
		colOption.RefDB = parts[0]
		colOption.RefTable = parts[1]
		colOption.RefColumn = parts[2]
	default:
		colOption.RefColumn = ref
	}
	return colOption
}
