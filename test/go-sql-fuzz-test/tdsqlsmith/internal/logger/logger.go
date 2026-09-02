// Package logger provides structured loggers that observe SQL fuzzing events such as generation, parsing, execution, and errors.
//
// Package logger 提供结构化日志器，用于观察 SQL fuzzing 过程中的生成、解析、执行与错误等事件。
package logger

import (
	"encoding/xml"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"sqlparser"
	"tdsqlsmith/internal/impedance"
)

// Event carries the context of a single fuzzing step passed to logger callbacks.
//
// Event 携带单个 fuzzing 步骤的上下文，传递给日志器回调。
type Event struct {
	RunID   string              // identifier of the current fuzz run / 当前 fuzz 运行的标识
	QueryNo int64               // sequential number of the query within the run / 查询在本次运行中的序号
	CaseID  string              // identifier of the case the query belongs to / 查询所属用例的标识
	Rule    string              // grammar rule associated with the query / 与查询关联的文法规则
	SQL     string              // generated SQL text / 生成的 SQL 文本
	Stmt    sqlparser.Statement // parsed statement, when available / 解析得到的语句（如果有）
}

// Logger receives callbacks at each stage of the fuzzing lifecycle.
//
// Logger 在 fuzzing 生命周期的每个阶段接收回调。
type Logger interface {
	Generated(Event)            // called when a query is generated / 当查询被生成时调用
	Parsed(Event)               // called after a query is parsed / 当查询被解析后调用
	Executed(Event)             // called after a query is executed / 当查询被执行后调用
	Error(Event, string, error) // called when a query errors, with its class and error / 当查询出错时调用，携带其分类与错误
	Close() error               // flushes and releases the logger / 刷新并释放日志器
}

// QueryDumper is a Logger that prints each generated SQL statement to stdout.
//
// QueryDumper 是一个 Logger，将每条生成的 SQL 语句打印到 stdout。
type QueryDumper struct{}

// Generated prints the generated SQL to stdout, ensuring it ends with a semicolon.
//
// Generated 将生成的 SQL 打印到 stdout，并确保其以分号结尾。
func (q *QueryDumper) Generated(ev Event) {
	sqlText := strings.TrimSpace(ev.SQL)
	if strings.HasSuffix(sqlText, ";") {
		fmt.Println(sqlText)
		return
	}
	fmt.Println(sqlText + ";")
}

// Parsed is a no-op for QueryDumper.
//
// Parsed 对 QueryDumper 而言是空操作。
func (q *QueryDumper) Parsed(Event) {}

// Executed is a no-op for QueryDumper.
//
// Executed 对 QueryDumper 而言是空操作。
func (q *QueryDumper) Executed(Event) {}

// Error is a no-op for QueryDumper.
//
// Error 对 QueryDumper 而言是空操作。
func (q *QueryDumper) Error(Event, string, error) {}

// Close is a no-op for QueryDumper.
//
// Close 对 QueryDumper 而言是空操作。
func (q *QueryDumper) Close() error { return nil }

// ASTLogger is a Logger that writes each parsed statement's AST to a GraphML file.
//
// ASTLogger 是一个 Logger，将每条解析后语句的 AST 写入 GraphML 文件。
type ASTLogger struct {
	prefix string       // filename prefix for emitted GraphML files / 输出 GraphML 文件的文件名前缀
	count  atomic.Int64 // counter used to make file names unique / 用于使文件名唯一的计数器
}

// NewASTLogger returns an ASTLogger using prefix for output file names, defaulting to "sqlsmith".
//
// NewASTLogger 返回一个 ASTLogger，使用 prefix 作为输出文件名前缀，默认为 "sqlsmith"。
func NewASTLogger(prefix string) *ASTLogger {
	if strings.TrimSpace(prefix) == "" {
		prefix = "sqlsmith"
	}
	return &ASTLogger{prefix: prefix}
}

// Generated is a no-op for ASTLogger.
//
// Generated 对 ASTLogger 而言是空操作。
func (a *ASTLogger) Generated(Event) {}

// Parsed writes the parsed statement's AST to a uniquely named GraphML file.
//
// Parsed 将解析后语句的 AST 写入一个名称唯一的 GraphML 文件。
func (a *ASTLogger) Parsed(ev Event) {
	if ev.Stmt == nil {
		return
	}
	id := a.count.Add(1)
	path := fmt.Sprintf("%s-%d.graphml", a.prefix, id)
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	_ = dumpGraphML(f, ev.Stmt)
}

// Executed is a no-op for ASTLogger.
//
// Executed 对 ASTLogger 而言是空操作。
func (a *ASTLogger) Executed(Event) {}

// Error is a no-op for ASTLogger.
//
// Error 对 ASTLogger 而言是空操作。
func (a *ASTLogger) Error(Event, string, error) {}

// Close is a no-op for ASTLogger.
//
// Close 对 ASTLogger 而言是空操作。
func (a *ASTLogger) Close() error { return nil }

// CerrLogger is a Logger that prints a compact progress display and an error summary to stderr.
//
// CerrLogger 是一个 Logger，将紧凑的进度显示与错误摘要打印到 stderr。
type CerrLogger struct {
	queries atomic.Int64     // total queries observed / 已观察到的查询总数
	columns int              // number of progress marks printed per line / 每行打印的进度标记数量
	mu      sync.Mutex       // guards errors / 保护 errors
	errors  map[string]int64 // counts of distinct error first-lines / 各不同错误首行的计数
}

// NewCerrLogger returns a CerrLogger configured with an 80-column progress display.
//
// NewCerrLogger 返回一个配置为 80 列进度显示的 CerrLogger。
func NewCerrLogger() *CerrLogger {
	return &CerrLogger{columns: 80, errors: map[string]int64{}}
}

// Generated counts the query and periodically prints a full report.
//
// Generated 对查询计数，并周期性地打印完整报告。
func (c *CerrLogger) Generated(Event) {
	q := c.queries.Add(1)
	if q%(int64(c.columns)*10) == int64(c.columns*10-1) {
		c.Report()
	}
}

// Parsed is a no-op for CerrLogger.
//
// Parsed 对 CerrLogger 而言是空操作。
func (c *CerrLogger) Parsed(Event) {}

// Executed prints a progress dot, wrapping to a new line at the column width.
//
// Executed 打印一个进度点，到达列宽时换行。
func (c *CerrLogger) Executed(Event) {
	q := c.queries.Load()
	if q%int64(c.columns) == int64(c.columns-1) {
		fmt.Fprintln(os.Stderr)
	}
	fmt.Fprint(os.Stderr, ".")
}

// Error prints a class-specific symbol and tallies the error's first line for the summary report.
//
// Error 打印与分类对应的符号，并将错误首行计入摘要报告。
func (c *CerrLogger) Error(_ Event, class string, err error) {
	q := c.queries.Load()
	if q%int64(c.columns) == int64(c.columns-1) {
		fmt.Fprintln(os.Stderr)
	}
	sym := symbolForClass(class)
	fmt.Fprint(os.Stderr, sym)

	if err != nil {
		line := firstLine(err.Error())
		c.mu.Lock()
		c.errors[line]++
		c.mu.Unlock()
	}
}

// symbolForClass maps an error class to the single-character symbol shown in the progress display.
//
// symbolForClass 将错误分类映射为进度显示中所示的单字符符号。
func symbolForClass(class string) string {
	switch class {
	case "syntax", "parse_reject":
		return "S"
	case "timeout":
		return "t"
	case "conn_lost":
		return "C"
	default:
		return "e"
	}
}

// firstLine returns the trimmed first line of s.
//
// firstLine 返回 s 经修剪后的首行。
func firstLine(s string) string {
	if s == "" {
		return ""
	}
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

// Report prints the query total, the most frequent errors, the overall error rate, and the impedance table to stderr.
//
// Report 将查询总数、最频繁的错误、总体错误率以及阻抗表打印到 stderr。
func (c *CerrLogger) Report() {
	queries := c.queries.Load()
	if queries == 0 {
		fmt.Fprintln(os.Stderr, "queries: 0")
		return
	}
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "queries: %d\n", queries)
	pairs := make([]struct {
		Err   string
		Count int64
	}, 0, 32)
	c.mu.Lock()
	for k, v := range c.errors {
		pairs = append(pairs, struct {
			Err   string
			Count int64
		}{Err: k, Count: v})
	}
	c.mu.Unlock()
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Count == pairs[j].Count {
			return pairs[i].Err < pairs[j].Err
		}
		return pairs[i].Count > pairs[j].Count
	})
	total := int64(0)
	for _, p := range pairs {
		total += p.Count
		errLine := p.Err
		if len(errLine) > 80 {
			errLine = errLine[:80]
		}
		fmt.Fprintf(os.Stderr, "%d\t%s\n", p.Count, errLine)
	}
	fmt.Fprintf(os.Stderr, "error rate: %.6f\n", float64(total)/float64(queries))
	fmt.Fprintln(os.Stderr, "impedance report:")
	for _, row := range impedance.Rows() {
		flag := ""
		if !impedance.Matched(row.Prod) {
			flag = " -> BLACKLISTED"
		}
		fmt.Fprintf(os.Stderr, "  %s: %d/%d (bad/ok)%s\n", row.Prod, row.Bad, row.OK, flag)
	}
}

// Close prints a final report and returns nil.
//
// Close 打印最终报告并返回 nil。
func (c *CerrLogger) Close() error {
	c.Report()
	return nil
}

// graphML is the root element of a GraphML document.
//
// graphML 是 GraphML 文档的根元素。
type graphML struct {
	XMLName xml.Name `xml:"graphml"`              // root element name / 根元素名称
	XMLNS   string   `xml:"xmlns,attr,omitempty"` // GraphML namespace / GraphML 命名空间
	Graph   graph    `xml:"graph"`                // the single graph contained in the document / 文档中包含的唯一 graph
}

// graph is a GraphML graph holding nodes and edges.
//
// graph 是一个持有节点与边的 GraphML 图。
type graph struct {
	ID          string `xml:"id,attr"`          // graph identifier / 图标识
	EdgeDefault string `xml:"edgedefault,attr"` // default edge direction / 默认边方向
	Nodes       []node `xml:"node"`             // graph nodes / 图节点
	Edges       []edge `xml:"edge"`             // graph edges / 图的边
}

// node is a GraphML node carrying arbitrary data entries.
//
// node 是一个携带任意数据项的 GraphML 节点。
type node struct {
	ID   string     `xml:"id,attr"` // node identifier / 节点标识
	Data []nodeData `xml:"data"`    // associated data entries / 关联的数据项
}

// nodeData is a key/value data entry attached to a GraphML node.
//
// nodeData 是附加到 GraphML 节点上的键/值数据项。
type nodeData struct {
	Key   string `xml:"key,attr"`  // data key (e.g. "label") / 数据键（如 "label"）
	Value string `xml:",chardata"` // data value / 数据值
}

// edge is a directed GraphML edge between two nodes.
//
// edge 是两个节点之间的有向 GraphML 边。
type edge struct {
	Source string `xml:"source,attr"` // source node id / 源节点 id
	Target string `xml:"target,attr"` // target node id / 目标节点 id
}

// dumpGraphML walks stmt's AST and encodes it as a GraphML document written to f.
//
// dumpGraphML 遍历 stmt 的 AST，并将其编码为写入 f 的 GraphML 文档。
func dumpGraphML(f *os.File, stmt sqlparser.Statement) error {
	nodes := []node{}
	edges := []edge{}
	idx := 0
	prev := ""
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (bool, error) {
		id := fmt.Sprintf("n%d", idx)
		idx++
		nodes = append(nodes, node{ID: id, Data: []nodeData{{Key: "label", Value: fmt.Sprintf("%T", n)}}})
		if prev != "" {
			edges = append(edges, edge{Source: prev, Target: id})
		}
		prev = id
		return true, nil
	}, stmt)

	gm := graphML{
		XMLNS: "http://graphml.graphdrawing.org/xmlns",
		Graph: graph{ID: "ast", EdgeDefault: "directed", Nodes: nodes, Edges: edges},
	}
	_, _ = f.WriteString(xml.Header)
	enc := xml.NewEncoder(f)
	enc.Indent("", "  ")
	return enc.Encode(gm)
}
