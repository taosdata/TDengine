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

type Event struct {
	RunID   string
	QueryNo int64
	CaseID  string
	Rule    string
	SQL     string
	Stmt    sqlparser.Statement
}

type Logger interface {
	Generated(Event)
	Parsed(Event)
	Executed(Event)
	Error(Event, string, error)
	Close() error
}

type QueryDumper struct{}

func (q *QueryDumper) Generated(ev Event) {
	sqlText := strings.TrimSpace(ev.SQL)
	if strings.HasSuffix(sqlText, ";") {
		fmt.Println(sqlText)
		return
	}
	fmt.Println(sqlText + ";")
}
func (q *QueryDumper) Parsed(Event)               {}
func (q *QueryDumper) Executed(Event)             {}
func (q *QueryDumper) Error(Event, string, error) {}
func (q *QueryDumper) Close() error               { return nil }

type ASTLogger struct {
	prefix string
	count  atomic.Int64
}

func NewASTLogger(prefix string) *ASTLogger {
	if strings.TrimSpace(prefix) == "" {
		prefix = "sqlsmith"
	}
	return &ASTLogger{prefix: prefix}
}

func (a *ASTLogger) Generated(Event) {}
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
func (a *ASTLogger) Executed(Event)             {}
func (a *ASTLogger) Error(Event, string, error) {}
func (a *ASTLogger) Close() error               { return nil }

type CerrLogger struct {
	queries atomic.Int64
	columns int
	mu      sync.Mutex
	errors  map[string]int64
}

func NewCerrLogger() *CerrLogger {
	return &CerrLogger{columns: 80, errors: map[string]int64{}}
}

func (c *CerrLogger) Generated(Event) {
	q := c.queries.Add(1)
	if q%(int64(c.columns)*10) == int64(c.columns*10-1) {
		c.Report()
	}
}

func (c *CerrLogger) Parsed(Event) {}

func (c *CerrLogger) Executed(Event) {
	q := c.queries.Load()
	if q%int64(c.columns) == int64(c.columns-1) {
		fmt.Fprintln(os.Stderr)
	}
	fmt.Fprint(os.Stderr, ".")
}

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

func firstLine(s string) string {
	if s == "" {
		return ""
	}
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

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

func (c *CerrLogger) Close() error {
	c.Report()
	return nil
}

type graphML struct {
	XMLName xml.Name `xml:"graphml"`
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	Graph   graph    `xml:"graph"`
}

type graph struct {
	ID          string `xml:"id,attr"`
	EdgeDefault string `xml:"edgedefault,attr"`
	Nodes       []node `xml:"node"`
	Edges       []edge `xml:"edge"`
}

type node struct {
	ID   string     `xml:"id,attr"`
	Data []nodeData `xml:"data"`
}

type nodeData struct {
	Key   string `xml:"key,attr"`
	Value string `xml:",chardata"`
}

type edge struct {
	Source string `xml:"source,attr"`
	Target string `xml:"target,attr"`
}

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
