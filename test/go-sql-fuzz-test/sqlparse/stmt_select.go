package sqlparser

type SelectStmt struct {
	IsDistinct bool
	Hint       *HintOption
	//TimeLineFromOrderBy       bool
	//IsEmptyResult             bool
	//IsSubquery                bool
	//HasAggFuncs               bool
	//HasRepeatScanFuncs        bool
	//HasIndefiniteRowsFunc     bool
	//HasMultiRowsFunc          bool
	//HasSelectFunc             bool
	//HasSelectValFunc          bool
	//HasOtherVectorFunc        bool
	//HasUniqueFunc             bool
	//HasTailFunc               bool
	//HasInterpFunc             bool
	//HasInterpPseudoColFunc    bool
	//HasForecastFunc           bool
	//HasForecastPseudoColFunc  bool
	//HasGenericAnalysisFunc    bool
	//HasLastRowFunc            bool
	//HasLastFunc               bool
	//HasTimeLineFunc           bool
	//HasCountFunc              bool
	//HasUdaf                   bool
	//HasStateKey               bool
	//HasTwaOrElapsedFunc       bool
	//OnlyHasKeepOrderFunc      bool
	//GroupSort                 bool
	TagScan    bool
	Select     []Expr
	From       TableExpr
	Where      Expr
	Partition  Expr
	Range      Expr
	Every      Literal
	InterpFill *FillExpr
	Window     WindowExpr
	GroupBy    *GroupByExpr
	Having     Expr
	OrderBy    []OrderByExpr
	SLimit     *LimitExpr
	Limit      *LimitExpr
	Left       *SelectStmt
	Right      *SelectStmt
	SetOp      string
	SetAll     bool
	//JoinContains              bool
	//MixSysTableAndActualTable bool
}

func (s *SelectStmt) iStatement() {}
func (s *SelectStmt) iExpr()      {}

func (s *SelectStmt) replace(from, to Expr) bool {
	if s == nil {
		return false
	}
	for i := range s.Select {
		if s.Select[i] == from {
			s.Select[i] = to
			return true
		}
		if s.Select[i] != nil && s.Select[i].replace(from, to) {
			return true
		}
	}
	if s.Where == from {
		s.Where = to
		return true
	}
	if s.Where != nil && s.Where.replace(from, to) {
		return true
	}
	if s.Partition == from {
		s.Partition = to
		return true
	}
	if s.Partition != nil && s.Partition.replace(from, to) {
		return true
	}
	if s.Range == from {
		s.Range = to
		return true
	}
	if s.Range != nil && s.Range.replace(from, to) {
		return true
	}
	if s.Having == from {
		s.Having = to
		return true
	}
	if s.Having != nil && s.Having.replace(from, to) {
		return true
	}
	if s.GroupBy != nil {
		for i := range s.GroupBy.Exprs {
			if s.GroupBy.Exprs[i] == from {
				s.GroupBy.Exprs[i] = to
				return true
			}
			if s.GroupBy.Exprs[i] != nil && s.GroupBy.Exprs[i].replace(from, to) {
				return true
			}
		}
	}
	for i := range s.OrderBy {
		if s.OrderBy[i].Expr == from {
			s.OrderBy[i].Expr = to
			return true
		}
		if s.OrderBy[i].Expr != nil && s.OrderBy[i].Expr.replace(from, to) {
			return true
		}
	}
	if s.Window.Session == from {
		s.Window.Session = to
		return true
	}
	if s.Window.Session != nil && s.Window.Session.replace(from, to) {
		return true
	}
	if s.Window.StateWindow == from {
		s.Window.StateWindow = to
		return true
	}
	if s.Window.StateWindow != nil && s.Window.StateWindow.replace(from, to) {
		return true
	}
	if s.Window.EventWindowStart == from {
		s.Window.EventWindowStart = to
		return true
	}
	if s.Window.EventWindowStart != nil && s.Window.EventWindowStart.replace(from, to) {
		return true
	}
	if s.Window.EventWindowEnd == from {
		s.Window.EventWindowEnd = to
		return true
	}
	if s.Window.EventWindowEnd != nil && s.Window.EventWindowEnd.replace(from, to) {
		return true
	}
	if s.Window.AnomalyWindow == from {
		s.Window.AnomalyWindow = to
		return true
	}
	if s.Window.AnomalyWindow != nil && s.Window.AnomalyWindow.replace(from, to) {
		return true
	}
	if s.InterpFill != nil {
		for i := range s.InterpFill.Values {
			if s.InterpFill.Values[i] == from {
				s.InterpFill.Values[i] = to
				return true
			}
			if s.InterpFill.Values[i] != nil && s.InterpFill.Values[i].replace(from, to) {
				return true
			}
		}
	}
	if s.Left != nil && s.Left.replace(from, to) {
		return true
	}
	if s.Right != nil && s.Right.replace(from, to) {
		return true
	}
	return false
}

func (s *SelectStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	writeTail := func() {
		if len(s.OrderBy) > 0 {
			buf.Myprintf(" order by ")
			for i := range s.OrderBy {
				if i > 0 {
					buf.Myprintf(", ")
				}
				buf.Myprintf("%v", &s.OrderBy[i])
			}
		}
		if s.SLimit != nil {
			buf.Myprintf(" %v", s.SLimit)
		}
		if s.Limit != nil {
			buf.Myprintf(" %v", s.Limit)
		}
	}
	if s.Left != nil && s.Right != nil && s.SetOp != "" {
		buf.Myprintf("%v %s", s.Left, s.SetOp)
		if s.SetAll {
			buf.Myprintf(" all")
		}
		buf.Myprintf(" %v", s.Right)
		writeTail()
		return
	}
	buf.Myprintf("select")
	if s.Hint != nil {
		if h := hintTypeToSQL(s.Hint.HintType); h != "" {
			buf.Myprintf(" /*+ %s */", h)
		}
	}
	if s.IsDistinct {
		buf.Myprintf(" distinct")
	}
	if len(s.Select) == 0 {
		if s.Hint != nil && s.From == nil && s.Where == nil && s.GroupBy == nil && s.Having == nil {
			// Keep empty projection for hint-only statements like `select /*+ ... */;`.
		} else {
			buf.Myprintf(" *")
		}
	} else {
		for i, expr := range s.Select {
			if i == 0 {
				buf.Myprintf(" ")
			} else {
				buf.Myprintf(", ")
			}
			if sub, ok := expr.(*SelectStmt); ok {
				buf.Myprintf("(%v)", sub)
				continue
			}
			buf.Myprintf("%v", expr)
		}
	}
	if s.From != nil {
		buf.Myprintf(" from %v", s.From)
	}
	if s.Where != nil {
		buf.Myprintf(" where %v", s.Where)
	}
	if s.GroupBy != nil && len(s.GroupBy.Exprs) > 0 {
		buf.Myprintf(" group by %v", s.GroupBy)
	}
	if s.Having != nil {
		buf.Myprintf(" having %v", s.Having)
	}
	if s.Partition != nil {
		buf.Myprintf(" partition by %v", s.Partition)
	}
	if s.Range != nil {
		buf.Myprintf(" range %v", s.Range)
	}
	if len(s.Every.Val.Bytes) > 0 {
		buf.Myprintf(" every(%v)", s.Every)
	}
	if s.InterpFill != nil {
		buf.Myprintf(" fill(%v)", s.InterpFill)
	}
	if !s.Window.isEmpty() {
		buf.Myprintf(" %v", &s.Window)
	}
	writeTail()
}

func (s *SelectStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	for _, expr := range s.Select {
		if err := Walk(visit, expr); err != nil {
			return err
		}
	}
	if err := Walk(
		visit,
		s.From,
		s.Where,
		s.Partition,
		s.Range,
		s.InterpFill,
		&s.Window,
		s.GroupBy,
		s.Having,
		s.SLimit,
		s.Limit,
		s.Left,
		s.Right,
	); err != nil {
		return err
	}
	for _, ob := range s.OrderBy {
		if err := Walk(visit, ob.Expr); err != nil {
			return err
		}
	}
	return nil
}

func (s *SelectStmt) SetSelectStmtTagMode(tagMode bool) {
	s.TagScan = tagMode
}

type HintType int

const (
	HINT_NO_BATCH_SCAN HintType = iota + 1
	HINT_BATCH_SCAN
	HINT_SORT_FOR_GROUP
	HINT_PARTITION_FIRST
	HINT_PARA_TABLES_SORT
	HINT_SMALLDATA_TS_SORT
	HINT_HASH_JOIN
	HINT_SKIP_TSMA
	HINT_WIN_OPTIMIZE_BATCH
	HINT_WIN_OPTIMIZE_SINGLE
)

type HintOption struct {
	HintType HintType
}

func NewHintOption(hintType HintType) *HintOption {
	return &HintOption{
		HintType: hintType,
	}
}

func hintTypeToSQL(h HintType) string {
	switch h {
	case HINT_NO_BATCH_SCAN:
		return "no_batch_scan()"
	case HINT_BATCH_SCAN:
		return "batch_scan()"
	case HINT_SORT_FOR_GROUP:
		return "sort_for_group()"
	case HINT_PARTITION_FIRST:
		return "partition_first()"
	case HINT_PARA_TABLES_SORT:
		return "para_tables_sort()"
	case HINT_SMALLDATA_TS_SORT:
		return "smalldata_ts_sort()"
	case HINT_HASH_JOIN:
		return "hash_join()"
	case HINT_SKIP_TSMA:
		return "skip_tsma()"
	case HINT_WIN_OPTIMIZE_BATCH:
		return "win_optimize_batch()"
	case HINT_WIN_OPTIMIZE_SINGLE:
		return "win_optimize_single()"
	default:
		return ""
	}
}
