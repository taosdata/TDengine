package sqlparser

type ResetQueryCacheStmt struct{}

func (*ResetQueryCacheStmt) iStatement() {}

func (*ResetQueryCacheStmt) Format(buf *TrackedBuffer) {
	buf.Myprintf("reset query cache")
}

func (*ResetQueryCacheStmt) walkSubtree(visit Visit) error { return nil }
