package sqlparser

type KillStmt struct {
	Kind   string
	Target string
}

func (k *KillStmt) iStatement() {}

func (k *KillStmt) Format(buf *TrackedBuffer) {
	if k == nil {
		return
	}
	if k.Kind == "query" {
		buf.Myprintf("kill query '%s'", k.Target)
		return
	}
	buf.Myprintf("kill %s %s", k.Kind, k.Target)
}

func (k *KillStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewKillStmt(kind string, target Token) *KillStmt {
	return &KillStmt{
		Kind:   kind,
		Target: string(target.Bytes),
	}
}
