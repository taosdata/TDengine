package sqlparser

// AliasedExpr defines an aliased SELECT expression.
type AliasedExpr struct {
	Expr  Expr
	As    ColIdent
	Alias string
}

func (node *AliasedExpr) iSelectExpr() {}
func (node *AliasedExpr) iExpr()       {}

func (node *AliasedExpr) replace(from, to Expr) bool {
	if node == nil || node.Expr == nil {
		return false
	}
	if node.Expr == from {
		node.Expr = to
		return true
	}
	return node.Expr.replace(from, to)
}

// Format formats the node.
func (node *AliasedExpr) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	if node.Expr == nil {
		return
	}
	buf.Myprintf("%v", node.Expr)
	if node.Alias != "" {
		buf.Myprintf(" as %s", node.Alias)
		return
	}
	if !node.As.IsEmpty() {
		buf.Myprintf(" as %v", node.As)
	}
}

func (node *AliasedExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
		node.As,
	)
}
