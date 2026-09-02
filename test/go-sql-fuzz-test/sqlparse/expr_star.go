package sqlparser

type StarExpr struct {
	TableName string
}

func (node *StarExpr) iSelectExpr() {}
func (node *StarExpr) iExpr()       {}

func (node *StarExpr) replace(from, to Expr) bool {
	return false
}

// Format formats the node.
func (node *StarExpr) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	if node.TableName != "" {
		buf.Myprintf("%s.", node.TableName)
	}
	buf.Myprintf("*")
}

func (node *StarExpr) walkSubtree(visit Visit) error {
	return nil
}
