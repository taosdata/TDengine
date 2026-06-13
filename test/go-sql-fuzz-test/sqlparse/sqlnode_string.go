package sqlparser

import "bytes"

// SQLNodeToString renders a parser node back to SQL text for raw-clause carriers.
func SQLNodeToString(node SQLNode) string {
	if node == nil {
		return ""
	}
	tb := &TrackedBuffer{Buffer: &bytes.Buffer{}}
	node.Format(tb)
	return tb.String()
}
