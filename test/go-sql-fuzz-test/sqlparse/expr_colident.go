package sqlparser

import (
	"encoding/json"
	"strings"
)

// ColIdent is a case insensitive SQL identifier. It will be escaped with
// backquotes if necessary.
type ColIdent struct {
	// This artifact prevents this struct from being compared
	// with itself. It consumes no space as long as it's not the
	// last field in the struct.
	_            [0]struct{ _ []byte }
	val, lowered string
}

// NewColIdent makes a new ColIdent.
func NewColIdent(str string) ColIdent {
	return ColIdent{
		val: str,
	}
}

// Format formats the node.
func (node ColIdent) Format(buf *TrackedBuffer) {
	formatID(buf, node.val, node.Lowered())
}

func (node ColIdent) walkSubtree(visit Visit) error {
	return nil
}

// IsEmpty returns true if the name is empty.
func (node ColIdent) IsEmpty() bool {
	return node.val == ""
}

// String returns the unescaped column name. It must
// not be used for SQL generation. Use sqlparser.String
// instead. The Stringer conformance is for usage
// in templates.
func (node ColIdent) String() string {
	return node.val
}

// CompliantName returns a compliant id name
// that can be used for a bind var.
func (node ColIdent) CompliantName() string {
	return compliantName(node.val)
}

// Lowered returns a lower-cased column name.
// This function should generally be used only for optimizing
// comparisons.
func (node ColIdent) Lowered() string {
	if node.val == "" {
		return ""
	}
	if node.lowered == "" {
		node.lowered = strings.ToLower(node.val)
	}
	return node.lowered
}

// Equal performs a case-insensitive compare.
func (node ColIdent) Equal(in ColIdent) bool {
	return node.Lowered() == in.Lowered()
}

// EqualString performs a case-insensitive compare with str.
func (node ColIdent) EqualString(str string) bool {
	return node.Lowered() == strings.ToLower(str)
}

// MarshalJSON marshals into JSON.
func (node ColIdent) MarshalJSON() ([]byte, error) {
	return json.Marshal(node.val)
}

// UnmarshalJSON unmarshals from JSON.
func (node *ColIdent) UnmarshalJSON(b []byte) error {
	var result string
	err := json.Unmarshal(b, &result)
	if err != nil {
		return err
	}
	node.val = result
	return nil
}
