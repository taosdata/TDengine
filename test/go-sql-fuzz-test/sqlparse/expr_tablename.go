package sqlparser

import (
	"bytes"
	"encoding/json"
	"sqlparser/tool"
	"strings"
)

// TableIdent is a case sensitive SQL identifier. It will be escaped with
// backquotes if necessary.
type TableIdent struct {
	v string
}

// NewTableIdent creates a new TableIdent.
func NewTableIdent(str string) TableIdent {
	return TableIdent{v: str}
}

// Format formats the node.
func (node TableIdent) Format(buf *TrackedBuffer) {
	formatID(buf, node.v, strings.ToLower(node.v))
}

func formatID(buf *TrackedBuffer, original, lowered string) {
	isDbSystemVariable := false
	if len(original) > 1 && original[:2] == "@@" {
		isDbSystemVariable = true
	}

	for i, c := range original {
		if !tool.IsLetter(uint16(c)) && (!isDbSystemVariable || !tool.IsCarat(uint16(c))) {
			if i == 0 || !tool.IsDigit(uint16(c)) {
				goto mustEscape
			}
		}
	}
	if _, ok := Keywords[lowered]; ok {
		goto mustEscape
	}
	buf.Myprintf("%s", original)
	return

mustEscape:
	buf.WriteByte('`')
	for _, c := range original {
		buf.WriteRune(c)
		if c == '`' {
			buf.WriteByte('`')
		}
	}
	buf.WriteByte('`')
}
func (node TableIdent) walkSubtree(visit Visit) error {
	return nil
}

// IsEmpty returns true if TabIdent is empty.
func (node TableIdent) IsEmpty() bool {
	return node.v == ""
}

// String returns the unescaped table name. It must
// not be used for SQL generation. Use sqlparser.String
// instead. The Stringer conformance is for usage
// in templates.
func (node TableIdent) String() string {
	return node.v
}

// CompliantName returns a compliant id name
// that can be used for a bind var.
func (node TableIdent) CompliantName() string {
	return compliantName(node.v)
}

func compliantName(in string) string {
	var buf bytes.Buffer
	for i, c := range in {
		if !tool.IsLetter(uint16(c)) {
			if i == 0 || !tool.IsDigit(uint16(c)) {
				buf.WriteByte('_')
				continue
			}
		}
		buf.WriteRune(c)
	}
	return buf.String()
}

// MarshalJSON marshals into JSON.
func (node TableIdent) MarshalJSON() ([]byte, error) {
	return json.Marshal(node.v)
}

// UnmarshalJSON unmarshals from JSON.
func (node *TableIdent) UnmarshalJSON(b []byte) error {
	var result string
	err := json.Unmarshal(b, &result)
	if err != nil {
		return err
	}
	node.v = result
	return nil
}

// TableName represents a table  name.
// Qualifier, if specified, represents a database or keyspace.
// TableName is a value struct whose fields are case sensitive.
// This means two TableName vars can be compared for equality
// and a TableName can also be used as key in a map.
type TableName struct {
	Name, Qualifier TableIdent
}

// Format formats the node.
func (node TableName) Format(buf *TrackedBuffer) {
	if node.IsEmpty() {
		return
	}
	if !node.Qualifier.IsEmpty() {
		buf.Myprintf("%v.", node.Qualifier)
	}
	buf.Myprintf("%v", node.Name)
}

func (node TableName) walkSubtree(visit Visit) error {
	return Walk(
		visit,
		node.Name,
		node.Qualifier,
	)
}

// IsEmpty returns true if TableName is nil or empty.
func (node TableName) IsEmpty() bool {
	// If Name is empty, Qualifer is also empty.
	return node.Name.IsEmpty()
}

// ToViewName returns a TableName acceptable for use as a VIEW. VIEW names are
// always lowercase, so ToViewName lowercasese the name. Databases are case-sensitive
// so Qualifier is left untouched.
func (node TableName) ToViewName() TableName {
	return TableName{
		Qualifier: node.Qualifier,
		Name:      NewTableIdent(strings.ToLower(node.Name.v)),
	}
}
