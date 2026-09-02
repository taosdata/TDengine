package sqlparser

import (
	"strconv"
	"strings"
)

type XnodeStmt struct {
	Action       string
	ResourceType string
	Endpoint     string
	ID           int32
	Force        bool
	User         string
	Pass         string
	TaskFrom     string
	TaskTo       string
	TaskOptions  string
}

func (*XnodeStmt) iStatement() {}

func (s *XnodeStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("%s xnode", s.Action)
	if s.ResourceType != "" {
		buf.Myprintf(" %s", s.ResourceType)
	}
	if s.Endpoint != "" {
		buf.Myprintf(" %s", formatXnodeAtom(s.Endpoint))
	}
	if s.ID >= 0 {
		buf.Myprintf(" %s", strconv.FormatInt(int64(s.ID), 10))
	}
	if s.Force {
		buf.Myprintf(" force")
	}
	if s.User != "" {
		buf.Myprintf(" user %s", s.User)
	}
	if s.Pass != "" {
		buf.Myprintf(" pass '%s'", s.Pass)
	}
	if s.TaskFrom != "" {
		buf.Myprintf(" from %s", formatXnodeAtom(s.TaskFrom))
	}
	if s.TaskTo != "" {
		buf.Myprintf(" to %s", formatXnodeAtom(s.TaskTo))
	}
	if s.TaskOptions != "" {
		buf.Myprintf(" with %s", s.TaskOptions)
	}
}

func (s *XnodeStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewXnodeStmt(action, resourceType, endpoint string, id int32, force bool, user, pass string) *XnodeStmt {
	return &XnodeStmt{
		Action:       action,
		ResourceType: resourceType,
		Endpoint:     endpoint,
		ID:           id,
		Force:        force,
		User:         user,
		Pass:         pass,
	}
}

func formatXnodeAtom(v string) string {
	if _, err := strconv.ParseInt(v, 10, 64); err == nil {
		return v
	}
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}
