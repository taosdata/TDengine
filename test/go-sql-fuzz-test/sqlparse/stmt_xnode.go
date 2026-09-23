// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

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
