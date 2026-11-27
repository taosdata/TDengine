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

import "strconv"

type ExplainOptions struct {
	VerboseSet bool
	Verbose    bool
	RatioSet   bool
	Ratio      Literal
}

type ExplainStmt struct {
	Analyze bool
	Options ExplainOptions
	Target  Statement
}

func (*ExplainStmt) iStatement() {}

func (s *ExplainStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("explain")
	if s.Analyze {
		buf.Myprintf(" analyze")
	}
	if s.Options.VerboseSet {
		buf.Myprintf(" verbose %s", strconv.FormatBool(s.Options.Verbose))
	}
	if s.Options.RatioSet {
		buf.Myprintf(" ratio %v", s.Options.Ratio)
	}
	if s.Target != nil {
		buf.Myprintf(" %v", s.Target)
	}
}

func (s *ExplainStmt) walkSubtree(visit Visit) error {
	if s == nil {
		return nil
	}
	if s.Options.RatioSet {
		return Walk(visit, s.Target, s.Options.Ratio)
	}
	return Walk(visit, s.Target)
}
