package sqlparser

import (
	"sqlparser/tool"
	"strconv"
)

type BalanceVgroupStmt struct{}

func (s *BalanceVgroupStmt) iStatement() {}

func (s *BalanceVgroupStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("balance vgroup")
}

func (s *BalanceVgroupStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewBalanceVgroupStmt() *BalanceVgroupStmt {
	return &BalanceVgroupStmt{}
}

type BalanceVgroupLeaderStmt struct {
	Database string
	VgroupID int32
}

func (s *BalanceVgroupLeaderStmt) iStatement() {}

func (s *BalanceVgroupLeaderStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("balance vgroup leader")
	if s.Database != "" {
		buf.Myprintf(" database %s", s.Database)
		return
	}
	if s.VgroupID >= 0 {
		buf.Myprintf(" %s", strconv.FormatInt(int64(s.VgroupID), 10))
	}
}

func (s *BalanceVgroupLeaderStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewBalanceVgroupLeaderByDBStmt(db string) *BalanceVgroupLeaderStmt {
	return &BalanceVgroupLeaderStmt{
		Database: db,
		VgroupID: -1,
	}
}

func NewBalanceVgroupLeaderByIDStmt(tok Token) *BalanceVgroupLeaderStmt {
	id := int32(-1)
	if v, err := tool.ConvertBytesToInt32(tok.Bytes); err == nil {
		id = v
	}
	return &BalanceVgroupLeaderStmt{
		VgroupID: id,
	}
}

func NewBalanceVgroupLeaderByOptionalID(tok Token) *BalanceVgroupLeaderStmt {
	if len(tok.Bytes) == 0 {
		return &BalanceVgroupLeaderStmt{VgroupID: -1}
	}
	return NewBalanceVgroupLeaderByIDStmt(tok)
}

type AssignLeaderStmt struct{}

func (s *AssignLeaderStmt) iStatement() {}

func (s *AssignLeaderStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("assign leader force")
}

func (s *AssignLeaderStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAssignLeaderStmt() *AssignLeaderStmt {
	return &AssignLeaderStmt{}
}

type AlterVgroupKeepStmt struct {
	VgroupID int32
	Keep     int32
}

func (s *AlterVgroupKeepStmt) iStatement() {}

func (s *AlterVgroupKeepStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf(
		"alter vgroup %s set keep %s",
		strconv.FormatInt(int64(s.VgroupID), 10),
		strconv.FormatInt(int64(s.Keep), 10),
	)
}

func (s *AlterVgroupKeepStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewAlterVgroupKeepStmt(vgroupTok, keepTok Token) *AlterVgroupKeepStmt {
	vgroupID := int32(-1)
	if v, err := tool.ConvertBytesToInt32(vgroupTok.Bytes); err == nil {
		vgroupID = v
	}
	keep := int32(-1)
	if v, err := tool.ConvertBytesToInt32(keepTok.Bytes); err == nil {
		keep = v
	}
	return &AlterVgroupKeepStmt{
		VgroupID: vgroupID,
		Keep:     keep,
	}
}

type MergeVgroupStmt struct {
	SourceVgroupID int32
	TargetVgroupID int32
}

func (s *MergeVgroupStmt) iStatement() {}

func (s *MergeVgroupStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf(
		"merge vgroup %s %s",
		strconv.FormatInt(int64(s.SourceVgroupID), 10),
		strconv.FormatInt(int64(s.TargetVgroupID), 10),
	)
}

func (s *MergeVgroupStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewMergeVgroupStmt(sourceTok, targetTok Token) *MergeVgroupStmt {
	sourceID := int32(-1)
	if v, err := tool.ConvertBytesToInt32(sourceTok.Bytes); err == nil {
		sourceID = v
	}
	targetID := int32(-1)
	if v, err := tool.ConvertBytesToInt32(targetTok.Bytes); err == nil {
		targetID = v
	}
	return &MergeVgroupStmt{
		SourceVgroupID: sourceID,
		TargetVgroupID: targetID,
	}
}

type SplitVgroupStmt struct {
	VgroupID int32
	Force    bool
}

func (s *SplitVgroupStmt) iStatement() {}

func (s *SplitVgroupStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("split vgroup %s", strconv.FormatInt(int64(s.VgroupID), 10))
	if s.Force {
		buf.Myprintf(" force")
	}
}

func (s *SplitVgroupStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewSplitVgroupStmt(vgroupTok Token, force bool) *SplitVgroupStmt {
	vgroupID := int32(-1)
	if v, err := tool.ConvertBytesToInt32(vgroupTok.Bytes); err == nil {
		vgroupID = v
	}
	return &SplitVgroupStmt{
		VgroupID: vgroupID,
		Force:    force,
	}
}

type RedistributeVgroupStmt struct {
	VgroupID int32
	DnodeIDs []int32
}

func (s *RedistributeVgroupStmt) iStatement() {}

func (s *RedistributeVgroupStmt) Format(buf *TrackedBuffer) {
	if s == nil {
		return
	}
	buf.Myprintf("redistribute vgroup %s", strconv.FormatInt(int64(s.VgroupID), 10))
	for _, d := range s.DnodeIDs {
		buf.Myprintf(" dnode %s", strconv.FormatInt(int64(d), 10))
	}
}

func (s *RedistributeVgroupStmt) walkSubtree(visit Visit) error {
	return nil
}

func NewRedistributeVgroupStmt(vgroupTok Token, dnodeToks []Token) *RedistributeVgroupStmt {
	vgroupID := int32(-1)
	if v, err := tool.ConvertBytesToInt32(vgroupTok.Bytes); err == nil {
		vgroupID = v
	}
	ids := make([]int32, 0, len(dnodeToks))
	for _, tok := range dnodeToks {
		if v, err := tool.ConvertBytesToInt32(tok.Bytes); err == nil {
			ids = append(ids, v)
		}
	}
	return &RedistributeVgroupStmt{
		VgroupID: vgroupID,
		DnodeIDs: ids,
	}
}
