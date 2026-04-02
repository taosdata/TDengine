package stmt

import (
	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

// Deprecated: use unified.Stmt from package ws/unified instead.
type Stmt struct {
	core         *unified.Stmt
	connector    *Connector
	lastAffected int
}

func (s *Stmt) connectorClosed() bool {
	return s != nil && s.connector != nil && s.connector.isClosed()
}

// Deprecated: use (*unified.Stmt).Prepare instead.
func (s *Stmt) Prepare(sql string) error {
	if s.core == nil || s.connectorClosed() {
		return client.ClosedError
	}
	return mapUnifiedError(s.core.Prepare(0, sql))
}

// Deprecated: use (*unified.Stmt).SetTableName instead.
func (s *Stmt) SetTableName(name string) error {
	if s.core == nil || s.connectorClosed() {
		return client.ClosedError
	}
	return mapUnifiedError(s.core.SetTableName(name))
}

// Deprecated: use (*unified.Stmt).SetTags instead.
func (s *Stmt) SetTags(tags *param.Param, bindType *param.ColumnType) error {
	if s.core == nil || s.connectorClosed() {
		return client.ClosedError
	}
	return mapUnifiedError(s.core.SetTags(tags, bindType))
}

// Deprecated: use (*unified.Stmt).BindParam instead.
func (s *Stmt) BindParam(params []*param.Param, bindType *param.ColumnType) error {
	if s.core == nil || s.connectorClosed() {
		return client.ClosedError
	}
	return mapUnifiedError(s.core.BindParam(params, bindType))
}

// Deprecated: use (*unified.Stmt).AddBatch instead.
func (s *Stmt) AddBatch() error {
	if s.core == nil || s.connectorClosed() {
		return client.ClosedError
	}
	return mapUnifiedError(s.core.AddBatch())
}

// Deprecated: use (*unified.Stmt).Exec instead.
func (s *Stmt) Exec() error {
	if s.core == nil || s.connectorClosed() {
		return client.ClosedError
	}
	affected, err := s.core.Exec(0)
	if err != nil {
		return mapUnifiedError(err)
	}
	s.lastAffected = affected
	return nil
}

// Deprecated: use (*unified.Stmt).AffectedRows instead.
func (s *Stmt) GetAffectedRows() int {
	if s.core == nil {
		return s.lastAffected
	}
	return s.core.AffectedRows()
}

// Deprecated: use (*unified.Stmt).UseResult instead.
func (s *Stmt) UseResult() (*Rows, error) {
	if s.core == nil || s.connectorClosed() {
		return nil, client.ClosedError
	}
	result, err := s.core.UseResult(0)
	if err != nil {
		return nil, mapUnifiedError(err)
	}
	return newRowsFromResultSet(result), nil
}

// Deprecated: use (*unified.Stmt).Close instead.
func (s *Stmt) Close() error {
	if s.core == nil {
		return nil
	}
	return mapUnifiedError(s.core.Close(0))
}
