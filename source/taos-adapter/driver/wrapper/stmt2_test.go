package wrapper

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/driver/common/stmt"
	taosError "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

type stmt2Result struct {
	res      unsafe.Pointer
	affected int
	n        int
}
type StmtCallBackTest struct {
	ExecResult chan *stmt2Result
}

func (s *StmtCallBackTest) ExecCall(res unsafe.Pointer, affected int, code int) {
	s.ExecResult <- &stmt2Result{
		res:      res,
		affected: affected,
		n:        code,
	}
}

func NewStmtCallBackTest() *StmtCallBackTest {
	return &StmtCallBackTest{
		ExecResult: make(chan *stmt2Result, 1),
	}
}

func TestStmt2BindData(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2 precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2"))
	err = exec(conn, "use test_stmt2")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now().Round(time.Millisecond)
	next1S := now.Add(time.Second)
	next2S := now.Add(2 * time.Second)

	tests := []struct {
		name        string
		tbType      string
		pos         string
		params      []*stmt.TaosStmt2BindData
		expectValue [][]driver.Value
	}{
		{
			name:   "int",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{now},
					{int32(1)},
				},
			}},
			expectValue: [][]driver.Value{
				{now, int32(1)},
			},
		},
		{
			name:   "int null",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{now},
					{nil},
				},
			}},
			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "int null 3 cols",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int32(1),
						nil,
						int32(2),
					},
				},
			}},
			expectValue: [][]driver.Value{
				{now, int32(1)},
				{next1S, nil},
				{next2S, int32(2)},
			},
		},
		{
			name:   "bool",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {true}},
			}},

			expectValue: [][]driver.Value{{now, true}},
		},
		{
			name:   "bool false",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {false}},
			}},

			expectValue: [][]driver.Value{{now, false}},
		},
		{
			name:   "bool null",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {nil}},
			}},

			expectValue: [][]driver.Value{{now, nil}},
		},
		{
			name:   "bool null 3 cols",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						bool(true),
						nil,
						bool(false),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, true},
				{next1S, nil},
				{next2S, false},
			},
		},
		{
			name:   "tinyint",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {int8(1)}},
			}},

			expectValue: [][]driver.Value{
				{now, int8(1)},
			},
		},
		{
			name:   "tinyint null",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					}, {
						nil,
					}},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "tinyint null 3 cols",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					}, {
						int8(1),
						nil,
						int8(2),
					}},
			}},

			expectValue: [][]driver.Value{
				{now, int8(1)},
				{next1S, nil},
				{next2S, int8(2)},
			},
		},
		{
			name:   "smallint",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						int16(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int16(1)},
			},
		},
		{
			name:   "smallint null",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "smallint null 3 cols",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int16(1),
						nil,
						int16(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int16(1)},
				{next1S, nil},
				{next2S, int16(2)},
			},
		},
		{
			name:   "bigint",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						int64(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int64(1)},
			},
		},
		{
			name:   "bigint null",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "bigint null 3 cols",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int64(1),
						nil,
						int64(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int64(1)},
				{next1S, nil},
				{next2S, int64(2)},
			},
		},

		{
			name:   "tinyint unsigned",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint8(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint8(1)},
			},
		},
		{
			name:   "tinyint unsigned null",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "tinyint unsigned null 3 cols",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint8(1),
						nil,
						uint8(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint8(1)},
				{next1S, nil},
				{next2S, uint8(2)},
			},
		},

		{
			name:   "smallint unsigned",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint16(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint16(1)},
			},
		},
		{
			name:   "smallint unsigned null",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "smallint unsigned null 3 cols",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint16(1),
						nil,
						uint16(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint16(1)},
				{next1S, nil},
				{next2S, uint16(2)},
			},
		},

		{
			name:   "int unsigned",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint32(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint32(1)},
			},
		},
		{
			name:   "int unsigned null",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "int unsigned null 3 cols",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint32(1),
						nil,
						uint32(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint32(1)},
				{next1S, nil},
				{next2S, uint32(2)},
			},
		},

		{
			name:   "bigint unsigned",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint64(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint64(1)},
			},
		},
		{
			name:   "bigint unsigned null",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "bigint unsigned null 3 cols",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint64(1),
						nil,
						uint64(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint64(1)},
				{next1S, nil},
				{next2S, uint64(2)},
			},
		},

		{
			name:   "float",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						float32(1.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float32(1.2)},
			},
		},
		{
			name:   "float null",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "float null 3 cols",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						float32(1.2),
						nil,
						float32(2.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float32(1.2)},
				{next1S, nil},
				{next2S, float32(2.2)},
			},
		},

		{
			name:   "double",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						float64(1.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float64(1.2)},
			},
		},
		{
			name:   "double null",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "double null 3 cols",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						float64(1.2),
						nil,
						float64(2.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float64(1.2)},
				{next1S, nil},
				{next2S, float64(2.2)},
			},
		},

		{
			name:   "binary",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},
		{
			name:   "binary null",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "binary null 3 cols",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "varbinary",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
			},
		},
		{
			name:   "varbinary null",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},

		{
			name:   "varbinary null 3 cols",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
				{next1S, nil},
				{next2S, []byte("中文")},
			},
		},

		{
			name:   "geometry",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
			},
		},
		{
			name:   "geometry null",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "geometry null 3 cols",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
						nil,
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
				{next1S, nil},
				{next2S, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
			},
		},

		{
			name:   "nchar",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},
		{
			name:   "nchar null",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "nchar null 3 cols",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "nchar bind string",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},

		{
			name:   "nchar bind string null 3 cols",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "binary bind string",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},

		{
			name:   "binary bind string null 3 cols",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "varbinary bind string",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
			},
		},

		{
			name:   "varbinary bind string null 3 cols",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
				{next1S, nil},
				{next2S, []byte("中文")},
			},
		},

		{
			name:   "timestamp",
			tbType: "ts timestamp, v timestamp",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						now,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, now},
			},
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tbType := tc.tbType
			tbName := fmt.Sprintf("test_fast_insert_%02d", i)
			drop := fmt.Sprintf("drop table if exists %s", tbName)
			create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
			pos := tc.pos
			sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
			var err error
			if err = exec(conn, drop); err != nil {
				t.Error(err)
				return
			}
			if err = exec(conn, create); err != nil {
				t.Error(err)
				return
			}
			caller := NewStmtCallBackTest()
			handler := cgo.NewHandle(caller)
			insertStmt := TaosStmt2Init(conn, 0xcc123, false, false, handler)
			code := TaosStmt2Prepare(insertStmt, sql)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			isInsert, code := TaosStmt2IsInsert(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			assert.True(t, isInsert)
			code, count, cfields := TaosStmt2GetFields(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}

			defer TaosStmt2FreeFields(insertStmt, cfields)
			assert.Equal(t, 2, count)
			fields := Stmt2ParseAllFields(count, cfields)
			err = TaosStmt2BindParam(insertStmt, true, tc.params, fields, -1)
			if err != nil {
				t.Error(err)
				return
			}
			code = TaosStmt2Exec(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			r := <-caller.ExecResult
			if r.n != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(r.n, errStr)
				t.Error(err)
				return
			}
			t.Log(r.affected)
			//time.Sleep(time.Second)
			code = TaosStmt2Close(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			result, err := query(conn, fmt.Sprintf("select * from %s order by ts asc", tbName))
			if err != nil {
				t.Error(err)
				return
			}
			assert.Equal(t, tc.expectValue, result)
		})
	}

}

func TestStmt2BindBinary(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_binary")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_binary precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_binary"))

	err = exec(conn, "use test_stmt2_binary")
	if err != nil {
		t.Error(err)
		return
	}
	now := time.Now().Round(time.Millisecond)
	next1S := now.Add(time.Second)
	next2S := now.Add(2 * time.Second)

	tests := []struct {
		name        string
		tbType      string
		pos         string
		params      []*stmt.TaosStmt2BindData
		expectValue [][]driver.Value
	}{
		{
			name:   "int",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{now},
					{int32(1)},
				},
			}},
			expectValue: [][]driver.Value{
				{now, int32(1)},
			},
		},
		{
			name:   "int null",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{now},
					{nil},
				},
			}},
			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "int null 3 cols",
			tbType: "ts timestamp, v int",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int32(1),
						nil,
						int32(2),
					},
				},
			}},
			expectValue: [][]driver.Value{
				{now, int32(1)},
				{next1S, nil},
				{next2S, int32(2)},
			},
		},
		{
			name:   "bool",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {bool(true)}},
			}},

			expectValue: [][]driver.Value{{now, true}},
		},
		{
			name:   "bool null",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {nil}},
			}},

			expectValue: [][]driver.Value{{now, nil}},
		},
		{
			name:   "bool null 3 cols",
			tbType: "ts timestamp, v bool",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						bool(true),
						nil,
						bool(false),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, true},
				{next1S, nil},
				{next2S, false},
			},
		},
		{
			name:   "tinyint",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{{now}, {int8(1)}},
			}},

			expectValue: [][]driver.Value{
				{now, int8(1)},
			},
		},
		{
			name:   "tinyint null",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					}, {
						nil,
					}},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "tinyint null 3 cols",
			tbType: "ts timestamp, v tinyint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					}, {
						int8(1),
						nil,
						int8(2),
					}},
			}},

			expectValue: [][]driver.Value{
				{now, int8(1)},
				{next1S, nil},
				{next2S, int8(2)},
			},
		},
		{
			name:   "smallint",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						int16(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int16(1)},
			},
		},
		{
			name:   "smallint null",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "smallint null 3 cols",
			tbType: "ts timestamp, v smallint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int16(1),
						nil,
						int16(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int16(1)},
				{next1S, nil},
				{next2S, int16(2)},
			},
		},
		{
			name:   "bigint",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						int64(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int64(1)},
			},
		},
		{
			name:   "bigint null",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "bigint null 3 cols",
			tbType: "ts timestamp, v bigint",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						int64(1),
						nil,
						int64(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, int64(1)},
				{next1S, nil},
				{next2S, int64(2)},
			},
		},

		{
			name:   "tinyint unsigned",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint8(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint8(1)},
			},
		},
		{
			name:   "tinyint unsigned null",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "tinyint unsigned null 3 cols",
			tbType: "ts timestamp, v tinyint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint8(1),
						nil,
						uint8(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint8(1)},
				{next1S, nil},
				{next2S, uint8(2)},
			},
		},

		{
			name:   "smallint unsigned",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint16(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint16(1)},
			},
		},
		{
			name:   "smallint unsigned null",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "smallint unsigned null 3 cols",
			tbType: "ts timestamp, v smallint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint16(1),
						nil,
						uint16(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint16(1)},
				{next1S, nil},
				{next2S, uint16(2)},
			},
		},

		{
			name:   "int unsigned",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint32(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint32(1)},
			},
		},
		{
			name:   "int unsigned null",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "int unsigned null 3 cols",
			tbType: "ts timestamp, v int unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint32(1),
						nil,
						uint32(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint32(1)},
				{next1S, nil},
				{next2S, uint32(2)},
			},
		},

		{
			name:   "bigint unsigned",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						uint64(1),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint64(1)},
			},
		},
		{
			name:   "bigint unsigned null",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "bigint unsigned null 3 cols",
			tbType: "ts timestamp, v bigint unsigned",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						uint64(1),
						nil,
						uint64(2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, uint64(1)},
				{next1S, nil},
				{next2S, uint64(2)},
			},
		},

		{
			name:   "float",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						float32(1.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float32(1.2)},
			},
		},
		{
			name:   "float null",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "float null 3 cols",
			tbType: "ts timestamp, v float",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						float32(1.2),
						nil,
						float32(2.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float32(1.2)},
				{next1S, nil},
				{next2S, float32(2.2)},
			},
		},

		{
			name:   "double",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						float64(1.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float64(1.2)},
			},
		},
		{
			name:   "double null",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "double null 3 cols",
			tbType: "ts timestamp, v double",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						float64(1.2),
						nil,
						float64(2.2),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, float64(1.2)},
				{next1S, nil},
				{next2S, float64(2.2)},
			},
		},

		{
			name:   "binary",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},
		{
			name:   "binary null",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "binary null 3 cols",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "varbinary",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
			},
		},
		{
			name:   "varbinary null",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},

		{
			name:   "varbinary null 3 cols",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
				{next1S, nil},
				{next2S, []byte("中文")},
			},
		},

		{
			name:   "geometry",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
			},
		},
		{
			name:   "geometry null",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "geometry null 3 cols",
			tbType: "ts timestamp, v geometry(100)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
						nil,
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
				{next1S, nil},
				{next2S, []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}},
			},
		},

		{
			name:   "nchar",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						[]byte("yes"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},
		{
			name:   "nchar null",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						nil,
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, nil},
			},
		},
		{
			name:   "nchar null 3 cols",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						[]byte("yes"),
						nil,
						[]byte("中文"),
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "nchar bind string",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},

		{
			name:   "nchar bind string null 3 cols",
			tbType: "ts timestamp, v nchar(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "binary bind string",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
			},
		},

		{
			name:   "binary bind string null 3 cols",
			tbType: "ts timestamp, v binary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, "yes"},
				{next1S, nil},
				{next2S, "中文"},
			},
		},

		{
			name:   "varbinary bind string",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
					},
					{
						"yes",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
			},
		},

		{
			name:   "varbinary bind string null 3 cols",
			tbType: "ts timestamp, v varbinary(20)",
			pos:    "?, ?",
			params: []*stmt.TaosStmt2BindData{{
				Cols: [][]driver.Value{
					{
						now,
						next1S,
						next2S,
					},
					{
						"yes",
						nil,
						"中文",
					},
				},
			}},

			expectValue: [][]driver.Value{
				{now, []byte("yes")},
				{next1S, nil},
				{next2S, []byte("中文")},
			},
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tbType := tc.tbType
			tbName := fmt.Sprintf("test_fast_insert_%02d", i)
			drop := fmt.Sprintf("drop table if exists %s", tbName)
			create := fmt.Sprintf("create table if not exists %s(%s)", tbName, tbType)
			pos := tc.pos
			sql := fmt.Sprintf("insert into %s values(%s)", tbName, pos)
			var err error
			if err = exec(conn, drop); err != nil {
				t.Error(err)
				return
			}
			if err = exec(conn, create); err != nil {
				t.Error(err)
				return
			}
			caller := NewStmtCallBackTest()
			handler := cgo.NewHandle(caller)
			insertStmt := TaosStmt2Init(conn, 0xcc123, false, false, handler)
			code := TaosStmt2Prepare(insertStmt, sql)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			isInsert, code := TaosStmt2IsInsert(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			assert.True(t, isInsert)
			code, count, cfields := TaosStmt2GetFields(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			defer TaosStmt2FreeFields(insertStmt, cfields)
			assert.Equal(t, 2, count)
			fields := Stmt2ParseAllFields(count, cfields)
			bs, err := stmt.MarshalStmt2Binary(tc.params, true, fields)
			if err != nil {
				t.Error("marshal binary error:", err)
				return
			}
			err = TaosStmt2BindBinary(insertStmt, bs, -1)
			if !assert.NoError(t, err, bs) {
				return
			}
			//return
			code = TaosStmt2Exec(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			r := <-caller.ExecResult
			if r.n != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(r.n, errStr)
				t.Error(err)
				return
			}
			t.Log(r.affected)
			code = TaosStmt2Close(insertStmt)
			if code != 0 {
				errStr := TaosStmt2Error(insertStmt)
				err = taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			result, err := query(conn, fmt.Sprintf("select * from %s order by ts asc", tbName))
			if err != nil {
				t.Error(err)
				return
			}
			assert.Equal(t, tc.expectValue, result)
		})
	}

}

func TestStmt2AllType(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_all")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_all precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_all"))

	err = exec(conn, "use test_stmt2_all")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists all_stb("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))"+
		"tags("+
		"tts timestamp, "+
		"tv1 bool, "+
		"tv2 tinyint, "+
		"tv3 smallint, "+
		"tv4 int, "+
		"tv5 bigint, "+
		"tv6 tinyint unsigned, "+
		"tv7 smallint unsigned, "+
		"tv8 int unsigned, "+
		"tv9 bigint unsigned, "+
		"tv10 float, "+
		"tv11 double, "+
		"tv12 binary(20), "+
		"tv13 varbinary(20), "+
		"tv14 geometry(100), "+
		"tv15 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists ctb2("+"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))")
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	insertStmt := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into ? values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	code := TaosStmt2Prepare(insertStmt, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	params := []*stmt.TaosStmt2BindData{{
		TableName: "ctb2",
	}}
	err = TaosStmt2BindParam(insertStmt, true, params, nil, -1)
	if err != nil {
		t.Error(err)
		return
	}

	isInsert, code := TaosStmt2IsInsert(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	code, count, cFields := TaosStmt2GetFields(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.Equal(t, 17, count)
	fields := Stmt2ParseAllFields(count, cFields)
	expect := []*stmt.Stmt2AllField{
		{
			Name:      "tbname",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			Precision: 0,
			Scale:     0,
			Bytes:     271,
			BindType:  stmt.TAOS_FIELD_TBNAME,
		},
		{
			Name:      "ts",
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v1",
			FieldType: common.TSDB_DATA_TYPE_BOOL,
			Precision: 0,
			Scale:     0,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v2",
			FieldType: common.TSDB_DATA_TYPE_TINYINT,
			Precision: 0,
			Scale:     0,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v3",
			FieldType: common.TSDB_DATA_TYPE_SMALLINT,
			Precision: 0,
			Scale:     0,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v4",
			FieldType: common.TSDB_DATA_TYPE_INT,
			Precision: 0,
			Scale:     0,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v5",
			FieldType: common.TSDB_DATA_TYPE_BIGINT,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v6",
			FieldType: common.TSDB_DATA_TYPE_UTINYINT,
			Precision: 0,
			Scale:     0,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v7",
			FieldType: common.TSDB_DATA_TYPE_USMALLINT,
			Precision: 0,
			Scale:     0,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v8",
			FieldType: common.TSDB_DATA_TYPE_UINT,
			Precision: 0,
			Scale:     0,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v9",
			FieldType: common.TSDB_DATA_TYPE_UBIGINT,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v10",
			FieldType: common.TSDB_DATA_TYPE_FLOAT,
			Precision: 0,
			Scale:     0,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v11",
			FieldType: common.TSDB_DATA_TYPE_DOUBLE,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v12",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			Precision: 0,
			Scale:     0,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v13",
			FieldType: common.TSDB_DATA_TYPE_VARBINARY,
			Precision: 0,
			Scale:     0,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v14",
			FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
			Precision: 0,
			Scale:     0,
			Bytes:     102,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v15",
			FieldType: common.TSDB_DATA_TYPE_NCHAR,
			Precision: 0,
			Scale:     0,
			Bytes:     82,
			BindType:  stmt.TAOS_FIELD_COL,
		},
	}
	assert.Equal(t, expect, fields)
	TaosStmt2FreeFields(insertStmt, cFields)
	prepareInsertSql = "insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	code = TaosStmt2Prepare(insertStmt, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code = TaosStmt2IsInsert(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	code, count, cFields = TaosStmt2GetFields(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	defer TaosStmt2FreeFields(insertStmt, cFields)
	assert.Equal(t, 33, count)
	fields = Stmt2ParseAllFields(count, cFields)
	expect = []*stmt.Stmt2AllField{
		{
			Name:      "tbname",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			Precision: 0,
			Scale:     0,
			Bytes:     271,
			BindType:  stmt.TAOS_FIELD_TBNAME,
		},
		{
			Name:      "tts",
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		// 标签字段组
		{
			Name:      "tv1",
			FieldType: common.TSDB_DATA_TYPE_BOOL,
			Precision: 0,
			Scale:     0,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv2",
			FieldType: common.TSDB_DATA_TYPE_TINYINT,
			Precision: 0,
			Scale:     0,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv3",
			FieldType: common.TSDB_DATA_TYPE_SMALLINT,
			Precision: 0,
			Scale:     0,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv4",
			FieldType: common.TSDB_DATA_TYPE_INT,
			Precision: 0,
			Scale:     0,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv5",
			FieldType: common.TSDB_DATA_TYPE_BIGINT,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv6",
			FieldType: common.TSDB_DATA_TYPE_UTINYINT,
			Precision: 0,
			Scale:     0,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv7",
			FieldType: common.TSDB_DATA_TYPE_USMALLINT,
			Precision: 0,
			Scale:     0,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv8",
			FieldType: common.TSDB_DATA_TYPE_UINT,
			Precision: 0,
			Scale:     0,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv9",
			FieldType: common.TSDB_DATA_TYPE_UBIGINT,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv10",
			FieldType: common.TSDB_DATA_TYPE_FLOAT,
			Precision: 0,
			Scale:     0,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv11",
			FieldType: common.TSDB_DATA_TYPE_DOUBLE,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv12",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			Precision: 0,
			Scale:     0,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv13",
			FieldType: common.TSDB_DATA_TYPE_VARBINARY,
			Precision: 0,
			Scale:     0,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv14",
			FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
			Precision: 0,
			Scale:     0,
			Bytes:     102,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		{
			Name:      "tv15",
			FieldType: common.TSDB_DATA_TYPE_NCHAR,
			Precision: 0,
			Scale:     0,
			Bytes:     82,
			BindType:  stmt.TAOS_FIELD_TAG,
		},

		{
			Name:      "ts",
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v1",
			FieldType: common.TSDB_DATA_TYPE_BOOL,
			Precision: 0,
			Scale:     0,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v2",
			FieldType: common.TSDB_DATA_TYPE_TINYINT,
			Precision: 0,
			Scale:     0,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v3",
			FieldType: common.TSDB_DATA_TYPE_SMALLINT,
			Precision: 0,
			Scale:     0,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v4",
			FieldType: common.TSDB_DATA_TYPE_INT,
			Precision: 0,
			Scale:     0,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v5",
			FieldType: common.TSDB_DATA_TYPE_BIGINT,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v6",
			FieldType: common.TSDB_DATA_TYPE_UTINYINT,
			Precision: 0,
			Scale:     0,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v7",
			FieldType: common.TSDB_DATA_TYPE_USMALLINT,
			Precision: 0,
			Scale:     0,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v8",
			FieldType: common.TSDB_DATA_TYPE_UINT,
			Precision: 0,
			Scale:     0,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v9",
			FieldType: common.TSDB_DATA_TYPE_UBIGINT,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v10",
			FieldType: common.TSDB_DATA_TYPE_FLOAT,
			Precision: 0,
			Scale:     0,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v11",
			FieldType: common.TSDB_DATA_TYPE_DOUBLE,
			Precision: 0,
			Scale:     0,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v12",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			Precision: 0,
			Scale:     0,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v13",
			FieldType: common.TSDB_DATA_TYPE_VARBINARY,
			Precision: 0,
			Scale:     0,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v14",
			FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
			Precision: 0,
			Scale:     0,
			Bytes:     102,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			Name:      "v15",
			FieldType: common.TSDB_DATA_TYPE_NCHAR,
			Precision: 0,
			Scale:     0,
			Bytes:     82,
			BindType:  stmt.TAOS_FIELD_COL,
		},
	}
	assert.Equal(t, expect, fields)
	now := time.Now()
	//colTypes := []int8{
	//	common.TSDB_DATA_TYPE_TIMESTAMP,
	//	common.TSDB_DATA_TYPE_BOOL,
	//	common.TSDB_DATA_TYPE_TINYINT,
	//	common.TSDB_DATA_TYPE_SMALLINT,
	//	common.TSDB_DATA_TYPE_INT,
	//	common.TSDB_DATA_TYPE_BIGINT,
	//	common.TSDB_DATA_TYPE_UTINYINT,
	//	common.TSDB_DATA_TYPE_USMALLINT,
	//	common.TSDB_DATA_TYPE_UINT,
	//	common.TSDB_DATA_TYPE_UBIGINT,
	//	common.TSDB_DATA_TYPE_FLOAT,
	//	common.TSDB_DATA_TYPE_DOUBLE,
	//	common.TSDB_DATA_TYPE_BINARY,
	//	common.TSDB_DATA_TYPE_VARBINARY,
	//	common.TSDB_DATA_TYPE_GEOMETRY,
	//	common.TSDB_DATA_TYPE_NCHAR,
	//}
	params2 := []*stmt.TaosStmt2BindData{{
		TableName: "ctb1",
		Tags: []driver.Value{
			// TIMESTAMP
			now,
			// BOOL
			true,
			// TINYINT
			int8(1),
			// SMALLINT
			int16(1),
			// INT
			int32(1),
			// BIGINT
			int64(1),
			// UTINYINT
			uint8(1),
			// USMALLINT
			uint16(1),
			// UINT
			uint32(1),
			// UBIGINT
			uint64(1),
			// FLOAT
			float32(1.2),
			// DOUBLE
			float64(1.2),
			// BINARY
			[]byte("binary"),
			// VARBINARY
			[]byte("varbinary"),
			// GEOMETRY
			[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			// NCHAR
			"nchar",
		},
		Cols: [][]driver.Value{
			{
				now,
				now.Add(time.Second),
				now.Add(time.Second * 2),
			},
			{
				true,
				nil,
				false,
			},
			{
				int8(11),
				nil,
				int8(12),
			},
			{
				int16(11),
				nil,
				int16(12),
			},
			{
				int32(11),
				nil,
				int32(12),
			},
			{
				int64(11),
				nil,
				int64(12),
			},
			{
				uint8(11),
				nil,
				uint8(12),
			},
			{
				uint16(11),
				nil,
				uint16(12),
			},
			{
				uint32(11),
				nil,
				uint32(12),
			},
			{
				uint64(11),
				nil,
				uint64(12),
			},
			{
				float32(11.2),
				nil,
				float32(12.2),
			},
			{
				float64(11.2),
				nil,
				float64(12.2),
			},
			{
				[]byte("binary1"),
				nil,
				[]byte("binary2"),
			},
			{
				[]byte("varbinary1"),
				nil,
				[]byte("varbinary2"),
			},
			{
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				nil,
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			},
			{
				"nchar1",
				nil,
				"nchar2",
			},
		},
	}}

	err = TaosStmt2BindParam(insertStmt, true, params2, fields, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	t.Log(r.affected)

	code = TaosStmt2Close(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
}

func TestStmt2AllTypeBytes(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_all_bytes")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_all_bytes precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_all_bytes"))
	err = exec(conn, "use test_stmt2_all_bytes")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists all_stb("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))"+
		"tags("+
		"tts timestamp, "+
		"tv1 bool, "+
		"tv2 tinyint, "+
		"tv3 smallint, "+
		"tv4 int, "+
		"tv5 bigint, "+
		"tv6 tinyint unsigned, "+
		"tv7 smallint unsigned, "+
		"tv8 int unsigned, "+
		"tv9 bigint unsigned, "+
		"tv10 float, "+
		"tv11 double, "+
		"tv12 binary(20), "+
		"tv13 varbinary(20), "+
		"tv14 geometry(100), "+
		"tv15 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	insertStmt := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	code := TaosStmt2Prepare(insertStmt, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}

	isInsert, code := TaosStmt2IsInsert(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)

	code, count, cFields := TaosStmt2GetFields(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	defer TaosStmt2FreeFields(insertStmt, cFields)
	assert.Equal(t, 33, count)
	fields := Stmt2ParseAllFields(count, cFields)
	t.Log(fields)
	now := time.Now()
	//colTypes := []int8{
	//	common.TSDB_DATA_TYPE_TIMESTAMP,
	//	common.TSDB_DATA_TYPE_BOOL,
	//	common.TSDB_DATA_TYPE_TINYINT,
	//	common.TSDB_DATA_TYPE_SMALLINT,
	//	common.TSDB_DATA_TYPE_INT,
	//	common.TSDB_DATA_TYPE_BIGINT,
	//	common.TSDB_DATA_TYPE_UTINYINT,
	//	common.TSDB_DATA_TYPE_USMALLINT,
	//	common.TSDB_DATA_TYPE_UINT,
	//	common.TSDB_DATA_TYPE_UBIGINT,
	//	common.TSDB_DATA_TYPE_FLOAT,
	//	common.TSDB_DATA_TYPE_DOUBLE,
	//	common.TSDB_DATA_TYPE_BINARY,
	//	common.TSDB_DATA_TYPE_VARBINARY,
	//	common.TSDB_DATA_TYPE_GEOMETRY,
	//	common.TSDB_DATA_TYPE_NCHAR,
	//}
	params2 := []*stmt.TaosStmt2BindData{{
		TableName: "ctb1",
		Tags: []driver.Value{
			// TIMESTAMP
			now,
			// BOOL
			true,
			// TINYINT
			int8(1),
			// SMALLINT
			int16(1),
			// INT
			int32(1),
			// BIGINT
			int64(1),
			// UTINYINT
			uint8(1),
			// USMALLINT
			uint16(1),
			// UINT
			uint32(1),
			// UBIGINT
			uint64(1),
			// FLOAT
			float32(1.2),
			// DOUBLE
			float64(1.2),
			// BINARY
			[]byte("binary"),
			// VARBINARY
			[]byte("varbinary"),
			// GEOMETRY
			[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			// NCHAR
			"nchar",
		},
		Cols: [][]driver.Value{
			{
				now,
				now.Add(time.Second),
				now.Add(time.Second * 2),
			},
			{
				true,
				nil,
				false,
			},
			{
				int8(11),
				nil,
				int8(12),
			},
			{
				int16(11),
				nil,
				int16(12),
			},
			{
				int32(11),
				nil,
				int32(12),
			},
			{
				int64(11),
				nil,
				int64(12),
			},
			{
				uint8(11),
				nil,
				uint8(12),
			},
			{
				uint16(11),
				nil,
				uint16(12),
			},
			{
				uint32(11),
				nil,
				uint32(12),
			},
			{
				uint64(11),
				nil,
				uint64(12),
			},
			{
				float32(11.2),
				nil,
				float32(12.2),
			},
			{
				float64(11.2),
				nil,
				float64(12.2),
			},
			{
				[]byte("binary1"),
				nil,
				[]byte("binary2"),
			},
			{
				[]byte("varbinary1"),
				nil,
				[]byte("varbinary2"),
			},
			{
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				nil,
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			},
			{
				"nchar1",
				nil,
				"nchar2",
			},
		},
	}}
	bs, err := stmt.MarshalStmt2Binary(params2, true, fields)
	if err != nil {
		t.Error(err)
		return
	}
	err = TaosStmt2BindBinary(insertStmt, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	t.Log(r.affected)

	code = TaosStmt2Close(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
}

func TestStmt2Query(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_query")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_query precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_query"))
	err = exec(conn, "use test_stmt2_query")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists t(ts timestamp,v int)")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	stmt2 := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into t values (?,?)"
	code := TaosStmt2Prepare(stmt2, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code := TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	now := time.Now().Round(time.Millisecond)
	colTypes := []*stmt.Stmt2AllField{
		{
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: common.PrecisionMilliSecond,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_INT,
			BindType:  stmt.TAOS_FIELD_COL,
		},
	}
	params := []*stmt.TaosStmt2BindData{
		{
			TableName: "t",
			Cols: [][]driver.Value{
				{
					now,
					now.Add(time.Second),
				},
				{
					int32(1),
					int32(2),
				},
			},
		},
		{
			TableName: "t",
			Cols: [][]driver.Value{
				{
					now.Add(time.Second * 2),
					now.Add(time.Second * 3),
				},
				{
					int32(3),
					int32(4),
				},
			},
		},
	}
	err = TaosStmt2BindParam(stmt2, true, params, colTypes, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	assert.Equal(t, 4, r.affected)
	code = TaosStmt2Prepare(stmt2, "select * from t where ts >= ? and ts <= ?")
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code = TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.False(t, isInsert)
	params = []*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{
					now,
				},
				{
					now.Add(time.Second * 3),
				},
			},
		},
	}

	err = TaosStmt2BindParam(stmt2, false, params, nil, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r = <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	// test fetch after close stmt2
	res := TaosStmt2Result(stmt2)
	defer func() {
		TaosFreeResult(res)
	}()
	code = TaosStmt2Close(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	fieldCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fieldCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			err = taosError.NewError(errCode, errStr)
			t.Error(err)
			return
		}
		if columns == 0 {
			break
		}
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		assert.NoError(t, err)
		result = append(result, r...)
	}
	assert.Equal(t, 4, len(result))
	assert.Equal(t, now, result[0][0])
	assert.Equal(t, now.Add(time.Second), result[1][0])
	assert.Equal(t, now.Add(time.Second*2), result[2][0])
	assert.Equal(t, now.Add(time.Second*3), result[3][0])
	assert.Equal(t, int32(1), result[0][1])
	assert.Equal(t, int32(2), result[1][1])
	assert.Equal(t, int32(3), result[2][1])
	assert.Equal(t, int32(4), result[3][1])
}

func TestStmt2QueryBytes(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_query_bytes")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_query_bytes precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_query_bytes"))

	err = exec(conn, "use test_stmt2_query_bytes")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists t(ts timestamp,v int)")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	stmt2 := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into t values (?,?)"
	code := TaosStmt2Prepare(stmt2, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code := TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	now := time.Now().Round(time.Millisecond)
	colTypes := []*stmt.Stmt2AllField{
		{
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: common.PrecisionMilliSecond,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_INT,
			BindType:  stmt.TAOS_FIELD_COL,
		},
	}
	params := []*stmt.TaosStmt2BindData{
		{
			TableName: "t",
			Cols: [][]driver.Value{
				{
					now,
					now.Add(time.Second),
				},
				{
					int32(1),
					int32(2),
				},
			},
		},
		{
			TableName: "t",
			Cols: [][]driver.Value{
				{
					now.Add(time.Second * 2),
					now.Add(time.Second * 3),
				},
				{
					int32(3),
					int32(4),
				},
			},
		},
	}
	bs, err := stmt.MarshalStmt2Binary(params, true, colTypes)
	if err != nil {
		t.Error(err)
		return
	}
	err = TaosStmt2BindBinary(stmt2, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	assert.Equal(t, 4, r.affected)
	code = TaosStmt2Prepare(stmt2, "select * from t where ts >= ? and ts <= ?")
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code = TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.False(t, isInsert)
	params = []*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{
					now,
				},
				{
					now.Add(time.Second * 3),
				},
			},
		},
	}
	bs, err = stmt.MarshalStmt2Binary(params, false, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = TaosStmt2BindBinary(stmt2, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r = <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	res := r.res
	fieldCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fieldCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			err = taosError.NewError(errCode, errStr)
			t.Error(err)
			return
		}
		if columns == 0 {
			break
		}
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		assert.NoError(t, err)
		result = append(result, r...)
	}
	assert.Equal(t, 4, len(result))
	assert.Equal(t, now, result[0][0])
	assert.Equal(t, now.Add(time.Second), result[1][0])
	assert.Equal(t, now.Add(time.Second*2), result[2][0])
	assert.Equal(t, now.Add(time.Second*3), result[3][0])
	assert.Equal(t, int32(1), result[0][1])
	assert.Equal(t, int32(2), result[1][1])
	assert.Equal(t, int32(3), result[2][1])
	assert.Equal(t, int32(4), result[3][1])
	code = TaosStmt2Close(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
}

func TestStmt2QueryAllType(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_query_all")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_query_all precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_query_all"))

	err = exec(conn, "use test_stmt2_query_all")
	if err != nil {
		t.Error(err)
		return
	}

	err = exec(conn, "create table if not exists t("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	stmt2 := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	defer func() {
		code := TaosStmt2Close(stmt2)
		if code != 0 {
			errStr := TaosStmt2Error(stmt2)
			err = taosError.NewError(code, errStr)
			t.Error(err)
			return
		}
	}()
	prepareInsertSql := "insert into t values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	colTypes := []*stmt.Stmt2AllField{
		{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: common.PrecisionMilliSecond, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_BOOL, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_TINYINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_SMALLINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_INT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_BIGINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_UTINYINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_USMALLINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_UINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_UBIGINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_FLOAT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_DOUBLE, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_BINARY, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_VARBINARY, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_GEOMETRY, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: stmt.TAOS_FIELD_COL},
	}

	now := time.Now()
	params2 := []*stmt.TaosStmt2BindData{{
		TableName: "t",
		Cols: [][]driver.Value{
			{
				now,
				now.Add(time.Second),
				now.Add(time.Second * 2),
			},
			{
				true,
				nil,
				false,
			},
			{
				int8(11),
				nil,
				int8(12),
			},
			{
				int16(11),
				nil,
				int16(12),
			},
			{
				int32(11),
				nil,
				int32(12),
			},
			{
				int64(11),
				nil,
				int64(12),
			},
			{
				uint8(11),
				nil,
				uint8(12),
			},
			{
				uint16(11),
				nil,
				uint16(12),
			},
			{
				uint32(11),
				nil,
				uint32(12),
			},
			{
				uint64(11),
				nil,
				uint64(12),
			},
			{
				float32(11.2),
				nil,
				float32(12.2),
			},
			{
				float64(11.2),
				nil,
				float64(12.2),
			},
			{
				[]byte("binary1"),
				nil,
				[]byte("binary2"),
			},
			{
				[]byte("varbinary1"),
				nil,
				[]byte("varbinary2"),
			},
			{
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				nil,
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			},
			{
				"nchar1",
				nil,
				"nchar2",
			},
		},
	}}
	code := TaosStmt2Prepare(stmt2, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code := TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	err = TaosStmt2BindParam(stmt2, true, params2, colTypes, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	t.Log(r.affected)
	assert.Equal(t, 3, r.affected)
	code = TaosStmt2Prepare(stmt2, "select * from t where ts =? and v1 = ? and v2 = ? and v3 = ? and v4 = ? and v5 = ? and v6 = ? and v7 = ? and v8 = ? and v9 = ? and v10 = ? and v11 = ? and v12 = ? and v13 = ? and v14 = ? and v15 = ? ")
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code = TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.False(t, isInsert)
	params := []*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{now},
				{true},
				{int8(11)},
				{int16(11)},
				{int32(11)},
				{int64(11)},
				{uint8(11)},
				{uint16(11)},
				{uint32(11)},
				{uint64(11)},
				{float32(11.2)},
				{float64(11.2)},
				{[]byte("binary1")},
				{[]byte("varbinary1")},
				{"point(100 100)"},
				{"nchar1"},
			},
		},
	}
	err = TaosStmt2BindParam(stmt2, false, params, nil, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r = <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	res := r.res
	fieldCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fieldCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			err = taosError.NewError(errCode, errStr)
			t.Error(err)
			return
		}
		if columns == 0 {
			break
		}
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		assert.NoError(t, err)
		result = append(result, r...)
	}
	t.Log(result)
	assert.Len(t, result, 1)
}

func TestStmt2QueryAllTypeBytes(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_query_all_bytes")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_query_all_bytes precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_query_all_bytes"))

	err = exec(conn, "use test_stmt2_query_all_bytes")
	if err != nil {
		t.Error(err)
		return
	}

	err = exec(conn, "create table if not exists t("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	stmt2 := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	defer func() {
		code := TaosStmt2Close(stmt2)
		if code != 0 {
			errStr := TaosStmt2Error(stmt2)
			err = taosError.NewError(code, errStr)
			t.Error(err)
			return
		}
	}()
	now := time.Now()
	stmt2AllTypeBytesInsert(t, stmt2, caller, now)
	code := TaosStmt2Prepare(stmt2, "select * from t where ts =? and v1 = ? and v2 = ? and v3 = ? and v4 = ? and v5 = ? and v6 = ? and v7 = ? and v8 = ? and v9 = ? and v10 = ? and v11 = ? and v12 = ? and v13 = ? and v14 = ? and v15 = ? ")
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code := TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.False(t, isInsert)
	params := []*stmt.TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{now},
				{true},
				{int8(11)},
				{int16(11)},
				{int32(11)},
				{int64(11)},
				{uint8(11)},
				{uint16(11)},
				{uint32(11)},
				{uint64(11)},
				{float32(11.2)},
				{float64(11.2)},
				{[]byte("binary1")},
				{[]byte("varbinary1")},
				{"point(100 100)"},
				{"nchar1"},
			},
		},
	}
	bs, err := stmt.MarshalStmt2Binary(params, false, nil)
	if err != nil {
		t.Error(err)
		return
	}
	err = TaosStmt2BindBinary(stmt2, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	res := TaosStmt2Result(stmt2)
	defer func() {
		TaosFreeResult(res)
	}()
	// insert again then fetch result
	stmt2AllTypeBytesInsert(t, stmt2, caller, now)

	fieldCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fieldCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			err = taosError.NewError(errCode, errStr)
			t.Error(err)
			return
		}
		if columns == 0 {
			break
		}
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		assert.NoError(t, err)
		result = append(result, r...)
	}
	t.Log(result)
	assert.Len(t, result, 1)
}

func stmt2AllTypeBytesInsert(t *testing.T, stmt2 unsafe.Pointer, caller *StmtCallBackTest, now time.Time) {
	prepareInsertSql := "insert into t values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	colTypes := []*stmt.Stmt2AllField{
		{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: common.PrecisionMilliSecond, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_BOOL, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_TINYINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_SMALLINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_INT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_BIGINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_UTINYINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_USMALLINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_UINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_UBIGINT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_FLOAT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_DOUBLE, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_BINARY, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_VARBINARY, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_GEOMETRY, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: stmt.TAOS_FIELD_COL},
	}

	params2 := []*stmt.TaosStmt2BindData{{
		TableName: "t",
		Cols: [][]driver.Value{
			{
				now,
				now.Add(time.Second),
				now.Add(time.Second * 2),
			},
			{
				true,
				nil,
				false,
			},
			{
				int8(11),
				nil,
				int8(12),
			},
			{
				int16(11),
				nil,
				int16(12),
			},
			{
				int32(11),
				nil,
				int32(12),
			},
			{
				int64(11),
				nil,
				int64(12),
			},
			{
				uint8(11),
				nil,
				uint8(12),
			},
			{
				uint16(11),
				nil,
				uint16(12),
			},
			{
				uint32(11),
				nil,
				uint32(12),
			},
			{
				uint64(11),
				nil,
				uint64(12),
			},
			{
				float32(11.2),
				nil,
				float32(12.2),
			},
			{
				float64(11.2),
				nil,
				float64(12.2),
			},
			{
				[]byte("binary1"),
				nil,
				[]byte("binary2"),
			},
			{
				[]byte("varbinary1"),
				nil,
				[]byte("varbinary2"),
			},
			{
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				nil,
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			},
			{
				"nchar1",
				nil,
				"nchar2",
			},
		},
	}}
	code := TaosStmt2Prepare(stmt2, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err := taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	isInsert, code := TaosStmt2IsInsert(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err := taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)
	bs, err := stmt.MarshalStmt2Binary(params2, true, colTypes)
	if err != nil {
		t.Error(err)
		return
	}
	err = TaosStmt2BindBinary(stmt2, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	t.Log(r.affected)
	assert.Equal(t, 3, r.affected)
}

func TestStmt2Json(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_json")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_json precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_json"))

	err = exec(conn, "use test_stmt2_json")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists test_json_stb(ts timestamp, v int) tags (t json)")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	stmt2 := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	defer func() {
		code := TaosStmt2Close(stmt2)
		if code != 0 {
			errStr := TaosStmt2Error(stmt2)
			err = taosError.NewError(code, errStr)
			t.Error(err)
			return
		}
	}()
	prepareInsertSql := "insert into ? using test_json_stb tags(?) values (?,?)"
	code := TaosStmt2Prepare(stmt2, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	now := time.Now().Round(time.Millisecond)
	params := []*stmt.TaosStmt2BindData{{
		TableName: "ctb1",
		Tags:      []driver.Value{[]byte(`{"a":1,"b":"xx"}`)},
		Cols: [][]driver.Value{
			{now},
			{int32(1)},
		},
	}}
	types := []*stmt.Stmt2AllField{
		{FieldType: common.TSDB_DATA_TYPE_TIMESTAMP, Precision: common.PrecisionMilliSecond, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_INT, BindType: stmt.TAOS_FIELD_COL},
		{FieldType: common.TSDB_DATA_TYPE_JSON, BindType: stmt.TAOS_FIELD_TAG},
	}
	err = TaosStmt2BindParam(stmt2, true, params, types, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	assert.Equal(t, 1, r.affected)

	TaosStmt2Prepare(stmt2, "select * from test_json_stb where t->'a' = ?")
	params = []*stmt.TaosStmt2BindData{{
		Cols: [][]driver.Value{
			{int32(1)},
		},
	}}
	err = TaosStmt2BindParam(stmt2, false, params, nil, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r = <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(stmt2)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	res := r.res
	fieldCount := TaosNumFields(res)
	rh, err := ReadColumn(res, fieldCount)
	if err != nil {
		t.Error(err)
		return
	}
	precision := TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := TaosErrorStr(res)
			err = taosError.NewError(errCode, errStr)
			t.Error(err)
			return
		}
		if columns == 0 {
			break
		}
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		assert.NoError(t, err)
		result = append(result, r...)
	}
	t.Log(result)
	assert.Equal(t, 1, len(result))
}

func TestStmt2BindMultiTables(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_multi")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_multi precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_multi"))

	err = exec(conn, "use test_stmt2_multi")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists stb(ts timestamp, v bigint) tags(tv int)")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	insertStmt := TaosStmt2Init(conn, 0xcc123, false, false, handler)
	prepareInsertSql := "insert into ? using stb tags(?) values (?,?)"
	code := TaosStmt2Prepare(insertStmt, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	binds := []*stmt.TaosStmt2BindData{
		{
			TableName: "table1",
			Cols: [][]driver.Value{
				{
					// ts 1726803356466
					time.Unix(1726803356, 466000000),
				},
				{
					int64(1),
				},
			},
			Tags: []driver.Value{int32(1)},
		},
		{
			TableName: "table2",
			Cols: [][]driver.Value{
				{
					// ts 1726803356466
					time.Unix(1726803356, 466000000),
				},
				{
					int64(2),
				},
			},
			Tags: []driver.Value{int32(2)},
		},
		{
			TableName: "table3",
			Cols: [][]driver.Value{
				{
					// ts 1726803356466
					time.Unix(1726803356, 466000000),
				},
				{
					int64(3),
				},
			},
			Tags: []driver.Value{int32(3)},
		},
	}
	fields := []*stmt.Stmt2AllField{
		{
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: common.PrecisionMilliSecond,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_BIGINT,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_INT,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
	}
	isInsert, code := TaosStmt2IsInsert(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)

	err = TaosStmt2BindParam(insertStmt, true, binds, fields, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	t.Log(r.affected)

	code = TaosStmt2Close(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
}

func TestTaosStmt2BindBinaryParse(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_binary_parse")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_binary_parse precision 'ms' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_binary_parse"))

	err = exec(conn, "use test_stmt2_binary_parse")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table test1 (ts timestamp, v int)")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table stb (ts timestamp, v int) tags(tv int)")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table test2 (ts timestamp, v binary(100))")
	if err != nil {
		t.Error(err)
		return
	}
	type args struct {
		sql    string
		data   []byte
		colIdx int32
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "wrong data length",
			args: args{
				sql: "insert into ? values (?,?)",
				data: []byte{
					// total Length
					0x00, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "normal table name",
			args: args{
				sql: "insert into ? values (?,?)",
				data: []byte{
					// total Length
					0x24, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x1c, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x00, 0x00, 0x00, 0x00,
					// table names
					// TableNameLength
					0x06, 0x00,
					// test1
					0x74, 0x65, 0x73, 0x74, 0x31, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.NoError,
		},
		{
			name: "empty table name",
			args: args{
				sql: "insert into ? values (?,?)",
				data: []byte{
					// total Length
					0x1e, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x1c, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x00, 0x00, 0x00, 0x00,
					// table names
					// TableNameLength
					0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong total length",
			args: args{
				sql: "insert into ? values (?,?)",
				data: []byte{
					// total Length
					0x24, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x1c, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x00, 0x00, 0x00, 0x00,
					// table names
					// TableNameLength
					0x06, 0x00,
					// test1
					0x74, 0x65, 0x73, 0x74, 0x31, 0x00,
					//
					0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong table name offset",
			args: args{
				sql: "insert into ? values (?,?)",
				data: []byte{
					// total Length
					0x24, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x24, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x00, 0x00, 0x00, 0x00,
					// table names
					// TableNameLength
					0x06, 0x00,
					// test1
					0x74, 0x65, 0x73, 0x74, 0x31, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong table name length",
			args: args{
				sql: "insert into ? values (?,?)",
				data: []byte{
					// total Length
					0x24, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x1c, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x00, 0x00, 0x00, 0x00,
					// table names
					// TableNameLength
					0x07, 0x00,
					// test1
					0x74, 0x65, 0x73, 0x74, 0x31, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "normal col",
			args: args{
				sql: "insert into test1 values (?,?)",
				data: []byte{
					// total Length
					0x50, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x02, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x00, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x1c, 0x00, 0x00, 0x00,
					// cols
					0x30, 0x00, 0x00, 0x00,

					0x1a, 0x00, 0x00, 0x00,
					0x09, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xba, 0x08, 0x32, 0x27, 0x92, 0x01, 0x00, 0x00,

					0x16, 0x00, 0x00, 0x00,
					0x04, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x04, 0x00, 0x00, 0x00,
					0x7b, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.NoError,
		},
		{
			name: "col zero length",
			args: args{
				sql: "insert into test1 values (?,?)",
				data: []byte{
					// total Length
					0x50, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x02, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x00, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x1c, 0x00, 0x00, 0x00,
					// cols
					0x00, 0x00, 0x00, 0x00,

					0x1a, 0x00, 0x00, 0x00,
					0x09, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xba, 0x08, 0x32, 0x27, 0x92, 0x01, 0x00, 0x00,

					0x16, 0x00, 0x00, 0x00,
					0x04, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x04, 0x00, 0x00, 0x00,
					0x7b, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong col offset",
			args: args{
				sql: "insert into test1 values (?,?)",
				data: []byte{
					// total Length
					0x50, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x02, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x00, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x50, 0x00, 0x00, 0x00,
					// cols
					0x30, 0x00, 0x00, 0x00,

					0x1a, 0x00, 0x00, 0x00,
					0x09, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xba, 0x08, 0x32, 0x27, 0x92, 0x01, 0x00, 0x00,

					0x16, 0x00, 0x00, 0x00,
					0x04, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x04, 0x00, 0x00, 0x00,
					0x7b, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong col length",
			args: args{
				sql: "insert into test1 values (?,?)",
				data: []byte{
					// total Length
					0x50, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x02, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x00, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x1c, 0x00, 0x00, 0x00,
					// cols
					0x50, 0x00, 0x00, 0x00,

					0x1a, 0x00, 0x00, 0x00,
					0x09, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xba, 0x08, 0x32, 0x27, 0x92, 0x01, 0x00, 0x00,

					0x16, 0x00, 0x00, 0x00,
					0x04, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x04, 0x00, 0x00, 0x00,
					0x7b, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong col bind length",
			args: args{
				sql: "insert into test1 values (?,?)",
				data: []byte{
					// total Length
					0x50, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x02, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x00, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x1c, 0x00, 0x00, 0x00,
					// cols
					0x30, 0x00, 0x00, 0x00,

					0x1b, 0x00, 0x00, 0x00,
					0x09, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xba, 0x08, 0x32, 0x27, 0x92, 0x01, 0x00, 0x00,

					0x16, 0x00, 0x00, 0x00,
					0x04, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x04, 0x00, 0x00, 0x00,
					0x7b, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "normal col count",
			args: args{
				sql: "insert into test1 values (?,?)",
				data: []byte{
					// total Length
					0x50, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x00, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x1c, 0x00, 0x00, 0x00,
					// cols
					0x30, 0x00, 0x00, 0x00,

					0x1a, 0x00, 0x00, 0x00,
					0x09, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xba, 0x08, 0x32, 0x27, 0x92, 0x01, 0x00, 0x00,

					0x16, 0x00, 0x00, 0x00,
					0x04, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x04, 0x00, 0x00, 0x00,
					0x7b, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "normal tag",
			args: args{
				sql: "insert into ? using stb tags(?) values (?,?)",
				data: []byte{
					// total Length
					0x40, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x01, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x1c, 0x00, 0x00, 0x00,
					// TagsOffset
					0x22, 0x00, 0x00, 0x00,
					// ColOffset
					0x00, 0x00, 0x00, 0x00,
					// table names
					0x04, 0x00, 0x63, 0x74, 0x62, 0x00,
					// tags
					0x1a, 0x00, 0x00, 0x00,

					0x1a, 0x00, 0x00, 0x00,
					0x05, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xc8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.NoError,
		},
		{
			name: "tag zero length",
			args: args{
				sql: "insert into ? using stb tags(?) values (?,?)",
				data: []byte{
					// total Length
					0x40, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x01, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x1c, 0x00, 0x00, 0x00,
					// TagsOffset
					0x22, 0x00, 0x00, 0x00,
					// ColOffset
					0x00, 0x00, 0x00, 0x00,
					// table names
					0x04, 0x00, 0x63, 0x74, 0x62, 0x00,
					// tags
					0x00, 0x00, 0x00, 0x00,

					0x1a, 0x00, 0x00, 0x00,
					0x05, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xc8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong tag offset",
			args: args{
				sql: "insert into ? using stb tags(?) values (?,?)",
				data: []byte{
					// total Length
					0x40, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x01, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x1c, 0x00, 0x00, 0x00,
					// TagsOffset
					0x40, 0x00, 0x00, 0x00,
					// ColOffset
					0x00, 0x00, 0x00, 0x00,
					// table names
					0x04, 0x00, 0x63, 0x74, 0x62, 0x00,
					// tags
					0x1a, 0x00, 0x00, 0x00,

					0x1a, 0x00, 0x00, 0x00,
					0x05, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xc8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong tag length",
			args: args{
				sql: "insert into ? using stb tags(?) values (?,?)",
				data: []byte{
					// total Length
					0x40, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x01, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x1c, 0x00, 0x00, 0x00,
					// TagsOffset
					0x22, 0x00, 0x00, 0x00,
					// ColOffset
					0x00, 0x00, 0x00, 0x00,
					// table names
					0x04, 0x00, 0x63, 0x74, 0x62, 0x00,
					// tags
					0x40, 0x00, 0x00, 0x00,

					0x1a, 0x00, 0x00, 0x00,
					0x05, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xc8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong tag bind length",
			args: args{
				sql: "insert into ? using stb tags(?) values (?,?)",
				data: []byte{
					// total Length
					0x40, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x01, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x1c, 0x00, 0x00, 0x00,
					// TagsOffset
					0x22, 0x00, 0x00, 0x00,
					// ColOffset
					0x00, 0x00, 0x00, 0x00,
					// table names
					0x04, 0x00, 0x63, 0x74, 0x62, 0x00,
					// tags
					0x1a, 0x00, 0x00, 0x00,

					0x40, 0x00, 0x00, 0x00,
					0x05, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xc8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong tag count",
			args: args{
				sql: "insert into ? using stb tags(?) values (?,?)",
				data: []byte{
					// total Length
					0x40, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x00, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x1c, 0x00, 0x00, 0x00,
					// TagsOffset
					0x22, 0x00, 0x00, 0x00,
					// ColOffset
					0x00, 0x00, 0x00, 0x00,
					// table names
					0x04, 0x00, 0x63, 0x74, 0x62, 0x00,
					// tags
					0x1a, 0x00, 0x00, 0x00,

					0x1a, 0x00, 0x00, 0x00,
					0x05, 0x00, 0x00, 0x00,
					0x01, 0x00, 0x00, 0x00,
					0x00,
					0x00,
					0x08, 0x00, 0x00, 0x00,
					0xc8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "bind binary",
			args: args{
				sql: "insert into test2 values (?,?)",
				data: []byte{
					// total Length
					0x78, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x02, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x00, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x1c, 0x00, 0x00, 0x00,
					// cols
					// col length
					0x58, 0x00, 0x00, 0x00,
					//table 0 cols
					//col 0
					//total length
					0x2c, 0x00, 0x00, 0x00,
					//type
					0x09, 0x00, 0x00, 0x00,
					//num
					0x03, 0x00, 0x00, 0x00,
					//is null
					0x00,
					0x00,
					0x00,
					// haveLength
					0x00,
					// buffer length
					0x18, 0x00, 0x00, 0x00,
					0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00, 0x1a, 0x2f, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00, 0x02, 0x33, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

					//col 1
					//total length
					0x2c, 0x00, 0x00, 0x00,
					//type
					0x08, 0x00, 0x00, 0x00,
					//num
					0x03, 0x00, 0x00, 0x00,
					//is null
					0x00,
					0x01,
					0x00,
					// haveLength
					0x01,
					// length
					0x06, 0x00, 0x00, 0x00,
					0x00, 0x00, 0x00, 0x00,
					0x06, 0x00, 0x00, 0x00,
					// buffer length
					0x0c, 0x00, 0x00, 0x00,
					0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
				},
				colIdx: -1,
			},
			wantErr: assert.NoError,
		},
		{
			name: "empty buffer",
			args: args{
				sql: "insert into test2 values (?,?)",
				data: []byte{
					// total Length
					0x4c, 0x00, 0x00, 0x00,
					// tableCount
					0x01, 0x00, 0x00, 0x00,
					// TagCount
					0x00, 0x00, 0x00, 0x00,
					// ColCount
					0x02, 0x00, 0x00, 0x00,
					// TableNamesOffset
					0x00, 0x00, 0x00, 0x00,
					// TagsOffset
					0x00, 0x00, 0x00, 0x00,
					// ColOffset
					0x1c, 0x00, 0x00, 0x00,
					// cols
					// col length
					0x2c, 0x00, 0x00, 0x00,
					//table 0 cols
					//col 0
					//total length
					0x1a, 0x00, 0x00, 0x00,
					//type
					0x09, 0x00, 0x00, 0x00,
					//num
					0x01, 0x00, 0x00, 0x00,
					//is null
					0x00,
					// haveLength
					0x00,
					// buffer length
					0x08, 0x00, 0x00, 0x00,
					0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

					//col 1
					//total length
					0x12, 0x00, 0x00, 0x00,
					//type
					0x04, 0x00, 0x00, 0x00,
					//num
					0x01, 0x00, 0x00, 0x00,
					//is null
					0x01,
					// haveLength
					0x00,
					// buffer length
					0x00, 0x00, 0x00, 0x00,
				},
				colIdx: -1,
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caller := NewStmtCallBackTest()
			handler := cgo.NewHandle(caller)
			stmt2 := TaosStmt2Init(conn, 0xdd123, false, false, handler)
			defer TaosStmt2Close(stmt2)
			code := TaosStmt2Prepare(stmt2, tt.args.sql)
			if code != 0 {
				errStr := TaosStmt2Error(stmt2)
				err := taosError.NewError(code, errStr)
				t.Error(err)
				return
			}
			tt.wantErr(t, TaosStmt2BindBinary(stmt2, tt.args.data, tt.args.colIdx), fmt.Sprintf("TaosStmt2BindBinary(%v, %v, %v)", stmt2, tt.args.data, tt.args.colIdx))
		})
	}
}

func TestTaosStmt2GetStbFields(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_stb_fields")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database test_stmt2_stb_fields precision 'ns'")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_stb_fields"))

	err = exec(conn, "use test_stmt2_stb_fields")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists all_stb("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))"+
		"tags("+
		"tts timestamp, "+
		"tv1 bool, "+
		"tv2 tinyint, "+
		"tv3 smallint, "+
		"tv4 int, "+
		"tv5 bigint, "+
		"tv6 tinyint unsigned, "+
		"tv7 smallint unsigned, "+
		"tv8 int unsigned, "+
		"tv9 bigint unsigned, "+
		"tv10 float, "+
		"tv11 double, "+
		"tv12 binary(20), "+
		"tv13 varbinary(20), "+
		"tv14 geometry(100), "+
		"tv15 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists commontb("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	expectMap := map[string]*stmt.Stmt2AllField{
		"tts": {
			Name:      "tts",
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: common.PrecisionNanoSecond,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv1": {
			Name:      "tv1",
			FieldType: common.TSDB_DATA_TYPE_BOOL,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv2": {
			Name:      "tv2",
			FieldType: common.TSDB_DATA_TYPE_TINYINT,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv3": {
			Name:      "tv3",
			FieldType: common.TSDB_DATA_TYPE_SMALLINT,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv4": {
			Name:      "tv4",
			FieldType: common.TSDB_DATA_TYPE_INT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv5": {
			Name:      "tv5",
			FieldType: common.TSDB_DATA_TYPE_BIGINT,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv6": {
			Name:      "tv6",
			FieldType: common.TSDB_DATA_TYPE_UTINYINT,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv7": {
			Name:      "tv7",
			FieldType: common.TSDB_DATA_TYPE_USMALLINT,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv8": {
			Name:      "tv8",
			FieldType: common.TSDB_DATA_TYPE_UINT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv9": {
			Name:      "tv9",
			FieldType: common.TSDB_DATA_TYPE_UBIGINT,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv10": {
			Name:      "tv10",
			FieldType: common.TSDB_DATA_TYPE_FLOAT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv11": {
			Name:      "tv11",
			FieldType: common.TSDB_DATA_TYPE_DOUBLE,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv12": {
			Name:      "tv12",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv13": {
			Name:      "tv13",
			FieldType: common.TSDB_DATA_TYPE_VARBINARY,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv14": {
			Name:      "tv14",
			FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
			Bytes:     102,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv15": {
			Name:      "tv15",
			FieldType: common.TSDB_DATA_TYPE_NCHAR,
			Bytes:     82,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"ts": {
			Name:      "ts",
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: common.PrecisionNanoSecond,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v1": {
			Name:      "v1",
			FieldType: common.TSDB_DATA_TYPE_BOOL,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v2": {
			Name:      "v2",
			FieldType: common.TSDB_DATA_TYPE_TINYINT,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v3": {
			Name:      "v3",
			FieldType: common.TSDB_DATA_TYPE_SMALLINT,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v4": {
			Name:      "v4",
			FieldType: common.TSDB_DATA_TYPE_INT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v5": {
			Name:      "v5",
			FieldType: common.TSDB_DATA_TYPE_BIGINT,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v6": {
			Name:      "v6",
			FieldType: common.TSDB_DATA_TYPE_UTINYINT,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v7": {
			Name:      "v7",
			FieldType: common.TSDB_DATA_TYPE_USMALLINT,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v8": {
			Name:      "v8",
			FieldType: common.TSDB_DATA_TYPE_UINT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v9": {
			Name:      "v9",
			FieldType: common.TSDB_DATA_TYPE_UBIGINT,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v10": {
			Name:      "v10",
			FieldType: common.TSDB_DATA_TYPE_FLOAT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v11": {
			Name:      "v11",
			FieldType: common.TSDB_DATA_TYPE_DOUBLE,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v12": {
			Name:      "v12",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v13": {
			Name:      "v13",
			FieldType: common.TSDB_DATA_TYPE_VARBINARY,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v14": {
			Name:      "v14",
			FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
			Bytes:     102,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v15": {
			Name:      "v15",
			FieldType: common.TSDB_DATA_TYPE_NCHAR,
			Bytes:     82,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"tbname": {
			Name:      "tbname",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			Bytes:     271,
			BindType:  stmt.TAOS_FIELD_TBNAME,
		},
	}
	tests := []struct {
		name   string
		sql    string
		expect []string
	}{
		{
			name:   "with subTableName",
			sql:    "insert into tb1 using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			expect: []string{"tts", "tv1", "tv2", "tv3", "tv4", "tv5", "tv6", "tv7", "tv8", "tv9", "tv10", "tv11", "tv12", "tv13", "tv14", "tv15", "ts", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15"},
		},
		{
			name:   "using stb",
			sql:    "insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			expect: []string{"tbname", "tts", "tv1", "tv2", "tv3", "tv4", "tv5", "tv6", "tv7", "tv8", "tv9", "tv10", "tv11", "tv12", "tv13", "tv14", "tv15", "ts", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15"},
		},
		{
			name:   "tbname as value",
			sql:    "insert into all_stb (tbname,tts,tv1,tv2,tv3,tv4,tv5,tv6,tv7,tv8,tv9,tv10,tv11,tv12,tv13,tv14,tv15,ts,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			expect: []string{"tbname", "tts", "tv1", "tv2", "tv3", "tv4", "tv5", "tv6", "tv7", "tv8", "tv9", "tv10", "tv11", "tv12", "tv13", "tv14", "tv15", "ts", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15"},
		},
		{
			name:   "tbname as value random",
			sql:    "insert into all_stb (ts,v1,v2,v3,v4,v5,v6,tts,tv1,tv2,tv3,tv4,tv5,tv6,tv7,tv8,tv9,tv10,tv11,tv12,tv13,tv14,tbname,tv15,v7,v8,v9,v10,v11,v12,v13,v14,v15) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			expect: []string{"ts", "v1", "v2", "v3", "v4", "v5", "v6", "tts", "tv1", "tv2", "tv3", "tv4", "tv5", "tv6", "tv7", "tv8", "tv9", "tv10", "tv11", "tv12", "tv13", "tv14", "tbname", "tv15", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15"},
		},
		{
			name:   "common table",
			sql:    "insert into commontb values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			expect: []string{"ts", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v12", "v13", "v14", "v15"},
		},
	}
	for _, tt := range tests {
		caller := NewStmtCallBackTest()
		handler := cgo.NewHandle(caller)
		stmt2 := TaosStmt2Init(conn, 0xed123, false, false, handler)
		defer TaosStmt2Close(stmt2)
		code := TaosStmt2Prepare(stmt2, tt.sql)
		if code != 0 {
			errStr := TaosStmt2Error(stmt2)
			err := taosError.NewError(code, errStr)
			t.Error(err)
			return
		}
		code, count, fields := TaosStmt2GetFields(stmt2)
		if code != 0 {
			errStr := TaosStmt2Error(stmt2)
			err := taosError.NewError(code, errStr)
			t.Error(err)
			return
		}
		fs := Stmt2ParseAllFields(count, fields)
		TaosStmt2FreeFields(stmt2, fields)
		expect := make([]*stmt.Stmt2AllField, len(tt.expect))
		for i := 0; i < len(tt.expect); i++ {
			assert.Equal(t, expectMap[tt.expect[i]].Name, fs[i].Name)
			assert.Equal(t, expectMap[tt.expect[i]].FieldType, fs[i].FieldType)
			assert.Equal(t, expectMap[tt.expect[i]].Bytes, fs[i].Bytes)
			assert.Equal(t, expectMap[tt.expect[i]].BindType, fs[i].BindType)
			if expectMap[tt.expect[i]].FieldType == common.TSDB_DATA_TYPE_TIMESTAMP {
				assert.Equal(t, expectMap[tt.expect[i]].Precision, fs[i].Precision)
			}
			expect[i] = expectMap[tt.expect[i]]
		}
	}

	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	stmt2 := TaosStmt2Init(conn, 0xfd123, false, false, handler)
	defer TaosStmt2Close(stmt2)
	code := TaosStmt2Prepare(stmt2, "select * from commontb where ts = ? and v1 = ?")
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err := taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	code, count, fields := TaosStmt2GetFields(stmt2)
	if code != 0 {
		errStr := TaosStmt2Error(stmt2)
		err := taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	TaosStmt2FreeFields(stmt2, fields)
	assert.Equal(t, 2, count)
}

func TestWrongParseStmt2StbFields(t *testing.T) {
	fs := Stmt2ParseAllFields(0, nil)
	assert.Nil(t, fs)
	fs = Stmt2ParseAllFields(2, nil)
	assert.Nil(t, fs)
}

func TestStmt2BindTbnameAsValue(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer TaosClose(conn)
	defer func() {
		err = exec(conn, "drop database if exists test_stmt2_bind_tbname_as_value")
		if err != nil {
			t.Error(err)
			return
		}
	}()
	err = exec(conn, "create database if not exists test_stmt2_bind_tbname_as_value precision 'ns' keep 36500")
	if err != nil {
		t.Error(err)
		return
	}
	assert.NoError(t, ensureDBCreated(conn, "test_stmt2_bind_tbname_as_value"))

	err = exec(conn, "use test_stmt2_bind_tbname_as_value")
	if err != nil {
		t.Error(err)
		return
	}
	err = exec(conn, "create table if not exists all_stb("+
		"ts timestamp, "+
		"v1 bool, "+
		"v2 tinyint, "+
		"v3 smallint, "+
		"v4 int, "+
		"v5 bigint, "+
		"v6 tinyint unsigned, "+
		"v7 smallint unsigned, "+
		"v8 int unsigned, "+
		"v9 bigint unsigned, "+
		"v10 float, "+
		"v11 double, "+
		"v12 binary(20), "+
		"v13 varbinary(20), "+
		"v14 geometry(100), "+
		"v15 nchar(20))"+
		"tags("+
		"tts timestamp, "+
		"tv1 bool, "+
		"tv2 tinyint, "+
		"tv3 smallint, "+
		"tv4 int, "+
		"tv5 bigint, "+
		"tv6 tinyint unsigned, "+
		"tv7 smallint unsigned, "+
		"tv8 int unsigned, "+
		"tv9 bigint unsigned, "+
		"tv10 float, "+
		"tv11 double, "+
		"tv12 binary(20), "+
		"tv13 varbinary(20), "+
		"tv14 geometry(100), "+
		"tv15 nchar(20))")
	if err != nil {
		t.Error(err)
		return
	}
	caller := NewStmtCallBackTest()
	handler := cgo.NewHandle(caller)
	insertStmt := TaosStmt2Init(conn, 0xff1234, false, false, handler)
	prepareInsertSql := "insert into all_stb (ts ,v1 ,v2 ,v3 ,v4 ,v5 ,v6 ,v7 ,v8 ,v9 ,v10,v11,v12,v13,v14,v15,tbname,tts,tv1 ,tv2 ,tv3 ,tv4 ,tv5 ,tv6 ,tv7 ,tv8 ,tv9 ,tv10,tv11,tv12,tv13,tv14,tv15)  values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	code := TaosStmt2Prepare(insertStmt, prepareInsertSql)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}

	isInsert, code := TaosStmt2IsInsert(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	assert.True(t, isInsert)

	code, count, cFields := TaosStmt2GetFields(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	defer TaosStmt2FreeFields(insertStmt, cFields)
	assert.Equal(t, 33, count)
	fields := Stmt2ParseAllFields(count, cFields)
	assert.Equal(t, 33, len(fields))
	expectMap := map[string]*stmt.Stmt2AllField{
		"tts": {
			Name:      "tts",
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: common.PrecisionNanoSecond,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv1": {
			Name:      "tv1",
			FieldType: common.TSDB_DATA_TYPE_BOOL,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv2": {
			Name:      "tv2",
			FieldType: common.TSDB_DATA_TYPE_TINYINT,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv3": {
			Name:      "tv3",
			FieldType: common.TSDB_DATA_TYPE_SMALLINT,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv4": {
			Name:      "tv4",
			FieldType: common.TSDB_DATA_TYPE_INT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv5": {
			Name:      "tv5",
			FieldType: common.TSDB_DATA_TYPE_BIGINT,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv6": {
			Name:      "tv6",
			FieldType: common.TSDB_DATA_TYPE_UTINYINT,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv7": {
			Name:      "tv7",
			FieldType: common.TSDB_DATA_TYPE_USMALLINT,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv8": {
			Name:      "tv8",
			FieldType: common.TSDB_DATA_TYPE_UINT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv9": {
			Name:      "tv9",
			FieldType: common.TSDB_DATA_TYPE_UBIGINT,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv10": {
			Name:      "tv10",
			FieldType: common.TSDB_DATA_TYPE_FLOAT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv11": {
			Name:      "tv11",
			FieldType: common.TSDB_DATA_TYPE_DOUBLE,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv12": {
			Name:      "tv12",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv13": {
			Name:      "tv13",
			FieldType: common.TSDB_DATA_TYPE_VARBINARY,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv14": {
			Name:      "tv14",
			FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
			Bytes:     102,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"tv15": {
			Name:      "tv15",
			FieldType: common.TSDB_DATA_TYPE_NCHAR,
			Bytes:     82,
			BindType:  stmt.TAOS_FIELD_TAG,
		},
		"ts": {
			Name:      "ts",
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: common.PrecisionNanoSecond,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v1": {
			Name:      "v1",
			FieldType: common.TSDB_DATA_TYPE_BOOL,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v2": {
			Name:      "v2",
			FieldType: common.TSDB_DATA_TYPE_TINYINT,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v3": {
			Name:      "v3",
			FieldType: common.TSDB_DATA_TYPE_SMALLINT,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v4": {
			Name:      "v4",
			FieldType: common.TSDB_DATA_TYPE_INT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v5": {
			Name:      "v5",
			FieldType: common.TSDB_DATA_TYPE_BIGINT,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v6": {
			Name:      "v6",
			FieldType: common.TSDB_DATA_TYPE_UTINYINT,
			Bytes:     1,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v7": {
			Name:      "v7",
			FieldType: common.TSDB_DATA_TYPE_USMALLINT,
			Bytes:     2,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v8": {
			Name:      "v8",
			FieldType: common.TSDB_DATA_TYPE_UINT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v9": {
			Name:      "v9",
			FieldType: common.TSDB_DATA_TYPE_UBIGINT,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v10": {
			Name:      "v10",
			FieldType: common.TSDB_DATA_TYPE_FLOAT,
			Bytes:     4,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v11": {
			Name:      "v11",
			FieldType: common.TSDB_DATA_TYPE_DOUBLE,
			Bytes:     8,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v12": {
			Name:      "v12",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v13": {
			Name:      "v13",
			FieldType: common.TSDB_DATA_TYPE_VARBINARY,
			Bytes:     22,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v14": {
			Name:      "v14",
			FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
			Bytes:     102,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"v15": {
			Name:      "v15",
			FieldType: common.TSDB_DATA_TYPE_NCHAR,
			Bytes:     82,
			BindType:  stmt.TAOS_FIELD_COL,
		},
		"tbname": {
			Name:      "tbname",
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			Bytes:     271,
			BindType:  stmt.TAOS_FIELD_TBNAME,
		},
	}

	for i := 0; i < 33; i++ {
		expect := expectMap[fields[i].Name]
		assert.Equal(t, expect, fields[i])
	}

	now := time.Now()
	params2 := []*stmt.TaosStmt2BindData{{
		TableName: "ctb1",
		Tags: []driver.Value{
			// TIMESTAMP
			now,
			// BOOL
			true,
			// TINYINT
			int8(1),
			// SMALLINT
			int16(1),
			// INT
			int32(1),
			// BIGINT
			int64(1),
			// UTINYINT
			uint8(1),
			// USMALLINT
			uint16(1),
			// UINT
			uint32(1),
			// UBIGINT
			uint64(1),
			// FLOAT
			float32(1.2),
			// DOUBLE
			float64(1.2),
			// BINARY
			[]byte("binary"),
			// VARBINARY
			[]byte("varbinary"),
			// GEOMETRY
			[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			// NCHAR
			"nchar",
		},
		Cols: [][]driver.Value{
			{
				now,
				now.Add(time.Second),
				now.Add(time.Second * 2),
			},
			{
				true,
				nil,
				false,
			},
			{
				int8(11),
				nil,
				int8(12),
			},
			{
				int16(11),
				nil,
				int16(12),
			},
			{
				int32(11),
				nil,
				int32(12),
			},
			{
				int64(11),
				nil,
				int64(12),
			},
			{
				uint8(11),
				nil,
				uint8(12),
			},
			{
				uint16(11),
				nil,
				uint16(12),
			},
			{
				uint32(11),
				nil,
				uint32(12),
			},
			{
				uint64(11),
				nil,
				uint64(12),
			},
			{
				float32(11.2),
				nil,
				float32(12.2),
			},
			{
				float64(11.2),
				nil,
				float64(12.2),
			},
			{
				[]byte("binary1"),
				nil,
				[]byte("binary2"),
			},
			{
				[]byte("varbinary1"),
				nil,
				[]byte("varbinary2"),
			},
			{
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
				nil,
				[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
			},
			{
				"nchar1",
				nil,
				"nchar2",
			},
		},
	}}
	bs, err := stmt.MarshalStmt2Binary(params2, true, fields)
	assert.NoError(t, err)
	err = TaosStmt2BindBinary(insertStmt, bs, -1)
	if err != nil {
		t.Error(err)
		return
	}
	code = TaosStmt2Exec(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
	r := <-caller.ExecResult
	if r.n != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(r.n, errStr)
		t.Error(err)
		return
	}
	assert.Equal(t, 3, r.affected)

	code = TaosStmt2Close(insertStmt)
	if code != 0 {
		errStr := TaosStmt2Error(insertStmt)
		err = taosError.NewError(code, errStr)
		t.Error(err)
		return
	}
}
