package workload

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	TxnBegin           = "TxnBegin"
	TxnCommit          = "TxnCommit"
	TxnRollback        = "TxnRollback"
	DDLCreateTable     = "DDLCreateTable"
	DDLAlterTable      = "DDLAlterTable"
	DDLCreateIndex     = "DDLCreateIndex"
	DMLSelect          = "DMLSelect"
	DMLSelectForUpdate = "DMLSelectForUpdate"
	DMLDelete          = "DMLDelete"
	DMLUpdate          = "DMLUpdate"
	DMLInsert          = "DMLInsert"
)

var Stmts = []string{
	TxnBegin,
	TxnCommit,
	TxnRollback,
	DDLCreateTable,
	DDLAlterTable,
	DDLCreateIndex,
	DMLSelect,
	DMLSelectForUpdate,
	DMLDelete,
	DMLUpdate,
	DMLInsert,
}

// Config follows go-sqlsmith statement weight design.
type Config struct {
	TxnBegin           int `toml:"txn-begin"`
	TxnCommit          int `toml:"txn-commit"`
	TxnRollback        int `toml:"txn-rollback"`
	DDLCreateTable     int `toml:"ddl-create-table"`
	DDLAlterTable      int `toml:"ddl-alter-table"`
	DDLCreateIndex     int `toml:"ddl-create-index"`
	DMLSelect          int `toml:"dml-select"`
	DMLSelectForUpdate int `toml:"dml-select-for-update"`
	DMLDelete          int `toml:"dml-delete"`
	DMLUpdate          int `toml:"dml-update"`
	DMLInsert          int `toml:"dml-insert"`
}

func DefaultConfig() Config {
	return Config{
		TxnBegin:           0,
		TxnCommit:          0,
		TxnRollback:        0,
		DDLCreateTable:     1,
		DDLAlterTable:      10,
		DDLCreateIndex:     10,
		DMLSelect:          240,
		DMLSelectForUpdate: 0,
		DMLDelete:          10,
		DMLUpdate:          30,
		DMLInsert:          30,
	}
}

func (c *Config) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid config line %d: %q", lineNo, line)
		}
		key := strings.TrimSpace(parts[0])
		rawVal := strings.TrimSpace(parts[1])
		v, err := strconv.Atoi(rawVal)
		if err != nil {
			return fmt.Errorf("invalid integer at line %d: %q", lineNo, rawVal)
		}
		switch key {
		case "txn-begin":
			c.TxnBegin = v
		case "txn-commit":
			c.TxnCommit = v
		case "txn-rollback":
			c.TxnRollback = v
		case "ddl-create-table":
			c.DDLCreateTable = v
		case "ddl-alter-table":
			c.DDLAlterTable = v
		case "ddl-create-index":
			c.DDLCreateIndex = v
		case "dml-select":
			c.DMLSelect = v
		case "dml-select-for-update":
			c.DMLSelectForUpdate = v
		case "dml-delete":
			c.DMLDelete = v
		case "dml-update":
			c.DMLUpdate = v
		case "dml-insert":
			c.DMLInsert = v
		default:
			return fmt.Errorf("unknown config key at line %d: %s", lineNo, key)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return nil
}

func (c Config) Weight(name string) int {
	switch name {
	case TxnBegin:
		return c.TxnBegin
	case TxnCommit:
		return c.TxnCommit
	case TxnRollback:
		return c.TxnRollback
	case DDLCreateTable:
		return c.DDLCreateTable
	case DDLAlterTable:
		return c.DDLAlterTable
	case DDLCreateIndex:
		return c.DDLCreateIndex
	case DMLSelect:
		return c.DMLSelect
	case DMLSelectForUpdate:
		return c.DMLSelectForUpdate
	case DMLDelete:
		return c.DMLDelete
	case DMLUpdate:
		return c.DMLUpdate
	case DMLInsert:
		return c.DMLInsert
	default:
		return 0
	}
}
