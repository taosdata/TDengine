/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package models

import (
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/taosdata/alert/utils"
	"github.com/taosdata/alert/utils/log"
)

var db *sqlx.DB

func Init() error {
	xdb, e := sqlx.Connect("sqlite3", utils.Cfg.Database)
	if e == nil {
		db = xdb
	}
	return upgrade()
}

func Uninit() error {
	db.Close()
	return nil
}

func getStringOption(tx *sqlx.Tx, name string) (string, error) {
	const qs = "SELECT * FROM `option` WHERE `name`=?"

	var (
		e error
		o struct {
			Name  string `db:"name"`
			Value string `db:"value"`
		}
	)

	if tx != nil {
		e = tx.Get(&o, qs, name)
	} else {
		e = db.Get(&o, qs, name)
	}

	if e != nil {
		return "", e
	}

	return o.Value, nil
}

func getIntOption(tx *sqlx.Tx, name string) (int, error) {
	s, e := getStringOption(tx, name)
	if e != nil {
		return 0, e
	}
	v, e := strconv.ParseInt(s, 10, 64)
	return int(v), e
}

func setOption(tx *sqlx.Tx, name string, value interface{}) error {
	const qs = "REPLACE INTO `option`(`name`, `value`) VALUES(?, ?);"

	var (
		e  error
		sv string
	)

	switch v := value.(type) {
	case time.Time:
		sv = v.Format(time.RFC3339)
	default:
		sv = fmt.Sprint(value)
	}

	if tx != nil {
		_, e = tx.Exec(qs, name, sv)
	} else {
		_, e = db.Exec(qs, name, sv)
	}

	return e
}

var upgradeScripts = []struct {
	ver   int
	stmts []string
}{
	{
		ver: 0,
		stmts: []string{
			"CREATE TABLE `option`( `name` VARCHAR(63) PRIMARY KEY, `value` VARCHAR(255) NOT NULL) WITHOUT ROWID;",
			"CREATE TABLE `rule`( `name` VARCHAR(63) PRIMARY KEY, `enabled` TINYINT(1) NOT NULL, `created_at` DATETIME NOT NULL, `updated_at` DATETIME NOT NULL, `content` TEXT(65535) NOT NULL);",
		},
	},
}

func upgrade() error {
	const dbVersion = "database version"

	ver, e := getIntOption(nil, dbVersion)
	if e != nil { // regards all errors as schema not created
		ver = -1 // set ver to -1 to execute all statements
	}

	tx, e := db.Beginx()
	if e != nil {
		return e
	}

	for _, us := range upgradeScripts {
		if us.ver <= ver {
			continue
		}
		log.Info("upgrading database to version: ", us.ver)
		for _, s := range us.stmts {
			if _, e = tx.Exec(s); e != nil {
				tx.Rollback()
				return e
			}
		}
		ver = us.ver
	}

	if e = setOption(tx, dbVersion, ver); e != nil {
		tx.Rollback()
		return e
	}

	return tx.Commit()
}
