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

import "time"

const (
	sqlSelectAllRule = "SELECT * FROM `rule`;"
	sqlSelectRule    = "SELECT * FROM `rule` WHERE `name` = ?;"
	sqlInsertRule    = "INSERT INTO `rule`(`name`, `enabled`, `created_at`, `updated_at`, `content`) VALUES(:name, :enabled, :created_at, :updated_at, :content);"
	sqlUpdateRule    = "UPDATE `rule` SET `content` = :content, `updated_at` = :updated_at WHERE `name` = :name;"
	sqlEnableRule    = "UPDATE `rule` SET `enabled` = :enabled, `updated_at` = :updated_at WHERE `name` = :name;"
	sqlDeleteRule    = "DELETE FROM `rule` WHERE `name` = ?;"
)

type Rule struct {
	Name      string    `db:"name"`
	Enabled   bool      `db:"enabled"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
	Content   string    `db:"content"`
}

func AddRule(r *Rule) error {
	r.CreatedAt = time.Now()
	r.Enabled = true
	r.UpdatedAt = r.CreatedAt
	_, e := db.NamedExec(sqlInsertRule, r)
	return e
}

func UpdateRule(name string, content string) error {
	r := Rule{
		Name:      name,
		UpdatedAt: time.Now(),
		Content:   content,
	}
	_, e := db.NamedExec(sqlUpdateRule, &r)
	return e
}

func EnableRule(name string, enabled bool) error {
	r := Rule{
		Name:      name,
		Enabled:   enabled,
		UpdatedAt: time.Now(),
	}

	if res, e := db.NamedExec(sqlEnableRule, &r); e != nil {
		return e
	} else if n, e := res.RowsAffected(); n != 1 {
		return e
	}

	return nil
}

func DeleteRule(name string) error {
	_, e := db.Exec(sqlDeleteRule, name)
	return e
}

func GetRuleByName(name string) (*Rule, error) {
	r := Rule{}
	if e := db.Get(&r, sqlSelectRule, name); e != nil {
		return nil, e
	}
	return &r, nil
}

func LoadAllRule() ([]Rule, error) {
	var rules []Rule
	if e := db.Select(&rules, sqlSelectAllRule); e != nil {
		return nil, e
	}
	return rules, nil
}
