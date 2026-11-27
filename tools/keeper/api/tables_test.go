// Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

package api

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/db"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/testutil"
	"github.com/taosdata/taoskeeper/util"
)

func TestCreateClusterInfoSql(t *testing.T) {
	conn, _ := db.NewConnector(testutil.TestUsername(), testutil.TestPassword(), "127.0.0.1", 6041, false)
	defer conn.Close()

	dbName := "db_202412031539"
	conn.Exec(context.Background(), "create database "+dbName, util.GetQidOwn(config.Conf.InstanceID))
	defer conn.Exec(context.Background(), "drop database "+dbName, util.GetQidOwn(config.Conf.InstanceID))

	conn, _ = db.NewConnectorWithDb(testutil.TestUsername(), testutil.TestPassword(), "127.0.0.1", 6041, dbName, false)
	defer conn.Close()

	conn.Exec(context.Background(), CreateClusterInfoSql, util.GetQidOwn(config.Conf.InstanceID))

	testCases := []struct {
		ep      string
		wantErr bool
	}{
		{"", false},
		{"hello", false},
		{strings.Repeat("a", 128), false},
		{strings.Repeat("a", 255), false},
		{strings.Repeat("a", 256), true},
	}

	conn.Exec(context.Background(),
		"create table d0 using cluster_info tags('cluster_id')", util.GetQidOwn(config.Conf.InstanceID))

	for _, tc := range testCases {
		sql := fmt.Sprintf("insert into d0 (ts, first_ep) values(%d, '%s')", time.Now().UnixMilli(), tc.ep)
		_, err := conn.Exec(context.Background(), sql, util.GetQidOwn(config.Conf.InstanceID))
		if tc.wantErr {
			assert.Error(t, err) // [0x2653] Value too long for column/tag: endpoint
		} else {
			assert.NoError(t, err)
		}
	}
}
