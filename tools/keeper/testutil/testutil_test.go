// Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

package testutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTestUsername(t *testing.T) {
	os.Setenv("TEST_USERNAME", "test_user")
	defer os.Unsetenv("TEST_USERNAME")
	username := TestUsername()
	assert.Equal(t, "test_user", username)
}

func TestTestUsernameDefault(t *testing.T) {
	username := TestUsername()
	assert.Equal(t, "root", username)
}

func TestTestPassword(t *testing.T) {
	os.Setenv("TEST_PASSWORD", "test_pass")
	defer os.Unsetenv("TEST_PASSWORD")
	password := TestPassword()
	assert.Equal(t, "test_pass", password)
}

func TestTestPasswordDefault(t *testing.T) {
	password := TestPassword()
	assert.Equal(t, "taosdata", password)
}
