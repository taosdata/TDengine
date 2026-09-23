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

package random

import "testing"

func TestDeterministicSequence(t *testing.T) {
	a := New(12345)
	b := New(12345)
	for i := 0; i < 200; i++ {
		if a.Uint64() != b.Uint64() {
			t.Fatalf("sequence mismatch at %d", i)
		}
	}
}

func TestSerializeDeserialize(t *testing.T) {
	r := New(987654321)
	_ = r.Uint64()
	_ = r.Uint64()
	state := r.Serialize()

	r2 := New(1)
	if err := r2.Deserialize(state); err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}
	for i := 0; i < 100; i++ {
		if r.Uint64() != r2.Uint64() {
			t.Fatalf("deserialized stream mismatch at %d", i)
		}
	}
}
