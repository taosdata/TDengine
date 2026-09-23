/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.TagValue;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SubTableServiceTest {

    private SubTableService service;

    private List<SubTableMeta> subTables;

    @Before
    public void before() {
        subTables = new ArrayList<>();
        for (int i = 1; i <= 1; i++) {
            SubTableMeta subTableMeta = new SubTableMeta();
            subTableMeta.setDatabase("test");
            subTableMeta.setSupertable("weather");
            subTableMeta.setName("t" + i);
            List<TagValue> tags = new ArrayList<>();
            tags.add(new TagValue("location", "beijing"));
            tags.add(new TagValue("groupId", i));
            subTableMeta.setTags(tags);
            subTables.add(subTableMeta);
        }
    }

    @Test
    public void testCreateSubTable() {

    }

    @Test
    public void testCreateSubTableList() {

    }
}