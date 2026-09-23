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

package com.taosdata.taosdemo.service.data;

import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.domain.TagMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class SuperTableMetaGeneratorImplTest {
    private SuperTableMeta meta;

    @Test
    public void generate() {
        String sql = "create table test.weather (ts timestamp, temperature float, humidity int) tags(location nchar(64), groupId int)";
        meta = SuperTableMetaGenerator.generate(sql);
        Assert.assertEquals("test", meta.getDatabase());
        Assert.assertEquals("weather", meta.getName());
        Assert.assertEquals(3, meta.getFields().size());
        Assert.assertEquals("ts", meta.getFields().get(0).getName());
        Assert.assertEquals("timestamp", meta.getFields().get(0).getType());
        Assert.assertEquals("temperature", meta.getFields().get(1).getName());
        Assert.assertEquals("float", meta.getFields().get(1).getType());
        Assert.assertEquals("humidity", meta.getFields().get(2).getName());
        Assert.assertEquals("int", meta.getFields().get(2).getType());

        Assert.assertEquals("location", meta.getTags().get(0).getName());
        Assert.assertEquals("nchar(64)", meta.getTags().get(0).getType());
        Assert.assertEquals("groupid", meta.getTags().get(1).getName());
        Assert.assertEquals("int", meta.getTags().get(1).getType());
    }

    @Test
    public void generate2() {
        meta = SuperTableMetaGenerator.generate("test", "weather", 10, "col", 10, "tag");
        Assert.assertEquals("test", meta.getDatabase());
        Assert.assertEquals("weather", meta.getName());
        Assert.assertEquals(11, meta.getFields().size());
        for (FieldMeta fieldMeta : meta.getFields()) {
            Assert.assertNotNull(fieldMeta.getName());
            Assert.assertNotNull(fieldMeta.getType());
        }
        for (TagMeta tagMeta : meta.getTags()) {
            Assert.assertNotNull(tagMeta.getName());
            Assert.assertNotNull(tagMeta.getType());
        }
    }

    @After
    public void after() {
        System.out.println(meta.getDatabase());
        System.out.println(meta.getName());
        for (FieldMeta fieldMeta : meta.getFields()) {
            System.out.println(fieldMeta);
        }
        for (TagMeta tagMeta : meta.getTags()) {
            System.out.println(tagMeta);
        }
    }
}