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

import com.taosdata.taosdemo.domain.TagMeta;
import com.taosdata.taosdemo.domain.TagValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TagValueGeneratorTest {
    List<TagValue> tagvalues;

    @Test
    public void generate() {
        List<TagMeta> tagMetaList = new ArrayList<>();
        tagMetaList.add(new TagMeta("location", "nchar(10)"));
        tagMetaList.add(new TagMeta("groupId", "int"));
        tagMetaList.add(new TagMeta("ts", "timestamp"));
        tagMetaList.add(new TagMeta("temperature", "float"));
        tagMetaList.add(new TagMeta("humidity", "double"));
        tagMetaList.add(new TagMeta("text", "binary(10)"));
        tagvalues = TagValueGenerator.generate(tagMetaList);
        Assert.assertEquals("location", tagvalues.get(0).getName());
        Assert.assertEquals("groupId", tagvalues.get(1).getName());
        Assert.assertEquals("ts", tagvalues.get(2).getName());
        Assert.assertEquals("temperature", tagvalues.get(3).getName());
        Assert.assertEquals("humidity", tagvalues.get(4).getName());
        Assert.assertEquals("text", tagvalues.get(5).getName());
    }

    @After
    public void after() {
        tagvalues.stream().forEach(System.out::println);
    }
}