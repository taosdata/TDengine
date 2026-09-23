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
import com.taosdata.taosdemo.utils.DataGenerator;

import java.util.ArrayList;
import java.util.List;

public class TagValueGenerator {

    // Create tag values using tagMetas
    public static List<TagValue> generate(List<TagMeta> tagMetas) {
        List<TagValue> tagValues = new ArrayList<>();
        for (int i = 0; i < tagMetas.size(); i++) {
            TagMeta tagMeta = tagMetas.get(i);
            TagValue tagValue = new TagValue();
            tagValue.setName(tagMeta.getName());
            tagValue.setValue(DataGenerator.randomValue(tagMeta.getType()));
            tagValues.add(tagValue);
        }
        return tagValues;
    }
}
