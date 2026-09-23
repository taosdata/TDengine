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

import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.domain.TagMeta;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SuperTableServiceTest {

    private static SuperTableService service;

    @Test
    public void testCreate() {
        SuperTableMeta superTableMeta = new SuperTableMeta();
        superTableMeta.setDatabase("test");
        superTableMeta.setName("weather");
        List<FieldMeta> fields = new ArrayList<>();
        fields.add(new FieldMeta("ts", "timestamp"));
        fields.add(new FieldMeta("temperature", "float"));
        fields.add(new FieldMeta("humidity", "int"));
        superTableMeta.setFields(fields);
        List<TagMeta> tags = new ArrayList<>();
        tags.add(new TagMeta("location", "nchar(64)"));
        tags.add(new TagMeta("groupId", "int"));
        superTableMeta.setTags(tags);
        service.create(superTableMeta);
    }

    @BeforeClass
    public static void beforeClass() throws ClassNotFoundException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:TAOS://127.0.0.1:6030/?charset=UTF-8&locale=en_US.UTF-8&timezone=UTC-8");
        config.setUsername("root");
        config.setPassword("taosdata");
        HikariDataSource dataSource = new HikariDataSource(config);
        service = new SuperTableService(dataSource);
    }

}