/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.taosdata.example.mybatisplusdemo.domain.Meters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Timestamp;
import java.util.Random;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class MetersMapperTest {

    private static Random random = new Random(System.currentTimeMillis());

    @Autowired
    private MetersMapper mapper;

    @Before
    public void createTable(){
        mapper.dropTable();
        mapper.createTable();
        Meters one = new Meters();
        one.setTbname("test_10001");
        one.setGroupid(10001);
        one.setCurrent(random.nextFloat());
        one.setPhase(random.nextFloat());
        one.setCurrent(12345);
        one.setTs(new Timestamp(1605024000000l));
        one.setLocation("望京".getBytes());
        mapper.insertOne(one);
    }

    @Test
    public void testSelectList() {
        mapper.selectList(null).forEach(System.out::println);
    }

    @Test
    public void testSelectOne() {
        QueryWrapper<Meters> wrapper = new QueryWrapper<>();
        wrapper.eq("location", "望京".getBytes());
        Meters one = mapper.selectOne(wrapper);
        System.out.println(one);
        Assert.assertEquals(12345, one.getCurrent(), 0.00f);
        Assert.assertArrayEquals("望京".getBytes(), one.getLocation());
    }

    @Test
    public void testSelectObjs() {
        mapper.selectObjs(null).forEach(System.out::println);
    }

    @Test
    public void testSelectCount() {
        System.out.println(mapper.selectCount(null));
    }

    @Test
    public void testSelectPage() {
        IPage page = new Page(1, 2);
        IPage<Meters> metersIPage = mapper.selectPage(page, null);
        System.out.println("total : " + metersIPage.getTotal());
        System.out.println("pages : " + metersIPage.getPages());
        metersIPage.getRecords().forEach(System.out::println);
    }

    @Test
    public void testCountBySql() {
        int count = mapper.countBySql("test_10001");
        System.out.println(count);
        Assert.assertEquals(1, count);
    }
}
