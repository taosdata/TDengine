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

package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.taosdata.example.mybatisplusdemo.domain.Weather;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class WeatherMapperTest {

    private static Random random = new Random(System.currentTimeMillis());

    @Autowired
    private WeatherMapper mapper;

    @Before 
    public void createTable(){
        mapper.dropTable();
        mapper.createTable();
        Weather one = new Weather();
        one.setTs(new Timestamp(1605024000000l));
        one.setTemperature(12.22f);
        one.setLocation("望京");
        one.setHumidity(100);
        mapper.insertOne(one);
    }

    @Test
    public void testSelectList() {
        List<Weather> weathers = mapper.selectList(null);
        weathers.forEach(System.out::println);
    }

    @Test
    public void testInsert() {
        Weather one = new Weather();
        one.setTs(new Timestamp(1605024000000l));
        one.setTemperature(random.nextFloat() * 50);
        one.setHumidity(random.nextInt(100));
        one.setLocation("望京");
        int affectRows = mapper.insertOne(one);
        Assert.assertEquals(1, affectRows);
    }

    @Test
    public void testSelectOne() {
        QueryWrapper<Weather> wrapper = new QueryWrapper<>();
        wrapper.eq("location", "望京");
        Weather one = mapper.selectOne(wrapper);
        System.out.println(one);
        Assert.assertEquals(12.22f, one.getTemperature(), 0.00f);
        Assert.assertEquals("望京", one.getLocation());
    }

    // @Test
    // public void testSelectByMap() {
    //     Map<String, Object> map = new HashMap<>();
    //     map.put("location", "beijing");
    //     List<Weather> weathers = mapper.selectByMap(map);
    //     Assert.assertEquals(1, weathers.size());
    // }

    @Test
    public void testSelectObjs() {
        List<Object> ts = mapper.selectObjs(null);
        System.out.println(ts);
    }

    @Test
    public void testSelectCount() {
        long count = mapper.selectCount(null);
//        Assert.assertEquals(5, count);
        System.out.println(count);
    }

    @Test
    public void testSelectPage() {
        IPage page = new Page(1, 2);
        IPage<Weather> weatherIPage = mapper.selectPage(page, null);
        System.out.println("total : " + weatherIPage.getTotal());
        System.out.println("pages : " + weatherIPage.getPages());
        for (Weather weather : weatherIPage.getRecords()) {
            System.out.println(weather);
        }
    }

}