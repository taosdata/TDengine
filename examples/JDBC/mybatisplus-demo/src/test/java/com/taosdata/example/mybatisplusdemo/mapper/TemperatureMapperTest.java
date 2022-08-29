package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.taosdata.example.mybatisplusdemo.domain.Temperature;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
public class TemperatureMapperTest {

    private static Random random = new Random(System.currentTimeMillis());
    private static String[] locations = {"北京", "上海", "深圳", "广州", "杭州"};

    @Before
    public void before() {
        mapper.dropSuperTable();
        // create table temperature
        mapper.createSuperTable();
        // create table t_X using temperature
        for (int i = 0; i < 10; i++) {
            mapper.createTable("t" + i, locations[random.nextInt(locations.length)], i);
        }
        // insert into table
        int affectRows = 0;
        // insert 10 tables
        for (int i = 0; i < 10; i++) {
            // each table insert 5 rows
            for (int j = 0; j < 5; j++) {
                Temperature one = new Temperature();
                one.setTs(new Timestamp(1605024000000l));
                one.setTemperature(random.nextFloat() * 50);
                one.setLocation("望京");
                one.setTbIndex(i);
                affectRows += mapper.insertOne(one);
            }
        }
        Assert.assertEquals(50, affectRows);
    }

    @After
    public void after() {
        mapper.dropSuperTable();
    }

    @Autowired
    private TemperatureMapper mapper;

    /***
     * test SelectList
     * **/
    @Test
    public void testSelectList() {
        List<Temperature> temperatureList = mapper.selectList(null);
        temperatureList.forEach(System.out::println);
    }

    /***
     * test InsertOne which is a custom metheod
     * ***/
    @Test
    public void testInsert() {
        Temperature one = new Temperature();
        one.setTs(new Timestamp(1605024000000l));
        one.setTemperature(random.nextFloat() * 50);
        one.setLocation("望京");
        int affectRows = mapper.insertOne(one);
        Assert.assertEquals(1, affectRows);
    }

    /***
     * test  select By map
     * ***/
    @Test
    public void testSelectByMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("location", "北京");
        List<Temperature> temperatures = mapper.selectByMap(map);
        Assert.assertTrue(temperatures.size() > 1);
    }

    /***
     * test selectObjs
     * **/
    @Test
    public void testSelectObjs() {
        List<Object> ts = mapper.selectObjs(null);
        System.out.println(ts);
    }

    /**
     * test selectC ount
     * **/
    @Test
    public void testSelectCount() {
        int count = mapper.selectCount(null);
        Assert.assertEquals(10, count);
    }

    /****
     * 分页
     */
    @Test
    public void testSelectPage() {
        IPage page = new Page(1, 2);
        IPage<Temperature> temperatureIPage = mapper.selectPage(page, null);
        System.out.println("total : " + temperatureIPage.getTotal());
        System.out.println("pages : " + temperatureIPage.getPages());
        for (Temperature temperature : temperatureIPage.getRecords()) {
            System.out.println(temperature);
        }
    }

}