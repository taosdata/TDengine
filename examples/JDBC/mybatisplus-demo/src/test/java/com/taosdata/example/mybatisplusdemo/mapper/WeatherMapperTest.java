package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.taosdata.example.mybatisplusdemo.domain.Weather;
import org.junit.Assert;
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
public class WeatherMapperTest {

    private static Random random = new Random(System.currentTimeMillis());

    @Autowired
    private WeatherMapper mapper;

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
        int affectRows = mapper.insert(one);
        Assert.assertEquals(1, affectRows);
    }

    @Test
    public void testSelectOne() {
        QueryWrapper<Weather> wrapper = new QueryWrapper<>();
        wrapper.eq("location", "beijing");
        Weather one = mapper.selectOne(wrapper);
        System.out.println(one);
        Assert.assertEquals(12.22f, one.getTemperature(), 0.00f);
        Assert.assertEquals("beijing", one.getLocation());
    }

    @Test
    public void testSelectByMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("location", "beijing");
        List<Weather> weathers = mapper.selectByMap(map);
        Assert.assertEquals(1, weathers.size());
    }

    @Test
    public void testSelectObjs() {
        List<Object> ts = mapper.selectObjs(null);
        System.out.println(ts);
    }

    @Test
    public void testSelectCount() {
        int count = mapper.selectCount(null);
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