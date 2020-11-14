package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
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
        List<Weather> weatherList = mapper.selectList(null);
//        Assert.assertEquals(5, weatherList.size());
        weatherList.forEach(System.out::println);
    }

    @Test
    public void testInsert() {
        Weather weather = new Weather();
        weather.setTs(new Timestamp(System.currentTimeMillis()));
        weather.setTemperature(random.nextFloat() * 50);
        weather.setHumidity(random.nextInt(100));
        weather.setLocation("望京");
        int affectRows = mapper.insert(weather);
        Assert.assertEquals(1, affectRows);
    }

    @Test
    public void testDelete() {
        mapper.delete(new QueryWrapper<Weather>().eq("location", "望京"));
    }

    @Test
    public void testDeleteByMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("location", "望京");
        int affectRows = mapper.deleteByMap(map);
//        Assert.assertEquals(0, affectRows);
    }

    @Test
    public void testSelectOne() {
        QueryWrapper<Weather> wrapper = new QueryWrapper<>();
        wrapper.eq("location", "beijing");
        Weather weather = mapper.selectOne(wrapper);
        System.out.println(weather);
        Assert.assertEquals(12.22f, weather.getTemperature(), 0.00f);
        Assert.assertEquals(45, weather.getHumidity());
        Assert.assertEquals("beijing", weather.getLocation());
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
//        Assert.assertEquals(5, ts.size());
    }

    @Test
    public void testSelectCount() {
        int count = mapper.selectCount(null);
        Assert.assertEquals(5, count);
    }


}