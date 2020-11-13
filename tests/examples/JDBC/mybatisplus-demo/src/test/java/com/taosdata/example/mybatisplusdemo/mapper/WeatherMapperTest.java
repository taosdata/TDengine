package com.taosdata.example.mybatisplusdemo.mapper;

import com.taosdata.example.mybatisplusdemo.domain.Weather;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class WeatherMapperTest {

    @Autowired
    private WeatherMapper mapper;

    @Test
    public void testSelect() {
        System.out.println(("----- selectAll method test ------"));
        List<Weather> weatherList = mapper.selectList(null);
        Assert.assertEquals(5, weatherList.size());
        weatherList.forEach(System.out::println);
    }


}