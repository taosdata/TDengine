package com.taosdata.example.springbootdemo.service;

import com.taosdata.example.springbootdemo.domain.Weather;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class WeatherServiceTest {

    @Autowired
    private WeatherService weatherService;

    @Test
    public void testInit() {
        int count = weatherService.init();
        Assert.assertEquals("Init should insert 20 records", 20, count);
    }

    @Test
    public void testQuery() {
        // First initialize with some data
        weatherService.init();

        // Query data
        List<Weather> list = weatherService.query(10L, 0L);
        Assert.assertTrue("Should return at least 1 record", list.size() >= 1);
    }

    @Test
    public void testSave() {
        // First initialize the database
        weatherService.init();

        int countBefore = weatherService.count();

        // Save a new record
        int result = weatherService.save(22.5f, 55.0f);
        Assert.assertEquals("Insert should return 1", 1, result);

        int countAfter = weatherService.count();
        Assert.assertEquals("Count should increase by 1", countBefore + 1, countAfter);
    }

    @Test
    public void testCount() {
        weatherService.init();
        int count = weatherService.count();
        Assert.assertEquals("Count should be 20", 20, count);
    }

    @Test
    public void testGetSubTables() {
        weatherService.init();
        List<String> subTables = weatherService.getSubTables();
        Assert.assertTrue("Should return at least 1 subtable", subTables.size() >= 1);
    }

    @Test
    public void testAvg() {
        weatherService.init();
        List<Weather> avgResult = weatherService.avg();
        Assert.assertNotNull("Avg result should not be null", avgResult);
    }

    @Test
    public void testLastOne() {
        weatherService.init();
        Weather weather = weatherService.lastOne();
        Assert.assertNotNull("Last one should not be null", weather);
        Assert.assertNotNull("Ts should not be null", weather.getTs());
        Assert.assertNotNull("Temperature should not be null", weather.getTemperature());
        Assert.assertNotNull("Humidity should not be null", weather.getHumidity());
    }
}
