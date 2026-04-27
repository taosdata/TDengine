package com.taosdata.example.springbootdemo.dao;

import com.taosdata.example.springbootdemo.domain.Weather;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class WeatherMapperTest {

    @Autowired
    private WeatherMapper weatherMapper;

    @Before
    public void setUp() {
        weatherMapper.dropDB();
        weatherMapper.createDB();
        weatherMapper.createSuperTable();
    }

    @Test
    public void testCreateSuperTable() {
        weatherMapper.dropDB();
        weatherMapper.createDB();
        weatherMapper.createSuperTable();
        // If no exception is thrown, the test passes
        Assert.assertTrue("Super table should be created successfully", true);
    }

    @Test
    public void testCreateTable() {
        Weather weather = new Weather();
        weather.setLocation("Beijing");
        weather.setGroupId(1);
        weatherMapper.createTable(weather);
        // If no exception is thrown, the test passes
        Assert.assertTrue("Table should be created successfully", true);
    }

    @Test
    public void testInsert() {
        Weather weather = new Weather();
        weather.setTs(new Timestamp(System.currentTimeMillis()));
        weather.setTemperature(25.5f);
        weather.setHumidity(60.0f);
        weather.setLocation("Shanghai");
        weather.setGroupId(1);
        weather.setNote("Test note");
        weather.setBytes("Test bytes".getBytes());

        weatherMapper.createTable(weather);
        int result = weatherMapper.insert(weather);

        Assert.assertEquals("Insert should return 1", 1, result);
    }

    @Test
    public void testSelect() {
        // Prepare test data
        Weather weather = new Weather();
        weather.setTs(new Timestamp(System.currentTimeMillis()));
        weather.setTemperature(25.5f);
        weather.setHumidity(60.0f);
        weather.setLocation("Guangzhou");
        weather.setGroupId(2);
        weather.setNote("Test note");
        weather.setBytes("Test bytes".getBytes());

        weatherMapper.createTable(weather);
        weatherMapper.insert(weather);

        // Query data
        List<Weather> list = weatherMapper.select(10L, 0L);
        Assert.assertTrue("Should return at least 1 record", list.size() >= 1);
    }

    @Test
    public void testCount() {
        weatherMapper.dropDB();
        weatherMapper.createDB();
        weatherMapper.createSuperTable();

        int count = weatherMapper.count();
        Assert.assertEquals("Initial count should be 0", 0, count);

        // Insert a record
        Weather weather = new Weather();
        weather.setTs(new Timestamp(System.currentTimeMillis()));
        weather.setTemperature(25.5f);
        weather.setHumidity(60.0f);
        weather.setLocation("Shenzhen");
        weather.setGroupId(1);
        weather.setNote("Test note");
        weather.setBytes("Test bytes".getBytes());

        weatherMapper.createTable(weather);
        weatherMapper.insert(weather);

        count = weatherMapper.count();
        Assert.assertEquals("Count should be 1 after insert", 1, count);
    }

    @Test
    public void testLastOne() {
        Weather weather = new Weather();
        weather.setTs(new Timestamp(System.currentTimeMillis()));
        weather.setTemperature(25.5f);
        weather.setHumidity(60.0f);
        weather.setLocation("Tianjin");
        weather.setGroupId(1);
        weather.setNote("Last note");
        weather.setBytes("Last bytes".getBytes());

        weatherMapper.createTable(weather);
        weatherMapper.insert(weather);

        Map<String, Object> result = weatherMapper.lastOne();
        Assert.assertNotNull("Result should not be null", result);
        Assert.assertTrue("Result should contain ts", result.containsKey("ts"));
        Assert.assertTrue("Result should contain temperature", result.containsKey("temperature"));
    }

    @Test
    public void testGetSubTables() {
        Weather weather = new Weather();
        weather.setTs(new Timestamp(System.currentTimeMillis()));
        weather.setTemperature(25.5f);
        weather.setHumidity(60.0f);
        weather.setLocation("Beijing");
        weather.setGroupId(1);
        weather.setNote("Test note");
        weather.setBytes("Test bytes".getBytes());

        weatherMapper.createTable(weather);
        weatherMapper.insert(weather);

        List<String> subTables = weatherMapper.getSubTables();
        Assert.assertTrue("Should return at least 1 subtable", subTables.size() >= 1);
    }

    @Test
    public void testAvg() {
        Weather weather = new Weather();
        weather.setTs(new Timestamp(System.currentTimeMillis()));
        weather.setTemperature(25.5f);
        weather.setHumidity(60.0f);
        weather.setLocation("Shanghai");
        weather.setGroupId(1);
        weather.setNote("Test note");
        weather.setBytes("Test bytes".getBytes());

        weatherMapper.createTable(weather);
        weatherMapper.insert(weather);

        List<Weather> avgResult = weatherMapper.avg();
        Assert.assertNotNull("Avg result should not be null", avgResult);
    }
}
