package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.taosdata.example.mybatisplusdemo.domain.Weather;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Update;

public interface WeatherMapper extends BaseMapper<Weather> {

    @Update("CREATE TABLE if not exists weather(ts timestamp, temperature float, humidity int, location nchar(100))")
    int createTable();

    @Insert("insert into weather (ts, temperature, humidity, location) values(#{ts}, #{temperature}, #{humidity}, #{location})")
    int insertOne(Weather one);

    @Update("drop table if exists weather")
    void dropTable();

    @Insert("INSERT INTO meters(tbname, location, groupId, ts, current, voltage,phase) \n" +
            "    values('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:34.630', 10.2, 219, 0.32) \n" +
            "    ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:35.779', 10.15, 217, 0.33)\n" +
            "    ('d31002', NULL, 2, '2021-07-13 14:06:34.255', 10.15, 217, 0.33)")
    int test();
}
