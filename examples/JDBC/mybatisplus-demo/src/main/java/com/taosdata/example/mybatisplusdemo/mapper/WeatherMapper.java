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
}
