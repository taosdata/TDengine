package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.taosdata.example.mybatisplusdemo.domain.Temperature;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

public interface TemperatureMapper extends BaseMapper<Temperature> {

    @Update("CREATE TABLE if not exists temperature(ts timestamp, temperature float) tags(location nchar(64), tbIndex int)")
    int createSuperTable();

    @Update("create table #{tbName} using temperature tags( #{location}, #{tbIndex})")
    int createTable(@Param("tbName") String tbName, @Param("location") String location, @Param("tbIndex") int tbIndex);

    @Update("drop table if exists temperature")
    void dropSuperTable();

    @Insert("insert into t${tbIndex}(ts, temperature) values(ts, temperature)")
    int insertOne(Temperature one);
}
