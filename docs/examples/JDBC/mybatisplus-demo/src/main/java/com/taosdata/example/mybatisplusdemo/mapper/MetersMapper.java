package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.taosdata.example.mybatisplusdemo.domain.Meters;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.executor.BatchResult;

import java.util.List;

public interface MetersMapper extends BaseMapper<Meters> {

    @Update("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
    int createTable();

    @Insert("insert into meters (tbname, ts, groupid, location, current, voltage, phase) values(#{tbname}, #{ts}, #{groupid}, #{location}, #{current}, #{voltage}, #{phase})")
    int insertOne(Meters one);
    @Update("drop stable if exists meters")
    void dropTable();

    @Select("select count(*) from meters where tbname = '${tbname}'")
    int countBySql(String tbname);
}
