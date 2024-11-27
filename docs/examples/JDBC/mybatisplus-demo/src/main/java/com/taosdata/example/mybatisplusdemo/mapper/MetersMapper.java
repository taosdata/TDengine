package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.taosdata.example.mybatisplusdemo.domain.Meters;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

import java.util.List;

public interface MetersMapper extends BaseMapper<Meters> {

    @Update("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))")
    int createTable();

    @Insert("insert into meters (tbname, ts, groupid, location, current, voltage, phase) values(#{tbname}, #{ts}, #{groupid}, #{location}, #{current}, #{voltage}, #{phase})")
    int insertOne(Meters one);

    @Insert({
            "<script>",
            "insert into meters (tbname, ts, groupid, location, current, voltage, phase) values ",
            "<foreach collection='list' item='item' index='index' separator=','>",
            "(#{item.tbname}, #{item.ts}, #{item.groupid}, #{item.location}, #{item.current}, #{item.voltage}, #{item.phase})",
            "</foreach>",
            "</script>"
    })
    int insertBatch(@Param("list") List<Meters> metersList);

    @Update("drop stable if exists meters")
    void dropTable();
}
