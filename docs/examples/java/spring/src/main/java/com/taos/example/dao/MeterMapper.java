package com.taos.example.dao;

import java.util.List;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface MeterMapper {

  @Select("select * from meters limit 10")
  List<Meter> find();

  int create(@Param("meter")Meter meter, @Param("tableName")String tableName);

  int save(@Param("meter")Meter meter, @Param("tableName")String tableName);

  Meter lastRow(@Param("tableName")String tableName);
}
