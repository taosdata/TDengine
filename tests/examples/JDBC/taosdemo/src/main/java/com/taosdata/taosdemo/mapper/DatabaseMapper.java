package com.taosdata.taosdemo.mapper;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public interface DatabaseMapper {

    // create database if not exists XXX
    int createDatabase(@Param("database") String dbname);

    // drop database if exists XXX
    int dropDatabase(@Param("database") String dbname);

    // create database if not exists XXX keep XX days XX replica XX
    int createDatabaseWithParameters(Map<String, String> map);

    // use XXX
    int useDatabase(@Param("database") String dbname);

    //TODO: alter database

    //TODO: show database

}
