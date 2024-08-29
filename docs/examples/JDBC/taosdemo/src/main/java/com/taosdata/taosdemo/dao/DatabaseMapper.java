package com.taosdata.taosdemo.dao;

import java.util.Map;

public interface DatabaseMapper {

    // create database if not exists XXX
    void createDatabase(String dbname);

    // drop database if exists XXX
    void dropDatabase(String dbname);

    // create database if not exists XXX keep XX days XX replica XX
    void createDatabaseWithParameters(Map<String, String> map);

    // use XXX
    void useDatabase(String dbname);

    //TODO: alter database

    //TODO: show database

}
