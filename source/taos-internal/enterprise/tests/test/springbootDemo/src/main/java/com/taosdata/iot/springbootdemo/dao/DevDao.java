package com.taosdata.iot.springbootdemo.dao;

import com.taosdata.iot.springbootdemo.entity.Dev;

/**
 * @author Jiangyi Hou
 * @since 18-12-12
 */
//@Repository
public interface DevDao {

    public Dev findDevByTs(String ts);
    public void displayAllDevs();

}
