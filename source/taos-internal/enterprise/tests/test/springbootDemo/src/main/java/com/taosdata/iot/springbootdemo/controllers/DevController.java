package com.taosdata.iot.springbootdemo.controllers;

import com.taosdata.iot.springbootdemo.dao.DevDaoImpl;
import com.taosdata.iot.springbootdemo.entity.Dev;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Jiangyi Hou
 * @since 18-12-12
 */

@RestController
public class DevController {

    @Autowired
//    private DevDao devDao;
    private DevDaoImpl devDao;

    @RequestMapping(method = RequestMethod.GET, value = "/index")
    public String greeting(@RequestParam(value="name", defaultValue = "TDengine") String name) {
        return new String("Hello, "+ name);
    }

    @RequestMapping(method = RequestMethod.GET, value = "testQuery")
    public Dev testQuery(@RequestParam(value = "ts", defaultValue = "0") String ts) {
        if ("first".equalsIgnoreCase(ts)) {
            return devDao.findDevByTs("2018-12-13 13:54:38.910");
        }
        return devDao.findDevByTs("2018-12-13 13:54:35.712");

    }

    @RequestMapping(method = RequestMethod.GET, value = "displayAll")
    public void testQuery() {
        devDao.displayAllDevs();
    }
}
