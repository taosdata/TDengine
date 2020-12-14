package com.taosdata.taosdemo.controller;

import com.taosdata.taosdemo.service.DatabaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping
public class DatabaseController {

    @Autowired
    private DatabaseService databaseService;

    /**
     * create database
     ***/
    @PostMapping
    public int create(@RequestBody Map<String, String> map) {
        return databaseService.createDatabase(map);
    }


    /**
     * drop database
     **/
    @DeleteMapping("/{dbname}")
    public int delete(@PathVariable("dbname") String dbname) {
        return databaseService.dropDatabase(dbname);
    }

    /**
     * use database
     **/
    @GetMapping("/{dbname}")
    public int use(@PathVariable("dbname") String dbname) {
        return databaseService.useDatabase(dbname);
    }
}
