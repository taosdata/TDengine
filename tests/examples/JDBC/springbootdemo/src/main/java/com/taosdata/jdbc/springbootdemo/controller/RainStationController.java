package com.taosdata.jdbc.springbootdemo.controller;


import com.taosdata.jdbc.springbootdemo.domain.Rainfall;
import com.taosdata.jdbc.springbootdemo.service.RainStationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/rainstation")
public class RainStationController {

    @Autowired
    private RainStationService service;

    @GetMapping("/init")
    public boolean init() {
        service.init();
        service.createTable();
        return true;
    }

    @PostMapping("/insert")
    public int insert(@RequestBody Rainfall rainfall){
        return service.insert(rainfall);
    }

}
