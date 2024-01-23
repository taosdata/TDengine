package com.taos.example.controller;

import com.taos.example.dao.Meter;
import com.taos.example.service.MeterService;
import java.util.List;
import java.util.Random;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/meter")
public class MeterController {

  @Autowired
  private MeterService meterService;

  @GetMapping("/list")
  public List<Meter> list() {
    return meterService.list();
  }

  @RequestMapping("/{tableName}/create")
  public String createTable(@PathVariable String tableName) {
    Meter meter = new Meter();
    meter.setGroupId(1);
    meter.setLocation("Los Angeles");
    try {
      meterService.create(meter, tableName);
      return "create sub-table " + tableName + " success";
    } catch (Exception e) {
      e.printStackTrace();
      return "fail";
    }
  }

  @RequestMapping("/{tableName}/insert")
  public String insert(@PathVariable String tableName) {
    // mock data
    Random random = new Random();
    Meter meter = new Meter();
    meter.setTs(new java.sql.Timestamp(System.currentTimeMillis()));
    meter.setCurrent(random.nextFloat());
    meter.setVoltage(random.nextInt());
    meter.setPhase(random.nextFloat());
    try {
      meterService.insert(meter, tableName);
      return "insert success";
    } catch (Exception e) {
      e.printStackTrace();
      return "insert failed!";
    }

  }

  @GetMapping("/{tableName}/lastRow")
  public Meter lastRow(@PathVariable String tableName) {
    return meterService.lastRow(tableName);
  }

}
