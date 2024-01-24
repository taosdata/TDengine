package com.taos.example.service;

import com.taos.example.dao.Meter;
import com.taos.example.dao.MeterMapper;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MeterService {

  @Autowired
  private MeterMapper meterMapper;

  public List<Meter> list() {
    return meterMapper.find();
  }

  public void create(Meter meter, String tableName) {
    meterMapper.create(meter, tableName);
  }

  public int insert(Meter meter, String tableName) {
    return meterMapper.save(meter, tableName);
  }

  public Meter lastRow(String tableName) {
    return meterMapper.lastRow(tableName);
  }
}
