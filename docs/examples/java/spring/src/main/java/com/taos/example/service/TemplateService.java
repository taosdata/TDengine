package com.taos.example.service;

import java.util.HashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class TemplateService {

  private final JdbcTemplate jdbcTemplate;

  @Autowired
  public TemplateService(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public void someMethod() {
    HashMap<String, Object> map = new HashMap<>();
    jdbcTemplate.query(
        "SELECT * FROM meters limit 1",
        (rs, rowNum) -> {
          map.put("current", rs.getFloat("current"));
          return map;
        }
    );
    System.out.println(map.get("current"));
  }
}
