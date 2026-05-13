package com.taosdata.example.jdbcTemplate.dao;

import com.taosdata.example.jdbcTemplate.domain.Weather;
import com.taosdata.example.jdbcTemplate.dao.WeatherDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

@Repository
public class WeatherDaoImpl implements WeatherDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public int add(Weather weather) {
        return jdbcTemplate.update(
                "insert into test.weather(ts, temperature, humidity) VALUES(?,?,?)",
                weather.getTs(), weather.getTemperature(), weather.getHumidity()
        );
    }

    @Override
    public int[] batchInsert(List<Weather> weatherList) {
        return jdbcTemplate.batchUpdate("insert into test.weather(ts, temperature, humidity) values( ?, ?, ?)", new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setTimestamp(1, weatherList.get(i).getTs());
                ps.setFloat(2, weatherList.get(i).getTemperature());
                ps.setInt(3, weatherList.get(i).getHumidity());
            }

            @Override
            public int getBatchSize() {
                return weatherList.size();
            }
        });
    }

    @Override
    public List<Weather> queryForList(int limit, int offset) {
        return jdbcTemplate.query("select * from test.weather limit ? offset ?", (rs, rowNum) -> {
            Timestamp ts = rs.getTimestamp("ts");
            float temperature = rs.getFloat("temperature");
            int humidity = rs.getInt("humidity");
            return new Weather(ts, temperature, humidity);
        }, limit, offset);
    }

    @Override
    public int count() {
        return jdbcTemplate.queryForObject("select count(*) from test.weather", Integer.class);
    }
}
