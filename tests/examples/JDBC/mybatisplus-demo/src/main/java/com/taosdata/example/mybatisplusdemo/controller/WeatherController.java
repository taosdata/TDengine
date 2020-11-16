package com.taosdata.example.mybatisplusdemo.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.taosdata.example.mybatisplusdemo.domain.Weather;
import com.taosdata.example.mybatisplusdemo.mapper.WeatherMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/weathers")
public class WeatherController {

    @Autowired
    private WeatherMapper mapper;

    @GetMapping
    public List<Weather> findAll() {
        Integer total = mapper.selectCount(null);
        final int pageSize = 3;
        IPage<Weather> page = new Page<>(1, pageSize);

        IPage<Weather> currentPage = mapper.selectPage(page, null);

        System.out.println("total : " + currentPage.getTotal());
        System.out.println("pages : " + currentPage.getPages());

//        System.out.println("countId : " + currentPage.getCountId());
//        System.out.println("maxLimit: " + currentPage.getMaxLimit());

        return currentPage.getRecords();
    }


}
