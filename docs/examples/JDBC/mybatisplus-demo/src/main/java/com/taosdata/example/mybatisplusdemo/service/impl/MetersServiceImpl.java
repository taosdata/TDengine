package com.taosdata.example.mybatisplusdemo.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.taosdata.example.mybatisplusdemo.domain.Meters;
import com.taosdata.example.mybatisplusdemo.mapper.MetersMapper;
import com.taosdata.example.mybatisplusdemo.service.MetersService;
import org.springframework.stereotype.Service;

/**
 * Service implementation for Meters entity.
 * Extends ServiceImpl to get MyBatis-Plus batch operations like saveBatch().
 */
@Service
public class MetersServiceImpl extends ServiceImpl<MetersMapper, Meters> implements MetersService {
}
