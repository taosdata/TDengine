package com.taosdata.example.mybatisplusdemo.mapper;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.taosdata.example.mybatisplusdemo.domain.Meters;
import com.taosdata.example.mybatisplusdemo.domain.Weather;
import org.apache.ibatis.executor.BatchResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static java.sql.Statement.SUCCESS_NO_INFO;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class MetersMapperTest {

    private static Random random = new Random(System.currentTimeMillis());

    @Autowired
    private MetersMapper mapper;

    @Before
    public void createTable(){
        mapper.dropTable();
        mapper.createTable();
        Meters one = new Meters();
        one.setTbname("test_10001");
        one.setGroupid(10001);
        one.setCurrent(random.nextFloat());
        one.setPhase(random.nextFloat());
        one.setCurrent(12345);
        one.setTs(new Timestamp(1605024000000l));
        one.setLocation("望京".getBytes());
        mapper.insertOne(one);
    }

    @Test
    public void testSelectList() {
        List<Meters> meters = mapper.selectList(null);
        meters.forEach(System.out::println);
    }

    @Test
    public void testInsertBatch() {
        List<Meters> metersList = new LinkedList<>();
        for (int i  = 0; i < 100; i++){
            Meters one = new Meters();
            one.setTbname("tb_" + i);
            one.setGroupid(i);
            one.setCurrent(random.nextFloat());
            one.setPhase(random.nextFloat());
            one.setCurrent(random.nextInt());
            one.setTs(new Timestamp(1605024000000l + i));
            one.setLocation(("望京" + i).getBytes());
            metersList.add(one);

        }
        List<BatchResult> affectRowsList = mapper.insert(metersList, 10000);

        long totalAffectedRows = 0;
        for (BatchResult batchResult : affectRowsList) {
            int[] updateCounts = batchResult.getUpdateCounts();
            for (int status : updateCounts) {
                if (status == SUCCESS_NO_INFO) {
                    totalAffectedRows++;
                }
            }
        }

        Assert.assertEquals(100, totalAffectedRows);
    }

    @Test
    public void testSelectOne() {
        QueryWrapper<Meters> wrapper = new QueryWrapper<>();
        wrapper.eq("location", "望京".getBytes());
        Meters one = mapper.selectOne(wrapper);
        System.out.println(one);
        Assert.assertEquals(12345, one.getCurrent(), 0.00f);
        Assert.assertArrayEquals("望京".getBytes(), one.getLocation());
    }

    // @Test
    // public void testSelectByMap() {
    //     Map<String, Object> map = new HashMap<>();
    //     map.put("location", "beijing");
    //     List<Weather> weathers = mapper.selectByMap(map);
    //     Assert.assertEquals(1, weathers.size());
    // }

    @Test
    public void testSelectObjs() {
        List<Object> ts = mapper.selectObjs(null);
        System.out.println(ts);
    }

    @Test
    public void testSelectCount() {
        long count = mapper.selectCount(null);
//        Assert.assertEquals(5, count);
        System.out.println(count);
    }

    @Test
    public void testSelectPage() {
        IPage page = new Page(1, 2);
        IPage<Meters> metersIPage = mapper.selectPage(page, null);
        System.out.println("total : " + metersIPage.getTotal());
        System.out.println("pages : " + metersIPage.getPages());
        for (Meters meters : metersIPage.getRecords()) {
            System.out.println(meters);
        }
    }

    @Test
    public void testCountBySql() {
        int count = mapper.countBySql("test_10001");
        System.out.println(count);
        Assert.assertEquals(1, count);
    }
}