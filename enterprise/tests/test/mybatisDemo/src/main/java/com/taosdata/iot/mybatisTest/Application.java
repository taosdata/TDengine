package com.taosdata.iot.mybatisTest;

import com.taosdata.iot.mybatisTest.entities.Record;
import com.taosdata.iot.mybatisTest.mappers.RecordMapper;
import com.taosdata.iot.mybatisTest.utils.MybatisUtils;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * This class is the entrance of the mybatisDemo project.
 * All demo cases are currently placed here.
 * To run the cases, please first run the schema_setup.sql script to create necessary databases, tables
 * and metrics, and then run the main method.
 *
 * @author Jiangyi Hou
 * @since 18-11-9
 */
public class Application {

    public static void main(String[] args) {

        Application app = new Application();

        System.out.println("TDengine Mybatis Support Demo");
        app.getRecordIntoMap();
        app.getRecordByDeviceIdAndTsTest();
        app.getAllRecordsTest();
        app.getRecordsByC1C3Test();
        app.getRecordsByTagsTest();
        app.getRecordByTag1Test();
        app.getRecordsByIdTest();
        app.insertRecordByIdTest();
        app.insertRecordBatchTest();
        app.mybatisBatchInsertTest();
        app.createDeviceTest();
    }

    public void getRecordIntoMap() {
        System.out.println("\n======getRecordIntoMap======");
        SqlSession sqlSession = MybatisUtils.openSession();
        RecordMapper recordMapper = sqlSession.getMapper(RecordMapper.class);
        List<Map<String, String>> res = recordMapper.getRecordIntoMap();
        System.out.println(res.size());
        System.out.println(res.iterator().next().toString());
    }

    public void getRecordByDeviceIdAndTsTest() {
        System.out.println("\n======getRecordByDeviceIdAndTsTest======");
        SqlSession sqlSession = MybatisUtils.openSession();
        RecordMapper recordMapper = sqlSession.getMapper(RecordMapper.class);
        Timestamp ts = new Timestamp(1537146000000l); // 2018/9/17 09:00:00
        int deviceId = 1;
        Record record = recordMapper.getRecordByDeviceIdAndTs(deviceId, ts);
        System.out.println(record.toString());
    }

    public void getAllRecordsTest() {

        System.out.println("\n======getAllRecordsTest======");
        SqlSession sqlSession = MybatisUtils.openSession();
        RecordMapper recordMapper = sqlSession.getMapper(RecordMapper.class);
        List<Record> records = recordMapper.getAllRecords();
        records.forEach(System.out::println);
        sqlSession.close();

    }

    public void getRecordsByC1C3Test() {
        System.out.println("\n======getRecordsByC1C3Test======");
        SqlSession sqlSession = MybatisUtils.openSession();
        RecordMapper recordMapper = sqlSession.getMapper(RecordMapper.class);
        int c1 = 1;
        int c3 = 1;
        List<Record> records = recordMapper.getRecordsByC1C3(c1, c3);
        records.forEach(System.out::println);
        sqlSession.close();
    }

    public void getRecordsByTagsTest() {
        System.out.println("\n======getRecordsByTagsTest======");
        SqlSession sqlSession = MybatisUtils.openSession();
        RecordMapper recordMapper = sqlSession.getMapper(RecordMapper.class);
        int deviceId = 1;
        String t2 = "beijing";
        List<Record> records = recordMapper.getRecordsByTags(deviceId, t2);
        records.forEach(System.out::println);
        sqlSession.close();
    }

    public void getRecordByTag1Test() {
        System.out.println("\n======getRecordByTag1Test======");
        SqlSession sqlSession = MybatisUtils.openSession();
        RecordMapper recordMapper = sqlSession.getMapper(RecordMapper.class);
        int deviceId = 1;
        List<Record> records = recordMapper.getRecordsByTag1(deviceId);
        records.forEach(System.out::println);
        sqlSession.close();
    }

    public void getRecordsByIdTest() {
        System.out.println("\n======getRecordsByIdTest======");
        SqlSession sqlSession = MybatisUtils.openSession();
        RecordMapper recordMapper = sqlSession.getMapper(RecordMapper.class);
        int id = 1;
        List<Record> records = recordMapper.getRecordsByDeviceId(id);
        records.forEach(System.out::println);
        sqlSession.close();
    }

    public void insertRecordByIdTest() {
        System.out.println("\n======insertRecordByIdTest======");
        SqlSession sqlSession = MybatisUtils.openSession();
        RecordMapper recordMapper = sqlSession.getMapper(RecordMapper.class);
        int deviceId = 1;
        Record record = new Record();
        record.setTs(new Timestamp(System.currentTimeMillis()));
        record.setC1(1);
        record.setC2("insrt");
        record.setC3((short)3);
        record.setC4(4l);
        record.setC5("c5");
        record.setC6(true);
        record.setC7((byte)7);
        record.setC8(8f);
        record.setC9(9d);
        record.setDeviceId(deviceId);
        int res = recordMapper.insertRecordByDeviceId(record);
        System.out.printf("res = %d", res);
        sqlSession.close();
    }

    public void createDeviceTest() {
        System.out.println("\n======createDeviceTest======");
        SqlSession sqlSession = MybatisUtils.openSession();
        RecordMapper recordMapper = sqlSession.getMapper(RecordMapper.class);
        Record record = new Record();
        int deviceId = 5;
        String t2 = "beijing";
        record.setDeviceId(deviceId);
        record.setT2(t2);
        int res = recordMapper.addDevice(record);
        System.out.printf("res = %d", res);
        sqlSession.close();
    }

    public void insertRecordBatchTest() {
        System.out.println("\n======insertRecordBatchTest======");
        SqlSession sqlSession = MybatisUtils.openSession();
        RecordMapper recordMapper = sqlSession.getMapper(RecordMapper.class);

        // set up record list
        List<Record> records = new ArrayList<>();
        long ts0 = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            Record record = new Record(new Timestamp(ts0 + i * 1000), i, "batch", (short)(i%5), (long)(i*10),
                    "dev", true, (byte)(i%27), new Float(i), new Double(i), (i%3 + 1));
            records.add(record);
        }
        int res = recordMapper.insertRecordBatch(records);
        System.out.printf("res = %d\n", res);
        sqlSession.close();
    }

    public void mybatisBatchInsertTest() {
        System.out.println("\n======mybatisBatchInsertTest======");
        SqlSession sqlSession = MybatisUtils.openSession(ExecutorType.BATCH);
        RecordMapper recordMapper = sqlSession.getMapper(RecordMapper.class);
        long ts0 = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            Record record = new Record(new Timestamp(ts0 + i * 1000), i, "batch", (short)(i%5), (long)(i*10),
                    "dev" + i, true, (byte)(i%27), new Float(i), new Double(i), (i%3 + 1));
            recordMapper.insertRecordByDeviceId(record);
        }
        sqlSession.commit();
        sqlSession.close();
    }
}
