package com.taosdata.iot.mybatisTest.mappers;

import com.taosdata.iot.mybatisTest.entities.Record;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.ResultType;
import org.apache.ibatis.annotations.Select;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * @author Jiangyi Hou
 * @since 18-11-9
 */
public interface RecordMapper {

    /**
     * Select a record from metric devices using given deviceId and ts
     * @param deviceId device Id
     * @param ts timestamp
     * @return the record at ts from device deviceId
     */
    Record getRecordByDeviceIdAndTs(@Param("deviceId") int deviceId, @Param("ts") Timestamp ts);

    /**
     * Select all records in metric <strong>devices</strong>
     * @return a list of records in devices
     */
    List<Record> getAllRecords();

    /**
     * Select records from metric whose c1 and c3 values match the given values
     * @param c1 column1
     * @param c3 column3
     * @return a list of <code>Record</code>
     */
    List<Record> getRecordsByC1C3(@Param("c1") int c1, @Param("c3") int c3);

    /**
     * Select records from metric devices based on tags filtering
     * @param deviceId tag1
     * @param t2 tag2
     * @return a list of <code>Record</code>
     */
    List<Record> getRecordsByTags(@Param("deviceId") int deviceId, @Param("t2") String t2);

    /**
     * Select records from metric based on devideId filtering
     * @param deviceId tag1
     * @return a list of <code>Record</code>
     */
    List<Record> getRecordsByTag1(@Param("deviceId") int deviceId);

    /**
     * Select records in a specific table
     * @param deviceId tableId
     * @return a list of <code>Record</code>
     */
    List<Record> getRecordsByDeviceId(@Param("deviceId") int deviceId);

    /**
     * Insert a new record into specific table (table must exist and be named as "device"+${deviceId})
     * @param record
     * @return number of rows affected
     */
    int insertRecordByDeviceId(Record record);

    /**
     * Create a new table with name like "device"+${deviceId}
     * @param record
     * @return
     */
    int addDevice(Record record);

    /**
     * Batch insert
     * @param records a collection of records to be inserted
     * @return number of rows affected
     */
    int insertRecordBatch(List<Record> records);

    @Select("select * from device1 limit 1")
    @ResultType(Map.class)
    List<Map<String, String>> getRecordIntoMap();

}
