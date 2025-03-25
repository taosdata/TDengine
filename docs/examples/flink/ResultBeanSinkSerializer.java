package com.taosdata.flink.entity;

import com.taosdata.flink.sink.entity.DataType;
import com.taosdata.flink.sink.entity.SinkMetaInfo;
import com.taosdata.flink.sink.entity.TDengineSinkRecord;
import com.taosdata.flink.sink.serializer.TDengineSinkRecordSerializer;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.taosdata.flink.sink.entity.DataType.DATA_TYPE_BINARY;

public class ResultBeanSinkSerializer implements TDengineSinkRecordSerializer<ResultBean> {

    @Override
    public List<TDengineSinkRecord> serialize(ResultBean record, List<SinkMetaInfo> sinkMetaInfos) throws IOException {
        List<Object> columnParams = new ArrayList<>(sinkMetaInfos.size());
        columnParams.add(record.getTs());
        columnParams.add(record.getCurrent());
        columnParams.add(record.getVoltage());
        columnParams.add(record.getPhase());
        columnParams.add(record.getLocation());
        columnParams.add(record.getGroupid());
        columnParams.add(record.getTbname());
        TDengineSinkRecord sinkRecord = new TDengineSinkRecord(columnParams);
        List<TDengineSinkRecord> sinkRecords = new ArrayList<>(1);
        sinkRecords.add(sinkRecord);
        return sinkRecords;

    }
}
