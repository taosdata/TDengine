package com.taosdata.utils.arrow;

import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * arrow工具类
 *
 * @author ZYP
 */
public class ArrowUtils {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * arrow数据结构信息
     */
    private Schema schema;

    /**
     * 构造时初始化schema信息
     *
     * @param influxdbMeasurementEntity
     * @throws Exception
     */
    public ArrowUtils(InfluxdbMeasurementEntity influxdbMeasurementEntity) throws Exception {
        // 根据Measurement获取arrow初始化信息
        ArrowInitDto arrowInitDto = getArrowInit(influxdbMeasurementEntity);
        // 封装meta信息
        Map<String, String> metaData = generateMeta(arrowInitDto);
        // 封装fields信息
        List<Field> fieldList = generateFieldList(arrowInitDto);
        // 生成schema信息
        this.schema = new Schema(fieldList, metaData);
        // 判断数据完整性
        if (arrowInitDto == null || metaData.size() == 0 || fieldList.size() == 0) {
            throw new Exception("数据异常，生成schema错误，Measurement=" + influxdbMeasurementEntity.toString());
        }
    }

    /**
     * 根据Measurement获取arrow初始化信息
     *
     * @param influxdbMeasurementEntity
     * @return
     */
    private ArrowInitDto getArrowInit(InfluxdbMeasurementEntity influxdbMeasurementEntity) {
        ArrowInitDto arrowInitDto = new ArrowInitDto();
        // name
        arrowInitDto.setName(influxdbMeasurementEntity.getBucket() + "_" + influxdbMeasurementEntity.getMeasurement());
        // columns
        List<ArrowInitDto.Column> columns = new ArrayList<>();
        columns.add(arrowInitDto.new Column("time", "timestamp"));
        influxdbMeasurementEntity.getFieldMap().forEach((field, type) -> {
            columns.add(arrowInitDto.new Column(field, type));
        });
        columns.add(arrowInitDto.new Column("__table_name__", "string"));
        arrowInitDto.setColumns(columns);
        // tags
        List<ArrowInitDto.Tag> tags = new ArrayList<>();
        influxdbMeasurementEntity.getTagSet().forEach(tag -> {
            tags.add(arrowInitDto.new Tag(tag, "string"));
        });
        arrowInitDto.setTags(tags);
        // 返回实体类
        return arrowInitDto;
    }

    /**
     * 生成meta信息
     *
     * @param arrowInitDto
     * @return
     */
    private static Map<String, String> generateMeta(ArrowInitDto arrowInitDto) {
        // TODO 目前不要响应，如果需要可以将none改为code/lush
        return new HashMap<String, String>() {{
            put("ack", "none");
            put("stream", "lush");
            put("version", "1.0");
            put("init", arrowInitDto.toString());
        }};
    }

    /**
     * 生成值域列表
     *
     * @param arrowInitDto
     * @return
     */
    private List<Field> generateFieldList(ArrowInitDto arrowInitDto) {
        // tag fields
        List<Field> tagFieldList = new ArrayList<>();
        for (ArrowInitDto.Tag tag : arrowInitDto.getTags()) {
            tagFieldList.add(new Field(tag.getName(), FieldType.nullable(getArrowType(tag.getType())), null));
        }
        // column fields
        List<Field> columnFieldList = new ArrayList<>();
        for (ArrowInitDto.Column column : arrowInitDto.getColumns()) {
            columnFieldList.add(new Field(column.getName(), FieldType.nullable(getArrowType(column.getType())), null));
        }
        // __type__
        Field typeField = new Field("__type__", FieldType.notNullable(new ArrowType.Int(8, false)), null);
        // __tables__
        List<Field> tableItemTagList = new ArrayList<>();
        tableItemTagList.add(new Field("__table_name__", FieldType.nullable(new ArrowType.Binary()), null));
        tableItemTagList.addAll(tagFieldList);
        Field tableItemField = new Field("item", FieldType.nullable(new ArrowType.Struct()), tableItemTagList);
        List<Field> tableFieldChildrenList = new ArrayList<>();
        tableFieldChildrenList.add(tableItemField);
        Field tableField = new Field("__tables__", FieldType.nullable(new ArrowType.List()), tableFieldChildrenList);
        // __attrs__
        List<Field> attrFieldChildrenList = new ArrayList<>();
        attrFieldChildrenList.add(new Field("__table_name__", FieldType.nullable(new ArrowType.Binary()), null));
        attrFieldChildrenList.addAll(tagFieldList);
        Field attrField = new Field("__attrs__", FieldType.nullable(new ArrowType.Struct()), attrFieldChildrenList);
        // __records__
        List<Field> recordFieldChildrenList = new ArrayList<>();
        recordFieldChildrenList.add(new Field("item", FieldType.nullable(new ArrowType.Struct()), columnFieldList));
        Field recordField = new Field("__records__", FieldType.nullable(new ArrowType.List()), recordFieldChildrenList);
        // field list
        return new ArrayList<Field>() {{
            add(typeField);
            add(tableField);
            add(attrField);
            add(recordField);
        }};
    }

    /**
     * 将实体类列表转换为apache-arrow的字节流
     *
     * @param influxdbBucketDataEntityList
     * @param first
     * @return
     * @throws IOException
     */
    public byte[] transform(List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList, boolean first) throws IOException {
        // 分配1G内存
        RootAllocator rootAllocator = new RootAllocator(1_000_000_000);
        // 创建arrow数据结构体
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(this.schema, rootAllocator);
        // 输出字节流，完整结构体的字节流
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            // 创建字典
            DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
            // 用于将数据写入Arrow格式的二进制流中，它接受一个VectorSchemaRoot对象作为输入，该对象包含要写入流中的Schema和矢量数据。在向这些矢量添加数据后，可以调用ArrowStreamWriter的writeBatch方法来刷新数据到输出流中
            ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, dictProvider, outputStream);
            // 开始写入
            writer.start();
            // 获取各值域
            UInt1Vector typeVector = (UInt1Vector) vectorSchemaRoot.getVector("__type__");
            ListVector tableVector = (ListVector) vectorSchemaRoot.getVector("__tables__");
            StructVector attrVector = (StructVector) vectorSchemaRoot.getVector("__attrs__");
            ListVector recordVector = (ListVector) vectorSchemaRoot.getVector("__records__");
            // 如果首次提交，需要写Tables数据与Insert数据，其后只写Insert数据
            if (first) {
                // 第一阶段提交type=2与tableVector数据
                typeVector.reset();
                tableVector.reset();
                attrVector.reset();
                recordVector.reset();
                /* __type__ */
                typeVector.setSafe(0, 2);
                // 设置tableVector写数据开始
                tableVector.startNewValue(0);
                // 遍历数据
                for (int i = 0; i < influxdbBucketDataEntityList.size(); i++) {
                    InfluxdbBucketDataEntity influxdbBucketDataEntity = influxdbBucketDataEntityList.get(i);
                    /* __tables__ */
                    StructVector tableDataVector = (StructVector) tableVector.getChildrenFromFields().get(0);
                    // 2023.04.17 使用setIndexDefined解决了StructVector=null的问题！！！
                    tableDataVector.setIndexDefined(i);
                    setData(tableDataVector, "__table_name__", influxdbBucketDataEntity.getTable(), "string", i);
                    for (String tagName : influxdbBucketDataEntity.getTags().keySet()) {
                        setData(tableDataVector, tagName, influxdbBucketDataEntity.getTags().get(tagName), "string", i);
                    }
                }
                // 设置tableVector写数据结束
                tableVector.endValue(0, influxdbBucketDataEntityList.size());
                // 这里固定传1
                vectorSchemaRoot.setRowCount(1);
                writer.writeBatch();
            }
            // 第二阶段提交type=3与attrVector&recordVector数据
            typeVector.reset();
            tableVector.reset();
            attrVector.reset();
            recordVector.reset();
            /* __type__ */
            typeVector.setSafe(0, 3);
            // 2023.04.04 使用startNewValue与endValue解决了valueCount=0的问题！！！
            // 2023.04.20 将startNewValue放到循环外解决了批量的问题！！！
            // 设置recordVector写数据开始
            recordVector.startNewValue(0);
            // 遍历数据
            for (int i = 0; i < influxdbBucketDataEntityList.size(); i++) {
                InfluxdbBucketDataEntity influxdbBucketDataEntity = influxdbBucketDataEntityList.get(i);
                /* __attrs__ */
                /* 暂时没有用到
                attrVector.setIndexDefined(i);
                setData(attrVector, "__table_name__", influxdbBucketDataEntity.getTable(), "string", i);
                for (String tagName : influxdbBucketDataEntity.getTags().keySet()) {
                    setData(attrVector, tagName, influxdbBucketDataEntity.getTags().get(tagName), "string", i);
                }
                */
                /* __records__ */
                StructVector recordDataVector = (StructVector) recordVector.getChildrenFromFields().get(0);
                recordDataVector.setIndexDefined(i);
                setData(recordDataVector, "__table_name__", influxdbBucketDataEntity.getTable(), "string", i);
                setData(recordDataVector, "time", influxdbBucketDataEntity.getTime(), "timestamp", i);
                setData(recordDataVector, influxdbBucketDataEntity.getField(), influxdbBucketDataEntity.getValue(), influxdbBucketDataEntity.getInfluxdbMeasurementEntity().getFieldMap().get(influxdbBucketDataEntity.getField()), i);
            }
            // 设置recordVector写数据结束
            recordVector.endValue(0, influxdbBucketDataEntityList.size());
            // 这里固定传1
            vectorSchemaRoot.setRowCount(1);
            writer.writeBatch();
            // 为了连续发送，此处不写结束信号
            // writer.end();
            // 如果首次提交，返回完整字节流，其后只提交RecordBatch
            if (first) {
                return outputStream.toByteArray();
            } else {
                // 2023.04.21 使用ArrowRecordBatch解决了后续数据无法传输的问题！！！
                // 创建新的输出流
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                // 获取RecordBatch
                ArrowRecordBatch arrowRecordBatch = new VectorUnloader(vectorSchemaRoot).getRecordBatch();
                // 序列化到输出流中
                MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), arrowRecordBatch, IpcOption.DEFAULT);
                // 关闭RecordBatch，否则执行rootAllocator.close()时会内存溢出
                arrowRecordBatch.close();
                // 返回字节流
                return out.toByteArray();
            }
        } catch (Exception e) {
            throw e;
        } finally {
            outputStream.close();
            vectorSchemaRoot.close();
            rootAllocator.close();
        }
    }

    /**
     * 获取arrow类型
     *
     * @param type
     * @return
     */
    private ArrowType getArrowType(String type) {
        switch (type) {
            case "boolean":
            case "bool":
                return new ArrowType.Bool();
            case "integer":
            case "int":
            case "long":
            case "bigint":
                return new ArrowType.Int(64, true);
            case "float":
            case "double":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "date":
            case "timestamp":
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            case "string":
            case "nchar(1000)":
            default: {
                return new ArrowType.Binary();
            }
        }
    }

    /**
     * arrow struct vector赋值
     *
     * @param structVector
     * @param dataName
     * @param dataValue
     * @param index
     */
    private void setData(StructVector structVector, String dataName, Object dataValue, String dataType, int index) {
        // 根据不同数据类型进行响应的赋值操作
        switch (dataType) {
            case "boolean":
            case "bool": {
                BitVector bitVector = (BitVector) structVector.getChild(dataName);
                bitVector.setSafe(index, ((Boolean) dataValue).booleanValue() ? 1 : 0);
                break;
            }
            case "integer":
            case "int":
            case "long":
            case "bigint": {
                BigIntVector bigIntVector = (BigIntVector) structVector.getChild(dataName);
                bigIntVector.setSafe(index, ((Number) dataValue).longValue());
                break;
            }
            case "float":
            case "double": {
                Float8Vector float8Vector = (Float8Vector) structVector.getChild(dataName);
                float8Vector.setSafe(index, ((Number) dataValue).doubleValue());
                break;
            }
            case "date":
            case "timestamp": {
                TimeStampMilliVector timeStampMilliVector = (TimeStampMilliVector) structVector.getChild(dataName);
                timeStampMilliVector.setSafe(index, ((Instant) dataValue).getEpochSecond() * 1000000 + ((Instant) dataValue).getNano());
                break;
            }
            case "string":
            case "nchar(1000)":
            default: {
                VarBinaryVector varBinaryVector = (VarBinaryVector) structVector.getChild(dataName);
                varBinaryVector.setSafe(index, dataValue.toString().getBytes());
                break;
            }
        }
    }
}
