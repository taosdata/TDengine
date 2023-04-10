package com.taosdata.utils.arrow;

import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.*;

public class ArrowUtils {

    /**
     * 将实体类转换为apache-arrow的字节流
     *
     * @param influxdbBucketDataEntity
     * @return
     */
    public static byte[] transform(InfluxdbBucketDataEntity influxdbBucketDataEntity) {
        List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = new ArrayList<>();
        influxdbBucketDataEntityList.add(influxdbBucketDataEntity);
        return transform(influxdbBucketDataEntityList);
    }

    /**
     * 将实体类列表转换为apache-arrow的字节流
     *
     * @param influxdbBucketDataEntityList
     * @return
     */
    public static byte[] transform(List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList) {
        /* 超级表表结构信息 */
        // 实体类列表不存在则返回空数组
        if (influxdbBucketDataEntityList == null || influxdbBucketDataEntityList.size() == 0) {
            return new byte[0];
        }
        ArrowInitDto arrowInitDto = new ArrowInitDto();
        // 使用第一条数据的measurement信息进行构建
        InfluxdbMeasurementEntity influxdbMeasurementEntity = influxdbBucketDataEntityList.get(0).getInfluxdbMeasurementEntity();
        // name
        arrowInitDto.setName(influxdbMeasurementEntity.getMeasurement());
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

        /* 转换实体类并返回字节流 */
        // 定义字节流用于接收转换的数据
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        // 转换arrow格式
        transform(arrowInitDto, influxdbBucketDataEntityList, outputStream);
        // 返回字节流
        return outputStream.toByteArray();
    }

    /**
     * 将实体类列表转换为apache-arrow的字节流
     *
     * @param arrowInitDto
     * @param influxdbBucketDataEntityList
     * @param out
     */
    private static void transform(ArrowInitDto arrowInitDto, List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList, OutputStream out) {
        /* 封装meta信息 */
        Map<String, String> metaData = new HashMap<>();
        metaData.put("ack", "none");
        metaData.put("stream", "lush");
        metaData.put("version", "1.0");
        metaData.put("init", arrowInitDto.toString());

        /* 封装fields信息 */
        // tag fields
        List<Field> tagFieldList = new ArrayList<>();
        for (ArrowInitDto.Tag tag : arrowInitDto.getTags()) {
            tagFieldList.add(new Field(tag.getName(), FieldType.notNullable(getArrowType(tag.getType())), null));
        }
        // column fields
        List<Field> columnFieldList = new ArrayList<>();
        for (ArrowInitDto.Column column : arrowInitDto.getColumns()) {
            columnFieldList.add(new Field(column.getName(), FieldType.notNullable(getArrowType(column.getType())), null));
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
        columnFieldList.add(new Field("__table_name__", FieldType.nullable(new ArrowType.Binary()), null));
        List<Field> recordFieldChildrenList = new ArrayList<>();
        recordFieldChildrenList.add(new Field("item", FieldType.nullable(new ArrowType.Struct()), columnFieldList));
        Field recordField = new Field("__records__", FieldType.nullable(new ArrowType.List()), recordFieldChildrenList);
        // field list
        List<Field> fieldList = new ArrayList<>();
        fieldList.add(typeField);
        fieldList.add(tableField);
        fieldList.add(attrField);
        fieldList.add(recordField);
        /* 填充表数据 */
        Schema schema = new Schema(fieldList, metaData);
        RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, rootAllocator);
        try {
            DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
            ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, dictProvider, out);
            writer.start();
            // 初始化
            UInt1Vector typeVector = (UInt1Vector) vectorSchemaRoot.getVector("__type__");
            ListVector tableVector = (ListVector) vectorSchemaRoot.getVector("__tables__");
            StructVector attrVector = (StructVector) vectorSchemaRoot.getVector("__attrs__");
            ListVector recordVector = (ListVector) vectorSchemaRoot.getVector("__records__");
            typeVector.reset();
            tableVector.reset();
            attrVector.reset();
            recordVector.reset();
            // 第一阶段提交type=2与tableVector数据
            /* __type__ */
            typeVector.setSafe(0, 2);
            // 遍历数据
            for (int i = 0; i < influxdbBucketDataEntityList.size(); i++) {
                InfluxdbBucketDataEntity influxdbBucketDataEntity = influxdbBucketDataEntityList.get(i);
                /* __tables__ */
                tableVector.startNewValue(i);
                StructVector tableDataVector = (StructVector) tableVector.getChildrenFromFields().get(0);
                setData(tableDataVector, "__table_name__", influxdbBucketDataEntity.getTable(), "string", i);
                for (String tagName : influxdbBucketDataEntity.getTags().keySet()) {
                    setData(tableDataVector, tagName, influxdbBucketDataEntity.getTags().get(tagName), "string", i);
                }
                tableVector.endValue(i, 1);
                /* __attrs__ */
                setData(attrVector, "__table_name__", influxdbBucketDataEntity.getTable(), "string", i);
                for (String tagName : influxdbBucketDataEntity.getTags().keySet()) {
                    setData(attrVector, tagName, influxdbBucketDataEntity.getTags().get(tagName), "string", i);
                }
            }
            writer.writeBatch();
            // 第二阶段提交type=3与attrVector&recordVector数据
            /* __type__ */
            typeVector.setSafe(0, 3);
            // 遍历数据
            for (int i = 0; i < influxdbBucketDataEntityList.size(); i++) {
                InfluxdbBucketDataEntity influxdbBucketDataEntity = influxdbBucketDataEntityList.get(i);

                /* __tables__ */
//                tableVector.startNewValue(i);
//                StructVector tableDataVector = (StructVector) tableVector.getChildrenFromFields().get(0);
//                setData(tableDataVector, "__table_name__", influxdbBucketDataEntity.getTable(), "string", i);
//                for (String tagName : influxdbBucketDataEntity.getTags().keySet()) {
//                    setData(tableDataVector, tagName, influxdbBucketDataEntity.getTags().get(tagName), "string", i);
//                }
//                tableVector.endValue(i, 1);
                /* __attrs__ */
//                setData(attrVector, "__table_name__", influxdbBucketDataEntity.getTable(), "string", i);
//                for (String tagName : influxdbBucketDataEntity.getTags().keySet()) {
//                    setData(attrVector, tagName, influxdbBucketDataEntity.getTags().get(tagName), "string", i);
//                }
                /* __records__ */
                // 2023.04.04 使用startNewValue与endValue解决了valueCount=0的问题！！！
                recordVector.startNewValue(i);
                StructVector recordDataVector = (StructVector) recordVector.getChildrenFromFields().get(0);
                setData(recordDataVector, "__table_name__", influxdbBucketDataEntity.getTable(), "string", i);
                setData(recordDataVector, "time", influxdbBucketDataEntity.getTime(), "timestamp", i);
                setData(recordDataVector, influxdbBucketDataEntity.getField(), influxdbBucketDataEntity.getValue(), influxdbBucketDataEntity.getInfluxdbMeasurementEntity().getFieldMap().get(influxdbBucketDataEntity.getField()), i);
                recordVector.endValue(i, 1);
            }
            // 这里固定传1
            vectorSchemaRoot.setRowCount(1);
            vectorSchemaRoot.getVector(3).setValueCount(influxdbBucketDataEntityList.size());
            writer.writeBatch();
            writer.end();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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
    private static ArrowType getArrowType(String type) {
        switch (type) {
            case "boolean":
            case "bool":
                return new ArrowType.Bool();
            case "integer":
            case "int":
                return new ArrowType.Int(32, true);
            case "long":
            case "bigint":
                return new ArrowType.Int(64, true);
            case "float":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "double":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "date":
            case "timestamp":
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case "string":
            case "varchar(1000)":
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
    private static void setData(StructVector structVector, String dataName, Object dataValue, String dataType, int index) {
        // 根据不同数据类型进行响应的赋值操作
        switch (dataType) {
            case "boolean":
            case "bool": {
                BitVector bitVector = (BitVector) structVector.getChild(dataName);
                bitVector.setSafe(index, ((boolean) dataValue) ? 1 : 0);
                break;
            }
            case "integer":
            case "int": {
                IntVector intVector = (IntVector) structVector.getChild(dataName);
                intVector.setSafe(index, (int) dataValue);
                break;
            }
            case "long":
            case "bigint": {
                BigIntVector bigIntVector = (BigIntVector) structVector.getChild(dataName);
                bigIntVector.setSafe(index, (long) dataValue);
                break;
            }
            case "float": {
                Float4Vector float4Vector = (Float4Vector) structVector.getChild(dataName);
                float4Vector.setSafe(index, ((Double) dataValue).floatValue());
                break;
            }
            case "double": {
                Float8Vector float8Vector = (Float8Vector) structVector.getChild(dataName);
                float8Vector.setSafe(index, (double) dataValue);
                break;
            }
            case "date":
            case "timestamp": {
                TimeStampMilliVector timeStampMilliVector = (TimeStampMilliVector) structVector.getChild(dataName);
                timeStampMilliVector.setSafe(index, ((Date) dataValue).getTime());
                break;
            }
            case "string":
            case "varchar(1000)":
            default: {
                VarBinaryVector varBinaryVector = (VarBinaryVector) structVector.getChild(dataName);
                varBinaryVector.setSafe(index, dataValue.toString().getBytes());
                break;
            }
        }
    }
}
