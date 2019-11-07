package com.taosdata.iot;

//import com.alibaba.fastjson.JSON;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jiangyi Hou
 * @since 19-5-7
 */
public class ParquetUtils {
    public static void main(String[] args) throws Exception{
        ParquetUtils utils = new ParquetUtils();
//        utils.testGetSchema();
        utils.testParquetReader();
        String path = "/home/jyhou/parquetData/data.gz.parquet";
        viewParquet(path, 10000000);
    }

    private static final String csvDelimiter = ",";
    public static Map<String, List<String[]>> viewParquet(String path, int maxLine) throws IOException {
        Map<String,List<String[]>> parquetInfo=new HashMap<>();
        List<String[]> dataList = new ArrayList<>();
        Schema.Field[] fields = null;
        String[] fieldNames = new String[0];
        try (
//                ParquetReader<GenericData.Record> reader =
//                        AvroParquetReader.<GenericData.Record>builder(new Path(path)).build()
                AvroParquetReader<GenericData.Record> reader = new
                        AvroParquetReader<GenericData.Record>(new Path(path))
        ){
            int  x=0;
            GenericData.Record record;
            //解析Parquet数据逐行读取
            while ((record = reader.read()) != null && x < maxLine) {
                //读取第一行获取列头信息
                if (fields == null) {
                    final List<Schema.Field> fieldsList = record.getSchema().getFields();
                    fieldNames = getFieldNames(fields = fieldsList.toArray(new Schema.Field[0]));
                    System.out.println("列头:" + String.join(csvDelimiter, fieldNames));
                    dataList.add(fieldNames);
                    parquetInfo.put("head", dataList);
                    dataList = new ArrayList<>();

//                    System.out.println(JSON.parse(record.getSchema().toString()));
                    break;
                }

                int i = 0;
                String[] dataString = new String[fieldNames.length];
                //读取数据获取列头信息
                File destFile = new File("/home/jyhou/dest1.csv");
                FileWriter fileWriter = new FileWriter(destFile, true);
                BufferedWriter bufferedWriter = new BufferedWriter(fileWriter, 4096);
                StringBuilder line = new StringBuilder("");
                for (final String fieldName : fieldNames) {
                    String recordData = record.get(fieldName).toString();
                    line.append(recordData).append(",");
//                    if (recordData.contains("type")) {
//                        List<HashMap> dataFormValue = JSONArray.parseArray(JSONObject.parseObject(recordData).get("values").toString(), HashMap.class);
//                        StringBuilder datas = new StringBuilder();
//                        for (HashMap data : dataFormValue) {
//                            datas.append(data.get("element").toString()).append(",");
//                        }
//                        datas.deleteCharAt(datas.length() - 1);
//                        recordData = datas.toString();
//                    }
//                    dataString[i++] = recordData;
                }
                line.deleteCharAt(line.length() - 1 );
                line.append("\n");
                bufferedWriter.write( line.toString());
                bufferedWriter.flush();
                fileWriter.close();
                bufferedWriter.close();
//                dataList.add(dataString);
                ++x;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        parquetInfo.put("data",dataList);
        return parquetInfo;
    }

    private static String[] getFieldNames(final Schema.Field[] fields) {
        final String[] fieldNames = new String[fields.length];
        int i = 0;
        for (final Schema.Field field : fields) {
            fieldNames[i++] = field.name();
        }
        return fieldNames;
    }


    public static void testGetSchema() throws Exception {
        Configuration configuration = new Configuration();
//        // windows 下测试入库impala需要这个配置
//        System.setProperty("hadoop.home.dir",
//                "E:\\mvtech\\software\\hadoop-common-2.2.0-bin-master");
        ParquetMetadata readFooter = null;
        Path parquetFilePath = new Path("/home/jyhou/parquetData/data.gz.parquet");
        readFooter = ParquetFileReader.readFooter(configuration,
                parquetFilePath, ParquetMetadataConverter.NO_FILTER);
        MessageType schema =readFooter.getFileMetaData().getSchema();
        System.out.println(schema.toString());
    }

    private static void testParquetReader() throws IOException{
        Path file = new Path("/home/jyhou/parquetData/data.gz.parquet");
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), file);
        ParquetReader<Group> reader = builder.build();
//        SimpleGroup group =(SimpleGroup) reader.read();
        Group group = reader.read();
        System.out.println("schema: "+group.getType().toString());
//        System.out.println("idc_id: "+group.getString(1, 0));
        System.out.println("vin: "+group.getString("vin", 0));
        System.out.println("useraccount: "+group.getString("useraccount", 0));
        System.out.println("recordtimestamp: "+group.getString("recordtimestamp", 0));
        System.out.println("cardatatimestamp: "+group.getString("cardatatimestamp", 0));
        System.out.println("longitude: "+group.getDouble("longitude", 0));
        System.out.println("latitude: "+group.getDouble("latitude", 0));
        System.out.println("kilometremileage: "+group.getDouble("kilometremileage", 0));
        System.out.println("flpressure: "+group.getDouble("flpressure", 0));
        System.out.println("frpressure: "+group.getDouble("frpressure", 0));
        System.out.println("rlpressure: "+group.getDouble("rlpressure", 0));
        System.out.println("rrpressure: "+group.getDouble("rrpressure", 0));
        System.out.println("statdoorajarfl: "+group.getDouble("statdoorajarfl", 0));
        System.out.println("statdoorajarfr: "+group.getDouble("statdoorajarfr", 0));
        System.out.println("statdoorajarrl: "+group.getDouble("statdoorajarrl", 0));
        System.out.println("statdoorajarrr: "+group.getDouble("statdoorajarrr", 0));
        System.out.println("stattrunkajar: "+group.getDouble("stattrunkajar", 0));
        System.out.println("fuellevel: "+group.getDouble("fuellevel", 0));
        System.out.println("drivingdirection: "+group.getDouble("drivingdirection", 0));
        System.out.println("cartime: "+group.getString("cartime", 0));
        System.out.println("instantconsum: "+group.getDouble("instantconsum", 0));
        System.out.println("longacceleration: "+group.getDouble("longacceleration", 0));
        System.out.println("steeringpos: "+group.getDouble("steeringpos", 0));
        System.out.println("vehiclespeed: "+group.getDouble("vehiclespeed", 0));
        System.out.println("enginespeed: "+group.getDouble("enginespeed", 0));

        int count = 1;
        File destFile = new File("/home/jyhou/dest.csv");
        FileWriter writer = new FileWriter(destFile, true);
        while ((group = reader.read())!= null) {
            count++;
            for (int i = 0; i < 23; i++) {
                writer.write(group.getString(i, 0) + ", ");
            }
            writer.write(group.getString(23,0) + "\n");
            System.out.println("vin: " + group.getString("vin", 0));
        }
        System.out.println("count: " + count);
    }
}
