package com.taosdata.hdfs.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * @author Jiangyi Hou
 * @since 19-5-7
 */
public class ParquetUtils {

    private Properties props = new Properties();
    public static void main(String[] args) throws Exception{
        ParquetUtils parquetUtils = new ParquetUtils();
        parquetUtils.run(args);
    }

    public void run(String[] args) throws Exception{
        String configFilePath = "." + File.separator + "parquetTransporter.properties";
        if (args.length > 0) {
            configFilePath = args[0];
        }
        FileInputStream configProps = new FileInputStream(configFilePath);
        this.props.load(configProps);
        ParquetUtils utils = new ParquetUtils();
//        InputStream inputStream = fetchSingleParquetFile(this.props.getProperty("uri"), this.props.getProperty("targetPath"),
//                this.props.getProperty("localTmpDir"), Integer.parseInt(this.props.getProperty("ioBufferSize")));
//        fetchFilesInTargetDirectoryFromHDFS(this.props.getProperty("uri"), this.props.getProperty("targetPath"),
//                this.props.getProperty("localTmpDir"), Integer.parseInt(this.props.getProperty("ioBufferSize")));
        fetchFilesInTargetDirectoryFromLocal( this.props.getProperty("targetPath"),
                this.props.getProperty("localTmpDir"), Integer.parseInt(this.props.getProperty("ioBufferSize")));
        System.out.printf("Parquet files tranported to local dir: %s\n", this.props.getProperty("localTmpDir"));
    }

    public InputStream fetchSingleParquetFileFromHDFS(String uri, String targetFile, String tmpDir, int buffSize) throws IOException {
        Schema.Field[] fields = null;
        String[] fieldNames = new String[0];
        InputStream inputStream = null;
        try {
            FileSystem fs = FilterFileSystem.get(new URI(uri) , new Configuration());
            Path path = new Path(targetFile);
            if (fs.isFile(path) != true) {
                throw new IOException("Found a director while expecting a file");
            }
            AvroParquetReader<GenericData.Record> reader = new AvroParquetReader<GenericData.Record>(path);
            GenericData.Record record;
            String tmpFilePath = tmpDir + path.getName();
            tmpFilePath = tmpFilePath + ".csv";
            File destFile = new File(tmpFilePath);
            FileWriter fileWriter = new FileWriter(destFile, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter, buffSize);
            while ((record = (GenericData.Record) reader.read()) != null) {
                if (fields == null) {
                    final List<Schema.Field> fieldsList = record.getSchema().getFields();
                    fieldNames = getFieldNames(fields = fieldsList.toArray(new Schema.Field[0]));
                }
                StringBuilder line = new StringBuilder("");
                for (final String fieldName : fieldNames) {
                    String recordData = record.get(fieldName).toString();
                    line.append(recordData).append(",");
                }
                line.deleteCharAt(line.length() - 1 );
                line.append("\n");
                bufferedWriter.write( line.toString());
            }
            bufferedWriter.flush();
            fileWriter.close();
            bufferedWriter.close();
            reader.close();
            inputStream = new FileInputStream(destFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return inputStream;
    }

    public void fetchFilesInTargetDirectoryFromHDFS(String uri, String targetPath, String localTmpDir, int ioBufferSize) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            FileSystem fs = FilterFileSystem.get(new URI(uri), conf);
            RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(new Path(targetPath), true);
            List<Object[]> processedInputStreams = new ArrayList<Object[]>();

            while (fileList.hasNext()) {
                LocatedFileStatus fileStatus = fileList.next();
                if (fileStatus.isFile()) {
                    Object f[] = new Object[2];
                    f[0] = fetchSingleParquetFileFromHDFS(uri, fileStatus.getPath().toString(), localTmpDir, ioBufferSize);
                    f[1] = fileStatus.getPath().toString();
                    processedInputStreams.add(f);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String[] getFieldNames(final Schema.Field[] fields) {
        final String[] fieldNames = new String[fields.length];
        int i = 0;
        for (final Schema.Field field : fields) {
            fieldNames[i++] = field.name();
        }
        return fieldNames;
    }

    public InputStream fetchSingleParquetFileFromLocal(String targetFile, String tmpDir, int buffSize) throws IOException {
        Schema.Field[] fields = null;
        String[] fieldNames = new String[0];
        InputStream inputStream = null;
        try {
            Path path = new Path(targetFile);
            if (new File(targetFile).isFile() != true) {
                throw new IOException("Found a director while expecting a file");
            }
            AvroParquetReader<GenericData.Record> reader = new AvroParquetReader<GenericData.Record>(path);
            GenericData.Record record;
            String tmpFilePath = tmpDir + path.getName();
            tmpFilePath = tmpFilePath + ".csv";
            File destFile = new File(tmpFilePath);
            FileWriter fileWriter = new FileWriter(destFile, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter, buffSize);
            while ((record = (GenericData.Record) reader.read()) != null) {
                if (fields == null) {
                    final List<Schema.Field> fieldsList = record.getSchema().getFields();
                    fieldNames = getFieldNames(fields = fieldsList.toArray(new Schema.Field[0]));
                }
                StringBuilder line = new StringBuilder("");
                for (final String fieldName : fieldNames) {
                    String recordData = record.get(fieldName).toString();
                    line.append(recordData).append(",");
                }
                line.deleteCharAt(line.length() - 1 );
                line.append("\n");
                bufferedWriter.write( line.toString());
            }
            bufferedWriter.flush();
            fileWriter.close();
            bufferedWriter.close();
            reader.close();
            inputStream = new FileInputStream(destFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return inputStream;
    }

    public void fetchFilesInTargetDirectoryFromLocal(String targetPath, String localTmpDir, int ioBufferSize) {
        try {
            File target = new File(targetPath);
            File[] files = target.listFiles();
            List<Object[]> processedInputStreams = new ArrayList<Object[]>();

            if (files != null) {
                // sort files by file name
                List<File> sortedFileList = Arrays.asList(files);
                sortedFileList.sort((file1, file2)-> file1.getName().compareTo(file2.getName()));
                Iterator<File> sortedFileListIterator = sortedFileList.iterator();
                while (sortedFileListIterator.hasNext()) {
                    File file = sortedFileListIterator.next();
                    if (file.isFile()) {
                        Object f[] = new Object[2];
                        f[0] = fetchSingleParquetFileFromLocal(file.getPath(), localTmpDir, ioBufferSize);
                        f[1] = file.getPath();
                        processedInputStreams.add(f);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sortFilesByName(File[] files) {
        List fileList = Arrays.asList(files);
        fileList.sort(new Comparator<File>() {
            @Override
            public int compare(File f1, File f2) {
                return 0;
            }
        });
    }

    public static void testGetSchema() throws Exception {
        Configuration configuration = new Configuration();
//        // windows 下测试入库impala需要这个配置
//        System.setProperty("hadoop.home.dir",
//                "E:\\mvtech\\software\\hadoop-internel-2.2.0-bin-master");
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
