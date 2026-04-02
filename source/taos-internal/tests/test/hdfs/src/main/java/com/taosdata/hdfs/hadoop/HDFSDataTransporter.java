package com.taosdata.hdfs.hadoop;

import org.anarres.lzo.LzopInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.util.Properties;

/**
 * @author Jiangyi Hou
 * @since 19-5-13
 */
public class HDFSDataTransporter {

    private Properties transporterProps = new Properties();
    private int ioBuffSize = 4096;
    private Configuration conf = new Configuration();

    // Default configuration parameters
    private static final String DEFAULT_URI = "hdfs://localhost:54310";
    private static final String DEFAULT_BUFFER_SIZE = "1024";
    private static final String DEFAULT_GRANULARITY = "file";

    public static void main(String[] args) throws Exception {
        String configFilePath = "." + File.separator + "HDFSTransporter.properties";
        if (args.length > 0) {
            configFilePath = args[0];
        }
        HDFSDataTransporter hdfsDataTransporter = new HDFSDataTransporter();
//        hdfsDataTransporter.lzoCompress();
//        hdfsDataTransporter.generateLzoExample();
//        hdfsDataTransporter.lzoDecompress();
        hdfsDataTransporter.config(configFilePath);
        if ("file".equals(hdfsDataTransporter.transporterProps.getProperty("granularity", "file"))) {
            hdfsDataTransporter.transportData();
        } else if ("directory".equals(hdfsDataTransporter.transporterProps.getProperty("granularity"))) {
            hdfsDataTransporter.transportDataDirectory();
        }
    }

    /**
     * Configure HDFSDataTransporter
     * @param configFilePath
     */
    public void config(String configFilePath) {
        try {
            FileInputStream fileInputStream = new FileInputStream(configFilePath);
            this.transporterProps.load(fileInputStream);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Config file for com.taosdata.hdfs.HDFSDataTransporter is not found, please provide the correct path to 'HDFSTransporter.properties'.");
        }
    }

    public void transportDataDirectory() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        String targetPath = this.transporterProps.getProperty("targetPath");
        FileSystem fs = FileSystem.get(URI.create(this.transporterProps.getProperty("uri", DEFAULT_URI)), conf);
        RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(new Path(targetPath), false);

        while (fileList.hasNext()) {
            LocatedFileStatus fileStatus = fileList.next();
            if (fileStatus.isFile()) {
                String fileName = fileStatus.getPath().getName();
                InputStream in = fs.open(fileStatus.getPath());
                OutputStream out = new FileOutputStream(this.transporterProps.getProperty("destPath") + File.separator + fileName);
                if (Boolean.valueOf(this.transporterProps.getProperty("decompressData")) == true) {
                    String dataType = this.transporterProps.getProperty("compressedDataFormat").toLowerCase();
                    switch (dataType) {
                        case "lzo":
                            LzopInputStream lzopInputStream = new ExtendedLzopInputStream(in);
                            InputStreamReader inputStreamReader = new InputStreamReader(lzopInputStream, transporterProps.getProperty("targetFileEncoding"));
                            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(out, transporterProps.getProperty("destFileEncoding"));
                            char[] buffer = new char[1024];
                            int read = 0;
                            while (inputStreamReader != null) {
                                read = inputStreamReader.read(buffer);
                                if (read != -1) {
                                    outputStreamWriter.write(buffer, 0, read);
                                } else {
                                    break;
                                }
                            }
                            inputStreamReader.close();
                            outputStreamWriter.flush();
                            outputStreamWriter.close();
                            in.close();
                            out.close();
                            break;
                        default:
                            throw new Exception("Unrecoganized compressed data type");
                    }
                } else {
                    IOUtils.copyBytes(in, out, Integer.parseInt(this.transporterProps.getProperty("ioStreamBuffSize")), true);
                }
            }
        }
    }

    public void transportData() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create(this.transporterProps.getProperty("uri", DEFAULT_URI)), conf);
        InputStream in = fs.open(new Path(this.transporterProps.getProperty("targetPath")));
        OutputStream out = new FileOutputStream(this.transporterProps.getProperty("destPath"));
        try {
            if (Boolean.valueOf(this.transporterProps.getProperty("decompressData")) == true) {
                String dataType = this.transporterProps.getProperty("compressedDataFormat").toLowerCase();
                switch (dataType) {
                    case "lzo":
//                        LzoAlgorithm lzoAlgorithm = LzoAlgorithm.LZO1X;
//                        LzoDecompressor lzoDecompressor = LzoLibrary.getInstance().newDecompressor(lzoAlgorithm, null);
                        LzopInputStream lzopInputStream = new ExtendedLzopInputStream(in);
//                        IOUtils.copyBytes(lzoInputStream, out, Integer.parseInt(this.transporterProps.getProperty("ioStreamBuffSize")), true);
                        InputStreamReader inputStreamReader = new InputStreamReader(lzopInputStream, transporterProps.getProperty("targetFileEncoding"));
//                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(out, transporterProps.getProperty("destFileEncoding"));
//                        BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter, 1024);
//                        char[] buffer = new char[1024];
//                        int read = 0;
//                        while (inputStreamReader != null && read != -1) {
//                            read = bufferedReader.read(buffer);
//                            bufferedWriter.write(buffer, 0, read);
//                        }
//                        bufferedReader.close();
//                        bufferedWriter.flush();
//                        bufferedWriter.close();

                        char[] buffer = new char[1024];
                        int read = 0;
                        while (inputStreamReader != null) {
                            read = inputStreamReader.read(buffer);
                            if (read != -1) {
                                outputStreamWriter.write(buffer, 0, read);
                            } else {
                                break;
                            }
                        }
                        inputStreamReader.close();
                        outputStreamWriter.flush();
                        break;
                    default:
                        throw new Exception("Unrecoganized compressed data type");
                }
            } else {
                IOUtils.copyBytes(in, out, Integer.parseInt(this.transporterProps.getProperty("ioStreamBuffSize")), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            in.close();
        }
    }

    private void transportData(Path targetPath) throws Exception {

    }

//    public void generateLzoExample() throws Exception {
//        OutputStream outCompresed = new FileOutputStream("/home/jyhou/jhou/hdfs_test/tmp/hw2.lzo");
//        LzoAlgorithm lzoAlgorithm = LzoAlgorithm.LZO1X;
//        LzoCompressor lzoCompressor = LzoLibrary.getInstance().newCompressor(lzoAlgorithm, null);
//        LzoOutputStream lzoOutputStream = new LzoOutputStream(outCompresed, lzoCompressor, 4096);
//        lzoOutputStream.write("Hello, world!\nHello, world!".getBytes("UTF-8"));
//        lzoOutputStream.close();
//    }

//    public void lzoCompress() throws Exception {
//        String destFilePath = "/home/jyhou/jhou/hdfs_test/tmp/sinoiov.demo.lzo";
//        String targetFilePath = "/home/jyhou/jhou/hdfs_test/sinoiov.demo.txt";
//        InputStream inputStream = new FileInputStream(targetFilePath);
//        OutputStream outputStream = new FileOutputStream(destFilePath, true);
//        LzoAlgorithm lzoAlgorithm = LzoAlgorithm.LZO1X;
//        LzoCompressor lzoCompressor = LzoLibrary.getInstance().newCompressor(lzoAlgorithm, null);
//        LzoOutputStream lzoOutputStream = new LzoOutputStream(outputStream, lzoCompressor, 16384);
//        int read = 0;
//        byte[] buffer = new byte[1024 * 16];
//        while (inputStream != null) {
//            read = inputStream.read(buffer);
//            if (read != -1) {
//                lzoOutputStream.write(buffer, 0, read);
//            } else {
//                break;
//            }
//        }
//        lzoOutputStream.flush();
//        lzoOutputStream.close();
//        outputStream.close();
//        inputStream.close();
//    }

    public void lzoDecompress() throws Exception {

//        LzoCodec lzopCodec = new LzoCodec();
//        lzopCodec.setConf(configuration);
        InputStream in = new FileInputStream("/home/jyhou/jhou/hdfs_test/kc-23-xxx-sink0.1546354800880.lzo");
//        CompressionInputStream lzopInputStream = lzopCodec.createInputStream(in);
//        OutputStream out = new FileOutputStream(this.transporterProps.getProperty("destPath"));
        OutputStream out = new FileOutputStream("/home/jyhou/jhou/hdfs_test/tmp/dest.txt");
//        LzoAlgorithm lzoAlgorithm = LzoAlgorithm.LZO2A;
//        LzoDecompressor lzoDecompressor = LzoLibrary.getInstance().newDecompressor(lzoAlgorithm, null);
//        LzoInputStream lzoInputStream = new LzoInputStream(in, lzoDecompressor);
//        LzopInputStream lzoInputStream = new ExtendedLzopInputStream(in);
        LzopInputStream lzoInputStream = new ExtendedLzopInputStream(in);
        InputStreamReader inputStreamReader = new InputStreamReader(lzoInputStream);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(out, "UTF-8");
        char[] buffer = new char[1024];
        int read = 0;
        while (inputStreamReader != null) {
            read = inputStreamReader.read(buffer);
            if (read != -1) {
                outputStreamWriter.write(buffer, 0, read);
            } else {
                break;
            }
        }
        inputStreamReader.close();
        outputStreamWriter.flush();
    }

    public void decompressLzoUsingHDFS() {
        Configuration configuration = new Configuration();
        configuration.set("io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec");
    }
}
