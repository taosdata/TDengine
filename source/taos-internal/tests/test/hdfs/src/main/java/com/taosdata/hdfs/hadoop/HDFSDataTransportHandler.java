package com.taosdata.hdfs.hadoop;

import com.taosdata.hdfs.utils.ParquetUtils;
import org.anarres.lzo.LzopInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * @author Jiangyi Hou
 * @since 19-5-16
 */
public class HDFSDataTransportHandler {

    // Constants
    private static final String DEFAULT_URI = "hdfs://localhost:54310";
    private static final String DEFAULT_ENCODING = "";

    private Properties transporterProps = new Properties();
    private String hdfsURI = DEFAULT_URI;
    private String targetFileEncoding = DEFAULT_ENCODING;
    private Configuration conf;
    private FileSystem fs = null;

    public HDFSDataTransportHandler(Properties transporterProps, Configuration hadoopConf) {
        this.hdfsURI = transporterProps.getProperty("uri", DEFAULT_URI);
        this.targetFileEncoding = transporterProps.getProperty("targetFileEncoding", "");
        this.conf = hadoopConf;
        this.conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        this.conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            this.fs = FileSystem.get(URI.create(hdfsURI), conf);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to access the given HDFS");
        }
    }

//    public void config(String propertyFilePath) {
//        try {
//            FileInputStream configFile = new FileInputStream(propertyFilePath);
//            this.transporterProps.load(configFile);
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.out.println("Config file for com.taosdata.hdfs.HDFSDataTransporter is not found, please provide the correct path to 'HDFSTransporter.properties'.");
//        }
//    }

    /**
     * Fetch files in a given directory on HDFS
     * @param targetPath path to the target file on HDFS
     * @param decompressData whether to decompress data
     * @param compressedDataFormat
     * @param recursive whether to recursively fetch files in subdirectories
     * @return
     * @throws Exception
     */
    public List<Object[]> fetchFilesInTargetDirectory(String targetPath, boolean decompressData, String compressedDataFormat, boolean recursive) throws Exception {
        RemoteIterator<LocatedFileStatus> fileList = this.fs.listFiles(new Path(targetPath), recursive);
        List<Object[]> processedInputStreams = new ArrayList<Object[]>();
        List<LocatedFileStatus> sortedFileList = new ArrayList<>();
        while (fileList.hasNext()) {
            LocatedFileStatus fileStatus = fileList.next();
            if (fileStatus.isFile()) {
                sortedFileList.add(fileStatus);
            }
        }
        sortedFileList.sort((fileStatus1, fileStatus2) -> fileStatus1.getPath().getName().compareTo(fileStatus2.getPath().getName()));

        Iterator<LocatedFileStatus> sortedFileListIterator = sortedFileList.iterator();
        while (sortedFileListIterator.hasNext()) {
            LocatedFileStatus fileStatus = sortedFileListIterator.next();
            if (fileStatus.isFile()) {
                Object f[] = new Object[2];
                f[0] = fetchSingleFileData(fileStatus.getPath(), decompressData, compressedDataFormat);
                f[1] = fileStatus.getPath().toString();
                processedInputStreams.add(f);
            }
        }
        return processedInputStreams;
    }

    /**
     * Fetch files with a given list of file paths on HDFS
     * @param targetFilesPathList path to the target file on HDFS
     * @param decompressData whether to decompress data
     * @param compresedDataFormat compression format
     * @return
     * @throws Exception
     */
    public List<InputStream> fetchFiles(Collection<String> targetFilesPathList, boolean decompressData, String compresedDataFormat) throws Exception {

        if (targetFilesPathList == null) {
            return null;
        }
        Path path = null;
        List<InputStream> processedInputStreams = new ArrayList<InputStream>(targetFilesPathList.size());
        for (String targetFilePath:targetFilesPathList) {
            path = new Path(targetFilePath);
            if (this.fs.isFile(path)) {
                processedInputStreams.add(fetchSingleFileData(path, decompressData, compresedDataFormat));
            } else {
                throw new IOException("Found a directory while expecting a file with the given path");
            }
        }
        return processedInputStreams;
    }

    /**
     * Fetch data from a single file with the given path on a HDFS
     * @param targetPath path to the target file on HDFS
     * @param decompressData whether to decompress data
     * @param compressedDataFormat compression format
     * @return
     * @throws Exception
     */
    public InputStream fetchFile(String targetPath, boolean decompressData, String compressedDataFormat)  throws Exception {
        Path path = new Path(targetPath);
        if (this.fs.isFile(path)) {
            return fetchSingleFileData(path, decompressData, compressedDataFormat);
        } else {
            throw new IOException("Found a directory while expecting a file with the given path");
        }
    }

    /**
     * Fetch data in a designated file on HDFS, decompress data if necessary, return processed in an InputStream
     * @param path path to file on HDFS
     * @param decompressData whether to decompress data during fetching
     * @param compressedDataFormat compression format
     * @return
     * @throws Exception
     */
    private InputStream fetchSingleFileData(Path path, boolean decompressData, String compressedDataFormat) throws Exception {
        InputStream in = this.fs.open(path);
        InputStream processedInputStream = null;
        try {
            if (decompressData == true) {
                switch (compressedDataFormat.toLowerCase()) {
                    case "lzo":
                    {
                        processedInputStream = new ExtendedLzopInputStream(in);
                        break;
                    }
                    case "parquet":
                    {
                        ParquetUtils parquetUtils = new ParquetUtils();
                        processedInputStream = parquetUtils.fetchSingleParquetFileFromHDFS(hdfsURI, path.toString(), transporterProps.getProperty("localDir"), Integer.parseInt(transporterProps.getProperty("ioStreamBuffSize", "4096")));
                    }

                    default:
                        throw new Exception("Unrecoganized compressed data type");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
//            in.close();
            }
            return processedInputStream;
        }

        public void lzoDecompress() throws Exception {

            InputStream in = new FileInputStream("/home/jyhou/jhou/hdfs_test/kc-23-xxx-sink0.1546354800880.lzo");
            OutputStream out = new FileOutputStream("/home/jyhou/jhou/hdfs_test/tmp/dest.txt");
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

        public void close() {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
