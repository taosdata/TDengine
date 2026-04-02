package com.taosdata.hdfs;

import com.taosdata.hdfs.csv.internal.*;
import com.taosdata.hdfs.csv.*;
import com.taosdata.hdfs.hadoop.*;
import org.apache.hadoop.conf.Configuration;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Test {
    Connection connection;

    public static void main(String[] args) throws Exception {
        String configFile;
        if (args.length < 1) {
            String targetPath = Test.class.getClassLoader().getResource("./").getPath();
            configFile = targetPath + "../../zjxl.cfg";
            TDLog.print(String.format("config file is required, default is %s", configFile));
        } else {
            configFile = args[0];
        }
        TDLog.print(String.format("use config file %s", configFile));

        if (!TDCsvFactory.init(configFile)) {
            TDLog.error(String.format("failed to read config file %s", configFile));
            System.exit(4);
        }

        if (TDConfig.localFile != "") {
            TDLog.print(String.format("file:%s, is handling", TDConfig.localFile));
            TDCsv csv = TDCsvFactory.createCsv(TDConfig.localFile);
            csv.parseFile();
        } else if (TDConfig.localDir != "") {
            ArrayList<String> allFiles = TDUtil.getAllFiles(TDConfig.localDir);
            TDLog.print(String.format("dir:%s, %d files will be disposed", TDConfig.localDir, allFiles.size()));
            for (String fileName : allFiles) {
                TDLog.print(String.format("file:%s, is handling", fileName));
                TDCsv csv = TDCsvFactory.createCsv(fileName);
                csv.parseFile();
            }
        } else if (TDConfig.jdbcUrl != "") {
            TDLog.error("jdbc url not implemented");
        } else if (TDConfig.hdfsUrl != "") {
            Properties transporterProps = new Properties();
            transporterProps.setProperty("uri", TDConfig.hdfsUrl);
            transporterProps.setProperty("ioStreamBuffSize", TDConfig.hdfsUrl);
            transporterProps.setProperty("targetFileEncoding", TDConfig.hdfsFileEncoding);
            transporterProps.setProperty("decompressData", TDConfig.hdfsDecompressData ? "true" : "false");
            transporterProps.setProperty("compressedDataFormat", TDConfig.hdfsCompressedDataFormat);
            HDFSDataTransportHandler hdsfHandler = new HDFSDataTransportHandler(transporterProps, new Configuration());

            try {
                if (TDConfig.hdfsDir != "") {
                    List<Object[]> allHdfsFiles = hdsfHandler.fetchFilesInTargetDirectory(TDConfig.hdfsDir, TDConfig.hdfsDecompressData, TDConfig.hdfsCompressedDataFormat, true);
                    TDLog.print(String.format("url:%s, dir:%s, %d files will be disposed", TDConfig.hdfsUrl, TDConfig.hdfsDir, allHdfsFiles.size()));

                    for (Object[] hdfsFile : allHdfsFiles) {
                        String fileName = "";
                        InputStream inputStream = null;
                        InputStreamReader hdfsStreamReader = null;
                        try {
                            inputStream = (InputStream) hdfsFile[0];
                            fileName = (String) hdfsFile[1];
                            if (fileName.endsWith(".index")) {
                                TDLog.print(String.format("file:%s, is an index file, ignore it", fileName));
                                continue;
                            }

                            TDLog.print(String.format("file:%s, is handling", fileName));
                            TDCsv csv = TDCsvFactory.createCsv(fileName);
                            hdfsStreamReader = new InputStreamReader(inputStream, transporterProps.getProperty("targetFileEncoding"));//to be implemented
                            if (hdfsStreamReader != null) {
                                csv.parseStream(hdfsStreamReader);
                            } else {
                                TDLog.error(String.format("file:%s, open stream error", fileName));
                            }


                        } catch (Exception e) {
                            e.printStackTrace();
                            TDLog.error(String.format("file:%s, read error:%s", fileName, e.getMessage()));
                        }
                        finally {
                            try {
                                hdfsStreamReader.close();
                                inputStream.close();
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }

                } else if (TDConfig.hdfsFile != "") {
                    TDLog.print(String.format("url:%s file:%s, is handling", TDConfig.hdfsUrl, TDConfig.hdfsFile));
                    TDCsv csv = TDCsvFactory.createCsv(TDConfig.hdfsFile);
                    InputStream inputStream = hdsfHandler.fetchFile(TDConfig.hdfsFile, TDConfig.hdfsDecompressData, TDConfig.hdfsCompressedDataFormat);
                    InputStreamReader hdfsStreamReader = new InputStreamReader(inputStream, transporterProps.getProperty("targetFileEncoding"));//to be implemented
                    csv.parseStream(hdfsStreamReader);
                    inputStream.close();
                } else {
                    TDLog.error("no data input");
                }
            } catch (Exception e) {
                e.printStackTrace();
                TDLog.error(String.format("failed to read from hdfs url:%s, file:%s, dir:%s", TDConfig.hdfsUrl, TDConfig.hdfsFile, TDConfig.hdfsDir));
            } finally {
                hdsfHandler.close();
            }
        } else {
            TDLog.error("no data input");
        }
    }
}
