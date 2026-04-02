package com.taosdata.iot.javaTestSuit.suit_01;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ImportTester extends TaosTester{

//    private Logger logger = LoggerFactory.getLogger(ImportTester.class);

    public static void main(String[] args) {

        ImportTester importTester = new ImportTester();
        Properties properties = new Properties();
        importTester.runImportTestTask(properties);
    }

    public void runImportTestTask(Properties properties) {
        int threadNum = 3;

        List<ImportTestTask> tasks = new ArrayList<>(threadNum);
        for (int i = 0; i < threadNum; i++) {
            ImportTestTask task = new ImportTestTask();
            tasks.add(task);
        }
        runMultiThreadTest(tasks);
    }

}
