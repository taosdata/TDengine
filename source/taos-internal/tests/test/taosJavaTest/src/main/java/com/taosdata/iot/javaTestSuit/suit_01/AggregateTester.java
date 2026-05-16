package com.taosdata.iot.javaTestSuit.suit_01;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AggregateTester extends TaosTester{

    public static void main(String[] args) {

        int threadNum = 1;
        AggregateTester aggregateTester = new AggregateTester();
        Properties properties = new Properties();
        aggregateTester.runAggregateTestTask(properties);
    }

    public void runAggregateTestTask(Properties properties) {
        int threadNum = 1;
        List<AggregateTestTask> tasks = new ArrayList<>(threadNum);
        for (int i = 0; i < threadNum; i++) {
            AggregateTestTask task = new AggregateTestTask();
            tasks.add(task);
        }
        runMultiThreadTest(tasks);
    }

}
