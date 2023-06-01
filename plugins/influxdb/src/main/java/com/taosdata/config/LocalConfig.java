package com.taosdata.config;

import java.util.HashSet;
import java.util.Set;

/**
 * 本地配置
 *
 * @author ZYP
 */
public class LocalConfig {

    /**
     * 线程运行标志，用于安全退出前的处理
     */
    public static boolean isRunBucketThread = true;
    public static boolean isRunScheduleThread = true;
    public static boolean isRunPushPrepareThread = true;

    /**
     * 读取数据任务过滤集合，用于断点续传
     */
    public static Set<String> fetchFilterSet = new HashSet<>();
}
