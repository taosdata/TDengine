package com.taosdata.caches;

import com.taosdata.model.dto.bum.InfluxdbInfo;
import com.taosdata.model.dto.bum.NettyInfo;
import com.taosdata.model.dto.bum.QueueInfo;
import com.taosdata.model.dto.bum.ThreadInfo;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.utils.flux.FluxEnums;
import com.taosdata.utils.flux.FluxManager;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 状态缓存
 *
 * @author ZYP
 */
public class StatusCache {

    /**
     * 启动时间
     */
    @Getter
    @Setter
    private static Date startTime;

    /**
     * 状态
     */
    @Getter
    @Setter
    private static int status;

    /**
     * 描述
     */
    @Getter
    @Setter
    private static String description;

    /**
     * 线程列表
     */
    private static ConcurrentHashMap<String, ThreadInfo> threadInfoMap = new ConcurrentHashMap<>();

    /**
     * 内存队列列表
     */
    private static LinkedHashMap<String, QueueInfo> queueInfoMap = new LinkedHashMap<>();

    /**
     * Influxdb数据库
     */
    @Getter
    private static InfluxdbInfo influxdbInfo = new InfluxdbInfo();

    /**
     * Netty服务端
     */
    @Getter
    private static NettyInfo nettyInfo = new NettyInfo();

    /**
     * 记录线程运行信息
     *
     * @param threadInfo
     */
    public static void noteThread(ThreadInfo threadInfo) {
        threadInfoMap.put(threadInfo.getName(), threadInfo);
    }

    /**
     * 记录线程运行信息
     *
     * @param name
     * @param start
     * @param end
     * @param status
     * @param desc
     */
    public static void noteThread(String name, long start, long end, int status, String desc) {
        // 缓存中不存在则新建
        if (!threadInfoMap.containsKey(name)) {
            ThreadInfo threadInfo = new ThreadInfo();
            threadInfo.setName(name);
            threadInfo.setStartTime(new Date(start));
            threadInfoMap.put(name, threadInfo);
        }
        threadInfoMap.get(name).setLastTime(new Date(start));
        threadInfoMap.get(name).setLastTake(end - start);
        threadInfoMap.get(name).setStatus(status);
        threadInfoMap.get(name).setDescription(desc);
    }

    /**
     * 清除线程信息
     *
     * @param name
     */
    public static void forgetThread(String name) {
        threadInfoMap.remove(name);
    }

    /**
     * 记录内存队列信息
     *
     * @param name
     * @param limit
     * @param length
     */
    public static void noteQueue(String name, long limit, long length) {
        // 缓存中不存在则新建
        if (!queueInfoMap.containsKey(name)) {
            QueueInfo queueInfo = new QueueInfo();
            queueInfo.setName(name);
            queueInfo.setLimit(limit);
            queueInfo.setLength(length);
            queueInfoMap.put(name, queueInfo);
        }
        queueInfoMap.get(name).setLimit(limit);
        queueInfoMap.get(name).setLength(length);
    }

    /**
     * 记录Influxdb连接信息
     *
     * @param url
     */
    public static void noteInfluxdb(String url) {
        influxdbInfo.setServerAddr(url);
        influxdbInfo.setStatus(StatusEnums.UNKNOWN.getCode());
        influxdbInfo.setDescription(StatusEnums.UNKNOWN.getDesc());
    }

    /**
     * 记录Influxdb连接信息
     *
     * @param createdCount
     * @param destroyedCount
     * @param borrowedCount
     * @param returnedCount
     */
    public static void noteInfluxdb(long createdCount, long destroyedCount, long borrowedCount, long returnedCount) {
        influxdbInfo.setCreatedCount(createdCount);
        influxdbInfo.setDestroyedCount(destroyedCount);
        influxdbInfo.setBorrowedCount(borrowedCount);
        influxdbInfo.setReturnedCount(returnedCount);
    }

    /**
     * 记录Influxdb连接信息
     *
     * @param statusEnums
     */
    public static void noteInfluxdb(StatusEnums statusEnums) {
        influxdbInfo.setStatus(statusEnums.getCode());
        influxdbInfo.setDescription(statusEnums.getDesc());
    }

    /**
     * 记录Netty连接信息
     *
     * @param ip
     * @param port
     */
    public static void noteNetty(String ip, int port) {
        nettyInfo.setServerAddr(ip + ":" + port);
        nettyInfo.setConnectionMap(new ConcurrentHashMap<>());
    }

    /**
     * 记录Netty连接信息
     *
     * @param clientId
     */
    public static void noteNetty(String clientId) {
        if (!nettyInfo.getConnectionMap().containsKey(clientId)) {
            nettyInfo.getConnectionMap().put(clientId, nettyInfo.new Connection(clientId));
        }
        nettyInfo.getConnectionMap().get(clientId).setCreateTime(new Date());
        nettyInfo.getConnectionMap().get(clientId).setStatus(StatusEnums.NORMAL.getCode());
        nettyInfo.getConnectionMap().get(clientId).setDescription(StatusEnums.NORMAL.getDesc());
    }

    /**
     * 记录Netty连接信息
     *
     * @param clientId
     * @param statusEnums
     */
    public static void noteNetty(String clientId, StatusEnums statusEnums) {
        if (!nettyInfo.getConnectionMap().containsKey(clientId)) {
            noteNetty(clientId);
        }
        nettyInfo.getConnectionMap().get(clientId).setStatus(statusEnums.getCode());
        nettyInfo.getConnectionMap().get(clientId).setDescription(statusEnums.getDesc());
    }

    /**
     * 记录Netty连接信息
     *
     * @param clientId
     * @param statusEnums
     */
    public static void noteNetty(String clientId, StatusEnums statusEnums, Date activeTime) {
        if (!nettyInfo.getConnectionMap().containsKey(clientId)) {
            noteNetty(clientId);
        }
        nettyInfo.getConnectionMap().get(clientId).setStatus(statusEnums.getCode());
        nettyInfo.getConnectionMap().get(clientId).setDescription(statusEnums.getDesc());
        nettyInfo.getConnectionMap().get(clientId).setActiveTime(activeTime);
    }

    /**
     * 清除Netty连接信息
     *
     * @param clientId
     */
    public static void forgetNetty(String clientId) {
        nettyInfo.getConnectionMap().remove(clientId);
    }

    /**
     * 转化为打印字符串
     *
     * @return
     */
    public static String toPrintString() {
        StringBuffer sb = new StringBuffer();
        sb.append("\n");
        sb.append("系统启动时间：" + startTime + "\n");
        sb.append("系统状态：" + status + ", " + description + "\n");
        sb.append("线程信息：" + threadInfoMap + "\n");
        sb.append("InfluxDB信息：" + influxdbInfo + "\n");
        sb.append("Netty信息：" + nettyInfo + "\n");
        sb.append("Read Speed：" + FluxManager.getInstance().getFluxControl(FluxEnums.ReadData.getCode()).getSpeed() + "\n");
        sb.append("Push Speed：" + FluxManager.getInstance().getFluxControl(FluxEnums.PushData.getCode()).getSpeed() + "\n");
        sb.append("Total Read：" + StatisticCache.totalRead.get() + ", Total Push: " + StatisticCache.totalPush.get() + "\n");
        return sb.toString();
    }

    /**
     * 获取线程信息
     *
     * @return
     */
    public static List<ThreadInfo> getThreadInfo() {
        return new ArrayList<>(threadInfoMap.values());
    }

    /**
     * 获取内存队列信息
     *
     * @return
     */
    public static List<QueueInfo> getQueueInfo() {
        return new ArrayList<>(queueInfoMap.values());
    }
}
