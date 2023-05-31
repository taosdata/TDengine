package com.taosdata.utils;

import java.util.Date;

/**
 * 序列号工具类
 *
 * @author ZYP
 */
public class IdUtils {

    // 以2000-01-01 00:00:00为时间起点
    private long epoch = 946656000000L;
    // 应用节点
    private long workerId;
    // 数据中心
    private long dataCenterId;
    // 上次时间戳
    private long lastTimestamp = -1L;
    // 数字序列
    private long sequence = 1L;
    // 应用节点5位
    private long workerIdBits = 5L;
    // 数据中心5位
    private long dataCenterIdBits = 5L;
    // 数字序列12位
    private long sequenceBits = 12L;
    // 数字序列掩码，最大值循环4095
    private long sequenceMask = -1L ^ (-1L << sequenceBits);
    // 最大支持应用节点数0~31，一共32个
    private long maxWorkerId = -1L ^ (-1L << workerIdBits);
    // 最大支持数据中心数0~31，一共32个
    private long maxDataCenterId = -1L ^ (-1L << dataCenterIdBits);
    // 应用节点左移12位
    private long workerIdShift = sequenceBits;
    // 数据中心左移17位
    private long dataCenterIdShift = sequenceBits + workerIdBits;
    // 时间毫秒左移22位
    private long timestampLeftShift = sequenceBits + workerIdBits + dataCenterIdBits;

    /**
     * 无参构造方法
     */
    public IdUtils() {
        this(0L, 0L);
    }

    /**
     * 带参构造方法
     *
     * @param workerId
     * @param dataCenterId
     */
    public IdUtils(long workerId, long dataCenterId) {
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("应用节点ID不能大于%d或小于0", maxWorkerId));
        }
        if (dataCenterId > maxDataCenterId || dataCenterId < 0) {
            throw new IllegalArgumentException(String.format("数据中心ID不能大于%d或小于0", maxDataCenterId));
        }
        this.workerId = workerId;
        this.dataCenterId = dataCenterId;
    }

    /**
     * 生成序列号
     *
     * @return
     */
    public long nextId() {
        return nextId(new Date());
    }

    /**
     * 生成序列号（自定义时间）
     *
     * @param date
     * @return
     */
    public long nextId(Date date) {
        // 获取时间毫秒数
        long timestamp = date.getTime();
        // 如果上次生成时间与当前时间相同
        if (timestamp == lastTimestamp) {
            // sequence自增，因为sequence只有12位，所以相与sequenceMask一下，去掉高位
            sequence = (sequence + 1) & sequenceMask;
            // 判断是否溢出
            if (sequence == 0) {
                // 自动跳到下一毫秒
                timestamp++;
                // 重新计数
                sequence = 1L;
            }
        } else {
            // 如果与上次生成时间不同，sequence计数重新从1开始累加
            sequence = 1L;
        }
        lastTimestamp = timestamp;
        // 按照规则拼出ID：时间42位、应用节点5位、数据中心5位、数字序列12位
        // 000000000000000000000000000000000000000000 00000 00000 000000000000
        return ((timestamp - epoch) << timestampLeftShift) | (dataCenterId << dataCenterIdShift) | (workerId << workerIdShift) | sequence;
    }
}
