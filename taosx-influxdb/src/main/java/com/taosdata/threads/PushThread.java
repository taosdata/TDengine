package com.taosdata.threads;

import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.BucketDataCache;
import com.taosdata.caches.StatisticCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.model.entity.InfluxdbBucketDataEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.netty.consts.NettyConsts;
import com.taosdata.netty.model.dto.MessageDto;
import com.taosdata.netty.model.enums.MessageTypeEnums;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.arrow.ArrowUtils;
import com.taosdata.utils.flux.FluxEnums;
import com.taosdata.utils.flux.FluxManager;
import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 推送数据线程
 *
 * @author ZYP
 */
public class PushThread implements Runnable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 线程名
     */
    private String name;

    /**
     * 数据源Bucket,Measurement,Table
     */
    private String dataSourceKey;

    /**
     * Socket通道
     */
    private Channel channel;

    public PushThread(String dataSourceKey, Channel channel) {
        this.dataSourceKey = dataSourceKey;
        this.channel = channel;
    }

    /**
     * 性能配置
     */
    private PerformanceConfig performanceConfig = ApplicationContextProvider.getBean(PerformanceConfig.class);

    /**
     * 批次首条
     */
    private boolean first = true;

    /**
     * 当前线程/schema的arrow工具类
     */
    private ArrowUtils arrowUtils = null;

    @Override
    public void run() {
        while (this.channel.isOpen()) {
            long start = System.currentTimeMillis();
            try {
                this.name = Thread.currentThread().getName();
                if (StringUtils.isEmpty(this.name)) {
                    this.name = "PushThread";
                }
                logger.debug(this.name + "#线程运行开始#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                // 读取内存中的数据
                List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList = BucketDataCache.getBucketData(this.dataSourceKey, this.performanceConfig.getLimitBatch());
                // 判断是否读到数据
                if (influxdbBucketDataEntityList == null || influxdbBucketDataEntityList.size() == 0) {
                    // 睡眠后继续
                    sleep(this.performanceConfig.getThread().getPushEmptyInterval(), start, StatusEnums.NORMAL);
                    continue;
                }
                // 速度控制
                FluxManager.getInstance().getFluxControl(FluxEnums.PushData.getCode()).cycleCheck(influxdbBucketDataEntityList.size(), this.performanceConfig.getLimitSpeed());
                // 推送数据
                push(influxdbBucketDataEntityList);
                // 线程结束
                sleep(this.performanceConfig.getThread().getPushInterval(), start, StatusEnums.NORMAL);
            } catch (InterruptedException e) {
                exception(start, StatusEnums.EXCEPTION, e);
                break;
            } catch (Exception e) {
                exception(start, StatusEnums.EXCEPTION, e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    this.logger.error(this.name + "#线程睡眠异常#" + e.getMessage(), e);
                }
            }
        }
        exit();
    }

    /**
     * 线程睡眠
     *
     * @param interval
     * @param start
     * @param statusEnums
     * @throws InterruptedException
     */
    private void sleep(long interval, long start, StatusEnums statusEnums) throws InterruptedException {
        // 线程结束
        long end = System.currentTimeMillis();
        this.logger.debug(this.name + "#线程运行结束（耗时" + (end - start) + "ms）#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
        // 记录线程信息
        StatusCache.noteThread(this.name, start, end, statusEnums.getCode(), statusEnums.getDesc());
        // 睡眠
        Thread.sleep(interval);
    }

    /**
     * 线程异常
     *
     * @param start
     * @param e
     */
    private void exception(long start, StatusEnums statusEnums, Exception e) {
        // 线程结束
        long end = System.currentTimeMillis();
        this.logger.error(this.name + "#线程运行异常（耗时" + (end - start) + "ms）#" + e.getMessage(), e);
        // 记录线程信息
        StatusCache.noteThread(this.name, start, end, statusEnums.getCode(), statusEnums.getDesc() + ": " + e.getMessage());
    }

    /**
     * 线程结束
     */
    private void exit() {
        // 线程结束
        this.logger.info(this.name + "#线程正常退出#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
        // 清除线程信息
        StatusCache.forgetThread(this.name);
    }

    /**
     * 推送数据到taosx
     *
     * @param influxdbBucketDataEntityList
     */
    private void push(List<InfluxdbBucketDataEntity> influxdbBucketDataEntityList) {
        // TODO 获取并判断响应
        try {
            // 根据Measurement获取arrow初始化信息
            if (this.first || this.arrowUtils == null) {
                // 判断数据非空且首条数据包含Measurement信息
                if (influxdbBucketDataEntityList != null && influxdbBucketDataEntityList.size() > 0 && influxdbBucketDataEntityList.get(0).getInfluxdbMeasurementEntity() != null) {
                    this.arrowUtils = new ArrowUtils(influxdbBucketDataEntityList.get(0).getInfluxdbMeasurementEntity());
                } else {
                    return;
                }
            }
            MessageDto messageDto = new MessageDto();
            messageDto.setVersion(NettyConsts.VERSION);
            messageDto.setMsgType(MessageTypeEnums.MSG_REQ.getValue());
            messageDto.setBody(this.arrowUtils.transform(influxdbBucketDataEntityList, this.first));
            this.channel.writeAndFlush(messageDto);
            // 修改当前线程/schema的首条标记
            this.first = false;
            // 记录统计信息
            StatisticCache.totalPush.addAndGet(influxdbBucketDataEntityList.size());
        } catch (Exception e) {
            this.logger.error("推送数据失败，重新写回内存队列", e);
            // 写回
            BucketDataCache.addBucketData(influxdbBucketDataEntityList);
        }
    }
}
