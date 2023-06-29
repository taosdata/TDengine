package com.taosdata.threads;

import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.BucketCache;
import com.taosdata.caches.StatisticCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.LocalConfig;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.config.TaskConfig;
import com.taosdata.model.entity.InfluxdbMeasurementEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.service.InfluxdbService;
import com.taosdata.service.impl.InfluxdbServiceImpl;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.exception.ArtificialException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Bucket数据读取任务创建线程
 *
 * @author ZYP
 */
public class BucketThread implements Runnable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 线程名
     */
    private String name;

    /**
     * influxdb orgId
     */
    private String orgId;

    /**
     * influxdb bucket
     */
    private String bucket;

    public BucketThread(String orgId, String bucket) {
        this.orgId = orgId;
        this.bucket = bucket;
    }

    /**
     * 任务配置
     */
    private TaskConfig taskConfig = ApplicationContextProvider.getBean(TaskConfig.class);

    /**
     * 性能配置
     */
    private PerformanceConfig performanceConfig = ApplicationContextProvider.getBean(PerformanceConfig.class);

    /**
     * influxdb数据库操作
     */
    private InfluxdbService influxdbService = ApplicationContextProvider.getBean(InfluxdbServiceImpl.class);

    /**
     * 当前已经处理完第几个，结合beginTime与readWindow确定读取时间
     */
    private int index = 0;

    /**
     * 上次结束时间，当上次窗口不完整时依此进行调整
     */
    private long lastEnd = 0L;

    @Override
    public void run() {
        while (LocalConfig.isRunBucketThread) {
            long start = System.currentTimeMillis();
            try {
                this.name = Thread.currentThread().getName();
                if (StringUtils.isEmpty(this.name)) {
                    this.name = "BucketThread";
                }
                logger.debug(this.name + "#线程运行开始#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                // 判断内存中bucket子线程队列大小
                if (BucketCache.getBucketDataThreadQueueSize(this.bucket) >= this.performanceConfig.getQueueSizeT()) {
                    // 睡眠后继续
                    sleep(this.performanceConfig.getThread().getCreateBucketFullInterval(), start, StatusEnums.NORMAL);
                    continue;
                }
                // 处理新增的measurement
                additionalMeasurement();
                // 下一个时间段，英文逗号分割
                String timeRange = getTimeRange(this.index);
                // 字符串格式不正确则睡眠后继续（应该是没有任务了）
                if (StringUtils.isEmpty(timeRange) || timeRange.indexOf(",") <= 0) {
                    // 如果设置了endTime并且now>endTime并且任务已运行完成，正常退出进程
                    if (StringUtils.isNotEmpty(taskConfig.getEndTime())) {
                        // 判断是否可以退出进程
                        if (StatisticCache.createdTaskSet.size() >= StatisticCache.totalReadTaskEstimated && StatisticCache.completedTaskSet.size() >= StatisticCache.createdTaskSet.size() && StatisticCache.totalPush.get() >= StatisticCache.totalRead.get()) {
                            Thread.sleep(5000L);
                            logger.info("任务执行完成，正常退出");
                            System.exit(0);
                        }
                    }
                    // 睡眠后继续
                    sleep(this.performanceConfig.getThread().getCreateBucketFullInterval(), start, StatusEnums.NORMAL);
                    continue;
                }
                // 拆分时间段
                String[] timeRangeArr = timeRange.split(",");
                // 生成bucket子线程并放入队列中
                BucketCache.measurementMap.forEach((k, v) -> {
                    // 如果任务中指定了measurement则过滤
                    if (taskConfig.getMeasurements().size() > 0 && !taskConfig.getMeasurements().contains(v.getMeasurement())) {
                        return;
                    }
                    if (this.bucket.equals(v.getBucket()) && StringUtils.isNotEmpty(v.getMeasurement())) {
                        BucketCache.addBucketDataThread(this.bucket, new BucketDataThread(this.orgId, this.bucket, v.getMeasurement(), timeRangeArr[0], timeRangeArr[1]));
                        // 读取数据任务计数
                        StatisticCache.noteCreatedTask(this.bucket, v.getMeasurement(), timeRangeArr[0], timeRangeArr[1]);
                    }
                });
                // 更新序号
                this.index++;
                // 线程结束
                sleep(this.performanceConfig.getThread().getCreateBucketInterval(), start, StatusEnums.NORMAL);
            } catch (InterruptedException e) {
                exception(start, StatusEnums.EXCEPTION, e);
                break;
            } catch (Exception e) {
                exception(start, StatusEnums.EXCEPTION, e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    logger.error(this.name + "#线程睡眠异常#" + e.getMessage(), e);
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
        logger.debug(this.name + "#线程运行结束（耗时" + (end - start) + "ms）#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
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
        logger.error(this.name + "#线程运行异常（耗时" + (end - start) + "ms）#" + e.getMessage(), e);
        // 记录线程信息
        StatusCache.noteThread(this.name, start, end, statusEnums.getCode(), statusEnums.getDesc() + ": " + e.getMessage());
    }

    /**
     * 线程结束
     */
    private void exit() {
        // 线程结束
        logger.info(this.name + "#线程正常退出#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
        // 清除线程信息
        StatusCache.forgetThread(this.name);
    }

    /**
     * 处理新增的measurement
     *
     * @return
     */
    private void additionalMeasurement() throws ArtificialException {
        // 查询bucket中所有measurement信息
        List<InfluxdbMeasurementEntity> influxdbMeasurementEntityList = influxdbService.selectAllMeasurements(this.bucket);
        // 判断结果空
        if (influxdbMeasurementEntityList == null || influxdbMeasurementEntityList.size() == 0) {
            return;
        }
        // 遍历判断是否有新增
        influxdbMeasurementEntityList.forEach(influxdbMeasurementEntity -> {
            if (influxdbMeasurementEntity == null || StringUtils.isEmpty(influxdbMeasurementEntity.getMeasurement())) {
                return;
            }
            String measurement = influxdbMeasurementEntity.getMeasurement();
            // 如果不在缓存中
            if (!BucketCache.measurementMap.containsKey(this.bucket + ":" + measurement)) {
                // 添加从0到index-1的所有时间段
                for (int i = 0; i < this.index; i++) {
                    try {
                        // 获取时间段
                        String timeRange = getTimeRange(i);
                        // 字符串格式不正确则继续
                        if (StringUtils.isEmpty(timeRange) || timeRange.indexOf(",") <= 0) {
                            continue;
                        }
                        // 拆分时间段
                        String[] timeRangeArr = timeRange.split(",");
                        // 生成bucket子线程并放入队列中
                        BucketCache.addBucketDataThread(this.bucket, new BucketDataThread(this.orgId, this.bucket, measurement, timeRangeArr[0], timeRangeArr[1]));
                        // 读取数据任务计数
                        StatisticCache.noteCreatedTask(this.bucket, measurement, timeRangeArr[0], timeRangeArr[1]);
                    } catch (Exception e) {
                        logger.error(this.name + "#线程运行异常#" + e.getMessage(), e);
                    }
                }
                // 添加到缓存中
                BucketCache.measurementMap.put(this.bucket + ":" + measurement, influxdbMeasurementEntity);
            }
        });
    }

    /**
     * 获取时间范围
     *
     * @param index
     * @return
     * @throws Exception
     */
    private String getTimeRange(int index) throws Exception {
        // 获取配置信息
        String beginTime = this.taskConfig.getBeginTime();
        String endTime = this.taskConfig.getEndTime();
        String readWindow = this.performanceConfig.getReadWindow();
        // 判断开始时间与结束时间
        if (StringUtils.isEmpty(beginTime)) {
            throw new Exception("parameter beginTime configuration error.");
        } else if (beginTime.matches(DateUtils.PATTERN_YMD)) {
            beginTime += " 00:00:00";
        } else if (!beginTime.matches(DateUtils.PATTERN_YMDHMS)) {
            throw new Exception("parameter beginTime configuration error.");
        }
        if (StringUtils.isEmpty(endTime)) {
            endTime = DateUtils.getTime(DateUtils.DATE_FORMAT_15, TimeZone.getTimeZone("GMT"));
        } else if (endTime.matches(DateUtils.PATTERN_YMD)) {
            endTime += " 23:59:59";
        } else if (!endTime.matches(DateUtils.PATTERN_YMDHMS)) {
            throw new Exception("parameter endTime configuration error.");
        }
        // 转换格式
        Date begin = DateUtils.stringToDate(beginTime, DateUtils.DATE_FORMAT_15, TimeZone.getTimeZone("GMT"));
        Date end = DateUtils.stringToDate(endTime, DateUtils.DATE_FORMAT_15, TimeZone.getTimeZone("GMT"));
        // 默认按天拆分
        if (StringUtils.isEmpty(readWindow)) {
            readWindow = "D";
        }
        // 根据不同拆分方式得到相应计算结果
        if (readWindow.equalsIgnoreCase("D")) {
            return getTimeRangeByDay(begin, end, index);
        } else if (readWindow.equalsIgnoreCase("H")) {
            return getTimeRangeByHour(begin, end, index);
        } else if (readWindow.equalsIgnoreCase("M")) {
            return getTimeRangeByMinute(begin, end, index);
        } else {
            throw new Exception("parameter readWindow configuration error.");
        }
    }

    /**
     * 根据天得到时间段
     *
     * @param beginTime
     * @param endTime
     * @param index
     * @return
     */
    private String getTimeRangeByDay(Date beginTime, Date endTime, int index) {
        // 根据index计算开始时间与结束时间
        long begin = beginTime.getTime() + index * 24 * 60 * 60 * 1000;
        long end = begin + 24 * 60 * 60 * 1000;
        // 判断上次结束时间是否完成一个窗口，未完成则回退窗口并继续
        if (begin > this.lastEnd && this.lastEnd > 0) {
            return resumeByLastEnd(begin, endTime.getTime());
        }
        // 判断是否超过指定时间范围
        if (begin >= endTime.getTime()) {
            return null;
        }
        // 调整结束时间
        if (end > endTime.getTime()) {
            end = endTime.getTime();
        }
        // 更新lastEnd
        this.lastEnd = end;
        // 返回时间范围
        return DateUtils.toOffsetDateTime(new Date(begin)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "," + DateUtils.toOffsetDateTime(new Date(end)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    /**
     * 根据小时得到时间段
     *
     * @param beginTime
     * @param endTime
     * @param index
     * @return
     */
    private String getTimeRangeByHour(Date beginTime, Date endTime, int index) {
        // 根据index计算开始时间与结束时间
        long begin = beginTime.getTime() + index * 60 * 60 * 1000;
        long end = begin + 60 * 60 * 1000;
        // 判断上次结束时间是否完成一个窗口，未完成则回退窗口并继续
        if (begin > this.lastEnd && this.lastEnd > 0) {
            return resumeByLastEnd(begin, endTime.getTime());
        }
        // 判断是否超过指定时间范围
        if (begin >= endTime.getTime()) {
            return null;
        }
        // 调整结束时间
        if (end > endTime.getTime()) {
            end = endTime.getTime();
        }
        // 更新lastEnd
        this.lastEnd = end;
        // 返回时间范围
        return DateUtils.toOffsetDateTime(new Date(begin)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "," + DateUtils.toOffsetDateTime(new Date(end)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    /**
     * 根据分钟得到时间段
     *
     * @param beginTime
     * @param endTime
     * @param index
     * @return
     */
    private String getTimeRangeByMinute(Date beginTime, Date endTime, int index) {
        // 根据index计算开始时间与结束时间
        long begin = beginTime.getTime() + index * 60 * 1000;
        long end = begin + 60 * 1000;
        // 判断上次结束时间是否完成一个窗口，未完成则回退窗口并继续
        if (begin > this.lastEnd && this.lastEnd > 0) {
            return resumeByLastEnd(begin, endTime.getTime());
        }
        // 判断是否超过指定时间范围
        if (begin >= endTime.getTime()) {
            return null;
        }
        // 调整结束时间
        if (end > endTime.getTime()) {
            end = endTime.getTime();
        }
        // 更新lastEnd
        this.lastEnd = end;
        // 返回时间范围
        return DateUtils.toOffsetDateTime(new Date(begin)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "," + DateUtils.toOffsetDateTime(new Date(end)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    /**
     * 恢复上次未完成的窗口
     *
     * @param newBegin
     * @param newEnd
     * @return
     */
    private String resumeByLastEnd(long newBegin, long newEnd) {
        // 以30秒为最小窗口，如果newEnd距上次结束已经超过30秒，则回退一个窗口并以lastEnd--min(newBegin, newEnd)为窗口，否则返回null
        if (newEnd > this.lastEnd + 30 * 1000) {
            // 回退窗口
            this.index--;
            // 以lastEnd为开始
            long begin = this.lastEnd;
            // 以min(newBegin, newEnd)为结束
            long end = Math.min(newBegin, newEnd);
            // 更新lastEnd
            this.lastEnd = end;
            // 返回时间范围
            return DateUtils.toOffsetDateTime(new Date(begin)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "," + DateUtils.toOffsetDateTime(new Date(end)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } else {
            return null;
        }
    }
}
