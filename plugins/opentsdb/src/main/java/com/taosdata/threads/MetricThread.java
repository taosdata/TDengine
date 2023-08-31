package com.taosdata.threads;

import com.alibaba.fastjson.JSONArray;
import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.MetricCache;
import com.taosdata.caches.StatisticCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.LocalConfig;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.config.TaskConfig;
import com.taosdata.model.entity.OpentsdbMetricEntity;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.service.OpentsdbService;
import com.taosdata.service.impl.OpentsdbServiceImpl;
import com.taosdata.utils.DateUtils;
import com.taosdata.utils.exception.ArtificialException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashSet;

/**
 * Metric数据读取任务创建线程
 *
 * @author ZYP
 */
public class MetricThread implements Runnable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 线程名
     */
    private String name;

    public MetricThread() {
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
     * opentsdb数据库操作
     */
    private OpentsdbService opentsdbService = ApplicationContextProvider.getBean(OpentsdbServiceImpl.class);

    /**
     * 当前已经处理完第几个，结合beginTime与readWindow确定读取时间
     */
    private int index = 0;

    /**
     * 上次结束时间，当上次窗口不完整时依此进行调整
     */
    private long lastEnd = 0L;

    /**
     * 任务结束时间，用于判断进程退出
     */
    private Date endTime = null;

    @Override
    public void run() {
        while (LocalConfig.isRunMetricThread) {
            long start = System.currentTimeMillis();
            try {
                this.name = Thread.currentThread().getName();
                if (StringUtils.isEmpty(this.name)) {
                    this.name = "MetricThread";
                }
                logger.debug(this.name + "#Thread start#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                // 判断内存中metric子线程队列大小
                if (MetricCache.getMetricDataThreadQueueSize() >= this.performanceConfig.getQueueSizeT()) {
                    // 睡眠后继续
                    sleep(this.performanceConfig.getThread().getCreateMetricFullInterval(), start, StatusEnums.NORMAL);
                    continue;
                }
                // 处理新增的metric
                additionalMetric();
                // 下一个时间段，英文逗号分割
                String timeRange = getTimeRange(this.index);
                // 字符串格式不正确则睡眠后继续（应该是没有任务了）
                if (StringUtils.isEmpty(timeRange) || timeRange.indexOf(",") <= 0) {
                    // 如果设置了endTime并且now>endTime并且任务已运行完成，正常退出进程
                    if (StringUtils.isNotEmpty(taskConfig.getEndTime()) && this.endTime.before(new Date())) {
                        // 判断是否可以退出进程
                        if (StatisticCache.createdTaskSet.size() >= StatisticCache.totalReadTaskEstimated && StatisticCache.completedTaskSet.size() >= StatisticCache.createdTaskSet.size() && StatisticCache.totalPush.get() >= StatisticCache.totalRead.get() && StatisticCache.totalResp.get() >= StatisticCache.totalPush.get()) {
                            Thread.sleep(5000L);
                            logger.info("Task execution completed, normal exit.");
                            logger.info(StatusCache.toPrintString());
                            System.exit(0);
                        }
                    }
                    // 睡眠后继续
                    sleep(this.performanceConfig.getThread().getCreateMetricFullInterval(), start, StatusEnums.NORMAL);
                    continue;
                }
                // 拆分时间段
                String[] timeRangeArr = timeRange.split(",");
                // 生成metric子线程并放入队列中
                MetricCache.metricMap.forEach((k, v) -> {
                    // 如果任务中指定了metric则过滤
                    if (taskConfig.getMetrics().size() > 0 && !taskConfig.getMetrics().contains(v.getMetric())) {
                        return;
                    }
                    if (StringUtils.isNotEmpty(v.getMetric())) {
                        MetricCache.addMetricDataThread(new MetricDataThread(v.getMetric(), timeRangeArr[0], timeRangeArr[1]));
                        // 读取数据任务计数
                        StatisticCache.noteCreatedTask(v.getMetric(), timeRangeArr[0], timeRangeArr[1]);
                    }
                });
                // 更新序号
                this.index++;
                // 线程结束
                sleep(this.performanceConfig.getThread().getCreateMetricInterval(), start, StatusEnums.NORMAL);
            } catch (InterruptedException e) {
                exception(start, StatusEnums.EXCEPTION, e);
                break;
            } catch (Exception e) {
                exception(start, StatusEnums.EXCEPTION, e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    logger.error(this.name + "#Thread sleep exception#" + e.getMessage(), e);
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
        logger.debug(this.name + "#Thread finished (Take time " + (end - start) + " ms)#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
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
        logger.error(this.name + "#Thread exception (Take time " + (end - start) + " ms)#" + e.getMessage(), e);
        // 记录线程信息
        StatusCache.noteThread(this.name, start, end, statusEnums.getCode(), statusEnums.getDesc() + ": " + e.getMessage());
    }

    /**
     * 线程结束
     */
    private void exit() {
        // 线程结束
        logger.info(this.name + "#Thread completed and exited#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
        // 清除线程信息
        StatusCache.forgetThread(this.name);
    }

    /**
     * 处理新增的metric
     *
     * @return
     */
    private void additionalMetric() throws ArtificialException {
        // 获取所有metric
        JSONArray metricArray = opentsdbService.fetchMetricList(null);
        // 判断结果空
        if (metricArray == null || metricArray.size() == 0) {
            return;
        }
        // 遍历判断是否有新增
        metricArray.forEach(metric -> {
            if (metric == null || StringUtils.isEmpty(metric.toString())) {
                return;
            }
            // 如果不在缓存中
            if (!MetricCache.metricMap.containsKey(metric)) {
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
                        // 生成metric子线程并放入队列中
                        MetricCache.addMetricDataThread(new MetricDataThread(metric.toString(), timeRangeArr[0], timeRangeArr[1]));
                        // 读取数据任务计数
                        StatisticCache.noteCreatedTask(metric.toString(), timeRangeArr[0], timeRangeArr[1]);
                    } catch (Exception e) {
                        logger.error(this.name + "#Thread exception#" + e.getMessage(), e);
                    }
                }
                // 添加到缓存中
                OpentsdbMetricEntity opentsdbMetricEntity = new OpentsdbMetricEntity();
                opentsdbMetricEntity.setMetric(metric.toString());
                opentsdbMetricEntity.setTagSet(new HashSet<>());
                MetricCache.metricMap.put(metric.toString(), opentsdbMetricEntity);
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
        } else if (!beginTime.matches(DateUtils.PATTERN_YMDHMS_TZ)) {
            throw new Exception("parameter beginTime configuration error.");
        }
        if (StringUtils.isEmpty(endTime)) {
            endTime = DateUtils.getTime(DateUtils.DATE_FORMAT_21);
        } else if (!endTime.matches(DateUtils.PATTERN_YMDHMS_TZ)) {
            throw new Exception("parameter endTime configuration error.");
        }
        // 转换格式
        Date begin = DateUtils.stringWithZoneToDate(beginTime);
        Date end = DateUtils.stringWithZoneToDate(endTime);
        this.endTime = end;
        // 默认按分钟拆分
        if (StringUtils.isEmpty(readWindow)) {
            readWindow = "M";
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
        // 当前时间
        long now = new Date().getTime();
        // 如果begin晚于now，返回空
        if (begin >= now) {
            return null;
        }
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
            // end晚于endTime，改为endTime
            end = endTime.getTime();
        } else if (end > now) {
            // end晚于now，改为now（设置了一个晚于now的endTime）
            end = now;
        }
        // 更新lastEnd
        this.lastEnd = end;
        // 返回时间范围
        return new Date(begin).getTime() + "," + new Date(end).getTime();
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
        // 当前时间
        long now = new Date().getTime();
        // 如果begin晚于now，返回空
        if (begin >= now) {
            return null;
        }
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
            // end晚于endTime，改为endTime
            end = endTime.getTime();
        } else if (end > now) {
            // end晚于now，改为now（设置了一个晚于now的endTime）
            end = now;
        }
        // 更新lastEnd
        this.lastEnd = end;
        // 返回时间范围
        return new Date(begin).getTime() + "," + new Date(end).getTime();
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
        // 当前时间
        long now = new Date().getTime();
        // 如果begin晚于now，返回空
        if (begin >= now) {
            return null;
        }
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
            // end晚于endTime，改为endTime
            end = endTime.getTime();
        } else if (end > now) {
            // end晚于now，改为now（设置了一个晚于now的endTime）
            end = now;
        }
        // 更新lastEnd
        this.lastEnd = end;
        // 返回时间范围
        return new Date(begin).getTime() + "," + new Date(end).getTime();
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
            return new Date(begin).getTime() + "," + new Date(end).getTime();
        } else {
            return null;
        }
    }
}
