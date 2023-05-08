package com.taosdata.threads;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taosdata.ApplicationContextProvider;
import com.taosdata.caches.MessageCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.config.PerformanceConfig;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.netty.model.dto.MessageBodyDto;
import com.taosdata.netty.model.dto.MessageBodyInfluxdbDto;
import com.taosdata.netty.model.dto.MessageDto;
import com.taosdata.netty.model.enums.MessageTypeEnums;
import com.taosdata.utils.DateUtils;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TaosX消息处理线程
 *
 * @author ZYP
 */
public class MessageThread implements Runnable {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 线程名
     */
    private String name;

    public MessageThread() {
    }

    /**
     * 性能配置
     */
    private PerformanceConfig performanceConfig = ApplicationContextProvider.getBean(PerformanceConfig.class);

    @Override
    public void run() {
        while (true) {
            long start = System.currentTimeMillis();
            try {
                this.name = Thread.currentThread().getName();
                if (StringUtils.isEmpty(this.name)) {
                    this.name = "MessageThread";
                }
                logger.debug(this.name + "#线程运行开始#" + DateUtils.getTime(DateUtils.DATE_FORMAT_15));
                // 取出内存中消息
                MessageDto messageDto = MessageCache.getReqMessage();
                // 判断消息类型并解析消息包体
                if (messageDto == null) {
                    // 睡眠后继续
                    sleep(this.performanceConfig.getThread().getProcessMessageEmptyInterval(), start, StatusEnums.NORMAL);
                    continue;
                } else if (messageDto.getMsgType() == MessageTypeEnums.MSG_REQ.getValue()) {
                    // 服务端请求序列号
                    long seq = messageDto.getSeq();
                    // 服务端主动请求的数据
                    byte[] body = messageDto.getBody();
                    // 解析
                    if (body != null && body.length > 0) {
                        processMessage(seq, body);
                    } else {
                        logger.error("服务端REQ消息内容为空，不予处理，Message={}", messageDto.toString());
                    }
                } else if (messageDto.getMsgType() == MessageTypeEnums.MSG_RES.getValue()) {
                    // 服务端返回的响应数据
                    // TODO
                }
                // 线程结束
                sleep(this.performanceConfig.getThread().getProcessMessageInterval(), start, StatusEnums.NORMAL);
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
     * 识别消息类型并进行相应的处理
     *
     * @param seq
     * @param body
     */
    private void processMessage(long seq, byte[] body) {
        try {
            // 将消息体转换为字符串
            String bodyStr = new String(body, CharsetUtil.UTF_8);
            // 使用抽象类解析
            MessageBodyDto messageBodyDto = new ObjectMapper().readValue(bodyStr, MessageBodyDto.class);
            // 判断消息类型
            if (messageBodyDto == null) {
                logger.error("解析为未定义的消息体类型，bodyStr=" + bodyStr);
            } else if (messageBodyDto instanceof MessageBodyInfluxdbDto) {
                // Influxdb信息，创建Influxdb连接并启动BucketThread线程与ScheduleThread线程
                // initInfluxdb((MessageBodyInfluxdbDto) messageBodyDto);
            }
            // TODO 生成响应
        } catch (Exception e) {
            logger.error("解析消息体过程中发生异常，body=" + body, e);
        }
    }
}
