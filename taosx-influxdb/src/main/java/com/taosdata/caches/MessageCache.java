package com.taosdata.caches;

import com.taosdata.netty.model.dto.MessageDto;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 消息缓存
 *
 * @author ZYP
 */
public class MessageCache {

    private static Queue<MessageDto> reqMessageQueue = new ConcurrentLinkedQueue<>();

    private static Queue<MessageDto> resMessageQueue = new ConcurrentLinkedQueue<>();

    /**
     * 添加请求消息并获取队列大小
     *
     * @param messageDto
     * @return
     */
    public static int addReqMessage(MessageDto messageDto) {
        // 放入队列中
        reqMessageQueue.add(messageDto);
        // 返回当前队列大小
        return reqMessageQueue.size();
    }

    /**
     * 添加响应消息并获取队列大小
     *
     * @param messageDto
     * @return
     */
    public static int addResMessage(MessageDto messageDto) {
        // 放入队列中
        resMessageQueue.add(messageDto);
        // 返回当前队列大小
        return resMessageQueue.size();
    }

    /**
     * 获取请求消息
     *
     * @return
     */
    public static MessageDto getReqMessage() {
        return reqMessageQueue.poll();
    }

    /**
     * 获取响应消息
     *
     * @return
     */
    public static MessageDto getResMessage() {
        return resMessageQueue.poll();
    }

    /**
     * 获取请求消息队列大小
     *
     * @return
     */
    public static int getReqMessageQueueSize() {
        return reqMessageQueue.size();
    }

    /**
     * 获取响应消息队列大小
     *
     * @return
     */
    public static int getResMessageQueueSize() {
        return resMessageQueue.size();
    }
}
