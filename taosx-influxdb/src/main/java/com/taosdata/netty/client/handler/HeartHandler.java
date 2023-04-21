package com.taosdata.netty.client.handler;

import com.taosdata.caches.StatusCache;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.netty.client.NettyClient;
import com.taosdata.netty.client.config.NettyClientConfig;
import com.taosdata.netty.consts.NettyConsts;
import com.taosdata.netty.model.dto.MessageDto;
import com.taosdata.netty.model.enums.MessageTypeEnums;
import com.taosdata.utils.IdUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Netty客户端消息处理器
 *
 * @author ZYP
 */
public class HeartHandler extends ChannelInboundHandlerAdapter {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private NettyClientConfig nettyConfig;

    /**
     * 客户端类
     */
    private NettyClient nettyClient;

    /**
     * 序列号工具类
     */
    private IdUtils idUtils;

    /**
     * 发送心跳时间
     */
    private long heartTime = 0L;

    /**
     * 客户端连续N次收不到服务端的PONG消息
     */
    private int unPongTimes = 0;

    public HeartHandler(NettyClientConfig nettyConfig, NettyClient nettyClient) {
        this.nettyConfig = nettyConfig;
        this.nettyClient = nettyClient;
        this.idUtils = new IdUtils();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext channelHandlerContext, Object event) throws Exception {
        logger.info("Receive event: {}", event.toString());
        // 连接空闲
        /*
        if (event instanceof IdleStateEvent) {
            // 记录Netty连接信息
            StatusCache.noteNetty(channelHandlerContext.channel().id().asShortText(), StatusEnums.FAILED);
            // 判断失败次数，超出次数则断开连接
            if (this.unPongTimes < this.nettyConfig.getUnPongRetryTimes()) {
                // 发送心跳消息
                // sendPing(channelHandlerContext);
                // 计数增加
                this.unPongTimes++;
            } else {
                channelHandlerContext.channel().close();
            }
        } else {
            super.userEventTriggered(channelHandlerContext, event);
        }
        */
        super.userEventTriggered(channelHandlerContext, event);
    }

    /**
     * 处理断开重连
     *
     * @param channelHandlerContext
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext channelHandlerContext) throws Exception {
        // 清除Netty连接信息
        StatusCache.forgetNetty(channelHandlerContext.channel().id().asShortText());
        /*
        logger.error("检测到心跳服务断开，将在5秒后进行重连");
        final EventLoop eventLoop = channelHandlerContext.channel().eventLoop();
        eventLoop.schedule(() -> this.nettyClient.connect(new Bootstrap(), eventLoop), 5L, TimeUnit.SECONDS);
        */
        super.channelInactive(channelHandlerContext);
    }

    @Override
    public void channelRead(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
        // 记录Netty连接信息
        StatusCache.noteNetty(channelHandlerContext.channel().id().asShortText(), StatusEnums.NORMAL, new Date());
        /*
        // 对象类型正确则处理
        if (msg instanceof MessageDto) {
            MessageDto messageDto = (MessageDto) msg;
            // 处理PONG消息
            if (messageDto.getMsgType() == MessageTypeEnums.PONG.getValue()) {
                // 更新变量
                this.unPongTimes = 0;
                // 延迟
                long delay = (System.currentTimeMillis() - this.heartTime) / 2;
                // TODO 监控信息
                logger.info("客户端与服务端的PING时间为" + delay + "毫秒");
            }
        }
        */
        super.channelRead(channelHandlerContext, msg);
    }

    /**
     * 发送心跳消息
     *
     * @param channelHandlerContext
     */
    private void sendPing(ChannelHandlerContext channelHandlerContext) {
        // 更新心跳时间
        this.heartTime = System.currentTimeMillis();
        // 封装消息体
        MessageDto messageDto = new MessageDto();
        messageDto.setVersion(NettyConsts.VERSION);
        messageDto.setMsgType(MessageTypeEnums.PING.getValue());
        messageDto.setSeq(this.idUtils.nextId());
        messageDto.setBody(new byte[0]);
        // 发送消息
        channelHandlerContext.writeAndFlush(messageDto);
    }
}
