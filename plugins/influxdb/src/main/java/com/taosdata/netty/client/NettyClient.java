package com.taosdata.netty.client;

import com.taosdata.caches.BucketDataCache;
import com.taosdata.caches.StatusCache;
import com.taosdata.model.dto.bum.ThreadInfo;
import com.taosdata.model.enums.StatusEnums;
import com.taosdata.netty.client.config.NettyClientConfig;
import com.taosdata.netty.client.initializer.ClientChannelInitializer;
import com.taosdata.threads.PushThread;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Netty客户端
 *
 * @author ZYP
 */
@Component
public class NettyClient {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private NettyClientConfig nettyConfig;

    @Resource
    private ClientChannelInitializer clientChannelInitializer;

    public void run(String dataSourceKey) {
        // 异步线程
        Thread waitThread = new Thread(() -> {
            // 创建配置参数
            Bootstrap bootstrap = new Bootstrap();
            // 创建workGroup用于接收消息数据
            EventLoopGroup workGroup = new NioEventLoopGroup();
            // 建立连接
            connect(bootstrap, workGroup, dataSourceKey);
        });
        waitThread.start();
    }

    /**
     * 建立socket连接
     *
     * @param bootstrap
     * @param workGroup
     */
    public void connect(Bootstrap bootstrap, EventLoopGroup workGroup, String dataSourceKey) {
        try {
            // 传入当前客户端
            this.clientChannelInitializer.setNettyClient(NettyClient.this);
            // 配置参数
            bootstrap
                    // 绑定处理Group
                    .group(workGroup)
                    // Socket通道类
                    .channel(NioSocketChannel.class)
                    // 保持连接
                    .option(ChannelOption.SO_KEEPALIVE, this.nettyConfig.isSoKeepalive())
                    // 有数据立即发送
                    .option(ChannelOption.TCP_NODELAY, this.nettyConfig.isTcpNoDelay())
                    // 处理新连接
                    .handler(this.clientChannelInitializer);
            // 请求的地址
            InetSocketAddress inetSocketAddress = new InetSocketAddress(this.nettyConfig.getHost(), this.nettyConfig.getPort());
            // 建立连接并且添加监听
            ChannelFuture channelFuture = bootstrap.connect(inetSocketAddress).addListener((ChannelFuture listener) -> {
                // 当前执行器
                final EventLoop eventLoop = listener.channel().eventLoop();
                // 客户端ID
                String clientId = listener.channel().id().asShortText();
                // 判断是否连接成功
                if (listener.isSuccess()) {
                    // 记录Netty连接信息
                    StatusCache.noteNetty(clientId);
                    // 将Socket连接记录到推送数据缓存信息中
                    BucketDataCache.socketMap.put(dataSourceKey, listener.channel());
                    // 线程名
                    String threadName = "PushThread-" + clientId;
                    // 启动线程PushThread
                    PushThread push = new PushThread(dataSourceKey, listener.channel());
                    Thread pushThread = new Thread(push);
                    pushThread.setName(threadName);
                    pushThread.start();
                    ThreadInfo threadInfo = new ThreadInfo();
                    threadInfo.setName(threadName);
                    threadInfo.setStartTime(new Date());
                    threadInfo.setStatus(StatusEnums.LOADING.getCode());
                    threadInfo.setDescription(StatusEnums.LOADING.getDesc());
                    StatusCache.noteThread(threadInfo);
                    logger.info("Successfully established connection and created sending thread，thread: {}", threadName);
                } else {
                    logger.error("Failed to establish connection, will reconnect in 5 seconds.");
                    listener.channel().eventLoop().schedule(() -> connect(new Bootstrap(), eventLoop, dataSourceKey), 5, TimeUnit.SECONDS);
                    // 删除Netty连接信息
                    StatusCache.forgetNetty(clientId);
                }
            });
            // 监听到结束信号后关闭
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            logger.error("Netty client startup failed", e);
        } finally {
            // 如果加了这行就没有重连机制
            // workGroup.shutdownGracefully();
        }
    }
}
