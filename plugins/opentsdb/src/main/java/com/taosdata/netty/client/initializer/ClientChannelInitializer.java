package com.taosdata.netty.client.initializer;

import com.taosdata.netty.client.NettyClient;
import com.taosdata.netty.client.config.NettyClientConfig;
import com.taosdata.netty.client.handler.HeartHandler;
import com.taosdata.netty.client.handler.MessageHandler;
import com.taosdata.netty.decoder.MessageDecoder;
import com.taosdata.netty.encoder.MessageEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Setter;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * Netty服务端通道初始化
 *
 * @author ZYP
 */
@Component
public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Resource
    private NettyClientConfig nettyConfig;

    @Setter
    private NettyClient nettyClient;

    @Override
    protected void initChannel(SocketChannel socketChannel) {
        // 增加任务处理
        ChannelPipeline channelPipeline = socketChannel.pipeline();
        // 检测空闲
        channelPipeline.addLast("idleStateHandler", new IdleStateHandler(this.nettyConfig.getIdleReader(), this.nettyConfig.getIdleWriter(), this.nettyConfig.getIdleAll(), TimeUnit.SECONDS));
        // 添加消息解码器
        channelPipeline.addLast(new MessageDecoder());
        // 添加消息编码器
        channelPipeline.addLast(new MessageEncoder());
        // 添加消息处理器
        channelPipeline.addLast(new MessageHandler());
        // 添加心跳处理器
        channelPipeline.addLast("idleTimeoutHandler", new HeartHandler(this.nettyConfig, this.nettyClient));
    }
}
