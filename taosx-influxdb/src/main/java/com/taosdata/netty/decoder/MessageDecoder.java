package com.taosdata.netty.decoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Netty客户端消息解码器
 *
 * @author ZYP
 */
public class MessageDecoder extends ByteToMessageDecoder {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf in, List<Object> out) {
        // 获取可读取长度
        int len = in.readableBytes();
        // 创建字节数组
        byte[] bytes = new byte[len];
        // 读取字节流
        in.readBytes(bytes);
        // TODO 目前不处理上行消息，仅将字节流输出到log文件
        logger.info("receive byte array on socket: {}, bytes: {}", channelHandlerContext.channel().id(), Arrays.toString(bytes));
    }
}
