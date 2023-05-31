package com.taosdata.netty.encoder;

import com.taosdata.netty.model.dto.MessageDto;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Netty客户端消息编码器
 *
 * @author ZYP
 */
public class MessageEncoder extends MessageToByteEncoder<MessageDto> {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, MessageDto messageDto, ByteBuf out) throws Exception {
        // TODO 目前仅发送apache arrow字节流
        out.writeBytes(messageDto.getBody());
        // 将字节流输出到log文件
        logger.info("push byte array on socket: {}, bytes: {}", channelHandlerContext.channel().id(), Arrays.toString(messageDto.getBody()));
    }
}
