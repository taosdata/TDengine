package com.taosdata.netty.decoder;

import com.taosdata.netty.consts.NettyConsts;
import com.taosdata.netty.model.dto.MessageDto;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        // 判断长度：magic[4]+version[1]+type[1]+seq[8]+length[4]
        if (in.readableBytes() < 18) {
            // 位数不够，直接返回
            logger.info("字节流位数不足，忽略，length={}", in.readableBytes());
            return;
        }
        // 标记开始读取位置
        in.markReaderIndex();
        // 读取专属消息标志
        int magic = in.readInt();
        // 判断是否合法，否则断连并退出
        if (magic != NettyConsts.MAGIC) {
            channelHandlerContext.close();
            return;
        }
        // 依次读取版本号、消息类型、序列号、消息体长度
        byte version = in.readByte();
        byte msgType = in.readByte();
        long seq = in.readLong();
        int length = in.readInt();
        // 判断长度
        if (length < 0) {
            logger.info("字节流位数错误，忽略，length={}", length);
            channelHandlerContext.close();
            return;
        } else if (in.readableBytes() < length) {
            logger.info("字节流位数不足，恢复到开始读取位置，length={}", in.readableBytes());
            in.resetReaderIndex();
            return;
        }
        // 读取消息体
        byte[] body = new byte[length];
        in.readBytes(body);
        // 封装消息
        MessageDto messageDto = new MessageDto();
        messageDto.setVersion(version);
        messageDto.setMsgType(msgType);
        messageDto.setSeq(seq);
        messageDto.setBody(body);
        // 输出结果
        out.add(messageDto);
    }
}
