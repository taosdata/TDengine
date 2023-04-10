package com.taosdata.netty.encoder;

import com.taosdata.netty.model.dto.MessageDto;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Netty客户端消息编码器
 *
 * @author ZYP
 */
public class MessageEncoder extends MessageToByteEncoder<MessageDto> {

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, MessageDto messageDto, ByteBuf out) throws Exception {
//        ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(out);
//        byteBufOutputStream.writeInt(NettyConsts.MAGIC);
//        byteBufOutputStream.writeByte(messageDto.getVersion());
//        byteBufOutputStream.writeByte(messageDto.getMsgType());
//        byteBufOutputStream.writeLong(messageDto.getSeq());
//        if (messageDto.getBody() == null || messageDto.getBody().length == 0) {
//            byteBufOutputStream.writeInt(0);
//        } else {
//            byteBufOutputStream.writeInt(messageDto.getBody().length);
//            byteBufOutputStream.write(messageDto.getBody());
//        }
        out.writeBytes(messageDto.getBody());
    }
}
