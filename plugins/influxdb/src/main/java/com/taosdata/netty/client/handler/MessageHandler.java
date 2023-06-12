package com.taosdata.netty.client.handler;

import com.taosdata.caches.MessageCache;
import com.taosdata.netty.model.dto.MessageDto;
import com.taosdata.netty.model.enums.MessageTypeEnums;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty客户端消息处理器
 *
 * @author ZYP
 */
public class MessageHandler extends ChannelInboundHandlerAdapter {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void channelRead(ChannelHandlerContext channelHandlerContext, Object msg) throws Exception {
        logger.debug("receive message: {}", msg.toString());
        /*
        // 对象类型正确则处理
        if (msg instanceof MessageDto) {
            MessageDto messageDto = (MessageDto) msg;
            // 判断消息类型
            if (messageDto.getMsgType() == 0) {
                // 类型错误，立即发送失败响应
                sendTypeErrorResponse(channelHandlerContext, messageDto);
            } else if (messageDto.getMsgType() == MessageTypeEnums.MSG_REQ.getValue()) {
                // 记入接收消息队列
                MessageCache.addReqMessage(messageDto);
            } else if (messageDto.getMsgType() == MessageTypeEnums.MSG_RES.getValue()) {
                // TODO 查找发送记录，判断结果是否符合预期
            }
        }
        super.channelRead(channelHandlerContext, msg);
        */
        super.channelRead(channelHandlerContext, msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) {
        channelHandlerContext.close();
    }

    /**
     * 发送类型失败响应
     *
     * @param channelHandlerContext
     * @param req
     */
    private void sendTypeErrorResponse(ChannelHandlerContext channelHandlerContext, MessageDto req) {
        // 封装消息体
        MessageDto messageDto = new MessageDto();
        messageDto.setVersion(req.getVersion());
        messageDto.setMsgType(MessageTypeEnums.MSG_RES.getValue());
        messageDto.setSeq(req.getSeq());
        messageDto.setBody(new byte[0]);
        // 发送消息
        channelHandlerContext.writeAndFlush(messageDto);
    }
}
