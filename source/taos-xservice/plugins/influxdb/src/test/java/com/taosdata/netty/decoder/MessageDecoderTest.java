package com.taosdata.netty.decoder;

import com.taosdata.netty.consts.NettyConsts;
import com.taosdata.netty.model.dto.MessageDto;
import com.taosdata.netty.model.enums.MessageTypeEnums;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MessageDecoderTest {

    @Test
    void decode_shouldSkipDebugLogWhenDebugDisabled() {
        MessageDecoder decoder = new MessageDecoder();
        Logger logger = mock(Logger.class);
        decoder.logger = logger;
        when(logger.isDebugEnabled()).thenReturn(false);

        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        byte[] payload = new byte[]{1, 2, 3};
        try {
            Assertions.assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(payload)));
            MessageDto message = channel.readInbound();
            Assertions.assertNotNull(message);
            Assertions.assertEquals(NettyConsts.VERSION, message.getVersion());
            Assertions.assertEquals(MessageTypeEnums.MSG_RES.getValue(), message.getMsgType());
            Assertions.assertArrayEquals(payload, message.getBody());
            verify(logger).isDebugEnabled();
            verify(logger, never()).debug(anyString(), any(), any());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void decode_shouldLogByteArrayWhenDebugEnabled() {
        MessageDecoder decoder = new MessageDecoder();
        Logger logger = mock(Logger.class);
        decoder.logger = logger;
        when(logger.isDebugEnabled()).thenReturn(true);

        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        byte[] payload = new byte[]{4, 5, 6};
        try {
            Assertions.assertTrue(channel.writeInbound(Unpooled.wrappedBuffer(payload)));
            MessageDto message = channel.readInbound();
            Assertions.assertNotNull(message);
            verify(logger).isDebugEnabled();
            verify(logger).debug(eq("receive byte array on socket: {}, bytes: {}"), any(), eq(Arrays.toString(payload)));
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
