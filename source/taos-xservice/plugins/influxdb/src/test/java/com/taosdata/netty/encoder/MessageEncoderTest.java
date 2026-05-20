package com.taosdata.netty.encoder;

import io.netty.buffer.ByteBuf;
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

class MessageEncoderTest {

    @Test
    void encode_shouldSkipDebugLogWhenDebugDisabled() {
        MessageEncoder encoder = new MessageEncoder();
        Logger logger = mock(Logger.class);
        encoder.logger = logger;
        when(logger.isDebugEnabled()).thenReturn(false);

        EmbeddedChannel channel = new EmbeddedChannel(encoder);
        byte[] payload = new byte[]{7, 8, 9};
        try {
            Assertions.assertTrue(channel.writeOutbound(payload));
            ByteBuf encoded = channel.readOutbound();
            Assertions.assertNotNull(encoded);
            byte[] actual = new byte[encoded.readableBytes()];
            encoded.readBytes(actual);
            encoded.release();
            Assertions.assertArrayEquals(payload, actual);
            verify(logger).isDebugEnabled();
            verify(logger, never()).debug(anyString(), any(), any());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void encode_shouldLogByteArrayWhenDebugEnabled() {
        MessageEncoder encoder = new MessageEncoder();
        Logger logger = mock(Logger.class);
        encoder.logger = logger;
        when(logger.isDebugEnabled()).thenReturn(true);

        EmbeddedChannel channel = new EmbeddedChannel(encoder);
        byte[] payload = new byte[]{10, 11, 12};
        try {
            Assertions.assertTrue(channel.writeOutbound(payload));
            ByteBuf encoded = channel.readOutbound();
            Assertions.assertNotNull(encoded);
            encoded.release();
            verify(logger).isDebugEnabled();
            verify(logger).debug(eq("push byte array on socket: {}, bytes: {}"), any(), eq(Arrays.toString(payload)));
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
