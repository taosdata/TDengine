
/*
 * Copyright Copyright 2020-present Marvin Liao <coldljy@163.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.extensions.taosdata;

import com.alibaba.fastjson.JSONObject;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.packets.publish.PublishPacket;
import com.hivemq.extensions.taosdata.configuration.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * This is a very simple {@link PublishInboundInterceptor},
 * it changes the payload of every incoming PUBLISH with the topic 'hello/world' to 'Hello World!'.
 *
 * @author Yannick Weber
 * @since 4.3.1
 */
public class TaosInterceptor implements PublishInboundInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TaosInterceptor.class);
    private static final Charset charset = Charset.forName("UTF-8");
    private static CharsetDecoder decoder = charset.newDecoder();

    private Config cfg;
    private TaosDao dao;


    public TaosInterceptor(Config config, Properties properties) throws Exception {
        cfg = config;
        dao = new TaosDao(properties);
    }

    @Override
    public void onInboundPublish(final @NotNull PublishInboundInput publishInboundInput, final @NotNull PublishInboundOutput publishInboundOutput) {
        PublishPacket packet = publishInboundInput.getPublishPacket();
        String topic = packet.getTopic();
        if (!cfg.contains(topic)) {
            return;
        }

        Optional<ByteBuffer> payload = packet.getPayload();
        if (payload.isEmpty()) {
            return;
        }

        ByteBuffer buffer = payload.get();

        try {
            CharBuffer charBuffer = decoder.decode(buffer);
            String s = charBuffer.toString();
            log.debug("topic: {}, content: {}", topic, s);
            Map values = JSONObject.parseObject(s, Map.class);
            dao.save(cfg.get(topic), values);
        } catch (CharacterCodingException | SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
    }
}