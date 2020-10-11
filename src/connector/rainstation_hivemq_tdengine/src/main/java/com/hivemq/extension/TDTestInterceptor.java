
/*
 * Copyright 2018-present HiveMQ GmbH
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

package com.hivemq.extension;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.interceptor.subscribe.SubscribeInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.subscribe.parameter.SubscribeInboundInput;
import com.hivemq.extension.sdk.api.interceptor.subscribe.parameter.SubscribeInboundOutput;
import com.hivemq.extension.sdk.api.interceptor.unsubscribe.UnsubscribeInboundInterceptor;
import com.hivemq.extension.sdk.api.packets.publish.ModifiablePublishPacket;
import com.hivemq.extension.sdk.api.packets.publish.PublishPacket;
import com.tdengine.jdbc.RainStation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 *
 *
 *
 * @Author: fang xinliang
 * @Date: 2020/10/7 20:08
 */
public class TDTestInterceptor implements PublishInboundInterceptor {

    private final static Log logger = LogFactory.getLog(TDTestInterceptor.class);

    @Override
    public void onInboundPublish(final @NotNull PublishInboundInput publishInboundInput,
                                 final @NotNull PublishInboundOutput publishInboundOutput) {
        final ModifiablePublishPacket publishPacket = publishInboundOutput.getPublishPacket();
        if ("td/test".equals(publishPacket.getTopic())) {
            try {
                Charset charset = Charset.defaultCharset() ;//Charset.forName("gb2312");// Charset.defaultCharset();   utf-8,gbk,gb2312
                CharBuffer charBuffer = charset.decode(publishPacket.getPayload().get() );
                String s = charBuffer.toString();
                logger.info("receive data：" + s);
                ByteBuffer bb = publishPacket.getPayload().get();
                byte[] b = new byte[bb.remaining()];  //byte[] b = new byte[bb.capacity()]  is OK
                bb.get(b, 0, b.length);
                String  s1 = new String(b,"GB2312");
                logger.info("GB2312 receive data：" + s1);
                String  s2 = new String(b,"UTF-8");
                logger.info("UTF-8 receive data：" + s2);
                String  s3 = new String(b,"GBK");
                logger.info("GBK receive data：" + s3);
                String  s4 = new String(b,"ISO-8859-1");
                logger.info("ISO-8859-1 receive data：" + s4);

                String[] data = s.split(",");
                RainStation.txtSave2DB(data);
            } catch (IOException ex) {
                logger.error(ex.getMessage());
            } catch (Exception ex) {
                logger.error(ex.getMessage());
            }
            final ByteBuffer payload = ByteBuffer.wrap("Hello World!".getBytes(StandardCharsets.UTF_8));
            publishPacket.setPayload(payload);
        }

    }

}

