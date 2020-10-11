
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

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.testcontainer.core.MavenHiveMQExtensionSupplier;
import com.hivemq.testcontainer.junit5.HiveMQTestContainerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This tests the functionality of the {@link TDTestInterceptor}.
 * It uses the HiveMQ Testcontainer to automatically package and deploy this extension inside a HiveMQ docker container.
 *
 * @author Yannick Weber
 * @since 4.3.1
 */
class HelloWorldInterceptorIT {

    @RegisterExtension
    public final @NotNull HiveMQTestContainerExtension extension =
            new HiveMQTestContainerExtension()
                    .withExtension(MavenHiveMQExtensionSupplier.direct().get());

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void test_payload_modified() throws InterruptedException {
        final Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier("hello-world-client")
                .serverPort(extension.getMqttPort())
                .buildBlocking();
        client.connect();

        final Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
        client.subscribeWith().topicFilter("hello/world").send();

        client.publishWith().topic("hello/world").payload("Good Bye World!".getBytes(StandardCharsets.UTF_8)).send();

        final Mqtt5Publish receive = publishes.receive();
        assertTrue(receive.getPayload().isPresent());
        assertEquals("Hello World!", new String(receive.getPayloadAsBytes(), StandardCharsets.UTF_8));
    }
}