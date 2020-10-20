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

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.events.client.ClientLifecycleEventListener;
import com.hivemq.extension.sdk.api.events.client.parameters.AuthenticationSuccessfulInput;
import com.hivemq.extension.sdk.api.events.client.parameters.ConnectionStartInput;
import com.hivemq.extension.sdk.api.events.client.parameters.DisconnectEventInput;
import com.hivemq.extension.sdk.api.packets.general.MqttVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a very simple {@link ClientLifecycleEventListener}
 * which logs the MQTT version and identifier of every connecting client.
 *
 * @author Marvin Liao
 * @since 4.0.0
 */
public class TaosListener implements ClientLifecycleEventListener {

    private static final Logger log = LoggerFactory.getLogger(TaosMain.class);

    @Override
    public void onMqttConnectionStart(final @NotNull ConnectionStartInput connectionStartInput) {
        final MqttVersion version = connectionStartInput.getConnectPacket().getMqttVersion();
        switch (version) {
            case V_5:
                log.info("MQTT 5 client connected with id: {} ", connectionStartInput.getClientInformation().getClientId());
                break;
            case V_3_1_1:
                log.info("MQTT 3.1.1 client connected with id: {} ", connectionStartInput.getClientInformation().getClientId());
                break;
            case V_3_1:
                log.info("MQTT 3.1 client connected with id: {} ", connectionStartInput.getClientInformation().getClientId());
                break;
        }
    }

    @Override
    public void onAuthenticationSuccessful(final @NotNull AuthenticationSuccessfulInput authenticationSuccessfulInput) {

    }

    @Override
    public void onDisconnect(final @NotNull DisconnectEventInput disconnectEventInput) {
        log.info("Client disconnected with id: {} ", disconnectEventInput.getClientInformation().getClientId());
    }
}
