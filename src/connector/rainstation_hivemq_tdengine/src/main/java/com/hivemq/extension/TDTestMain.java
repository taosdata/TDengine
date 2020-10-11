
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

import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.client.ClientContext;
import com.hivemq.extension.sdk.api.client.parameter.InitializerInput;
import com.hivemq.extension.sdk.api.events.EventRegistry;
import com.hivemq.extension.sdk.api.interceptor.subscribe.SubscribeInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.subscribe.parameter.SubscribeInboundInput;
import com.hivemq.extension.sdk.api.interceptor.subscribe.parameter.SubscribeInboundOutput;
import com.hivemq.extension.sdk.api.parameter.*;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.auth.SecurityRegistry;
import com.hivemq.extension.sdk.api.services.intializer.ClientInitializer;
import com.hivemq.extension.sdk.api.services.intializer.InitializerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This is the main class of the extension,
 * which is instantiated either during the HiveMQ start up process (if extension is enabled)
 * or when HiveMQ is already started by enabling the extension.
 *
 * @Author: fang xinliang
 * @Date: 2020/10/7 20:08
 */
public class TDTestMain implements ExtensionMain {

    private static final @NotNull Logger log = LoggerFactory.getLogger(TDTestMain.class);

    @Override
    public void extensionStart(final @NotNull ExtensionStartInput extensionStartInput, final @NotNull ExtensionStartOutput extensionStartOutput) {

        try {

            addClientLifecycleEventListener();
            addPublishModifier();
            addAuthenticatorProvider();

            final ExtensionInformation extensionInformation = extensionStartInput.getExtensionInformation();
            log.info("Started " + extensionInformation.getName() + ":" + extensionInformation.getVersion());

        } catch (Exception e) {
            log.error("Exception thrown at extension start: ", e);
        }

    }

    @Override
    public void extensionStop(final @NotNull ExtensionStopInput extensionStopInput, final @NotNull ExtensionStopOutput extensionStopOutput) {

        final ExtensionInformation extensionInformation = extensionStopInput.getExtensionInformation();
        log.info("Stopped " + extensionInformation.getName() + ":" + extensionInformation.getVersion());

    }

    private void addClientLifecycleEventListener() {

        final EventRegistry eventRegistry = Services.eventRegistry();

        final TDTestListener tdTestListener = new TDTestListener();

        eventRegistry.setClientLifecycleEventListener(input -> tdTestListener);


    }

    private void addPublishModifier() {
        final InitializerRegistry initializerRegistry = Services.initializerRegistry();

        final TDTestInterceptor tdTestInterceptor = new TDTestInterceptor();

        initializerRegistry.setClientInitializer((initializerInput, clientContext) -> clientContext.addPublishInboundInterceptor(tdTestInterceptor));
    }

    private void addAuthenticatorProvider() {
        final SecurityRegistry securityRegistry = Services.securityRegistry();
        //create username_password map
        final Map<String, String> usernamePasswordMap = new HashMap<>();
        usernamePasswordMap.put("test_name_1", "password_1");
        usernamePasswordMap.put("test_name_2", "password_2");
        usernamePasswordMap.put("test_name_3", "password_3");
        //create provider
        final TDTestAuthenticatorProvider provider = new TDTestAuthenticatorProvider(usernamePasswordMap);
        //set provider
        securityRegistry.setAuthenticatorProvider(provider);

        log.info("SecurityRegistry");
    }

    private  void addSubscribeInboundInterceptor() {
        // create a new subscribe inbound interceptor
        final SubscribeInboundInterceptor subscribeInboundInterceptor = new SubscribeInboundInterceptor() {
            @Override
            public void onInboundSubscribe(@NotNull SubscribeInboundInput subscribeInboundInput, @NotNull SubscribeInboundOutput subscribeInboundOutput) {


                log.debug("Inbound subscribe message intercepted from client: "
                        + subscribeInboundInput.getClientInformation().getClientId()+ subscribeInboundInput.getSubscribePacket().getSubscriptions().toString());

            }
        };

        // create a new client initializer
        final ClientInitializer clientInitializer = new ClientInitializer() {
            @Override
            public void initialize(
                    final @NotNull InitializerInput initializerInput,
                    final @NotNull ClientContext clientContext) {
                // add the interceptor to the context of the connecting client
                clientContext.addSubscribeInboundInterceptor(subscribeInboundInterceptor);
            }
        };

        // register the client initializer
        Services.initializerRegistry().setClientInitializer(clientInitializer);
    }
}
