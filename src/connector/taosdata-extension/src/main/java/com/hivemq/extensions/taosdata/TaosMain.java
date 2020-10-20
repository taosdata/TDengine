
/*
 * Copyright 2020-present Marvin Liao <coldljy@163.com>
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

import com.alibaba.druid.pool.ha.PropertiesUtils;
import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.events.EventRegistry;
import com.hivemq.extension.sdk.api.parameter.*;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.intializer.InitializerRegistry;
import com.hivemq.extensions.taosdata.configuration.CfgUtils;
import com.hivemq.extensions.taosdata.configuration.Constants;
import com.hivemq.extensions.taosdata.configuration.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * This is the main class of the extension,
 * load configuration, init and register the interceptor.
 *
 * @author Marvin Liao
 * @since 4.0.0
 */
public class TaosMain implements ExtensionMain {
    private static final @NotNull Logger log = LoggerFactory.getLogger(TaosMain.class);

    @Override
    public void extensionStart(final @NotNull ExtensionStartInput extensionStartInput, final @NotNull ExtensionStartOutput extensionStartOutput) {
        try {
            Properties properties = PropertiesUtils.loadProperties(Constants.DEFAULT_TAOS_CFG_PATH);
            Config cfg = CfgUtils.load();

            addClientLifecycleEventListener();
            addPublishModifier(cfg, properties);

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
        final TaosListener helloWorldListener = new TaosListener();

        eventRegistry.setClientLifecycleEventListener(input -> helloWorldListener);
    }

    private void addPublishModifier(Config cfg, Properties properties) throws Exception {
        final InitializerRegistry initializerRegistry = Services.initializerRegistry();
        final TaosInterceptor helloWorldInterceptor = new TaosInterceptor(cfg, properties);

        initializerRegistry.setClientInitializer((initializerInput, clientContext) -> clientContext.addPublishInboundInterceptor(helloWorldInterceptor));
    }
}
