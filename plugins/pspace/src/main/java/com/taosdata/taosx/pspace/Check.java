package com.taosdata.taosx.pspace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sunwayland.pspace.PSpaceClient;
import com.sunwayland.pspace.entity.PsResult;
import com.sunwayland.pspace.entity.PsServerProp;
import com.taosdata.taosx.pspace.config.Configuration;

/**
 * Encapsulates pSpace configuration validation logic.
 */
public class Check {

    private static final Logger logger = LoggerFactory.getLogger(Check.class);

    /**
     * Perform a connectivity and configuration check against the pSpace server.
     *
     * @param config pSpace configuration
     * @return CheckResult representing whether the configuration is valid
     */
    public static CheckResult check(Configuration config) {
        logger.info("run pSpace check...");

        if (config == null) {
            logger.error("No configuration provided.");
            return CheckResult.invalid("No configuration provided");
        }

        PSpaceClient client;
        try {
            client = config.tryConnect();
        } catch (Exception e) {
            logger.error("Failed to connect to pSpace server: {}", e.getMessage());
            return CheckResult.invalid(e.getMessage());
        }

        PsResult<PsServerProp> result = client.serverGetProp();
        if (result.isSuccess()) {
            PsServerProp psServerProp = result.getData().get(0);
            String name = psServerProp.getServerName();
            String version = psServerProp.getVersion() + "." + psServerProp.getSubVersion();
            String user = psServerProp.getUserName();
            String status = psServerProp.getStatus().getDesc();

            ServerInfo serverInfo = new ServerInfo(name, version, user, status);
            logger.info("Connected to pSpace server: {}", serverInfo);

            return CheckResult.valid(version);
        }

        logger.error("Configuration check failed: {}", result);
        return CheckResult.invalid(result.toString());
    }
}
