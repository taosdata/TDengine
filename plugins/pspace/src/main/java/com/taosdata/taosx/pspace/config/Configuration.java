package com.taosdata.taosx.pspace.config;

import com.moandjiezana.toml.Toml;
import com.sunwayland.pspace.PSpaceClient;

import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the pspace.toml configuration and provides a simple loader.
 *
 * Expected TOML structure:
 * [connect]
 * server = "192.168.2.149"
 * port = 8889
 * username = "admin"
 * password = "admin888"
 */
@Data
@NoArgsConstructor
public class Configuration {
    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    private Connection connection;
    private NodesConfig nodes;
    private PointsConfig points;
    private RunConfig run;
    private ReportConfig report;
    private AdvancedOptionsConfig advancedOptions;

    /**
     * Load configuration from a TOML file path.
     *
     * @param path path to pspace.toml
     * @return loaded PspaceConfig
     * @throws IOException if file not found or cannot be read
     */
    public static Configuration tryFromPath(String path) throws IOException {
        Objects.requireNonNull(path, "config path must not be null");
        File f = new File(path);
        if (!f.exists()) {
            throw new IOException("Configuration file not found: " + f.getAbsolutePath());
        }

        Toml toml = new Toml().read(f);
        Configuration cfg = new Configuration();
        if (toml.getTable("connection") != null) {
            Connection c = toml.getTable("connection").to(Connection.class);
            cfg.setConnection(c);
        }
        if (toml.getTable("nodes") != null) {
            NodesConfig n = toml.getTable("nodes").to(NodesConfig.class);
            cfg.setNodes(n);
        }
        if (toml.getTable("points") != null) {
            PointsConfig p = toml.getTable("points").to(PointsConfig.class);
            cfg.setPoints(p);
        }
        if (toml.getTable("run") != null) {
            RunConfig r = toml.getTable("run").to(RunConfig.class);
            cfg.setRun(r);
        }
        if (toml.getTable("report") != null) {
            ReportConfig rpt = toml.getTable("report").to(ReportConfig.class);
            cfg.setReport(rpt);
        }
        if (toml.getTable("advanced_options") != null) {
            AdvancedOptionsConfig adv = toml.getTable("advanced_options").to(AdvancedOptionsConfig.class);
            cfg.setAdvancedOptions(adv);
        }
        return cfg;
    }

    public PSpaceClient tryConnect() throws Exception {
        if (connection == null) {
            throw new Exception("No connection configuration provided");
        }
        long timeoutSec = connection.getTimeoutSec();
        PSpaceClient client = connection.toPSpaceClient();

        // Use a daemon thread + CountDownLatch instead of ExecutorService.
        // ExecutorService.shutdownNow() calls Thread.interrupt() on the pool
        // thread, which can break PSpaceClient SDK's internal push-data
        // receiver thread (it may depend on the thread that called connect()).
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> connectError = new AtomicReference<>();

        Thread connectThread = new Thread(() -> {
            try {
                client.connect();
            } catch (Exception e) {
                connectError.set(e);
            } finally {
                latch.countDown();
            }
        }, "pspace-connect");
        connectThread.setDaemon(true);
        connectThread.start();

        if (!latch.await(timeoutSec, TimeUnit.SECONDS)) {
            throw new Exception(String.format(
                    "Connect to pSpace server %s:%d timed out after %d seconds",
                    connection.getServer(), connection.getPort(), timeoutSec));
        }

        if (connectError.get() != null) {
            throw new Exception("Failed to connect to pSpace server: "
                    + connectError.get().getMessage(), connectError.get());
        }

        logger.info("Connected to pSpace server {}:{}", connection.getServer(), connection.getPort());
        return client;
    }

}
