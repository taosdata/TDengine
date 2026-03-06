package com.taosdata.taosx;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.taosdata.taosx.pspace.Check;
import com.taosdata.taosx.pspace.CheckResult;
import com.taosdata.taosx.pspace.Node;
import com.taosdata.taosx.pspace.Nodes;
import com.taosdata.taosx.pspace.Point;
import com.taosdata.taosx.pspace.Points;
import com.taosdata.taosx.pspace.VersionProvider;
import com.taosdata.taosx.pspace.config.AdvancedOptionsConfig;
import com.taosdata.taosx.pspace.config.Configuration;
import com.taosdata.taosx.pspace.config.CommandMode;
import com.taosdata.taosx.pspace.config.RunConfig;
import com.taosdata.taosx.pspace.run.QueryTask;
import com.taosdata.taosx.pspace.run.QuerySyncTask;
import com.taosdata.taosx.pspace.run.SubscribeTask;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.List;
import java.util.concurrent.Callable;
import java.io.IOException;

@Command(name = "taosx-pspace", mixinStandardHelpOptions = true, versionProvider = VersionProvider.class, description = "taosX pSpace plugin - command line tool to run pSpace tasks")
public class TaosXpSpaceMain
        implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(TaosXpSpaceMain.class);

    @Option(names = { "-c", "--config" }, description = "Path to configuration file", required = true)
    private String config;

    @Option(names = { "-m",
            "--mode" }, description = "Task mode: ${COMPLETION-CANDIDATES}", completionCandidates = CommandMode.class, required = true)
    private String mode;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TaosXpSpaceMain()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        logger.info("taosX pSpace plugin started. mode={}, config={}", mode, config);

        // Validate mode
        if (!CommandMode.isValid(mode)) {
            logger.error("Unknown mode: {}. Allowed: {}", mode, String.join(", ", CommandMode.list()));
            return -1;
        }

        // parse config
        Configuration cfg;
        try {
            cfg = Configuration.tryFromPath(config);
        } catch (IOException e) {
            logger.error("Failed to load configuration from {}: {}", config, e.getMessage());
            return -1;
        }
        logger.info("Loaded pspace config: {}", cfg);

        // Apply log level from advanced_options if configured
        applyLogLevel(cfg.getAdvancedOptions());

        switch (mode) {
            case "check":
                CheckResult chk_res = Check.check(cfg);
                System.out.println(new Gson().toJson(chk_res)); // print to stdout for taosX to consume
                return chk_res.isValid() ? 0 : -1;
            case "nodes":
                List<Node> nodes_res = Nodes.load(cfg);
                System.out.println(new Gson().toJson(nodes_res)); // print to stdout for taosX to consume
                return 0;
            case "points":
                List<Point> points_res = Points.load(cfg);
                System.out.println(new Gson().toJson(points_res)); // print to stdout for taosX to consume
                return 0;
            case "run":
                return runTask(cfg);
            default:
                logger.error("Unknown mode: {}", mode);
                return -1;
        }
    }

    /**
     * Dispatch the "run" mode based on config's [run].mode field.
     * Supported run modes: Query, Subscribe, QuerySync.
     */
    private int runTask(Configuration cfg) throws Exception {
        RunConfig runCfg = cfg.getRun();
        if (runCfg == null || runCfg.getMode() == null) {
            logger.error("Missing [run] section or run.mode in configuration");
            return -1;
        }

        String taskMode = runCfg.getMode();
        logger.info("Running pSpace task, run.mode={}", taskMode);

        switch (taskMode) {
            case "Query":
                return new QueryTask().execute(cfg);
            case "Subscribe":
                logger.info("Subscribe mode started");
                return new SubscribeTask().execute(cfg);
            case "QuerySync":
                return new QuerySyncTask().execute(cfg);
            default:
                logger.error("Unknown run.mode: {}. Supported: Query, Subscribe, QuerySync", taskMode);
                return -1;
        }
    }

    /**
     * Apply log level from [advanced_options].log_level to the Log4j2 root logger.
     * Supported values: Error, Warn, Info, Debug, Trace (case-insensitive).
     */
    private void applyLogLevel(AdvancedOptionsConfig advOpts) {
        if (advOpts == null || advOpts.getLogLevel() == null) {
            return;
        }
        String levelStr = advOpts.getLogLevel();
        try {
            Level level = Level.toLevel(levelStr, null);
            if (level == null) {
                logger.warn("Unknown log_level '{}', ignoring", levelStr);
                return;
            }
            Configurator.setRootLevel(level);
            logger.info("Log level set to {} from advanced_options", level);
        } catch (Exception e) {
            logger.warn("Failed to set log level '{}': {}", levelStr, e.getMessage());
        }
    }
}
