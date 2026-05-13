package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class RebalanceTestUtil {

    private static final Logger log = LoggerFactory.getLogger(RebalanceTestUtil.class);
    public static void waitHealthCheckFinished(Endpoint endpoint) throws InterruptedException, SQLException {
        if (RebalanceManager.getInstance().getBgHealthCheckInstanceCount() == 0) {
            TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "Health check thread is not running");
        }

        int maxWaitMs = 3000;
        while (!RebalanceManager.getInstance().isRebalancing(endpoint) && maxWaitMs > 0) {
            Thread.sleep(100);
            maxWaitMs -= 100;
        }
        // wait a bit more to ensure health check is done
        Thread.sleep(100);
        if (!RebalanceManager.getInstance().isRebalancing(endpoint)) {
            log.error("Health check did not complete in expected time for endpoint: {}", endpoint);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "Health check did not complete in expected time");
        }
    }

    public static void waitHealthCheckFinishedIgnoreException(Endpoint endpoint) {
        try {
            waitHealthCheckFinished(endpoint);
        } catch (Exception e) {
            // Ignore exceptions
        }
    }
}
