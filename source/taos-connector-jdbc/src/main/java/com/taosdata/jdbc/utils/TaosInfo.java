package com.taosdata.jdbc.utils;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

public class TaosInfo implements TaosInfoMBean {
    private final AtomicLong connectOpen = new AtomicLong();
    private final AtomicLong connectClose = new AtomicLong();
    private final AtomicLong statementCount = new AtomicLong();

    static {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("TaosInfoMBean:name=TaosInfo");
            server.registerMBean(TaosInfo.getInstance(), name);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException ignored) {
            throw new RuntimeException("registerMBean failed");
        }
    }

    @Override
    public long getConnectOpen() {
        return connectOpen.get();
    }

    @Override
    public long getConnectClose() {
        return connectClose.get();
    }

    @Override
    public long getConnect_active() {
        return connectOpen.get() - connectClose.get();
    }

    @Override
    public long getStatementCount() {
        return statementCount.get();
    }

    /*******************************************************/

    public void connOpenIncrement() {
        connectOpen.incrementAndGet();
    }

    public void connectCloseIncrement() {
        connectClose.incrementAndGet();
    }

    public void stmtCountIncrement() {
        statementCount.incrementAndGet();
    }

    /********************************************************************************/
    private TaosInfo() {
    }

    private static class TaosInfoHolder {
        private static final TaosInfo INSTANCE = new TaosInfo();
    }

    public static TaosInfo getInstance() {
        return TaosInfoHolder.INSTANCE;
    }

}
