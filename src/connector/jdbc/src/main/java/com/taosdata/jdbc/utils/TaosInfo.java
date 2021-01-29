package com.taosdata.jdbc.utils;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TaosInfo implements TaosInfoMBean {

    private static volatile TaosInfo instance;
    private AtomicInteger open_count = new AtomicInteger();
    private AtomicInteger close_count = new AtomicInteger();

    static {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("TaosInfoMBean:name=TaosInfo");
            server.registerMBean(TaosInfo.getInstance(), name);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getConnectionCount() {
        return open_count.get() - close_count.get();
    }

    @Override
    public int getStatementCount() {
        return 0;
    }

    public AtomicInteger getOpen_count() {
        return open_count;
    }

    public AtomicInteger getClose_count() {
        return close_count;
    }

    private TaosInfo() {
    }

    public static TaosInfo getInstance() {
        if (instance == null) {
            synchronized (TaosInfo.class) {
                if (instance == null) {
                    instance = new TaosInfo();
                }
            }
        }
        return instance;
    }

}
