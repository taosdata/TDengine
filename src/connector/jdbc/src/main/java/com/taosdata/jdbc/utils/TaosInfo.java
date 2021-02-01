package com.taosdata.jdbc.utils;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TaosInfo implements TaosInfoMBean {

    private static volatile TaosInfo instance;
    private AtomicLong connect_open = new AtomicLong();
    private AtomicLong connect_close = new AtomicLong();
    private AtomicLong statement_count = new AtomicLong();

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
    public long getConnect_open() {
        return connect_open.get();
    }

    @Override
    public long getConnect_close() {
        return connect_close.get();
    }

    @Override
    public long getConnect_active() {
        return connect_open.get() - connect_close.get();
    }

    @Override
    public long getStatement_count() {
        return statement_count.get();
    }

    /*******************************************************/

    public void conn_open_increment() {
        connect_open.incrementAndGet();
    }

    public void connect_close_increment() {
        connect_close.incrementAndGet();
    }

    public void stmt_count_increment() {
        statement_count.incrementAndGet();
    }

    /********************************************************************************/
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
