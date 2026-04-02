package com.taosdata.jdbc.common;

import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConsumerManager {

    private ConsumerManager() {

    }

    // List of registered Consumer
    private static final CopyOnWriteArrayList<ConsumerFactory> registeredConsumers = new CopyOnWriteArrayList<>();
    private static final Object logSync = new Object();
    private static PrintWriter logWriter = null;

    static {
        loadConsumerFactories();
    }

    @SuppressWarnings("all")
    private static void loadConsumerFactories() {
        AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            ServiceLoader<ConsumerFactory> consumerFactoryLoader = ServiceLoader.load(ConsumerFactory.class);
            Iterator<ConsumerFactory> iterator = consumerFactoryLoader.iterator();

            try {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            } catch (Throwable t) {
                // Do nothing
            }
            return null;
        });
    }

    public static void register(ConsumerFactory factory) {
        if (factory != null) {
            registeredConsumers.addIfAbsent(factory);
        } else {
            throw new NullPointerException();
        }

        println("registerConsumerFactory: " + factory);
    }

    public static Consumer<?> getConsumer(String type) throws SQLException {
        for (ConsumerFactory factory : registeredConsumers) {
            if (factory.acceptsType(type)) {

                Consumer<?> consumer = factory.getConsumer();
                if (consumer != null) {
                    // Success!
                    println("getConsumerFactory returning " + factory.getClass().getName());
                    return consumer;
                }
            }
        }

        println("getConsumer: no suitable consumer found for " + type);
        throw new SQLException("No suitable consumer found for " + type, "");
    }

    @SuppressWarnings("unused")
    public static PrintWriter getLogWriter() {
        return logWriter;
    }

    @SuppressWarnings("unused")
    public static void setLogWriter(PrintWriter out) {
        logWriter = out;
    }

    public static void println(String message) {
        synchronized (logSync) {
            if (logWriter != null) {
                logWriter.println(message);

                logWriter.flush();
            }
        }
    }
}
