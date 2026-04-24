package com.taos.example.security;

import com.alibaba.nacos.api.config.ConfigService;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SecurityTmqDemoTest {

    @After
    public void tearDown() throws Exception {
        setNacosConfigService(null);
    }

    @Test
    public void testCloseConfigServiceQuietlyShutdownAndClearReference() throws Exception {
        AtomicInteger shutdownCount = new AtomicInteger();
        ConfigService proxy = createConfigServiceProxy(shutdownCount, false);
        setNacosConfigService(proxy);

        invokeCloseConfigServiceQuietly();

        assertEquals(1, shutdownCount.get());
        assertNull(getNacosConfigService());
    }

    @Test
    public void testCloseConfigServiceQuietlyOnShutdownErrorStillClearsReference() throws Exception {
        AtomicInteger shutdownCount = new AtomicInteger();
        ConfigService proxy = createConfigServiceProxy(shutdownCount, true);
        setNacosConfigService(proxy);

        invokeCloseConfigServiceQuietly();

        assertEquals(1, shutdownCount.get());
        assertNull(getNacosConfigService());
    }

    @Test
    public void testCloseConfigServiceQuietlyWhenServiceIsNull() throws Exception {
        setNacosConfigService(null);

        invokeCloseConfigServiceQuietly();

        assertNull(getNacosConfigService());
    }

    private static void invokeCloseConfigServiceQuietly() throws Exception {
        try {
            Method method = SecurityTmqDemo.class.getDeclaredMethod("closeConfigServiceQuietly");
            method.setAccessible(true);
            method.invoke(null);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    private static ConfigService getNacosConfigService() throws Exception {
        Field field = SecurityTmqDemo.class.getDeclaredField("nacosConfigService");
        field.setAccessible(true);
        return (ConfigService) field.get(null);
    }

    private static void setNacosConfigService(ConfigService configService) throws Exception {
        Field field = SecurityTmqDemo.class.getDeclaredField("nacosConfigService");
        field.setAccessible(true);
        field.set(null, configService);
    }

    private static ConfigService createConfigServiceProxy(AtomicInteger shutdownCount, boolean throwOnShutdown) {
        return (ConfigService) Proxy.newProxyInstance(
                ConfigService.class.getClassLoader(),
                new Class<?>[]{ConfigService.class},
                (proxy, method, args) -> {
                    if ("shutDown".equals(method.getName())) {
                        shutdownCount.incrementAndGet();
                        if (throwOnShutdown) {
                            throw new RuntimeException("shutdown failure");
                        }
                        return null;
                    }

                    Class<?> returnType = method.getReturnType();
                    if (returnType.equals(boolean.class)) {
                        return false;
                    }
                    if (returnType.equals(int.class)) {
                        return 0;
                    }
                    if (returnType.equals(long.class)) {
                        return 0L;
                    }
                    if (returnType.equals(float.class)) {
                        return 0F;
                    }
                    if (returnType.equals(double.class)) {
                        return 0D;
                    }
                    if (returnType.equals(short.class)) {
                        return (short) 0;
                    }
                    if (returnType.equals(byte.class)) {
                        return (byte) 0;
                    }
                    if (returnType.equals(char.class)) {
                        return '\0';
                    }
                    return null;
                });
    }
}
