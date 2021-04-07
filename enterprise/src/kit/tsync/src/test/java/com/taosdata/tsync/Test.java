package com.taosdata.tsync;

import java.util.Random;
import java.util.concurrent.*;

public class Test {

    private static final long sessionTimeout = 30000;
    private static final long expirationInterval = 2000;

    public static void main(String[] args) {

        ExecutorService executor = Executors.newCachedThreadPool();
        Future<Double> future = executor.submit(new Callable<Double>() {
            @Override
            public Double call() throws Exception {
                TimeUnit.SECONDS.sleep(1);
                return new Random(System.currentTimeMillis()).nextDouble();
            }
        });

        try {
            Double result = future.get(10, TimeUnit.SECONDS);
            System.out.println("result >> " + result);
        } catch (ExecutionException e) {
            System.out.println("计算抛出异常");
            e.printStackTrace();
        } catch (TimeoutException e) {
            System.out.println("超时");
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println("线程等待过程中被打断");
            e.printStackTrace();
        }

//        for (int i = 0; i < 10; i++) {
//            long current = System.currentTimeMillis();
//            long expirationTime = calculateExpirationTime(current);
//            System.out.println("current: " + current + ", expirationTime: " + expirationTime);
//            TimeUnit.SECONDS.sleep(1);
//        }

//        String aaa = "Person{name='name_0', age=-1499755842, sex=true}";
//        aaa = aaa.replaceAll("'", "\\\\\\\\'");
//        System.out.println(aaa);
//        aaa = "'" + aaa + "'";
//        System.out.println(aaa);
//        String sql = "insert into p9 values(?,?,?)";
//        System.out.println(sql.replaceFirst("[?]", aaa));

//        DecimalFormat df = new DecimalFormat();
//        df.setMaximumFractionDigits(5);
//        System.out.println(df.format(Float.MAX_VALUE).replaceAll(",",""));

//        String s = new BigDecimal(new Float(Float.MAX_VALUE).toString()).divide(new BigDecimal(1), 5, BigDecimal.ROUND_HALF_UP).toString();
//        System.out.println(s);
    }

    private static long calculateExpirationTime(long current) {
        long expirationTime = current + sessionTimeout;
        expirationTime = (expirationTime / expirationInterval + 1) * expirationInterval;
        return expirationTime;
    }
}
