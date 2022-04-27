package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.ws.entity.Action;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Unfinished execution
 */
public class InFlightRequest {
    private final int timeoutSec;
    private final Semaphore semaphore;
    private final Map<String, ConcurrentHashMap<Long, ResponseFuture>> futureMap = new HashMap<>();
    private final Map<String, PriorityBlockingQueue<ResponseFuture>> expireMap = new HashMap<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r);
        t.setName("timer-" + t.getId());
        return t;
    });

    public InFlightRequest(int timeoutSec, int concurrentNum) {
        this.timeoutSec = timeoutSec;
        this.semaphore = new Semaphore(concurrentNum);
        scheduledExecutorService.scheduleWithFixedDelay(this::removeTimeoutFuture,
                timeoutSec, timeoutSec, TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(scheduledExecutorService::shutdown));
        for (Action value : Action.values()) {
            String action = value.getAction();
            if (Action.CONN.getAction().equals(action))
                continue;
            futureMap.put(action, new ConcurrentHashMap<>());
            expireMap.put(action, new PriorityBlockingQueue<>());
        }
    }

    public void put(ResponseFuture rf) throws InterruptedException, TimeoutException {
        if (semaphore.tryAcquire(timeoutSec, TimeUnit.MILLISECONDS)) {
            futureMap.get(rf.getAction()).put(rf.getId(), rf);
            expireMap.get(rf.getAction()).put(rf);
        } else {
            throw new TimeoutException();
        }
    }

    public ResponseFuture remove(String action, Long id) {
        ResponseFuture future = futureMap.get(action).remove(id);
        if (null != future) {
            expireMap.get(action).remove(future);
            semaphore.release();
        }
        return future;
    }

    private void removeTimeoutFuture() {
        expireMap.forEach((k, v) -> {
            while (true) {
                ResponseFuture response = v.peek();
                if (null == response || (System.nanoTime() - response.getTimestamp()) < timeoutSec * 1_000_000L)
                    break;

                try {
                    v.poll();
                    futureMap.get(k).remove(response.getId());
                    response.getFuture().completeExceptionally(new TimeoutException());
                } finally {
                    semaphore.release();
                }
            }
        });
    }
}
