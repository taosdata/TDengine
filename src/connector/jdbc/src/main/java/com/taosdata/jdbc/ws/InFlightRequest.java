package com.taosdata.jdbc.ws;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Unfinished execution
 */
public class InFlightRequest implements AutoCloseable {
    private final int timeoutSec;
    private final Semaphore semaphore;
    private final Map<String, ResponseFuture> futureMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledFuture<?> scheduledFuture;

    public InFlightRequest(int timeoutSec, int concurrentNum) {
        this.timeoutSec = timeoutSec;
        this.semaphore = new Semaphore(concurrentNum);
        this.scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(this::removeTimeoutFuture, timeoutSec, timeoutSec, TimeUnit.MILLISECONDS);
    }

    public void put(ResponseFuture responseFuture) throws InterruptedException, TimeoutException {
        if (semaphore.tryAcquire(timeoutSec, TimeUnit.MILLISECONDS)) {
            futureMap.put(responseFuture.getId(), responseFuture);
        } else {
            throw new TimeoutException();
        }
    }

    public ResponseFuture remove(String id) {
        ResponseFuture future = futureMap.remove(id);
        if (null != future) {
            semaphore.release();
        }
        return future;
    }

    private void removeTimeoutFuture() {
        futureMap.entrySet().removeIf(entry -> {
            if (System.nanoTime() - entry.getValue().getTimestamp() > timeoutSec * 1_000_000L) {
                try {
                    entry.getValue().getFuture().completeExceptionally(new TimeoutException());
                }finally {
                    semaphore.release();
                }
                return true;
            } else {
                return false;
            }
        });
    }

    @Override
    public void close() {
        scheduledFuture.cancel(true);
        scheduledExecutorService.shutdown();
    }
}
