package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.*;
import java.util.function.Function;

import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_QUERY_TIMEOUT;

public class CompletableFutureTimeout {
    private CompletableFutureTimeout() {
    }

    public static <T> CompletableFuture<T> orTimeout(CompletableFuture<T> future, long timeout, TimeUnit unit, String msg) {
        final CompletableFuture<T> timeoutFuture = timeoutAfter(timeout, unit, msg);
        future.whenCompleteAsync((result, throwable) -> {
            if (future.isDone() && !timeoutFuture.isDone()) {
                timeoutFuture.cancel(false);
            }
        });
        return future.applyToEither(timeoutFuture, Function.identity());
    }

    private static <T> CompletableFuture<T> timeoutAfter(long timeout, TimeUnit unit, String msg) {
        if (TimeUnit.MILLISECONDS.convert(timeout, unit)  <= TSDBConstants.DEFAULT_MESSAGE_WAIT_TIMEOUT) {
            return handleShortTimeout(timeout, unit, msg);
        } else {
            return handleLongTimeout(timeout, unit, msg);
        }
    }

    /**
     * handleShortTimeout(<= 60s), use netty timer
     */
    private static <T> CompletableFuture<T>  handleShortTimeout(
            long timeout,
            TimeUnit unit,
            String msg) {
        CompletableFuture<T> result = new CompletableFuture<>();

        TimerTask task = (Timeout t) -> {
            if (!result.isDone()) {
                Utils.getEventLoopGroup().execute(() -> {
                    if (!result.isDone()) {
                        result.completeExceptionally(TSDBError.createTimeoutException(ERROR_QUERY_TIMEOUT,
                                String.format("failed to complete the task:%s within the specified time : %d,%s", msg, timeout, unit))
                        );
                    }
                });
            }
        };

        io.netty.util.Timeout nettyTimeout = NETTY_TIMER.newTimeout(task, timeout, unit);

        result.whenComplete((res, ex) -> nettyTimeout.cancel());

        return  result;
    }

    /**
     * long timeout task (>60s), use JDK scheduler
     */
    private static <T> CompletableFuture<T>  handleLongTimeout(
            long timeout,
            TimeUnit unit,
            String msg) {

        CompletableFuture<T> result = new CompletableFuture<>();
        ScheduledFuture<?> scheduledFuture = Delayer.delayer.schedule(
                () -> result.completeExceptionally(TSDBError.createTimeoutException(ERROR_QUERY_TIMEOUT,
                        String.format("failed to complete the task:%s within the specified time : %d,%s", msg, timeout, unit)))
                , timeout, unit);

        // Use handle to ensure the scheduled task is cancelled when the CompletableFuture completes normally or exceptionally
        result.handle((res, ex) -> {
            scheduledFuture.cancel(false);
            return null;
        });

        return result;
    }


    /**
     * Singleton delay scheduler, used only for starting and * cancelling tasks.
     */
    static final class Delayer {
        private Delayer() {
        }

        @SuppressWarnings("all")
        static ScheduledFuture<?> delay(Runnable command, long delay,
                                        TimeUnit unit) {
            return delayer.schedule(command, delay, unit);
        }

        static final class DaemonThreadFactory implements ThreadFactory {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("taos-jdbc-long-timer-");
                return t;
            }
        }

        static final ScheduledThreadPoolExecutor delayer;

        static {
            delayer = new ScheduledThreadPoolExecutor(
                    1, new DaemonThreadFactory());
            delayer.setRemoveOnCancelPolicy(true);
        }
    }
    private static final Timer NETTY_TIMER = new HashedWheelTimer(
            new DefaultThreadFactory("taos-jdbc-short-timer", true),
            50, TimeUnit.MILLISECONDS,
            1024 // slots (2^10), covers 1024 * 50ms = 51.2s, a 60s task will be handled in 2 loops
    );
}