/***************************************************************************
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/
package com.taosdata.jdbc;

import javax.management.OperationsException;
import java.sql.SQLException;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.*;

public class TSDBSubscribe {
    private TSDBJNIConnector connecter = null;
    private static ScheduledExecutorService pool;
    private static Map<Long, TSDBTimerTask> timerTaskMap = new ConcurrentHashMap<>();
    private static Map<Long, ScheduledFuture> scheduledMap = new ConcurrentHashMap();

    private static class TimerInstance {
        private static final ScheduledExecutorService instance = Executors.newScheduledThreadPool(1);
    }

    public static ScheduledExecutorService getTimerInstance() {
        return TimerInstance.instance;
    }

    public TSDBSubscribe(TSDBJNIConnector connecter) throws SQLException {
        if (null != connecter) {
            this.connecter = connecter;
        } else {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }
    }

    /**
     * sync subscribe
     *
     * @param topic
     * @param sql
     * @param restart
     * @param period
     * @throws SQLException
     */
    public long subscribe(String topic, String sql, boolean restart, int period) throws SQLException {
        if (this.connecter.isClosed()) {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }
        if (period < 1000) {
            throw new SQLException(TSDBConstants.WrapErrMsg(TSDBConstants.INVALID_VARIABLES));
        }
        return this.connecter.subscribe(topic, sql, restart, period);
    }

    /**
     * async subscribe
     *
     * @param topic
     * @param sql
     * @param restart
     * @param period
     * @param callBack
     * @throws SQLException
     */
    public long subscribe(String topic, String sql, boolean restart, int period, TSDBSubscribeCallBack callBack) throws SQLException {
        if (this.connecter.isClosed()) {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }
        final long subscription = this.connecter.subscribe(topic, sql, restart, period);
        if (null != callBack) {
            pool = getTimerInstance();

            TSDBTimerTask timerTask = new TSDBTimerTask(subscription, callBack);

            timerTaskMap.put(subscription, timerTask);

            ScheduledFuture scheduledFuture = pool.scheduleAtFixedRate(timerTask, 1, 1000, TimeUnit.MILLISECONDS);
            scheduledMap.put(subscription, scheduledFuture);
        }
        return subscription;
    }

    public TSDBResultSet consume(long subscription) throws OperationsException, SQLException {
        if (this.connecter.isClosed()) {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }
        if (0 == subscription) {
            throw new OperationsException("Invalid use of consume");
        }
        long resultSetPointer = this.connecter.consume(subscription);

        if (resultSetPointer == TSDBConstants.JNI_CONNECTION_NULL) {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        } else if (resultSetPointer == TSDBConstants.JNI_NULL_POINTER) {
            return null;
        } else {
            return new TSDBResultSet(this.connecter, resultSetPointer);
        }
    }

    /**
     * cancel subscribe
     *
     * @param subscription
     * @param isKeep
     * @throws SQLException
     */
    public void unsubscribe(long subscription, boolean isKeep) throws SQLException {
        if (this.connecter.isClosed()) {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }

        if (null != timerTaskMap.get(subscription)) {
            synchronized (timerTaskMap.get(subscription)) {
                while (1 == timerTaskMap.get(subscription).getState()) {
                    try {
                        Thread.sleep(10);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                timerTaskMap.get(subscription).setState(2);
                if (!timerTaskMap.isEmpty() && timerTaskMap.containsKey(subscription)) {
                    timerTaskMap.get(subscription).cancel();
                    timerTaskMap.remove(subscription);
                    scheduledMap.get(subscription).cancel(false);
                    scheduledMap.remove(subscription);
                }
                this.connecter.unsubscribe(subscription, isKeep);
            }
        } else {
            this.connecter.unsubscribe(subscription, isKeep);
        }
    }

    class TSDBTimerTask extends TimerTask {
        private long subscription;
        private TSDBSubscribeCallBack callBack;
        // 0: not running 1: running 2: cancel
        private int state = 0;

        public TSDBTimerTask(long subscription, TSDBSubscribeCallBack callBack) {
            this.subscription = subscription;
            this.callBack = callBack;
        }

        public int getState() {
            return this.state;
        }

        public void setState(int state) {
            this.state = state;
        }

        @Override
        public void run() {
            synchronized (this) {
                if (2 == state) {
                    return;
                }

                state = 1;

                try {
                    callBack.invoke(consume(subscription));
                } catch (Exception e) {
                    this.cancel();
                    throw new RuntimeException(e);
                }
                state = 0;
            }
        }
    }
}

