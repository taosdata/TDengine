package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.*;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.SyncObj;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.stmt2.entity.*;
import io.netty.buffer.ByteBuf;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

// Efficient Writing Mode PreparedStatement
public class WSEWPreparedStatement extends AbsWSPreparedStatement {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(WSEWPreparedStatement.class);

    private final boolean copyData;
    private final int writeThreadNum;
    private final ThreadPoolExecutor writerThreads;
    private final ArrayList<EWBackendThreadInfo> backendThreadInfoList;
    private final AtomicInteger remainingUnprocessedRows = new AtomicInteger(0);
    private final AtomicInteger batchInsertedRows = new AtomicInteger(0);
    private final AtomicInteger flushIn = new AtomicInteger(0);
    private final List<WorkerThread> workerThreadList;
    private final SyncObj syncObj = new SyncObj();
    private static final ForkJoinPool serializeExecutor = Utils.getForkJoinPool();
    private int addBatchCounts = 0;

    public WSEWPreparedStatement(Transport transport, ConnectionParam param, String database, AbstractConnection connection, String sql, Long instanceId, Stmt2PrepareResp prepareResp) throws SQLException {
        super(transport, param, database, connection, sql, instanceId, prepareResp);
        copyData = param.isCopyData();
        writeThreadNum = param.getBackendWriteThreadNum();

        writerThreads =  (ThreadPoolExecutor) Executors.newFixedThreadPool(writeThreadNum);
        backendThreadInfoList = new ArrayList<>(writeThreadNum);
        for (int i = 0; i < writeThreadNum; i++){
            backendThreadInfoList.add(new EWBackendThreadInfo(param.getCacheSizeByRow(), param.getCacheSizeByRow() / param.getBatchSizeByRow()));
        }

        workerThreadList = new ArrayList<>(writeThreadNum);

        for (int i = 0; i < writeThreadNum; i++){
            WorkerThread workerThread = new WorkerThread(
                    backendThreadInfoList.get(i),
                    new StmtInfo(prepareResp, sql),
                    new PstmtConInfo(transport, param, database, connection, instanceId),
                    closed,
                    remainingUnprocessedRows,
                    batchInsertedRows,
                    flushIn,
                    syncObj
                    );
            workerThreadList.add(workerThread);
        }

        try {
            for (int i = 0; i < writeThreadNum; i++) {
                workerThreadList.get(i).initStmt(param.getRetryTimes());
            }
        } catch (SQLException e) { //NOSONAR
            for (WorkerThread workerThread : workerThreadList){
                workerThread.releaseStmt();
            }
            log.error("Failed to initialize prepared statement, sql: {}", sql, e);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "Failed to initialize prepared statement: " + e.getMessage());
        }

        for (WorkerThread workerThread : workerThreadList){
            writerThreads.submit(workerThread);
        }
    }

    private Map<Integer, Column> copyMap(Map<Integer, Column> originalMap) {
        Map<Integer, Column> dstMap = new HashMap<>();
        originalMap.forEach((key, src) -> {
            Column dst = src;
            if (copyData && src.getData() instanceof byte[]) {
                byte[] srcBytes = (byte[]) src.getData();
                byte[] copiedValue = new byte[srcBytes.length];
                System.arraycopy(srcBytes, 0, copiedValue, 0, srcBytes.length);
                dst = new Column(copiedValue, src.getType(), src.getIndex());
            }

            dstMap.put(key, dst);
        });
        return dstMap;
    }


    @Override
    public boolean execute() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        if (stmtInfo.isInsert()){
            executeUpdate();
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only support insert.");
        }

        return !stmtInfo.isInsert();
    }
    @Override
    public ResultSet executeQuery() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only support insert.");
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        waitWriteCompleted();

        Exception lastError = null;
        for (WorkerThread workerThread : workerThreadList) {
            Exception tempEx = workerThread.getAndClearLastError();
            if (tempEx != null && lastError == null) {
                lastError = tempEx;
            }
        }

        int totalRowsInserted = batchInsertedRows.getAndSet(0);
        if (lastError != null) {
            throw new SQLException("InsertedRows: " + totalRowsInserted + ", ErrorInfo: " + lastError.getMessage(), "", TSDBErrorNumbers.ERROR_FW_WRITE_ERROR);
        }

        return totalRowsInserted;
    }

    private void waitWriteCompleted() {
        flushIn.incrementAndGet();
        while (remainingUnprocessedRows.get() != 0) {
            try {
                syncObj.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void checkDataLength(Map<Integer, Column> map) throws SQLException {
        for (int i = 0; i < stmtInfo.getFields().size(); i++){
            Field field = stmtInfo.getFields().get(i);
            Column column = map.get(i + 1);
            if (DataLengthCfg.getDataLength(column.getType()) == null && field.getBindType() != FieldBindType.TAOS_FIELD_TBNAME.getValue()){
                if (column.getData() instanceof byte[]){
                    byte[] data = (byte[]) column.getData();
                    if (data.length > field.getBytes()){
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "data length is too long, column index " + i);
                    }
                }
                if (column.getData() instanceof String){
                    String data = (String) column.getData();
                    if (data.getBytes().length > field.getBytes()){
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "data length is too long, column index " + i);
                    }
                }
            }
        }
    }

    private static void triggerSerializeProgressive(EWBackendThreadInfo ewBackendThreadInfo,
                                                 StmtInfo stmtInfo,
                                                    int batchSize) {
        if (!ewBackendThreadInfo.getWriteQueue().isEmpty()
                && ewBackendThreadInfo.getSerialQueue().remainingCapacity() > 0
                && ewBackendThreadInfo.getSerializeRunning().compareAndSet(false, true)) {
            WSEWSerializationTask serializeTask = new WSEWSerializationTask(
                    ewBackendThreadInfo,
                    Math.min(ewBackendThreadInfo.getWriteQueue().size(), batchSize),
                    stmtInfo,
                    true);
            serializeExecutor.submit(serializeTask);
        }
    }
    private static void triggerSerializeIfNeeded(EWBackendThreadInfo ewBackendThreadInfo,
                                                 StmtInfo stmtInfo,
                                                 int batchSize) {
        if (ewBackendThreadInfo.getWriteQueue().size() >= batchSize
                && ewBackendThreadInfo.getSerialQueue().remainingCapacity() > 0
                && ewBackendThreadInfo.getSerializeRunning().compareAndSet(false, true)) {
            WSEWSerializationTask serializeTask = new WSEWSerializationTask(
                    ewBackendThreadInfo,
                    batchSize,
                    stmtInfo,
                    false);
            serializeExecutor.submit(serializeTask);
        }
    }
    @Override
    public void addBatch() throws SQLException {
        if (colOrderedMap.size() == stmtInfo.getFields().size()){
            // jdbc standard bind api
            Map<Integer, Column> map = copyMap(colOrderedMap);

            if (param.isStrictCheck()){
                checkDataLength(map);
            }

            int hashCode;
            Object o = map.get(stmtInfo.getToBeBindTableNameIndex() + 1).getData();
            if (o instanceof String){
                hashCode = o.hashCode();
            } else if (o instanceof byte[]){
                hashCode = Arrays.hashCode((byte[]) o);
            } else {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "error type tbname.");
            }

            if (hashCode < 0){
                hashCode = -hashCode;
            }

            int index = hashCode % writeThreadNum;
            EWBackendThreadInfo ewBackendThreadInfo = backendThreadInfoList.get(index);

            try {
                ewBackendThreadInfo.getWriteQueue().put(map);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted while adding batch", e);
            }

            triggerSerializeIfNeeded(ewBackendThreadInfo, stmtInfo, param.getBatchSizeByRow());

            remainingUnprocessedRows.incrementAndGet();
            addBatchCounts++;
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "Only support standard jdbc bind api.");
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {

        int[] ints = new int[addBatchCounts];
        for (int i = 0, len = ints.length; i < len; i++){
            ints[i] = SUCCESS_NO_INFO;
        }

        addBatchCounts = 0;
        return ints;
    }

    @Override
    public void close() throws SQLException {
        waitWriteCompleted();
        if (isClosed())
            return;

        super.close();

        // wait backFetchExecutor to finish
        if (writerThreads != null) {
            while (writerThreads.getActiveCount() != 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            if (!writerThreads.isShutdown()) {
                writerThreads.shutdown();
            }
        }

        for (WorkerThread workerThread : workerThreadList){
            workerThread.releaseStmt();
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, "Fast write mode only support insert.");
    }

    @Override
    public void columnDataAddBatch() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }


    @Override
    public void columnDataExecuteBatch() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public void columnDataCloseBatch() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    static class WSEWSerializationTask extends RecursiveAction {
        final transient ArrayBlockingQueue<Map<Integer, Column>> writeQueue;
        final transient ArrayBlockingQueue<EWRawBlock> serialQueue;
        final int batchSize;

        private transient TableInfo tableInfo = TableInfo.getEmptyTableInfo();
        private final HashMap<ByteBuffer, TableInfo> tableInfoMap = new HashMap<>();
        private final transient StmtInfo stmtInfo;
        private final AtomicBoolean running;
        private final boolean isProgressive;

        public WSEWSerializationTask(EWBackendThreadInfo ewBackendThreadInfo,
                                     int batchSize,
                                     StmtInfo stmtInfo,
                                     boolean isProgressive) {
            this.writeQueue = ewBackendThreadInfo.getWriteQueue();
            this.serialQueue = ewBackendThreadInfo.getSerialQueue();
            this.running = ewBackendThreadInfo.getSerializeRunning();
            this.batchSize = batchSize;
            this.stmtInfo = stmtInfo;
            this.isProgressive = isProgressive;
        }

        public void processOneRow(Map<Integer, Column> colOrderedMap) throws SQLException {
            if (isTableInfoEmpty()) {
                // first time, bind all
                bindAllToTableInfo(stmtInfo.getFields(), colOrderedMap, tableInfo);
            } else {
                Object tbname = colOrderedMap.get(stmtInfo.getToBeBindTableNameIndex() + 1).getData();
                ByteBuffer tempTableName;
                if (tbname instanceof String){
                    tempTableName = ByteBuffer.wrap(((String)tbname).getBytes(StandardCharsets.UTF_8));
                } else if (tbname instanceof byte[]){
                    tempTableName = ByteBuffer.wrap((byte[]) tbname);
                } else {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "table name must be string or binary");
                }

                if (tableInfo.getTableName().equals(tempTableName)){
                    // same table, only bind col
                    bindColToTableInfo(tableInfo, colOrderedMap);
                } else if (tableInfoMap.containsKey(tempTableName)){
                    // same table, only bind col
                    TableInfo tbInfo = tableInfoMap.get(tempTableName);
                    bindColToTableInfo(tbInfo, colOrderedMap);
                } else {
                    // different table, flush tableInfo and create a new one
                    tableInfoMap.put(tableInfo.getTableName(), tableInfo);
                    tableInfo = TableInfo.getEmptyTableInfo();
                    bindAllToTableInfo(stmtInfo.getFields(), colOrderedMap, tableInfo);
                }
            }
        }

        private boolean isTableInfoEmpty(){
            return tableInfo.getTableName().capacity() == 0
                    && tableInfo.getTagInfo().isEmpty()
                    && tableInfo.getDataList().isEmpty();
        }

        private void bindColToTableInfo(TableInfo tableInfo, Map<Integer, Column> colOrderedMap){
            for (ColumnInfo columnInfo : tableInfo.getDataList()) {
                columnInfo.add(colOrderedMap.get(columnInfo.getIndex()).getData());
            }
        }

        private void putEWRawBlock(EWRawBlock ewRawBlock){
            try {
                serialQueue.put(ewRawBlock);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        @Override
        protected void compute() {
            SQLException lastError = null;
            while (writeQueue.size() >= batchSize && serialQueue.remainingCapacity() > 0) {
                for (int i = 0; i < batchSize; i++) {
                    try {
                        Map<Integer, Column> map = writeQueue.take();
                        processOneRow(map);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (SQLException e) {
                        lastError = e;
                    }
                }
                if (!isTableInfoEmpty()) {
                    tableInfoMap.put(tableInfo.getTableName(), tableInfo);
                }

                if (lastError != null) {
                    tableInfo = TableInfo.getEmptyTableInfo();
                    tableInfoMap.clear();
                    putEWRawBlock(new EWRawBlock(null, batchSize, lastError));
                    break;
                }

                try {
                    long reqId = ReqId.getReqID();
                    ByteBuf rawBlock = SerializeBlock.getStmt2BindBlock(
                            tableInfoMap,
                            stmtInfo,
                            reqId);
                    putEWRawBlock(new EWRawBlock(rawBlock, batchSize, lastError));
                    log.trace("buffer allocated: {}", Integer.toHexString(System.identityHashCode(rawBlock)));
                } catch (SQLException e) {
                    lastError = e;
                    putEWRawBlock(new EWRawBlock(null, batchSize, lastError));
                    log.error("Error in serialize data to block, stmt id: {}", stmtInfo.getStmtId(), e);
                    break;
                } catch (Exception e) {
                    lastError = new SQLException("Error in serialize data to block, stmt id: " + stmtInfo.getStmtId(), e);
                    putEWRawBlock(new EWRawBlock(null, batchSize, lastError));
                    log.error("Error in serialize data to block, stmt id: {}", stmtInfo.getStmtId(), e);
                    break;
                } finally {
                    tableInfo = TableInfo.getEmptyTableInfo();
                    tableInfoMap.clear();
                }
                if (isProgressive){
                    break;
                }
            }
            running.set(false);
        }
    }

    static class WorkerThread extends WSRetryableStmt implements Runnable {
        private static final org.slf4j.Logger log = LoggerFactory.getLogger(WorkerThread.class);
        private final EWBackendThreadInfo backendThreadInfo;
        private final AtomicBoolean isClosed;
        private final AtomicInteger remainingUnprocessedRows;
        private final AtomicInteger flushIn;
        private final SyncObj syncObj;

        public WorkerThread(EWBackendThreadInfo backendThreadInfo,
                            StmtInfo stmtInfo,
                            PstmtConInfo conInfo,
                            AtomicBoolean isClosed,
                            AtomicInteger remainingUnprocessedRows,
                            AtomicInteger batchInsertedRows,
                            AtomicInteger flushIn,
                            SyncObj syncObj) {
            super(conInfo.getConnection(),
                    conInfo.getParam(),
                    conInfo.getDatabase(),
                    conInfo.getTransport(),
                    conInfo.getInstanceId(),
                    stmtInfo,
                    batchInsertedRows);
            this.backendThreadInfo = backendThreadInfo;
            this.stmtInfo = stmtInfo;
            this.isClosed = isClosed;
            this.remainingUnprocessedRows = remainingUnprocessedRows;
            this.flushIn = flushIn;
            this.syncObj = syncObj;
        }
        @Override
        public void run() {
            int flushInLocal = 0;
            ArrayBlockingQueue<Map<Integer, Column>> writeQueue = backendThreadInfo.getWriteQueue();
            ArrayBlockingQueue<EWRawBlock> serialQueue = backendThreadInfo.getSerialQueue();
            AtomicBoolean serializeRunning = backendThreadInfo.getSerializeRunning();
            while (!(isClosed.get()
                    && serialQueue.isEmpty()
                    && writeQueue.isEmpty()
                    && !serializeRunning.get())) {
                EWRawBlock ewRawBlock;
                int rowCount = 0;
                try {
                    if (flushIn.get() != flushInLocal) {
                        flushInLocal = flushIn.get();
                        syncObj.signal();
                    }

                    ewRawBlock = serialQueue.poll(50, TimeUnit.MILLISECONDS);
                    if (ewRawBlock == null) {
                        triggerSerializeProgressive(backendThreadInfo, stmtInfo, param.getBatchSizeByRow());
                        continue;
                    }

                    rowCount = ewRawBlock.getRowCount();
                    lastError.set(ewRawBlock.getLastError());
                    if (lastError.get() == null) {
                        writeBlockWithRetry(ewRawBlock.getByteBuf());
                        triggerSerializeIfNeeded(backendThreadInfo, stmtInfo, param.getBatchSizeByRow());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (SQLException e) {
                    lastError.set(e);
                    log.error("Error in write data to server, stmt id: {}" +
                                    "rows: {}, code: {}, msg: {}",
                            stmtInfo.getStmtId(),
                            rowCount, e.getErrorCode(), e.getMessage());
                } finally {
                    if (rowCount > 0) {
                        remainingUnprocessedRows.addAndGet(-rowCount);
                    }
                }
            }

            syncObj.signal();
        }

        public Exception getAndClearLastError() {
            Exception tmp = lastError.get();
            lastError.set(null);
            return tmp;
        }
    }
}
