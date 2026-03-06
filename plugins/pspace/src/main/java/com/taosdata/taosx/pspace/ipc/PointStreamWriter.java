package com.taosdata.taosx.pspace.ipc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.channels.Channels;
import java.util.*;

/**
 * Manages an Arrow IPC stream over a TCP socket to taosx.
 * <p>
 * Implements the "point" stream protocol expected by {@code ipc_point_reader}
 * on the taosx (Rust) side. Each instance handles one data-type group.
 *
 * <h3>Schema</h3>
 *
 * <pre>
 * id       : Utf8
 * name     : Utf8
 * ts       : Timestamp(MILLISECOND)
 * received : Timestamp(MILLISECOND)
 * value    : &lt;varies by data type group&gt;
 * status   : Int64
 * request  : Timestamp(MILLISECOND)
 *
 * metadata: {version: "1.0", stream: "point", ack: "lush"}
 * </pre>
 *
 * <h3>Usage</h3>
 *
 * <pre>
 * try (PointStreamWriter w = new PointStreamWriter("127.0.0.1:6055", arrowType)) {
 *     w.connect();
 *     w.writeData(tagId, name, tsMs, receivedMs, value, status, requestMs);
 *     w.flushBatch();
 * }
 * </pre>
 */
public class PointStreamWriter implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(PointStreamWriter.class);

    // column indices
    private static final int COL_ID = 0;
    private static final int COL_NAME = 1;
    private static final int COL_TS = 2;
    private static final int COL_RECEIVED = 3;
    private static final int COL_VALUE = 4;
    private static final int COL_STATUS = 5;
    private static final int COL_REQUEST = 6;

    private final String remote; // "host:port"
    private final ArrowType valueType; // Arrow type for the value column
    private final int batchSize;

    private BufferAllocator allocator;
    private VectorSchemaRoot root;
    private ArrowStreamWriter streamWriter;
    private Socket socket;
    private Thread ackThread;

    private int rowsInBatch = 0;
    private boolean started = false;
    private volatile boolean closed = false;

    /**
     * @param remote    taosx IPC address, e.g. "127.0.0.1:6055"
     * @param valueType Arrow type for the {@code value} column
     * @param batchSize max rows per batch before auto-flush
     */
    public PointStreamWriter(String remote, ArrowType valueType, int batchSize) {
        this.remote = remote;
        this.valueType = valueType;
        this.batchSize = batchSize;
    }

    /** Connect to taosx IPC socket and send the schema. */
    public void connect() throws IOException {
        String[] parts = remote.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        allocator = new RootAllocator(Long.MAX_VALUE);
        Schema schema = buildSchema(valueType);
        root = VectorSchemaRoot.create(schema, allocator);

        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(0); // no read timeout for ACK thread

        OutputStream out = socket.getOutputStream();
        streamWriter = new ArrowStreamWriter(root, null, Channels.newChannel(out));
        streamWriter.start();
        started = true;

        // Background thread to drain ACK responses from taosx
        InputStream in = socket.getInputStream();
        ackThread = new Thread(() -> drainAcks(in), "ack-drain-" + remote);
        ackThread.setDaemon(true);
        ackThread.start();

        logger.info("PointStreamWriter connected to {}, value type={}", remote, valueType);
    }

    /**
     * Buffer a single data row. Automatically flushes when batch is full.
     *
     * @param tagId      point ID as string
     * @param name       point name
     * @param tsMs       original timestamp (epoch ms)
     * @param receivedMs received timestamp (epoch ms)
     * @param value      data value — type must match {@link #valueType}
     * @param status     quality/status code
     * @param requestMs  request timestamp (epoch ms)
     */
    public synchronized void writeData(String tagId, String name,
            long tsMs, long receivedMs,
            Object value, long status,
            long requestMs) throws IOException {
        int idx = rowsInBatch;

        // id
        ((VarCharVector) root.getVector(COL_ID))
                .setSafe(idx, tagId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        // name
        ((VarCharVector) root.getVector(COL_NAME))
                .setSafe(idx, name.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        // ts
        ((TimeStampMilliVector) root.getVector(COL_TS)).setSafe(idx, tsMs);
        // received
        ((TimeStampMilliVector) root.getVector(COL_RECEIVED)).setSafe(idx, receivedMs);
        // value
        setValueVector(root.getVector(COL_VALUE), idx, value);
        // status
        ((BigIntVector) root.getVector(COL_STATUS)).setSafe(idx, status);
        // request
        ((TimeStampMilliVector) root.getVector(COL_REQUEST)).setSafe(idx, requestMs);

        rowsInBatch = idx + 1;

        if (rowsInBatch >= batchSize) {
            flushBatch();
        }
    }

    /** Flush current batch to the stream. */
    public synchronized void flushBatch() throws IOException {
        if (rowsInBatch == 0 || !started)
            return;
        root.setRowCount(rowsInBatch);
        streamWriter.writeBatch();
        rowsInBatch = 0;
        root.clear();
    }

    /** Close the stream (sends EOS), socket, and releases Arrow resources. */
    @Override
    public synchronized void close() {
        if (closed)
            return;
        closed = true;
        try {
            if (rowsInBatch > 0 && started) {
                flushBatch();
            }
        } catch (IOException e) {
            logger.warn("Error flushing final batch: {}", e.getMessage());
        }
        try {
            if (streamWriter != null) {
                streamWriter.end();
                streamWriter.close();
            }
        } catch (IOException e) {
            logger.warn("Error closing stream writer: {}", e.getMessage());
        }
        if (root != null) {
            root.close();
        }
        if (allocator != null) {
            allocator.close();
        }
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            logger.warn("Error closing socket: {}", e.getMessage());
        }
        if (ackThread != null) {
            ackThread.interrupt();
        }
        logger.info("PointStreamWriter closed ({})", remote);
    }

    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed() && !closed;
    }

    // ---- private helpers ----

    private static Schema buildSchema(ArrowType valueArrowType) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("version", "1.0");
        metadata.put("stream", "point");
        metadata.put("ack", "lush");

        List<Field> fields = Arrays.asList(
                new Field("id", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("ts", FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
                new Field("received", FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
                new Field("value", FieldType.nullable(valueArrowType), null),
                new Field("status", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("request", FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null));

        return new Schema(fields, metadata);
    }

    /**
     * Set a value into the value vector at the given index.
     * Handles different Arrow types by dispatching to the appropriate vector type.
     */
    @SuppressWarnings("unchecked")
    private void setValueVector(FieldVector vector, int idx, Object value) {
        if (value == null) {
            vector.setNull(idx);
            return;
        }

        if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(idx, toDouble(value));
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).setSafe(idx, toFloat(value));
        } else if (vector instanceof IntVector) {
            ((IntVector) vector).setSafe(idx, toInt(value));
        } else if (vector instanceof SmallIntVector) {
            ((SmallIntVector) vector).setSafe(idx, toShort(value));
        } else if (vector instanceof TinyIntVector) {
            ((TinyIntVector) vector).setSafe(idx, toByte(value));
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setSafe(idx, toLong(value));
        } else if (vector instanceof UInt1Vector) {
            ((UInt1Vector) vector).setSafe(idx, toByte(value));
        } else if (vector instanceof UInt2Vector) {
            ((UInt2Vector) vector).setSafe(idx, (char) toShort(value));
        } else if (vector instanceof UInt4Vector) {
            ((UInt4Vector) vector).setSafe(idx, toInt(value));
        } else if (vector instanceof UInt8Vector) {
            ((UInt8Vector) vector).setSafe(idx, toLong(value));
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).setSafe(idx, toBool(value) ? 1 : 0);
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setSafe(idx,
                    value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } else if (vector instanceof TimeStampMilliVector) {
            ((TimeStampMilliVector) vector).setSafe(idx, toLong(value));
        } else {
            // Fallback: try as double
            logger.warn("Unsupported value vector type {}, falling back to double", vector.getClass().getSimpleName());
            if (vector instanceof Float8Vector) {
                ((Float8Vector) vector).setSafe(idx, toDouble(value));
            } else {
                vector.setNull(idx);
            }
        }
    }

    private static double toDouble(Object v) {
        return v instanceof Number ? ((Number) v).doubleValue() : Double.parseDouble(v.toString());
    }

    private static float toFloat(Object v) {
        return v instanceof Number ? ((Number) v).floatValue() : Float.parseFloat(v.toString());
    }

    private static int toInt(Object v) {
        return v instanceof Number ? ((Number) v).intValue() : Integer.parseInt(v.toString());
    }

    private static short toShort(Object v) {
        return v instanceof Number ? ((Number) v).shortValue() : Short.parseShort(v.toString());
    }

    private static byte toByte(Object v) {
        return v instanceof Number ? ((Number) v).byteValue() : Byte.parseByte(v.toString());
    }

    private static long toLong(Object v) {
        return v instanceof Number ? ((Number) v).longValue() : Long.parseLong(v.toString());
    }

    private static boolean toBool(Object v) {
        if (v instanceof Boolean)
            return (Boolean) v;
        if (v instanceof Number)
            return ((Number) v).intValue() != 0;
        return Boolean.parseBoolean(v.toString());
    }

    /** Read and discard ACK messages from taosx. Logs errors. */
    private void drainAcks(InputStream in) {
        byte[] buf = new byte[4096];
        try {
            while (!closed && !Thread.currentThread().isInterrupted()) {
                int n = in.read(buf);
                if (n < 0) {
                    logger.info("ACK stream closed by taosx ({})", remote);
                    break;
                }
                // ACK data is read and discarded; errors are logged from taosx side
            }
        } catch (IOException e) {
            if (!closed) {
                logger.debug("ACK drain ended: {}", e.getMessage());
            }
        }
    }
}
