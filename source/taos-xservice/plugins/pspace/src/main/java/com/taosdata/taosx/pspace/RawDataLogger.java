package com.taosdata.taosx.pspace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Writes raw pSpace data to local files for auditing and debugging.
 * <p>
 * Each batch of data is appended to a daily-rotated file under the configured
 * directory. Thread-safe via synchronized write method.
 */
public class RawDataLogger implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(RawDataLogger.class);
    private static final SimpleDateFormat DAY_FMT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat TS_FMT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private final File baseDir;
    private String currentDay;
    private BufferedWriter writer;

    public RawDataLogger(String dir) throws IOException {
        this.baseDir = new File(dir);
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new IOException("Cannot create raw data directory: " + baseDir.getAbsolutePath());
        }
        logger.info("RawDataLogger initialized, dir={}", baseDir.getAbsolutePath());
    }

    /**
     * Write a single data record line.
     *
     * @param tagId   point ID
     * @param name    point name
     * @param tsMs    original timestamp (epoch ms)
     * @param value   data value
     * @param quality quality ordinal
     */
    public synchronized void write(long tagId, String name, long tsMs, Object value, int quality) {
        try {
            ensureWriter();
            writer.write(TS_FMT.format(new Date(tsMs)));
            writer.write('\t');
            writer.write(String.valueOf(tagId));
            writer.write('\t');
            writer.write(name != null ? name : "");
            writer.write('\t');
            writer.write(value != null ? value.toString() : "null");
            writer.write('\t');
            writer.write(String.valueOf(quality));
            writer.newLine();
        } catch (IOException e) {
            logger.warn("Failed to write raw data: {}", e.getMessage());
        }
    }

    /** Flush buffered data. */
    public synchronized void flush() {
        if (writer != null) {
            try {
                writer.flush();
            } catch (IOException e) {
                logger.warn("Failed to flush raw data: {}", e.getMessage());
            }
        }
    }

    @Override
    public synchronized void close() {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                logger.warn("Failed to close raw data writer: {}", e.getMessage());
            }
            writer = null;
        }
    }

    // --- internals ---

    private void ensureWriter() throws IOException {
        String day = DAY_FMT.format(new Date());
        if (writer != null && day.equals(currentDay)) {
            return; // still same day
        }
        // rotate
        if (writer != null) {
            writer.flush();
            writer.close();
        }
        currentDay = day;
        File file = new File(baseDir, "rawdata-" + day + ".tsv");
        boolean isNew = !file.exists();
        writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file, true), StandardCharsets.UTF_8),
                8192);
        if (isNew) {
            writer.write("timestamp\ttagId\tname\tvalue\tquality");
            writer.newLine();
        }
    }
}
