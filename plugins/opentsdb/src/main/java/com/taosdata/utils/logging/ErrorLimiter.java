package com.taosdata.utils.logging;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple error log rate limiter with debounce and suppression counting.
 * For the same key, only allow one stacktrace within a time window, and count suppressed occurrences.
 *
 * Notes on memory safety:
 * - This limiter keeps a bounded map of recent keys with idle eviction to avoid unbounded growth.
 * - Keys should be coarse enough to avoid high cardinality (avoid including full exception messages).
 */
public class ErrorLimiter {

    private static class Entry {
        long lastLogMillis = 0L;
        long lastAccessMillis = 0L;
        int suppressed = 0;
    }

    private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();
    private final long windowMillis;
    private final long expireAfterAccessMillis;
    private final int maxEntries;
    private final AtomicInteger insertCount = new AtomicInteger(0);

    /**
     * @param windowMillis debounce window in milliseconds
     */
    public ErrorLimiter(long windowMillis) {
        this(windowMillis, 4096, Math.max(windowMillis * 10, 5 * 60_000L));
    }

    /**
     * @param windowMillis debounce window in milliseconds
     * @param maxEntries maximum number of distinct keys to retain
     * @param expireAfterAccessMillis entries idle for at least this long are eligible for eviction
     */
    public ErrorLimiter(long windowMillis, int maxEntries, long expireAfterAccessMillis) {
        this.windowMillis = windowMillis;
        this.maxEntries = Math.max(128, maxEntries);
        this.expireAfterAccessMillis = Math.max(windowMillis, expireAfterAccessMillis);
    }

    /**
     * Whether we should log now for the given key. If returns false, it increases the suppressed counter.
     */
    public boolean shouldLog(String key) {
        long now = System.currentTimeMillis();
        Entry entry = map.computeIfAbsent(key, k -> {
            Entry e = new Entry();
            e.lastAccessMillis = now;
            // Throttle cleanup work to only run occasionally on insert path
            int n = insertCount.incrementAndGet();
            if ((n & 0x3F) == 0) { // every 64 inserts
                maybeCleanup(now);
            }
            return e;
        });
        synchronized (entry) {
            entry.lastAccessMillis = now;
            if (now - entry.lastLogMillis >= windowMillis) {
                entry.lastLogMillis = now;
                return true;
            } else {
                entry.suppressed++;
                return false;
            }
        }
    }

    /**
     * Get and reset suppressed count for the key.
     */
    public int getAndResetSuppressed(String key) {
        Entry entry = map.get(key);
        if (entry == null) return 0;
        synchronized (entry) {
            entry.lastAccessMillis = System.currentTimeMillis();
            int n = entry.suppressed;
            entry.suppressed = 0;
            return n;
        }
    }

    /**
     * Opportunistic cleanup to prevent unbounded growth. Removes idle entries and trims when above maxEntries.
     */
    private void maybeCleanup(long now) {
        if (map.size() <= maxEntries) {
            return;
        }

        // First pass: remove entries idle past expireAfterAccessMillis
        for (ConcurrentHashMap.Entry<String, Entry> e : map.entrySet()) {
            Entry val = e.getValue();
            if (now - val.lastAccessMillis >= expireAfterAccessMillis) {
                map.remove(e.getKey(), val);
            }
        }

        // If still over capacity, do a second pass to trim oldest entries best-effort
        int size = map.size();
        if (size > maxEntries) {
            // Best-effort: compute a soft threshold and remove entries older than that until under capacity
            long threshold = now - (expireAfterAccessMillis / 2);
            for (ConcurrentHashMap.Entry<String, Entry> e : map.entrySet()) {
                if (map.size() <= maxEntries) break;
                Entry val = e.getValue();
                if (val.lastAccessMillis < threshold) {
                    map.remove(e.getKey(), val);
                }
            }
        }
    }

    // -------------------------------- Utility helpers --------------------------------

    /**
     * Build a low-cardinality key for an exception category. Avoids using the full exception message
     * which can create high-cardinality keys and increase memory usage.
     *
     * Suggested usage: ErrorLimiter.key("db.write", throwable)
     */
    public static String key(String category, Throwable t) {
        StringBuilder sb = new StringBuilder();
        sb.append(category == null ? "unknown" : category);
        if (t != null) {
            Throwable root = rootCause(t);
            sb.append('|').append(root.getClass().getName());
            StackTraceElement[] st = root.getStackTrace();
            if (st != null && st.length > 0) {
                StackTraceElement top = st[0];
                sb.append('|').append(top.getClassName())
                  .append('#').append(top.getMethodName())
                  .append(':').append(top.getLineNumber());
            }
            // Optionally include a tiny hash of the message to disambiguate, but keep bounded
            String msg = root.getMessage();
            if (msg != null && !msg.isEmpty()) {
                int h = fastHash32(msg);
                sb.append('|').append(Integer.toHexString(h));
            }
        }
        return sb.toString();
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }

    private static int fastHash32(String s) {
        // Simple fast 32-bit hash (FNV-1a) over UTF-8 bytes; stable and cheap
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int hash = 0x811C9DC5;
        for (byte b : bytes) {
            hash ^= (b & 0xFF);
            hash *= 0x01000193;
        }
        return hash;
    }
}
