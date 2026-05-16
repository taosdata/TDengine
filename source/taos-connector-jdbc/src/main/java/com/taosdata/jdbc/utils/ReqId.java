package com.taosdata.jdbc.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class ReqId {

    private ReqId() {
    }

    private static final long T_UUID_HASH_ID;
    private static final long PID;
    private static final AtomicLong serialNo = new AtomicLong(0);

    static {
        String uuid = UUID.randomUUID().toString();
        long hash = murmurHash32(uuid.getBytes(StandardCharsets.UTF_8), uuid.length());
        T_UUID_HASH_ID = (hash & 0x07ff) << 52;
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        PID = (long) (Long.valueOf(runtimeMXBean.getName().split("@")[0]).intValue() & 0x0f) << 48;
    }

    public static long getReqID() {
        long ts = (System.currentTimeMillis() >> 8) & 0x3ffffff;
        long val = serialNo.incrementAndGet();
        return T_UUID_HASH_ID | PID | (ts << 20) | (val & 0xfffff);
    }

    public static long murmurHash32(byte[] data, int seed) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        final int r1 = 15;
        final int r2 = 13;
        final int m = 5;
        final int n = 0xe6546b64;

        int hash = seed;
        int length = data.length;

        for (int i = 0; i < length / 4; i++) {
            int k = ByteBuffer.wrap(data, i * 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();

            k *= c1;
            k = Integer.rotateLeft(k, r1);
            k *= c2;

            hash ^= k;
            hash = Integer.rotateLeft(hash, r2);
            hash = hash * m + n;
        }

        int k = 0;
        // TODO : check this
        switch (length & 3) {
            case 3: // NOSONAR
                k ^= data[length - 3] << 16;
            case 2: // NOSONAR
                k ^= data[length - 2] << 8;
            case 1:
                k ^= data[length - 1];
                k *= c1;
                k = Integer.rotateLeft(k, r1);
                k *= c2;
                hash ^= k;
        }

        hash ^= length;

        hash ^= hash >>> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >>> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >>> 16;

        return Integer.toUnsignedLong(hash);
    }

}
