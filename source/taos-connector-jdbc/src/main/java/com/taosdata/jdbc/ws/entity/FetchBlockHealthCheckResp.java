package com.taosdata.jdbc.ws.entity;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.List;

public class FetchBlockHealthCheckResp extends FetchBlockNewResp {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FetchBlockHealthCheckResp.class);

    public boolean isClusterAlive() {
        return isClusterAlive;
    }

    private boolean isClusterAlive;
    public FetchBlockHealthCheckResp(ByteBuf buffer) {
        super(buffer);
        super.init();

        if (getCode() == Code.SUCCESS.getCode() && !isCompleted()){
            try {
                int columns = 1;
                if (buffer != null) {
                    buffer.readIntLE(); // buffer length
                    int pHeader = buffer.readerIndex() + 28 + columns * 5;
                    buffer.skipBytes(8);
                    int numOfRows = buffer.readIntLE();

                    buffer.readerIndex(pHeader);
                    int bitMapOffset = bitmapLen(numOfRows);

                    List<Integer> lengths = new ArrayList<>(columns);
                    for (int i = 0; i < columns; i++) {
                        lengths.add(buffer.readIntLE());
                    }
                    pHeader = buffer.readerIndex();
                    int length = 0;
                    for (int i = 0; i < columns; i++) {
                        length = bitMapOffset;
                        byte[] tmp = new byte[bitMapOffset];
                        buffer.readBytes(tmp);
                        for (int j = 0; j < numOfRows; j++) {
                            int in = buffer.readIntLE();
                            isClusterAlive = in > 0;
                        }
                        pHeader += length + lengths.get(i);
                        buffer.readerIndex(pHeader);
                    }
                }
            } catch (Exception e){
                log.error("error on parse health check response", e);
            } finally {
                ReferenceCountUtil.safeRelease(buffer);
            }
        }
        else {
            isClusterAlive = false;
        }

    }

    public static FetchBlockHealthCheckResp getFailedResp() {
        FetchBlockHealthCheckResp resp = new FetchBlockHealthCheckResp(null);
        resp.isClusterAlive = false;
        return resp;
    }
    private int bitmapLen(int n) {
        return (n + 0x7) >> 3;
    }
}
