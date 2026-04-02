package com.taosdata.hdfs.hadoop;

import com.sun.istack.NotNull;
import org.anarres.lzo.LzoVersion;
import org.anarres.lzo.LzopConstants;
import org.anarres.lzo.LzopInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.Adler32;
import java.util.zip.CRC32;

/**
 * @author Jiangyi Hou
 * @since 19-5-14
 */
public class ExtendedLzopInputStream extends LzopInputStream {
    public ExtendedLzopInputStream(@NotNull InputStream in) throws IOException {
        super(in);
    }

    @Override
    protected int readHeader() throws IOException {

        final byte[] LZOP_MAGIC = new byte[]{
                -119, 'L', 'Z', 'O', 0, '\r', '\n', '\032', '\n'};
        final short LZO_LIBRARY_VERSION = 0x2060; // intentionally set the lzo_library_version to 0x2060, oringally 0x2050
        Log LOG = LogFactory.getLog(LzopInputStream.class);

        byte[] buf = new byte[9];
        super.readBytes(buf, 0, 9);
        if (!Arrays.equals(buf, LZOP_MAGIC))
            throw new IOException("Invalid LZO header");
        Arrays.fill(buf, (byte) 0);
        Adler32 adler = new Adler32();
        CRC32 crc32 = new CRC32();
        int hitem = readHeaderItem(buf, 2, adler, crc32); // lzop version
        if (hitem > LzopConstants.LZOP_VERSION) {
            LOG.debug("Compressed with later version of lzop: "
                    + Integer.toHexString(hitem) + " (expected 0x"
                    + Integer.toHexString(LzopConstants.LZOP_VERSION) + ")");
        }
        hitem = readHeaderItem(buf, 2, adler, crc32); // lzo library version
        if (hitem > LZO_LIBRARY_VERSION) {
            throw new IOException("Compressed with incompatible lzo version: 0x"
                    + Integer.toHexString(hitem) + " (expected 0x"
                    + Integer.toHexString(LZO_LIBRARY_VERSION) + ")");
        }
        hitem = readHeaderItem(buf, 2, adler, crc32); // lzop extract version
        if (hitem > LzopConstants.LZOP_VERSION) {
            throw new IOException("Compressed with incompatible lzop version: 0x"
                    + Integer.toHexString(hitem) + " (expected 0x"
                    + Integer.toHexString(LzopConstants.LZOP_VERSION) + ")");
        }
        hitem = readHeaderItem(buf, 1, adler, crc32); // method
        switch (hitem) {
            case LzopConstants.M_LZO1X_1:
            case LzopConstants.M_LZO1X_1_15:
            case LzopConstants.M_LZO1X_999:
                break;
            default:
                throw new IOException("Invalid strategy " + Integer.toHexString(hitem));
        }
        readHeaderItem(buf, 1, adler, crc32); // ignore level

        // flags
        int flags = readHeaderItem(buf, 4, adler, crc32);
        boolean useCRC32 = (flags & LzopConstants.F_H_CRC32) != 0;
        boolean extraField = (flags & LzopConstants.F_H_EXTRA_FIELD) != 0;
        if ((flags & LzopConstants.F_MULTIPART) != 0)
            throw new IOException("Multipart lzop not supported");
        if ((flags & LzopConstants.F_H_FILTER) != 0)
            throw new IOException("lzop filter not supported");
        if ((flags & LzopConstants.F_RESERVED) != 0)
            throw new IOException("Unknown flags in header");
        // known !F_H_FILTER, so no optional block

        readHeaderItem(buf, 4, adler, crc32); // ignore mode
        readHeaderItem(buf, 4, adler, crc32); // ignore mtime
        readHeaderItem(buf, 4, adler, crc32); // ignore gmtdiff
        hitem = readHeaderItem(buf, 1, adler, crc32); // fn len
        if (hitem > 0) {
            byte[] tmp = (hitem > buf.length) ? new byte[hitem] : buf;
            readHeaderItem(tmp, hitem, adler, crc32); // skip filename
        }
        int checksum = (int) (useCRC32 ? crc32.getValue() : adler.getValue());
        hitem = readHeaderItem(buf, 4, adler, crc32); // read checksum
        if (hitem != checksum) {
            throw new IOException("Invalid header checksum: "
                    + Long.toHexString(checksum) + " (expected 0x"
                    + Integer.toHexString(hitem) + ")");
        }
        if (extraField) { // lzop 1.08 ultimately ignores this
            LOG.debug("Extra header field not processed");
            adler.reset();
            crc32.reset();
            hitem = readHeaderItem(buf, 4, adler, crc32);
            readHeaderItem(new byte[hitem], hitem, adler, crc32);
            checksum = (int) (useCRC32 ? crc32.getValue() : adler.getValue());
            if (checksum != readHeaderItem(buf, 4, adler, crc32)) {
                throw new IOException("Invalid checksum for extra header field");
            }
        }

        return flags;
    }

    /**
     * Read bytes, update checksums, return first four bytes as an int, first
     * byte read in the MSB.
     */
    // @Nonnegative ?
    private int readHeaderItem(@Nonnull byte[] buf, @Nonnegative int len, @Nonnull Adler32 adler, @Nonnull CRC32 crc32) throws IOException {
        int ret = readInt(buf, len);
        adler.update(buf, 0, len);
        crc32.update(buf, 0, len);
        Arrays.fill(buf, (byte) 0);
        return ret;
    }

    /**
     * Read len bytes into buf, st LSB of int returned is the last byte of the
     * first word read.
     */
    // @Nonnegative ?
    private int readInt(@Nonnull byte[] buf, @Nonnegative int len)
            throws IOException {
        readBytes(buf, 0, len);
        int ret = (0xFF & buf[0]) << 24;
        ret |= (0xFF & buf[1]) << 16;
        ret |= (0xFF & buf[2]) << 8;
        ret |= (0xFF & buf[3]);
        return (len > 3) ? ret : (ret >>> (8 * (4 - len)));
    }
}
