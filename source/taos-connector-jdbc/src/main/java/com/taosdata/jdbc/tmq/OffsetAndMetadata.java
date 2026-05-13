package com.taosdata.jdbc.tmq;

public class OffsetAndMetadata {
    private final long offset;
    private final String metadata;


    public OffsetAndMetadata(long offset) {
        this(offset, "");
    }

    public OffsetAndMetadata(long offset, String metadata) {
        if (offset < 0 && offset != TMQConstants.INVALID_OFFSET)
            throw new IllegalArgumentException("Invalid negative offset: " + offset);

        this.offset = offset;

        this.metadata = "";
    }

    public long offset() {
        return offset;
    }

    public String metadata() {
        return metadata;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("OffsetAndMetadata{");
        sb.append("offset=").append(offset);
        sb.append('}');
        return sb.toString();
    }
}
