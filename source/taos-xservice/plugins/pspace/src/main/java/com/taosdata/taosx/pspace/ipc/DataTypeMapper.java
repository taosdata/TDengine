package com.taosdata.taosx.pspace.ipc;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.HashMap;
import java.util.Map;

/**
 * Maps pSpace data type names (from {@code PsDataTypeEnum.getName()}) to
 * Apache Arrow {@link ArrowType} values.
 * <p>
 * This mapping is consistent with the Rust-side {@code to_ipc_data_type}
 * in {@code crates/source-pspace/src/points.rs}.
 */
public final class DataTypeMapper {

    /** Default Arrow type when pSpace data type is unknown or unset. */
    public static final ArrowType DEFAULT_ARROW_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

    /** Canonical name used as a grouping key when data type is unknown. */
    public static final String DEFAULT_TYPE_KEY = "DOUBLE";

    private static final Map<String, ArrowType> TYPE_MAP = new HashMap<>();
    private static final Map<String, String> KEY_MAP = new HashMap<>();

    static {
        register("psDataType_Bool", new ArrowType.Bool(), "BOOL");
        register("psDataType_Int8", new ArrowType.Int(8, true), "INT8");
        register("psDataType_UInt8", new ArrowType.Int(8, false), "UINT8");
        register("psDataType_Int16", new ArrowType.Int(16, true), "INT16");
        register("psDataType_UInt16", new ArrowType.Int(16, false), "UINT16");
        register("psDataType_Int32", new ArrowType.Int(32, true), "INT32");
        register("psDataType_UInt32", new ArrowType.Int(32, false), "UINT32");
        register("psDataType_Int64", new ArrowType.Int(64, true), "INT64");
        register("psDataType_UInt64", new ArrowType.Int(64, false), "UINT64");
        register("psDataType_Float", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), "FLOAT");
        register("psDataType_Double", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), "DOUBLE");
        register("psDataType_Time", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), "TIMESTAMP");
        register("psDataType_String", new ArrowType.Utf8(), "STRING");
        register("psDataType_WString", new ArrowType.Utf8(), "STRING");
    }

    private static void register(String psType, ArrowType arrowType, String key) {
        TYPE_MAP.put(psType, arrowType);
        KEY_MAP.put(psType, key);
    }

    private DataTypeMapper() {
    }

    /**
     * Convert a pSpace data type name to the corresponding Arrow type.
     *
     * @param psDataType e.g. "psDataType_Double", may be {@code null}
     * @return Arrow type, defaults to Float64 if unknown
     */
    public static ArrowType toArrowType(String psDataType) {
        if (psDataType == null)
            return DEFAULT_ARROW_TYPE;
        return TYPE_MAP.getOrDefault(psDataType, DEFAULT_ARROW_TYPE);
    }

    /**
     * Return a canonical grouping key for the given pSpace data type.
     *
     * @param psDataType e.g. "psDataType_Double", may be {@code null}
     * @return grouping key such as "DOUBLE", "INT32", etc.
     */
    public static String toGroupKey(String psDataType) {
        if (psDataType == null)
            return DEFAULT_TYPE_KEY;
        return KEY_MAP.getOrDefault(psDataType, DEFAULT_TYPE_KEY);
    }
}
