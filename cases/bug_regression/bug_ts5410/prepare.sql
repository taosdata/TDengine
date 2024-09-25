drop database if exists DEFAULT_MEIOT_1;
create database if not exists DEFAULT_MEIOT_1;
use DEFAULT_MEIOT_1;


CREATE STABLE IF NOT EXISTS t_lingang_stable_sensor_shadow_pressure_1 (                         \
    ts TIMESTAMP,                       \
    mv INT                       \
) TAGS (                       \
    sensor_type BINARY(20),                       \
    next_stage BINARY(50),                       \
    notes BINARY(255),                       \
    atmos_pressure_bar FLOAT,                       \
    param_a DOUBLE,                       \
    param_b DOUBLE                       \
);                         \
            
create function udf_operator_int_linear_convert_v1 as '/root/taos-test-framework/TestNG/cases/bug_regression/bug_ts5410/operator_int_linear_convert_v1.0.py' outputtype double language 'Python';
create function udf_operator_int_unsigned_clamping_v1 as '/root/taos-test-framework/TestNG/cases/bug_regression/bug_ts5410/operator_int_unsigned_clamping_v1.0.py' outputtype double language 'Python';
create function udf_enc_atmos_abs_pres as '/root/taos-test-framework/TestNG/cases/bug_regression/bug_ts5410/udf_get_abspressure_atmos_encoded.py' outputtype double language 'Python';

CREATE STREAM IF NOT EXISTS t_lingang_pressure_sensor_shadow_to_vsensor_1\
TRIGGER AT_ONCE \
IGNORE UPDATE 0 \
IGNORE EXPIRED 0 \
WATERMARK 10d\
INTO DEFAULT_MEIOT_1.t_lingang_stable_vsensor_cache_pressure_1 (ts, pressure, atmos_pressure) \
TAGS (dptbname VARCHAR(100)) \
SUBTABLE(CONCAT("t_lingang_vsp_", dptbname)) AS \
SELECT \
    _wstart AS ts, \
    FIRST(udf_operator_int_unsigned_clamping_v1(udf_operator_int_linear_convert_v1(\
        udf_operator_int_linear_convert_v1(mv, param_a, param_b),\
        1,\
        udf_operator_int_unsigned_clamping_v1(udf_enc_atmos_abs_pres(atmos_pressure_bar))\
    ))) AS pressure, \
    FIRST(udf_operator_int_unsigned_clamping_v1( (atmos_pressure_bar))) AS atmos_pressure \
FROM DEFAULT_MEIOT_1.t_lingang_stable_sensor_shadow_pressure_1\
PARTITION BY next_stage dptbname INTERVAL(1a);
