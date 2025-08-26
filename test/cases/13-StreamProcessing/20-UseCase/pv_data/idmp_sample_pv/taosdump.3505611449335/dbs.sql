#!server_ver: ver:3.3.6.14.0717
#!taosdump_ver: 3.3.6.14.0711_fceced930a0056dbf7d1d782203fef2d12798bea
#!os_id: LINUX
#!escape_char: true
#!loose_mode: false
#!charset: UTF-8
CREATE DATABASE IF NOT EXISTS idmp_sample_pv REPLICA 1   DURATION 10d KEEP 3650d,3650d,3650d     PRECISION 'ms'   MINROWS 100 MAXROWS 4096 COMP 2 ;

CREATE TABLE IF NOT EXISTS `idmp_sample_pv`.`weather_sensors` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `ambient_temperature` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `module_temperature` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `irradiation` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium') TAGS (`pv_asset` NCHAR(128), `plant_id` VARCHAR(10), `weather_sensor_id` VARCHAR(20))
CREATE TABLE IF NOT EXISTS `idmp_sample_pv`.`inverters` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `dc_power` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `ac_power` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `daily_yield` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `total_yield` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium') TAGS (`pv_asset` NCHAR(128), `plant_id` VARCHAR(10), `inverter_id` VARCHAR(20))
