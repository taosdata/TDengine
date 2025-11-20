DROP STREAM IF EXISTS db2.stm_a8
DROP STREAM IF EXISTS db2.stm_a9 
DROP STREAM IF EXISTS db2.stm_f2 
DROP STREAM IF EXISTS db2.stm_f5 
DROP STREAM IF EXISTS db2.stm_f6 
DROP STREAM IF EXISTS db2.stm_f7 
DROP STREAM IF EXISTS db2.stm_t5 
DROP STREAM IF EXISTS db2.stm_t6 
DROP STREAM IF EXISTS db2.stm_t9
WAIT TRANSACTIONS OVER
DROP DATABASE IF EXISTS db2;
CREATE DATABASE db2 vgroups 8;
CREATE TABLE db2.`diagnostics` (`ts` TIMESTAMP, `fuel_state` DOUBLE, `current_load` DOUBLE, `status` BIGINT) TAGS (`name` VARCHAR(30), `fleet` VARCHAR(30), `driver` VARCHAR(30), `model` VARCHAR(30), `device_version` VARCHAR(30), `load_capacity` DOUBLE, `fuel_capacity` DOUBLE, `nominal_fuel_consumption` DOUBLE)