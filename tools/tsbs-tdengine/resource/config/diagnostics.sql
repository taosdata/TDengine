CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.`diagnostics`;
CREATE STABLE test.`diagnostics` (`ts` TIMESTAMP, `fuel_state` DOUBLE, `current_load` DOUBLE, `status` BIGINT) TAGS (`name` VARCHAR(30), `fleet` VARCHAR(30), `driver` VARCHAR(30), `model` VARCHAR(30), `device_version` VARCHAR(30), `load_capacity` DOUBLE, `fuel_capacity` DOUBLE, `nominal_fuel_consumption` DOUBLE)