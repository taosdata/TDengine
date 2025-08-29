#!server_ver: ver:3.3.6.13.alpha
#!taosdump_ver: 3.3.6.13.alpha_b78c03b4589ae84ac6a0a645207a6456bdae28a5
#!os_id: LINUX
#!escape_char: true
#!loose_mode: false
#!charset: UTF-8
CREATE DATABASE IF NOT EXISTS asset01 REPLICA 1   DURATION 10d KEEP 3650d,3650d,3650d     PRECISION 'ms'   MINROWS 100 MAXROWS 4096 COMP 2 ;

CREATE TABLE IF NOT EXISTS `asset01`.`electricity_meters` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `current` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `voltage` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `power` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `phase` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium') TAGS (`location` VARCHAR(50), `unit` TINYINT, `floor` TINYINT, `device_id` NCHAR(20))
CREATE TABLE IF NOT EXISTS `asset01`.`water_meters_01` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `rate` FLOAT ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `pressure` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`location` VARCHAR(50))
