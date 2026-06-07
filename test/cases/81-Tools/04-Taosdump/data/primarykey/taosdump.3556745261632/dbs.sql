#!server_ver: ver:3.4.1.0.alpha.enterprise
#!taosdump_ver: 3.4.1.6.alpha_28d9f6ca0d0caedb8ab176cac05089dbfa50ada9
#!os_id: LINUX
#!escape_char: true
#!loose_mode: false
#!charset: UTF-8
CREATE DATABASE IF NOT EXISTS pridb REPLICA 1 VGROUPS 2   DURATION 10d KEEP 3650d,3650d,3650d     PRECISION 'ms'   MINROWS 100 MAXROWS 4096 COMP 2 ;

CREATE STABLE IF NOT EXISTS `pridb`.`meters` (`ts` TIMESTAMP, `pk` INT COMPOSITE KEY, `bc` BOOL, `fc` FLOAT, `dc` DOUBLE, `ti` TINYINT, `si` SMALLINT, `ic` INT, `bi` BIGINT, `uti` TINYINT UNSIGNED, `usi` SMALLINT UNSIGNED, `ui` INT UNSIGNED, `ubi` BIGINT UNSIGNED, `bin` VARCHAR(32), `nch` NCHAR(64)) TAGS (`groupid` TINYINT, `location` VARCHAR(16)) SECURITY_LEVEL 0 SECURE_DELETE 0
