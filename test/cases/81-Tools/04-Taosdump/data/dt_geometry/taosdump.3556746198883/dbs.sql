#!server_ver: ver:3.4.1.0.alpha.enterprise
#!taosdump_ver: 3.4.1.6.alpha_28d9f6ca0d0caedb8ab176cac05089dbfa50ada9
#!os_id: LINUX
#!escape_char: true
#!loose_mode: false
#!charset: UTF-8
CREATE DATABASE IF NOT EXISTS geodb REPLICA 1 VGROUPS 2   DURATION 10d KEEP 3650d,3650d,3650d     PRECISION 'ms'   MINROWS 100 MAXROWS 4096 COMP 2 ;

CREATE STABLE IF NOT EXISTS `geodb`.`meters` (`ts` TIMESTAMP, `ic` INT, `usi` SMALLINT UNSIGNED, `geo1` GEOMETRY(32), `geo2` GEOMETRY(100), `geo3` GEOMETRY(1024)) TAGS (`tgeo1` GEOMETRY(30), `tgeo2` GEOMETRY(256)) SECURITY_LEVEL 0 SECURE_DELETE 0
