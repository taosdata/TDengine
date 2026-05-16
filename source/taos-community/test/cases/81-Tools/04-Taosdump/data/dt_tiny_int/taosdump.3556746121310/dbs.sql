#!server_ver: ver:3.4.1.0.alpha.enterprise
#!taosdump_ver: 3.4.1.6.alpha_28d9f6ca0d0caedb8ab176cac05089dbfa50ada9
#!os_id: LINUX
#!escape_char: true
#!loose_mode: false
#!charset: UTF-8
CREATE DATABASE IF NOT EXISTS db REPLICA 1 VGROUPS 2   DURATION 10d KEEP 3649d,3649d,3649d     PRECISION 'ms'   MINROWS 100 MAXROWS 4096 COMP 2 ;

CREATE STABLE IF NOT EXISTS `db`.`st` (`ts` TIMESTAMP, `c1` TINYINT) TAGS (`tntag` TINYINT) SECURITY_LEVEL 0 SECURE_DELETE 0
