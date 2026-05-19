#!server_ver: ver:3.4.1.0.alpha.enterprise
#!taosdump_ver: 3.4.1.6.alpha_28d9f6ca0d0caedb8ab176cac05089dbfa50ada9
#!os_id: LINUX
#!escape_char: true
#!loose_mode: false
#!charset: UTF-8
CREATE DATABASE IF NOT EXISTS test REPLICA 1 VGROUPS 3   DURATION 10d KEEP 3650d,3650d,3650d     PRECISION 'ms'   MINROWS 100 MAXROWS 4096 COMP 2 ;

CREATE STABLE IF NOT EXISTS `test`.`stb` (`ts` TIMESTAMP, `c1` VARCHAR(16374), `c2` VARCHAR(16374), `c3` VARCHAR(16374)) TAGS (`t1` NCHAR(256)) SECURITY_LEVEL 0 SECURE_DELETE 0
