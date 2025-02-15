#!server_ver: ver:3.1.0.0
#!taosdump_ver: 2.5.2_cf16c4d
#!os_id: LINUX
#!escape_char: true
#!loose_mode: false
#!charset: UTF-8
CREATE DATABASE IF NOT EXISTS test REPLICA 1   DURATION 14400m KEEP 5256000m,5256000m,5256000m     PRECISION 'ms'   MINROWS 100 MAXROWS 4096 COMP 2 ;

CREATE TABLE IF NOT EXISTS test.`meters`(`ts` timestamp,`current` float,`voltage` int,`phase` float) TAGS(`groupid` int,`location` binary(24));

