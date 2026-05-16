CREATE DATABASE db vgroups 4;
CREATE STABLE db.`stbd2` (`ts` TIMESTAMP, `bc` BOOL, `fc` FLOAT, `dc` DOUBLE, `ti` TINYINT, `si` SMALLINT, `ic` INT, `bi` BIGINT, `uti` TINYINT UNSIGNED, `usi` SMALLINT UNSIGNED, `ui` INT UNSIGNED, `ubi` BIGINT UNSIGNED, `bin` VARCHAR(16), `nch` NCHAR(16)) TAGS (`groupid` TINYINT, `location` VARCHAR(16));
