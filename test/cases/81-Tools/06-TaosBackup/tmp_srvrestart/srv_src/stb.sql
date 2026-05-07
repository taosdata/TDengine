CREATE STABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` VARCHAR(24)) SECURITY_LEVEL 0 SECURE_DELETE 0
