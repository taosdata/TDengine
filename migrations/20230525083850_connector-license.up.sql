-- Add up migration script here

CREATE TABLE IF NOT EXISTS `connector_transferred` (
	`cluster_id` BIGINT NOT NULL,
	`connector` VARCHAR NOT NULL,
	`tables` INT NOT NULL,
	`records` BIGINT NOT NULL,
	`points` BIGINT NOT NULL
);

CREATE UNIQUE INDEX cluster_connector_idx
on connector_transferred (cluster_id, connector);
