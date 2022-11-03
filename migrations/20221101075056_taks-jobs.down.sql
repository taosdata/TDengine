-- Add down migration script here
ALTER TABLE tasks DROP `jobs`;
ALTER TABLE tasks DROP `compression_level`;
ALTER TABLE tasks DROP `drop`;
