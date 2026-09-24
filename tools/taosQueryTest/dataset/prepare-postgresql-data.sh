#!/bin/sh
set -eu

: "${FQ_PG_HOST:?FQ_PG_HOST is required}"
: "${FQ_PG_PORT:?FQ_PG_PORT is required}"
: "${FQ_PG_USER:?FQ_PG_USER is required}"
: "${FQ_PG_PASSWORD:?FQ_PG_PASSWORD is required}"

psql_bin="$(command -v psql 2>/dev/null || true)"
: "${psql_bin:?psql client is required in PATH}"

export PGPASSWORD="$FQ_PG_PASSWORD"
"$psql_bin" --host="$FQ_PG_HOST" --port="$FQ_PG_PORT" --username="$FQ_PG_USER" --dbname=postgres --set=ON_ERROR_STOP=1 <<'SQL'
DROP DATABASE IF EXISTS fq_default;
CREATE DATABASE fq_default;
SQL
"$psql_bin" --host="$FQ_PG_HOST" --port="$FQ_PG_PORT" --username="$FQ_PG_USER" --dbname=fq_default --set=ON_ERROR_STOP=1 <<'SQL'
SET TIME ZONE 'UTC';
CREATE SCHEMA fq_default;
CREATE SCHEMA fq_alt;
CREATE TABLE fq_default.fq_series (ts TIMESTAMPTZ NOT NULL, device_id INTEGER NOT NULL, reading DOUBLE PRECISION, active BOOLEAN, region TEXT, note TEXT, jdoc JSONB, event_local TIMESTAMP(6), payload BYTEA, PRIMARY KEY (ts, device_id));
CREATE TABLE fq_default.fq_device (ts TIMESTAMPTZ NOT NULL, device_id INTEGER NOT NULL, device_name TEXT NOT NULL, tier TEXT NOT NULL, PRIMARY KEY (ts, device_id));
CREATE TABLE fq_default.fq_series_empty (LIKE fq_default.fq_series INCLUDING ALL);
INSERT INTO fq_default.fq_series VALUES ('2026-01-01 00:00:00.123000+00',1001,10.5,TRUE,'north','alpha','{"k":"v","n":1}','2026-01-01 08:00:00.123000',E'\\x0102'),('2026-01-01 00:00:01+00',1001,11.0,FALSE,'north',NULL,'{"k":"w","n":2}','2026-01-01 08:00:01',E'\\x0304'),('2026-01-01 00:00:02+00',1002,20.25,TRUE,'south','中文','{"k":"中","n":3}','2026-01-01 08:00:02',E'\\x0506'),('2026-01-01 00:00:03+00',1003,-1.5,TRUE,'east',E'%_\\\'','{"k":"x","n":4}','2026-01-01 08:00:03',E'\\x0708');
INSERT INTO fq_default.fq_device VALUES ('2026-01-01 00:00:00.123000+00',1001,'meter-a','gold'),('2026-01-01 00:00:01+00',1001,'meter-a','gold'),('2026-01-01 00:00:02+00',1002,'meter-b','silver'),('2026-01-01 00:00:03+00',1003,'meter-c','bronze');
CREATE TABLE fq_alt.fq_series_alt (LIKE fq_default.fq_series INCLUDING ALL);
INSERT INTO fq_alt.fq_series_alt SELECT * FROM fq_default.fq_series ORDER BY ts, device_id LIMIT 1;
SQL
