#!/usr/bin/env bash

# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -euo pipefail

QST_EXTERNAL_ENV_DIR=
QST_EXTERNAL_PIDS=()
QST_EXTERNAL_PG_CTL=
QST_EXTERNAL_RUNUSER=
QST_EXTERNAL_PORT=
QST_EXTERNAL_PORT_LEASE_FDS=()

qst_external_runuser() {
  local candidate=

  if [[ -n "$QST_EXTERNAL_RUNUSER" ]]; then
    return 0
  fi
  for candidate in /usr/sbin/runuser /sbin/runuser /usr/bin/runuser; do
    if [[ -x "$candidate" ]]; then
      QST_EXTERNAL_RUNUSER=$candidate
      return 0
    fi
  done
  return 1
}

qst_external_targets() {
  local argument=
  local previous=

  for argument in "$@"; do
    if [[ "$previous" == -q ]]; then
      printf '%s\n' "$argument"
      return 0
    fi
    previous=$argument
  done
  printf '%s\n' td
}

qst_external_acquire_port() {
  local port=

  for ((port = 20000 + RANDOM % 20000; port < 60000; ++port)); do
    if ! ss -ltn "sport = :$port" 2>/dev/null | grep -q LISTEN &&
        qst_port_lease_acquire "$port"; then
      QST_EXTERNAL_PORT=$port
      QST_EXTERNAL_PORT_LEASE_FDS+=("$QST_PORT_LEASE_FD")
      return 0
    fi
  done
  return 1
}

qst_external_secret() {
  local bytes=${1:-16}

  [[ "$bytes" =~ ^[1-9][0-9]*$ ]] || return 1
  # Consume a fixed amount of entropy so pipefail cannot turn an early reader
  # shutdown into a failed environment preparation.
  LC_ALL=C od -An -N "$bytes" -tx1 /dev/urandom | tr -d ' \n'
}

qst_external_wait() {
  local attempts=$1
  shift
  local attempt=

  for ((attempt = 0; attempt < attempts; ++attempt)); do
    "$@" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

qst_external_mysql() {
  local base=
  local mysql_bin=
  local mysqld_bin=
  local port=
  local password=

  mysql_bin=$(qst_find_client QST_MYSQL_CLIENT mysql '/opt/taostest/fq/mysql/*/bin/mysql') || return 1
  base=$(CDPATH= cd -- "$(dirname -- "$mysql_bin")/.." && pwd -P)
  mysqld_bin="$base/bin/mysqld"
  [[ -x "$mysqld_bin" ]] || return 1
  qst_external_acquire_port || return 1
  port=$QST_EXTERNAL_PORT
  install -d -m 0700 "$QST_EXTERNAL_ENV_DIR/mysql/data" "$QST_EXTERNAL_ENV_DIR/mysql/run" "$QST_EXTERNAL_ENV_DIR/mysql/log"
  "$mysqld_bin" --no-defaults --initialize-insecure --basedir="$base" \
    --datadir="$QST_EXTERNAL_ENV_DIR/mysql/data" --user="$(id -un)" \
    >"$QST_EXTERNAL_ENV_DIR/mysql/initialize.log" 2>&1 || return 1
  "$mysqld_bin" --no-defaults --basedir="$base" --datadir="$QST_EXTERNAL_ENV_DIR/mysql/data" \
    --port="$port" --bind-address=127.0.0.1 --socket="$QST_EXTERNAL_ENV_DIR/mysql/run/mysql.sock" \
    --pid-file="$QST_EXTERNAL_ENV_DIR/mysql/run/mysqld.pid" \
    --log-error="$QST_EXTERNAL_ENV_DIR/mysql/log/mysqld.log" --user="$(id -un)" &
  QST_EXTERNAL_PIDS+=("$!")
  qst_external_wait 30 "$mysql_bin" --protocol=TCP --host=127.0.0.1 --port="$port" --user=root -e 'SELECT 1' || return 1
  # The current external-source metadata path accepts at most 30 password
  # characters. Fifteen random bytes encoded as hex retain 120 bits of entropy.
  password=$(qst_external_secret 15)
  "$mysql_bin" --protocol=TCP --host=127.0.0.1 --port="$port" --user=root \
    -e "CREATE USER 'qst'@'%' IDENTIFIED WITH mysql_native_password BY '$password'; GRANT ALL PRIVILEGES ON *.* TO 'qst'@'%'; FLUSH PRIVILEGES;" >/dev/null
  export FQ_MYSQL_HOST=127.0.0.1 FQ_MYSQL_PORT="$port" FQ_MYSQL_USER=qst FQ_MYSQL_PASSWORD="$password"
}

qst_external_pg() {
  local pg_bin=
  local pg_dir=
  local initdb_bin=
  local pg_ctl_bin=
  local port=
  local password=

  pg_bin=${QST_PG_CLIENT:-$(command -v psql 2>/dev/null || true)}
  [[ -x "$pg_bin" ]] || return 1
  initdb_bin=$(dirname -- "$pg_bin")/initdb
  [[ -x "$initdb_bin" ]] || initdb_bin=$(find /usr/lib/postgresql -path '*/bin/initdb' -type f -perm -u+x 2>/dev/null | sort -V | tail -1)
  [[ -x "$initdb_bin" ]] || return 1
  pg_ctl_bin="$(dirname -- "$initdb_bin")/pg_ctl"
  [[ -x "$pg_ctl_bin" ]] || return 1
  qst_external_runuser || return 1
  pg_dir="$QST_EXTERNAL_ENV_DIR/pg/data"
  qst_external_acquire_port || return 1
  port=$QST_EXTERNAL_PORT
  # PostgreSQL must traverse the private parent directory to reach its data
  # directory; keep it inaccessible to other users while allowing postgres.
  chown postgres:postgres "$QST_EXTERNAL_ENV_DIR" || return 1
  chmod 0700 "$QST_EXTERNAL_ENV_DIR" || return 1
  install -d -m 0700 -o postgres -g postgres "$QST_EXTERNAL_ENV_DIR/pg"
  "$QST_EXTERNAL_RUNUSER" -u postgres -- "$initdb_bin" -D "$pg_dir" --auth=trust --username=postgres >"$QST_EXTERNAL_ENV_DIR/pg/initdb.log" 2>&1 || return 1
  printf "listen_addresses = '127.0.0.1'\nport = %s\n" "$port" >>"$pg_dir/postgresql.conf"
  "$QST_EXTERNAL_RUNUSER" -u postgres -- "$pg_ctl_bin" -D "$pg_dir" -l "$QST_EXTERNAL_ENV_DIR/pg/postgres.log" -w start >/dev/null || return 1
  QST_EXTERNAL_PG_CTL=$pg_ctl_bin
  "$pg_bin" --host=127.0.0.1 --port="$port" --username=postgres --dbname=postgres --command 'SELECT 1' >/dev/null || return 1
  password=$(qst_external_secret)
  "$pg_bin" --host=127.0.0.1 --port="$port" --username=postgres --dbname=postgres \
    --command "CREATE ROLE qst LOGIN PASSWORD '$password' SUPERUSER;" >/dev/null
  export FQ_PG_HOST=127.0.0.1 FQ_PG_PORT="$port" FQ_PG_USER=qst FQ_PG_PASSWORD="$password"
}

qst_external_influx() {
  local influx_bin=
  local port=
  local health_attempts=${QST_EXTERNAL_HEALTH_ATTEMPTS:-30}
  local health_attempt=
  local health_exit=1
  local health_url=
  local log_path=
  local pid=

  influx_bin=$(qst_find_client QST_INFLUXDB3_CLIENT influxdb3 '/opt/taostest/fq/influxdb/*/bin/influxdb3' QST_INFLUX_ADMIN_CLIENT) || {
    printf '%s\n' 'taosQueryTest: external InfluxDB preparation failed stage=resolve binary=unresolved' >&2
    return 1
  }
  [[ "$health_attempts" =~ ^[1-9][0-9]*$ ]] || return 1
  qst_external_acquire_port || return 1
  port=$QST_EXTERNAL_PORT
  health_url="http://127.0.0.1:$port/health"
  log_path="$QST_EXTERNAL_ENV_DIR/influx/influx.log"
  install -d -m 0700 "$QST_EXTERNAL_ENV_DIR/influx/data"
  "$influx_bin" serve --node-id "qst-$RANDOM-$RANDOM" --http-bind "127.0.0.1:$port" \
    --object-store file --data-dir "$QST_EXTERNAL_ENV_DIR/influx/data" --without-auth \
    >"$log_path" 2>&1 &
  pid=$!
  QST_EXTERNAL_PIDS+=("$pid")
  for ((health_attempt = 0; health_attempt < health_attempts; ++health_attempt)); do
    if curl --fail --silent --max-time 2 "$health_url" >/dev/null 2>&1; then
      health_exit=0
      break
    else
      health_exit=$?
    fi
    sleep 1
  done
  if [[ "$health_exit" != 0 ]]; then
    printf 'taosQueryTest: external InfluxDB preparation failed stage=health binary=%s port=%s pid=%s log=%s health_url=%s health_exit=%s\n' \
      "$influx_bin" "$port" "$pid" "$log_path" "$health_url" "$health_exit" >&2
    return 1
  fi
  export FQ_INFLUX_HOST=127.0.0.1 FQ_INFLUX_PORT="$port" FQ_INFLUX_TOKEN="qst-$(qst_external_secret)"
}

qst_external_prepare() {
  local targets=$1

  if [[ "$targets" == *mysql* ]]; then
    unset FQ_MYSQL_HOST FQ_MYSQL_PORT FQ_MYSQL_USER FQ_MYSQL_PASSWORD
    qst_external_mysql || return 1
  fi
  if [[ "$targets" == *pg* ]]; then
    unset FQ_PG_HOST FQ_PG_PORT FQ_PG_USER FQ_PG_PASSWORD
    qst_external_pg || return 1
  fi
  if [[ "$targets" == *influx* ]]; then
    unset FQ_INFLUX_HOST FQ_INFLUX_PORT FQ_INFLUX_TOKEN
    qst_external_influx || return 1
  fi
}

qst_external_cleanup() {
  local pid=
  local lease_fd=

  if [[ -n "${QST_EXTERNAL_PG_CTL:-}" && -n "${QST_EXTERNAL_ENV_DIR:-}" ]]; then
    "$QST_EXTERNAL_RUNUSER" -u postgres -- "$QST_EXTERNAL_PG_CTL" \
      -D "$QST_EXTERNAL_ENV_DIR/pg/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  for pid in "${QST_EXTERNAL_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${QST_EXTERNAL_PIDS[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
  for lease_fd in "${QST_EXTERNAL_PORT_LEASE_FDS[@]:-}"; do
    qst_port_lease_release "$lease_fd" || true
  done
  QST_EXTERNAL_PORT_LEASE_FDS=()
  QST_EXTERNAL_PORT=
  [[ -z "${QST_EXTERNAL_ENV_DIR:-}" ]] || rm -rf -- "$QST_EXTERNAL_ENV_DIR"
}
