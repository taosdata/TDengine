#!/bin/bash

# you shall never run this script outside ctest!!!

TAOS=`which taos`
${TAOS} --version                                                              &&
${TAOS} -s 'drop database if exists foo'                                       &&
${TAOS} -s 'create database if not exists foo'                                 &&
${TAOS} -s 'create table foo.t (ts timestamp, v1 int, v2 int)'                 &&
${TAOS} -s 'insert into foo.t (ts, v1, v2) values (now(), 1, 100)'             &&
${TAOS} -s 'insert into foo.t (ts, v1, v2) values (now(), 2, 102)'             &&     
${TAOS} -s 'select * from foo.t'                                               &&
echo ==Above all statements might not all succeeded because ${TAOS} never returns failure code when executing failed==

# export SQL='select * from t where v1 > ?'
# export SAMPLE_PREPARE_BIND_PARAM_EXECUTE_SQL=${SQL}
# export SAMPLE_PREPARE_BIND_PARAM_EXECUTE_PARAMSET_SIZE=2
# ../../samples/c/dump --connstr ${CONN} _prepare_bind_param_execute


export CONN='DSN=TAOS_ODBC_DSN;DB=foo'                                         &&
export SQL='select * from t where v1 > ?'                                      &&
export SAMPLE_PREPARE_BIND_PARAM_EXECUTE_SQL=${SQL}                            &&
export SAMPLE_PREPARE_BIND_PARAM_EXECUTE_PARAMSET_SIZE=1                       &&
../../samples/c/dump --connstr ${CONN} _prepare_bind_param_execute             &&
export SQL='select * from t where v1 > ? and v2 > ?'                           &&
export SAMPLE_PREPARE_BIND_PARAM_EXECUTE_SQL=${SQL}                            &&
export SAMPLE_PREPARE_BIND_PARAM_EXECUTE_PARAMSET_SIZE=1                       &&
../../samples/c/dump --connstr ${CONN} _prepare_bind_param_execute             &&
export SQL='select * from t'                                                   &&
export SAMPLE_PREPARE_BIND_PARAM_EXECUTE_SQL=${SQL}                            &&
export SAMPLE_PREPARE_BIND_PARAM_EXECUTE_PARAMSET_SIZE=1                       &&
../../samples/c/dump --connstr ${CONN} _prepare_bind_param_execute             &&
echo ==Done==

