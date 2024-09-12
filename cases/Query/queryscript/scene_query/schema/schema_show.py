###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import random
import os
import time
import sys
from itertools import combinations
from faker import Faker
import subprocess
from taostest import TDCase
from Query.queryutil.createdata import *
import threading
import multiprocessing

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        
        #basic_param
        self.db = "stable_show"
        
        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        
        self.firstEP = []
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(
                    self.taosd_setting['spec']['config']['firstEP'])
        self.target_taosd = self.firstEP[-1].split(':')
        print(self.target_taosd[0])
        self.service_host = self.target_taosd[0]

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# show variables
        '''
        return case_description
    
    
    def show_local_variables(self):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        show_local_sql = "show local variables;"
        self.tdSql.query(show_local_sql)  
        rows = self.tdSql.query_row
        
        list_both = ['firstEp','secondEp','tempDir','minimalTmpDirGB','shellActivityTimer','compressMsgSize','compressColData',
                     'maxRetryWaitTime','numOfRpcThreads','numOfRpcSessions','timeToGetAvailableConn','configDir','scriptDir','logDir',
                     'minimalLogDirGB','numOfLogLines','asyncLog','logKeepDays','debugFlag','simDebugFlag','tmrDebugFlag','uDebugFlag',
                     'rpcDebugFlag','timezone','qDebugFlag','locale','charset','assert','enableCoreFile','numOfCores','SSE42','AVX','AVX2',
                     'FMA','SIMD-builtins','tagFilterCache','openMax','streamMax','pageSizeKB','totalMemoryKB','os sysname',
                     'os nodename','os release','os version','os machine','version','compatible_version','gitinfo','buildinfo','keepAliveIdle','ssd42','avx','avx2',
                     'fma','avx512','simdEnable','experimental','crashReporting','monitor','monitorInterval','countAlwaysReturnValue','AVX512Enable','randErrorChance',
                     'randErrorDivisor','randErrorScope','','','','','','','','',];
        
        list_client = ['queryPolicy','enableQueryHb','enableScience','querySmaOptimize','queryPlannerTrace','queryNodeChunkSize','queryUseNodeAllocator',
                       'keepColumnName','smlChildTableName','smlTagName','maxInsertBatchRows','useAdapter','queryMaxConcurrentTables','metaCacheMaxSize',
                       'slowLogThreshold','slowLogScope','numOfTaskQueueThreads','cDebugFlag','jniDebugFlag','smlTsDefaultName','smlDot2Underline','smlAutoChildTableNameDelimiter',
                       'maxShellConns','smlAutoChildTableNameDelimiter','multiResultFunctionStarReturnTag','maxTsmaCalcDelay','tsmaDataDeleteMark','queryTableNotExistAsEmpty','','','',''];
        
        list_server = ['fqdn','serverPort','crashReporting','','','','','','','','','','','','','','','','','','','','','','','']
        
        for i in range(rows):
            if (self.tdSql.getData(i,0) in list_both) and (self.tdSql.getData(i,2) == 'both'):
                self.logger.debug('==show_local===num %d=====' %i)
                self.logger.debug(f"name = {self.tdSql.getData(i,0)},value = {self.tdSql.getData(i,1)},scope={self.tdSql.getData(i,2)}")
            elif (self.tdSql.getData(i,0) in list_client) and (self.tdSql.getData(i,2) == 'client'):
                self.logger.debug('==show_local===num %d=====' %i)
                self.logger.debug(f"name = {self.tdSql.getData(i,0)},value = {self.tdSql.getData(i,1)},scope={self.tdSql.getData(i,2)}")
            elif (self.tdSql.getData(i,0) in list_server) and (self.tdSql.getData(i,2) == 'server'):
                self.logger.debug('==show_local===num %d=====' %i)
                self.logger.debug(f"name = {self.tdSql.getData(i,0)},value = {self.tdSql.getData(i,1)},scope={self.tdSql.getData(i,2)}")
            
            else:
                raise AssertionError(f"checkEqual error, name = {self.tdSql.getData(i,0)},name={self.tdSql.getData(i,0)},scope={self.tdSql.getData(i,2)}")
                
    def show_dnode_variables(self):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        
        list_both = ['firstEp','secondEp','tempDir','minimalTmpDirGB','shellActivityTimer','compressMsgSize','compressColData',
                        'maxRetryWaitTime','numOfRpcThreads','numOfRpcSessions','timeToGetAvailableConn','configDir','scriptDir','logDir',
                        'minimalLogDirGB','numOfLogLines','asyncLog','logKeepDays','debugFlag','simDebugFlag','tmrDebugFlag','uDebugFlag',
                        'rpcDebugFlag','timezone','qDebugFlag','locale','charset','assert','enableCoreFile','numOfCores','SSE42','AVX','AVX2',
                        'FMA','SIMD-builtins','tagFilterCache','openMax','streamMax','pageSizeKB','totalMemoryKB','os sysname',
                        'os nodename','os release','os version','os machine','version','compatible_version','gitinfo','buildinfo',
                        'countAlwaysReturnValue','numOfRpcThreads','numOfRpcSessions','timeToGetAvailableConn','rpcQueueMemoryAllowed',
                        'crashReporting','telemetryReporting','telemetryInterval','telemetryServer','telemetryPort','configDir','scriptDir',
                        'logDir','minimalLogDirGB','numOfLogLines','asyncLog','logKeepDays','debugFlag','sDebugFlag','keepAliveIdle','rsyncPort',
                        'ssd42','avx','avx2','fma','avx512','simdEnable','experimental','monitor','monitorInterval','AVX512Enable','randErrorChance',
                        'randErrorDivisor','randErrorScope','','','','','',''];
    
        list_client = ['queryPolicy','enableQueryHb','enableScience','querySmaOptimize','queryPlannerTrace','queryNodeChunkSize','queryUseNodeAllocator',
                        'keepColumnName','smlChildTableName','smlTagName','maxInsertBatchRows','useAdapter','queryMaxConcurrentTables','metaCacheMaxSize',
                        'slowLogThreshold','slowLogScope','numOfTaskQueueThreads','cDebugFlag','jniDebugFlag','minSlidingTime','minIntervalTime',
                        'smlTsDefaultName','smlDot2Underline','smlAutoChildTableNameDelimiter','maxShellConns','multiResultFunctionStarReturnTag','maxTsmaCalcDelay',
                        'tsmaDataDeleteMark','queryTableNotExistAsEmpty','','','','','','','','','',''];
    
        list_server = ['fqdn','serverPort','crashReporting','dataDir','minimalDataDirGB','supportVnodes','maxShellConns','statusInterval',
                        'maxNumOfDistinctRes','queryBufferSize','printAuth','queryRspPolicy','numOfCommitThreads','numOfMnodeReadThreads',
                        'numOfVnodeQueryThreads','ratioOfVnodeStreamThreads','numOfVnodeFetchThreads','numOfVnodeRsmaThreads',
                        'numOfQnodeQueryThreads','numOfSnodeSharedThreads','numOfSnodeUniqueThreads','syncElectInterval','syncHeartbeatInterval',
                        'syncHeartbeatTimeout','vndCommitMaxInterval','mndSdbWriteDelta','mndLogRetention','skipGrant','monitor','monitorInterval',
                        'monitorFqdn','monitorPort','monitorMaxLogs','monitorComp','tmqMaxTopicNum','transPullupInterval','mqRebalanceInterval',
                        'ttlUnit','ttlPushInterval','ttlChangeOnWrite','uptimeInterval','queryRsmaTolerance','walFsyncDataSizeLimit','udf',
                        'udfdResFuncs','udfdLdLibPath','disableStream','streamBufferSize','checkpointInterval','cacheLazyLoadThreshold','filterScalarMode',
                        'keepTimeOffset','maxStreamBackendCache','pqSortMemThreshold','dDebugFlag','vDebugFlag','mDebugFlag','wDebugFlag',
                        'tsdbDebugFlag','tqDebugFlag','fsDebugFlag','udfDebugFlag','smaDebugFlag','idxDebugFlag','tdbDebugFlag','metaDebugFlag','grantMode',
                        'audit','auditFqdn','auditPort','ttlBatchDropNum','ttlFlushThreshold','trimVDbIntervalSec','resolveFQDNRetryTime','s3Accesskey',
                        's3Endpoint','s3BucketName','minDiskFreeSize','enableWhiteList','timeseriesThreshold','LossyColumns','FPrecision','DPrecision',
                        'MaxRange','CurRange','IfAdtFse','Compressor','s3BlockSize','s3BlockCacheSize','auditCreateTable','snodeAddress','checkpointBackupDir',
                        'streamSinkDataRate','lossyColumns','fPrecision','dPrecision','maxRange','curRange','ifAdtFse','compressor','s3PageCacheSize',
                        's3UploadDelaySec','sDebugFlag','stDebugFlag','sndDebugFlag','auditInterval','compactPullupInterval','encryptAlgorithm','encryptScope',
                        'syncSnapReplMaxWaitN','arbHeartBeatIntervalSec','arbCheckSyncIntervalSec','arbSetAssignedTimeoutSec','monitorLogProtocol',
                        'monitorIntervalForBasic','monitorForceV2','tmqRowSize','maxTsmaNum','s3MigrateIntervalSec','s3MigrateEnabled','streamAggCnt',
                        'concurrentCheckpoint','retentionSpeedLimitMB','slowLogThresholdTest','slowLogThreshold','slowLogMaxLen','slowLogScope','slowLogExceptDb','','','']
        
        
        dnodes_list = []
        show_local_sql = "show dnodes;"
        self.tdSql.query(show_local_sql)  
        dnodes_rows = self.tdSql.query_row
        for i in range(dnodes_rows):
            dnodes_list.append(self.tdSql.getData(i,0))
        
        for i in range(len(dnodes_list)):
            show_dnode_sql = "show dnode %d variables;" %(dnodes_list[i])
            self.tdSql.query(show_dnode_sql)  
            rows = self.tdSql.query_row
            #self.tdCreateData.data_check(rows,148)
            
            for i in range(rows):
                if (self.tdSql.getData(i,1) in list_both) and (self.tdSql.getData(i,3) == 'both'):
                    self.logger.debug('==show_dnode===num %d=====' %i)
                    self.logger.debug(f"dnode_id = {self.tdSql.getData(i,0)},name = {self.tdSql.getData(i,1)},value = {self.tdSql.getData(i,2)},scope={self.tdSql.getData(i,3)}")
                elif (self.tdSql.getData(i,1) in list_client) and (self.tdSql.getData(i,3) == 'client'):
                    self.logger.debug('==show_dnode===num %d=====' %i)
                    self.logger.debug(f"dnode_id = {self.tdSql.getData(i,0)},name = {self.tdSql.getData(i,1)},value = {self.tdSql.getData(i,2)},scope={self.tdSql.getData(i,3)}")
                elif (self.tdSql.getData(i,1) in list_server) and (self.tdSql.getData(i,3) == 'server'):
                    self.logger.debug('==show_dnode===num %d=====' %i)
                    self.logger.debug(f"dnode_id = {self.tdSql.getData(i,0)},name = {self.tdSql.getData(i,1)},value = {self.tdSql.getData(i,2)},scope={self.tdSql.getData(i,3)}")
                
                else:
                    raise AssertionError(f"checkEqual error, dnode_id = {self.tdSql.getData(i,0)},name={self.tdSql.getData(i,1)},value = {self.tdSql.getData(i,2)},scope={self.tdSql.getData(i,3)}")    

    def show_information_schema(self):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
        
        show_create_sql = "show create database information_schema;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'information_schema')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),'CREATE DATABASE `information_schema`')
        
        show_create_sql = "show create table information_schema.ins_dnodes;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_dnodes')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_dnodes` (`id` INT, `endpoint` VARCHAR(134), `vnodes` SMALLINT, `support_vnodes` SMALLINT, `status` VARCHAR(10), `create_time` TIMESTAMP, `reboot_time` TIMESTAMP, `note` VARCHAR(256), `machine_id` VARCHAR(24)) COMMENT ''")
        show_sql = "show dnodes;"
        select_sql = "select * from information_schema.ins_dnodes;"
        self.sql_check(show_sql,select_sql)
        
        show_create_sql = "show create table information_schema.ins_mnodes;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_mnodes')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_mnodes` (`id` INT, `endpoint` VARCHAR(134), `role` VARCHAR(12), `status` VARCHAR(9), `create_time` TIMESTAMP, `role_time` TIMESTAMP) COMMENT ''")
        
        #delete 3.1.1.0 version
        # show_create_sql = "show create table information_schema.ins_modules;"
        # self.tdSql.query(show_create_sql)          
        # self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_modules')
        # self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_modules` (`id` INT, `endpoint` VARCHAR(134), `module` VARCHAR(10)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_qnodes;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_qnodes')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_qnodes` (`id` INT, `endpoint` VARCHAR(134), `create_time` TIMESTAMP) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_snodes;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_snodes')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_snodes` (`id` INT, `endpoint` VARCHAR(134), `create_time` TIMESTAMP) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_cluster;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_cluster')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_cluster` (`id` BIGINT, `name` VARCHAR(40), `uptime` INT, `create_time` TIMESTAMP, `version` VARCHAR(10), `expire_time` TIMESTAMP) COMMENT ''")
        show_sql = "show cluster;"
        select_sql = "select * from information_schema.ins_cluster;"
        self.sql_check(show_sql,select_sql)
        
        show_create_sql = "show create table information_schema.ins_databases;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_databases')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_databases` (`name` VARCHAR(64), `create_time` TIMESTAMP, `vgroups` INT, `ntables` BIGINT, `replica` TINYINT, `strict` VARCHAR(4), `duration` VARCHAR(10), `keep` VARCHAR(32), `buffer` INT, `pagesize` INT, `pages` INT, `minrows` INT, `maxrows` INT, `comp` TINYINT, `precision` VARCHAR(2), `status` VARCHAR(10), `retentions` VARCHAR(60), `single_stable` BOOL, `cachemodel` VARCHAR(11), `cachesize` INT, `wal_level` TINYINT, `wal_fsync_period` INT, `wal_retention_period` INT, `wal_retention_size` BIGINT, `stt_trigger` SMALLINT, `table_prefix` SMALLINT, `table_suffix` SMALLINT, `tsdb_pagesize` INT, `keep_time_offset` INT, `s3_chunksize` INT, `s3_keeplocal` VARCHAR(10), `s3_compact` TINYINT, `with_arbitrator` TINYINT, `encrypt_algorithm` VARCHAR(16)) COMMENT ''")
        show_sql = "show databases;"
        select_sql = "select name from information_schema.ins_databases;"
        self.sql_check(show_sql,select_sql)
        
        show_create_sql = "show create table information_schema.ins_functions;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_functions')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_functions` (`name` VARCHAR(64), `comment` VARCHAR(4095), `aggregate` INT, `output_type` VARCHAR(31), `create_time` TIMESTAMP, `code_len` INT, `bufsize` INT, `func_language` VARCHAR(31), `func_body` VARCHAR(65517), `func_version` INT) COMMENT ''")
        show_sql = "show functions;"
        select_sql = "select * from information_schema.ins_functions;"
        self.sql_check(show_sql,select_sql)
        
        show_create_sql = "show create table information_schema.ins_indexes;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_indexes')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_indexes` (`index_name` VARCHAR(192), `db_name` VARCHAR(64), `table_name` VARCHAR(192), `vgroup_id` INT, `create_time` TIMESTAMP, `column_name` VARCHAR(192), `index_type` VARCHAR(192)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_stables;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_stables')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_stables` (`stable_name` VARCHAR(192), `db_name` VARCHAR(64), `create_time` TIMESTAMP, `columns` INT, `tags` INT, `last_update` TIMESTAMP, `table_comment` VARCHAR(1024), `watermark` VARCHAR(64), `max_delay` VARCHAR(64), `rollup` VARCHAR(128)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_tables;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_tables')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_tables` (`table_name` VARCHAR(192), `db_name` VARCHAR(64), `create_time` TIMESTAMP, `columns` INT, `stable_name` VARCHAR(192), `uid` BIGINT, `vgroup_id` INT, `ttl` INT, `table_comment` VARCHAR(1024), `type` VARCHAR(21)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_tags;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_tags')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_tags` (`table_name` VARCHAR(192), `db_name` VARCHAR(64), `stable_name` VARCHAR(192), `tag_name` VARCHAR(64), `tag_type` VARCHAR(32), `tag_value` VARCHAR(16384)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_columns;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_columns')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_columns` (`table_name` VARCHAR(192), `db_name` VARCHAR(64), `table_type` VARCHAR(21), `col_name` VARCHAR(64), `col_type` VARCHAR(32), `col_length` INT, `col_precision` INT, `col_scale` INT, `col_nullable` INT) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_users;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_users')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_users` (`name` VARCHAR(24), `super` TINYINT, `enable` TINYINT, `sysinfo` TINYINT, `createdb` TINYINT, `create_time` TIMESTAMP, `allowed_host` VARCHAR(49152)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_grants;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_grants')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_grants` (`version` VARCHAR(32), `expire_time` VARCHAR(19), `service_time` VARCHAR(19), `expired` VARCHAR(5), `state` VARCHAR(9), `timeseries` VARCHAR(21), `dnodes` VARCHAR(10), `cpu_cores` VARCHAR(13)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_vgroups;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_vgroups')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_vgroups` (`vgroup_id` INT, `db_name` VARCHAR(64), `tables` INT, `v1_dnode` SMALLINT, `v1_status` VARCHAR(9), `v2_dnode` SMALLINT, `v2_status` VARCHAR(9), `v3_dnode` SMALLINT, `v3_status` VARCHAR(9), `v4_dnode` SMALLINT, `v4_status` VARCHAR(9), `cacheload` INT, `cacheelements` INT, `tsma` TINYINT) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_configs;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_configs')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_configs` (`name` VARCHAR(32), `value` VARCHAR(64)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_dnode_variables;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_dnode_variables')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_dnode_variables` (`dnode_id` INT, `name` VARCHAR(32), `value` VARCHAR(64), `scope` VARCHAR(8)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_topics;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_topics')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_topics` (`topic_name` VARCHAR(192), `db_name` VARCHAR(64), `create_time` TIMESTAMP, `sql` VARCHAR(2048), `schema` VARCHAR(65517), `meta` VARCHAR(4), `type` VARCHAR(8)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_subscriptions;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_subscriptions')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_subscriptions` (`topic_name` VARCHAR(205), `consumer_group` VARCHAR(193), `vgroup_id` INT, `consumer_id` VARCHAR(256), `user` VARCHAR(24), `fqdn` VARCHAR(128), `offset` VARCHAR(64), `rows` BIGINT) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_streams;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_streams')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_streams` (`stream_name` VARCHAR(192), `create_time` TIMESTAMP, `stream_id` VARCHAR(16), `history_id` VARCHAR(16), `sql` VARCHAR(2048), `status` VARCHAR(20), `source_db` VARCHAR(64), `target_db` VARCHAR(64), `target_table` VARCHAR(192), `watermark` BIGINT, `trigger` VARCHAR(20), `sink_quota` VARCHAR(20), `checkpoint_interval` VARCHAR(20), `checkpoint_backup` VARCHAR(20), `history_scan_idle` VARCHAR(20)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_stream_tasks;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_stream_tasks')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_stream_tasks` (`stream_name` VARCHAR(64), `task_id` VARCHAR(16), `node_type` VARCHAR(10), `node_id` INT, `level` VARCHAR(10), `status` VARCHAR(12), `stage` BIGINT, `in_queue` VARCHAR(18), `process_total` VARCHAR(12), `process_throughput` VARCHAR(12), `out_total` VARCHAR(12), `out_throughput` VARCHAR(12), `info` VARCHAR(40), `start_time` TIMESTAMP, `start_id` BIGINT, `start_ver` BIGINT, `checkpoint_time` TIMESTAMP, `checkpoint_id` BIGINT, `checkpoint_ver` BIGINT, `checkpoint_size` VARCHAR(14), `checkpoint_backup` VARCHAR(14), `extra_info` VARCHAR(25), `history_task_id` VARCHAR(16), `history_task_status` VARCHAR(12)) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_vnodes;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_vnodes')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_vnodes` (`dnode_id` INT, `vgroup_id` INT, `db_name` VARCHAR(64), `status` VARCHAR(9), `role_time` TIMESTAMP, `start_time` TIMESTAMP, `restored` BOOL) COMMENT ''")
        
        show_create_sql = "show create table information_schema.ins_user_privileges;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'ins_user_privileges')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `ins_user_privileges` (`user_name` VARCHAR(24), `privilege` VARCHAR(10), `db_name` VARCHAR(65), `table_name` VARCHAR(193), `condition` VARCHAR(49152), `notes` VARCHAR(64)) COMMENT ''")
        
        show_indexes_sql = "show indexes from ins_dnodes from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_dnodes;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_dnodes from information_schema;" 
        self.tdSql.error(show_tags_sql)   
        show_tags_sql = "show tags from information_schema.ins_dnodes;" 
        self.tdSql.error(show_tags_sql)    
        show_tags_sql = "show table tags from ins_dnodes from information_schema;" 
        self.tdSql.error(show_tags_sql)    
        show_tags_sql = "show table tags from information_schema.ins_dnodes;" 
        self.tdSql.error(show_tags_sql)    
        
        show_indexes_sql = "show indexes from ins_mnodes from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_mnodes;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_mnodes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_mnodes;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_mnodes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_mnodes;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_modules from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_modules;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_modules from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_modules;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_modules from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_modules;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_qnodes from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_qnodes;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_qnodes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_qnodes;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_qnodes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_qnodes;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_snodes from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_snodes;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_snodes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_snodes;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_snodes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_snodes;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_databases from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_databases;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_databases from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_databases;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_databases from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_databases;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_functions from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_functions;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_functions from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_functions;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_functions from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_functions;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_indexes from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_indexes;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_indexes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_indexes;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_indexes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_indexes;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_stables from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_stables;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_stables from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_stables;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_stables from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_stables;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_tables from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_tables;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_tables from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_tables;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_tables from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_tables;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_tags from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_tags;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_tags from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_tags;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_tags from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_tags;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_columns from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_columns;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_columns from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_columns;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_columns from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_columns;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_users from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_users;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_users from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_users;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_users from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_users;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_grants from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_grants;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_grants from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_grants;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_grants from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_grants;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_vgroups from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_vgroups;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_vgroups from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_vgroups;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_vgroups from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_vgroups;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_configs from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_configs;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_configs from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_configs;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_configs from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_configs;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_dnode_variables from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_dnode_variables;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_dnode_variables from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_dnode_variables;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_dnode_variables from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_dnode_variables;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_topics from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_topics;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_topics from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_topics;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_topics from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_topics;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_subscriptions from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_subscriptions;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_subscriptions from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_subscriptions;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_subscriptions from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_subscriptions;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_streams from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_streams;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_streams from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_streams;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_streams from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_streams;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_stream_tasks from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_stream_tasks;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_stream_tasks from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_stream_tasks;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_stream_tasks from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_stream_tasks;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_vnodes from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_vnodes;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_vnodes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_vnodes;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_vnodes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_vnodes;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from ins_dnodes from information_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from information_schema.ins_dnodes;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from ins_dnodes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from information_schema.ins_dnodes;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from ins_dnodes from information_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from information_schema.ins_dnodes;" 
        self.tdSql.error(show_tags_sql) 
        
        
    def show_performance_schema(self):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
                
        show_create_sql = "show create database performance_schema;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'performance_schema')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),'CREATE DATABASE `performance_schema`')
        
        show_create_sql = "show create table performance_schema.perf_connections;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'perf_connections')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `perf_connections` (`conn_id` INT UNSIGNED, `user` VARCHAR(24), `app` VARCHAR(24), `pid` INT UNSIGNED, `end_point` VARCHAR(134), `login_time` TIMESTAMP, `last_access` TIMESTAMP) COMMENT ''")
        show_sql = "show connections;"
        select_sql = "select * from performance_schema.perf_connections;"
        self.sql_check(show_sql,select_sql,check_tag = "N")
        
        show_create_sql = "show create table performance_schema.perf_queries;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'perf_queries')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `perf_queries` (`kill_id` VARCHAR(26), `query_id` BIGINT UNSIGNED, `conn_id` INT UNSIGNED, `app` VARCHAR(24), `pid` INT, `user` VARCHAR(24), `end_point` VARCHAR(22), `create_time` TIMESTAMP, `exec_usec` BIGINT, `stable_query` BOOL, `sub_query` BOOL, `sub_num` INT, `sub_status` VARCHAR(1000), `sql` VARCHAR(2048)) COMMENT ''")
        
        show_create_sql = "show create table performance_schema.perf_consumers;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'perf_consumers')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `perf_consumers` (`consumer_id` VARCHAR(32), `consumer_group` VARCHAR(193), `client_id` VARCHAR(256), `user` VARCHAR(24), `fqdn` VARCHAR(128), `status` VARCHAR(20), `topics` VARCHAR(205), `up_time` TIMESTAMP, `subscribe_time` TIMESTAMP, `rebalance_time` TIMESTAMP, `parameters` VARCHAR(192)) COMMENT ''")
        show_sql = "show consumers;"
        select_sql = "select * from performance_schema.perf_consumers;"
        self.sql_check(show_sql,select_sql)
        
        show_create_sql = "show create table performance_schema.perf_trans;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'perf_trans')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `perf_trans` (`id` INT, `create_time` TIMESTAMP, `stage` VARCHAR(12), `oper` VARCHAR(14), `db` VARCHAR(64), `stable` VARCHAR(192), `failed_times` INT, `last_exec_time` TIMESTAMP, `last_action_info` VARCHAR(511)) COMMENT ''")
        
        show_create_sql = "show create table performance_schema.perf_apps;"
        self.tdSql.query(show_create_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'perf_apps')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),"CREATE TABLE `perf_apps` (`app_id` BIGINT UNSIGNED, `ip` VARCHAR(16), `pid` INT, `name` VARCHAR(24), `start_time` TIMESTAMP, `insert_req` BIGINT UNSIGNED, `insert_row` BIGINT UNSIGNED, `insert_time` BIGINT UNSIGNED, `insert_bytes` BIGINT UNSIGNED, `fetch_bytes` BIGINT UNSIGNED, `query_time` BIGINT UNSIGNED, `slow_query` BIGINT UNSIGNED, `total_req` BIGINT UNSIGNED, `current_req` BIGINT UNSIGNED, `last_access` TIMESTAMP) COMMENT ''")
        
        show_indexes_sql = "show indexes from perf_connections from performance_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from performance_schema.perf_connections;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from perf_connections from performance_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from performance_schema.perf_connections;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from perf_connections from performance_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from performance_schema.perf_connections;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from perf_queries from performance_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from performance_schema.perf_queries;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from perf_queries from performance_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from performance_schema.perf_queries;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from perf_queries from performance_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from performance_schema.perf_queries;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from perf_consumers from performance_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from performance_schema.perf_consumers;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from perf_consumers from performance_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from performance_schema.perf_consumers;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from perf_consumers from performance_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from performance_schema.perf_consumers;" 
        self.tdSql.error(show_tags_sql) 
        
        show_indexes_sql = "show indexes from perf_apps from performance_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from performance_schema.perf_apps;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from perf_apps from performance_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from performance_schema.perf_apps;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from perf_apps from performance_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from performance_schema.perf_apps;" 
        self.tdSql.error(show_tags_sql) 
        show_sql = "show apps;"
        select_sql = "select * from performance_schema.perf_apps;"
        self.sql_check(show_sql,select_sql,check_tag = "N")
        
        show_indexes_sql = "show indexes from perf_trans from performance_schema;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_indexes_sql = "show indexes from performance_schema.perf_trans;" 
        self.tdSql.query(show_indexes_sql)          
        self.tdSql.checkRow(0)
        show_tags_sql = "show tags from perf_trans from performance_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show tags from performance_schema.perf_trans;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from perf_trans from performance_schema;" 
        self.tdSql.error(show_tags_sql) 
        show_tags_sql = "show table tags from performance_schema.perf_trans;" 
        self.tdSql.error(show_tags_sql) 
                
    def show_stable_schema(self):
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))                
        self.tdCreateData.dropandcreateDB_random("%s" % self.db, 1)  
        
        show_create_sql = "show indexes from %s;" % self.db
        self.tdSql.query(show_create_sql)          
        self.tdSql.checkRow(0)
        
        show_create_sql = "show indexes from stable_1 from %s;" % self.db
        self.tdSql.query(show_create_sql) 
        select_index_sql = "select index_name,db_name,table_name from information_schema.ins_indexes where db_name = '%s' and table_name = 'stable_1';" % self.db
        self.tdSql.query(select_index_sql)         
        self.sql_check(select_index_sql,show_create_sql)
        
        show_create_sql = "show indexes from %s.stable_2;" % self.db
        self.tdSql.query(show_create_sql)  
        select_index_sql = "select index_name,db_name,table_name from information_schema.ins_indexes where db_name = '%s' and table_name = 'stable_2';" % self.db
        self.tdSql.query(select_index_sql)         
        self.sql_check(select_index_sql,show_create_sql)
        
        show_create_sql = "show tags from stable_1 from %s;" % self.db
        self.tdSql.query(show_create_sql)          
        self.tdSql.checkRow(0)
        show_create_sql = "show tags from %s.stable_2;" % self.db
        self.tdSql.query(show_create_sql)          
        self.tdSql.checkRow(0)
        
        show_create_sql = "show table tags from stable_1 from %s;" % self.db
        self.tdSql.query(show_create_sql)          
        self.tdSql.checkRow(6)
        show_create_sql = "show table tags from %s.stable_2;" % self.db
        self.tdSql.query(show_create_sql)          
        self.tdSql.checkRow(6)
        
        show_create_sql1 = "show table tags from stable_1_1 from %s;" % self.db
        show_create_sql2 = "show table tags from %s.stable_1_1;" % self.db
        self.sql_check(show_create_sql1,show_create_sql2) 
        show_create_sql1 = "show table tags from stable_2_1 from %s;" % self.db
        show_create_sql2 = "show table tags from %s.stable_2_1;" % self.db
        self.sql_check(show_create_sql1,show_create_sql2)
        
        
        show_create_sql1 = "show table distributed stable_1;"
        show_create_sql2 = "show table distributed %s.stable_1;" % self.db
        self.sql_check(show_create_sql1,show_create_sql2)
        show_create_sql1 = "show table distributed stable_2;"
        show_create_sql2 = "show table distributed %s.stable_2;" % self.db
        self.sql_check(show_create_sql1,show_create_sql2)
        show_create_sql1 = "show table distributed stable_null_data;"
        show_create_sql2 = "show table distributed %s.stable_null_data;" % self.db
        self.sql_check(show_create_sql1,show_create_sql2)
        
        #TS-4282
        ts_4282_sql = "insert into %s.`tb1`(ts,q_int) using %s.stable_1 tags(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)  values(now,1) ;"% (self.db,self.db)
        self.tdSql.execute(ts_4282_sql)  
        show_4282_sql = "show create table tb1"
        self.tdSql.query(show_4282_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'tb1')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),'CREATE TABLE `tb1` USING `stable_1` (`loc`, `t_int`, `t_bigint`, `t_smallint`, `t_tinyint`, `t_int_unsigned`, `t_bigint_unsigned`, `t_smallint_unsigned`, `t_tinyint_unsigned`, `t_bool`, `t_binary`, `t_nchar`, `t_float`, `t_double`, `t_ts`) TAGS (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)')
        
        ts_4282_sql = "insert into %s.`tb2`(ts,q_int) using %s.stable_1 tags(NULL,'NULL',NULL,'NULL',NULL,'NULL',NULL,'NULL',NULL,'NULL',NULL,'NULL',NULL,NULL,'NULL')  values(now,1) ;"% (self.db,self.db)
        self.tdSql.execute(ts_4282_sql)  
        show_4282_sql = "show create table tb2"
        self.tdSql.query(show_4282_sql)          
        self.tdCreateData.data_check(self.tdSql.getData(0,0),'tb2')
        self.tdCreateData.data_check(self.tdSql.getData(0,1),'CREATE TABLE `tb2` USING `stable_1` (`loc`, `t_int`, `t_bigint`, `t_smallint`, `t_tinyint`, `t_int_unsigned`, `t_bigint_unsigned`, `t_smallint_unsigned`, `t_tinyint_unsigned`, `t_bool`, `t_binary`, `t_nchar`, `t_float`, `t_double`, `t_ts`) TAGS (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "NULL", NULL, NULL, NULL)')
        

    def sql_check(self,sql1,sql2,check_tag = "Y"):
        # self.tdSql.query(sql1)
        # col1 = self.tdSql.query_cols
        # self.tdSql.query(sql2)
        # col2 = self.tdSql.query_cols
        if check_tag == "Y":
            result1 = self.tdSql.query(sql1)
            row_sql1 = result1.row_count
            column_sql1 = result1.field_count
            
            result2 = self.tdSql.query(sql2)
            row_sql2 = result2.row_count
            column_sql2 = result2.field_count
            self.logger.info(("======sql1:'%s' result != sql2:'%s' result======") %(row_sql1,row_sql2))
                    
            if row_sql1 == 0 :
                self.tdSql.checkEqual(row_sql1,row_sql2) 
            elif row_sql1 !=0 : 
                #check row
                for i in range(row_sql1):
                    self.tdSql.execute("reset query cache;")
                    self.tdSql.query(sql1)
                    value_both = self.tdSql.getData(i,0)
                
                    self.tdSql.execute("reset query cache;")  
                    self.tdSql.query(sql2)        
                    value_none = self.tdSql.getData(i,0)
                    
                    self.logger.info(("======check row:'%s' column:'%s' ======") %(i,0))
                    self.tdSql.checkEqual(value_both,value_none)   
                #check column    
                for j in range(column_sql1):
                    self.tdSql.execute("reset query cache;")
                    self.tdSql.query(sql1)
                    value_both = self.tdSql.getData(0,j)
                
                    self.tdSql.execute("reset query cache;")  
                    self.tdSql.query(sql2)        
                    value_none = self.tdSql.getData(0,j)
                
                    self.logger.info(("======check row:'%s' column:'%s'======") %(0,j))
                    self.tdSql.checkEqual(value_both,value_none)     
            
        elif  check_tag == "N":    
            result1 = self.tdSql.query(sql1)
            row_sql1 = result1.row_count
            
            result2 = self.tdSql.query(sql2)
            row_sql2 = result2.row_count
            self.logger.info(("==check_tag=N====sql1:'%s' result != sql2:'%s' result======") %(row_sql1,row_sql2))
                
                
                                            
    def run(self):
        startTime = time.time() 
        
        #os.system("nohup taostest --use=common_insert.yaml --case=Query/queryscript/scene_query/schema/compact_alldb.py --keep --disable_collection &")
                
        self.tdSql.query("alter local 'schedulePolicy' '%d';" %random.randint(1,3))
         
        self.show_local_variables() 
        for i in range(10):
            # self.show_local_variables() 
            # self.show_dnode_variables()
            
            self.show_information_schema()
            self.show_performance_schema()
            
            self.show_stable_schema()        
        
            self.tdCreateData.drop_db("%s" % self.db) 
        
        endTime = time.time()

        self.logger.info("total time %ds" % (endTime - startTime))
  

