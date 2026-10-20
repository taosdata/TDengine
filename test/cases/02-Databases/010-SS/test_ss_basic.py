# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, epath, sc, etool, eutil, eos, tdStream
import os
import time
import random
import string
import taos
import hashlib
import hmac
import http.client
import calendar
import time as _time
import urllib.parse
import uuid
import xml.etree.ElementTree as ET

# ==== ss bucket helper BEGIN (dependency-free SigV4; extractable for standalone tests) ====
#
# Concurrent CI runs must not share a bucket: two clusters writing to the same bucket use
# identical object keys and overwrite each other, which makes the case flaky. ssAccessString
# has no prefix/dir option (see the option list in source/libs/tss/src/s3.c), so isolation is
# only possible at bucket level: every run creates its own bucket with a globally unique name.
# Bucket names are unique server-side and CreateBucket is atomic, therefore colliding with
# another run is impossible (no polling, no pre-created bucket pool).

_S3_HOST = "s3.tdengine.net"
_S3_PORT = 9000
_S3_AK = "mkJO481qGlWR9hpkRweR"
_S3_SK = "2KBJOZ2DXrvjAhIB1Qr64IGzUPxUl566GKWv4Sak"
_S3_REGION = "us-east-1"
SS_BUCKET_PREFIX = "ci-ss-"
SS_BUCKET_GC_HOURS = 6          # leftover buckets with our prefix older than this are gc'ed


def _s3_sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _s3_request(method, path, query="", body=b"", timeout=30):
    """Minimal AWS SigV4 request; path is /bucket or /bucket/key (URL-encoded)."""
    now = _time.gmtime()
    amz_date = _time.strftime("%Y%m%dT%H%M%SZ", now)
    date_stamp = _time.strftime("%Y%m%d", now)
    payload_hash = hashlib.sha256(body).hexdigest()

    canonical_headers = (
        f"host:{_S3_HOST}:{_S3_PORT}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join([
        method, path, query, canonical_headers, signed_headers, payload_hash,
    ])
    scope = f"{date_stamp}/{_S3_REGION}/s3/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256", amz_date, scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    k_signing = _s3_sign(
        _s3_sign(_s3_sign(_s3_sign(f"AWS4{_S3_SK}".encode("utf-8"), date_stamp),
                          _S3_REGION), "s3"), "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"),
                         hashlib.sha256).hexdigest()

    headers = {
        "Host": f"{_S3_HOST}:{_S3_PORT}",
        "x-amz-date": amz_date,
        "x-amz-content-sha256": payload_hash,
        "Authorization": (
            f"AWS4-HMAC-SHA256 Credential={_S3_AK}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        ),
    }
    conn = http.client.HTTPConnection(_S3_HOST, _S3_PORT, timeout=timeout)
    try:
        conn.request(method, path + (f"?{query}" if query else ""),
                     body=body, headers=headers)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def _s3_xml(data):
    """Parse S3 XML after stripping the default namespace, else ./Buckets/Bucket matches nothing."""
    root = ET.fromstring(data)
    for el in root.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
    return root


def _s3_list_buckets():
    status, data = _s3_request("GET", "/")
    if status != 200:
        raise Exception(f"list buckets failed: http {status} {data[:200]!r}")
    root = _s3_xml(data)
    return [(b.findtext("Name"), b.findtext("CreationDate"))
            for b in root.findall("./Buckets/Bucket")]


def _s3_list_objects(bucket, missing_ok=False):
    status, data = _s3_request("GET", f"/{bucket}", query="list-type=2")
    if status == 404 and missing_ok:
        return None
    if status != 200:
        raise Exception(f"list objects in {bucket} failed: http {status}")
    root = _s3_xml(data)
    return [c.findtext("Key") for c in root.findall("./Contents")]


def _s3_delete_object(bucket, key):
    esc = "/".join(urllib.parse.quote(part, safe="") for part in key.split("/"))
    status, _ = _s3_request("DELETE", f"/{bucket}/{esc}")
    return status


def _ss_gc_buckets(max_age_hours=SS_BUCKET_GC_HOURS):
    """Remove leftover buckets with our prefix older than max_age_hours (skips ones likely in use)."""
    try:
        now = _time.time()
        for name, created in _s3_list_buckets():
            if not name or not name.startswith(SS_BUCKET_PREFIX):
                continue
            ts = None
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    # CreationDate is UTC: timegm(), not mktime() (local time), or every bucket
                    # looks hours older than it is and live buckets would be deleted.
                    ts = calendar.timegm(_time.strptime(created, fmt))
                    break
                except Exception:
                    continue
            if ts is None or now - ts < max_age_hours * 3600:
                continue
            for key in _s3_list_objects(name):
                _s3_delete_object(name, key)
            st, _ = _s3_request("DELETE", f"/{name}")
            tdLog.info(f"ss bucket gc: removed stale bucket {name} (http {st}, created {created})")
    except Exception as e:
        tdLog.info(f"ss bucket gc skipped: {type(e).__name__}: {e}")


_bucket_cache = None  # one bucket per process, even if this module body runs more than once


def _ss_alloc_bucket():
    """Claim a bucket: unique name + atomic CreateBucket, so no other run can conflict."""
    global _bucket_cache
    if _bucket_cache:
        return _bucket_cache
    _ss_gc_buckets()
    name = f"{SS_BUCKET_PREFIX}{uuid.uuid4().hex[:16]}"
    status, data = _s3_request("PUT", f"/{name}")
    if status != 200:
        raise Exception(f"create ss bucket {name} failed: http {status} {data[:200]!r}")
    _bucket_cache = name
    tdLog.info(f"ss bucket allocated: {name}")
    return name


def _ss_release_bucket(bucket):
    """Release the bucket: delete objects then the bucket (best-effort; leftovers are gc'ed later)."""
    global _bucket_cache
    _bucket_cache = None
    try:
        if not bucket or not bucket.startswith(SS_BUCKET_PREFIX):
            return
        # DROP DATABASE already frees the shared-storage objects and may remove the bucket, so a
        # missing bucket is the expected outcome, not an error.
        keys = _s3_list_objects(bucket, missing_ok=True)
        if keys is None:
            tdLog.info(f"ss bucket already gone: {bucket}")
            return
        for key in keys:
            _s3_delete_object(bucket, key)
        status, _ = _s3_request("DELETE", f"/{bucket}")
        tdLog.info(f"ss bucket released: {bucket} (http {status})")
    except Exception as e:
        tdLog.info(f"ss bucket release skipped: {type(e).__name__}: {e}")


# ==== ss bucket helper END ====

#  
# s3.tdengine.net (192.168.0.216) MINIO
#

'''
Common:
    ssEnabled               : 2,
    ssPageCacheSize         : 10240,
    ssUploadDelaySec        : 10,
    ssAutoMigrateIntervalSec: 600,

ssAccessString Common:
    Endpoint        : s3.tdengine.net:9000
    Protocol        : http
    UriStyle        : path
    ChunkSize       : 64MB
    MaxChunks       : 10000
    MaxRetry        : 3

ssAccessTring For CI:
    Bucket          : ci-ss-<uuid>, allocated per run (see _ss_alloc_bucket)
    AccessKeyId     : mkJO481qGlWR9hpkRweR
    SecretAccessKey : 2KBJOZ2DXrvjAhIB1Qr64IGzUPxUl566GKWv4Sak

ssAccessString For Test:
    Bucket          : same as above (unique per run, deleted afterwards)
    AccessKeyId     : mkJO481qGlWR9hpkRweR
    SecretAccessKey : 2KBJOZ2DXrvjAhIB1Qr64IGzUPxUl566GKWv4Sak
'''

# The taos connector raises ProgrammingError(errstr, errno) and the message can be unusable
# (observed '\x05'), so tolerate repeated ssmigrate by matching the numeric error code:
# TSDB_CODE_MND_SSMIGRATE_ALREADY_EXIST = TAOS_DEF_ERROR_CODE(0, 0x04C3)
_ERR_MND_SSMIGRATE_ALREADY_EXIST = -2147482429


def _err_code(e):
    """Read the connector error code through wrappers such as tdSql.execute."""
    while e is not None:
        code = getattr(e, "errno", None)
        if isinstance(code, int):
            return code
        for arg in e.args:
            if isinstance(arg, int):
                return arg
        e = e.__cause__ if e.__cause__ is not None else e.__context__
    return None


class TestSSBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # Exclusive bucket: a unique name per run, so concurrent runs never share objects
    bucketName = _ss_alloc_bucket()
    updatecfgDict = {
        "supportVnodes":"1000",
        "ssEnabled": "2",
        "ssAccessString": f's3:endpoint=s3.tdengine.net:9000;bucket={bucketName};uriStyle=path;protocol=http;accessKeyId=mkJO481qGlWR9hpkRweR;secretAccessKey=2KBJOZ2DXrvjAhIB1Qr64IGzUPxUl566GKWv4Sak;chunkSize=64;maxChunks=10000;maxRetry=3',
        'ssPageCacheSize': '10240',
        "ssUploadDelaySec": "10",
        'ssAutoMigrateIntervalSec': '600',
    }

    tdLog.info(f"assign bucketName is {bucketName}\n")
    maxFileSize = (128 + 10) * 1014 * 1024 # add 10M buffer

    def exit(self, log):
        self.dropDb(True)
        _ss_release_bucket(self.bucketName)
        tdLog.exit(log)

    def insertData(self):
        tdLog.info(f"insert data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "ssBasic.json")
        etool.benchMark(json=json)

        tdSql.execute(f"use {self.db}")
        # come from ss_basic.json
        self.childtable_count = 6
        self.insert_rows = 2000000
        self.timestamp_step = 100

    def createStream(self, sname):
        # 3.4 stream syntax:
        #   1) window moved to the trigger: interval(10s) from <table>
        #   2) options go into stream_options(...); a trigger is mandatory and "fill_history 1" is invalid
        #   3) subquery reads %%trows and the first output column must be a timestamp (_twstart)
        sql = f"create stream {sname} interval(10s) from {self.db}.{self.stb} stream_options(fill_history) into stm1 as select _twstart, count(*) from %%trows;"
        tdSql.execute(sql)

    def migrateDbSs(self):
        sql = f"ssmigrate database {self.db}"
        try:
            tdSql.execute(sql, queryTimes=60, show=True)
        except Exception as e:
            # An existing migration returns TSDB_CODE_MND_SSMIGRATE_ALREADY_EXIST.
            # tdSql.execute wraps the connector exception after retrying, so read its
            # error code from the exception chain. checkUploadToSs still checks uploads.
            if _err_code(e) == _ERR_MND_SSMIGRATE_ALREADY_EXIST:
                tdLog.info(f"ssmigrate already in progress: {e}")
            else:
                raise

    def checkDataFile(self, lines, maxFileSize):
        # ls -l
        # -rwxrwxrwx 1 root root  41652224 Apr 17 14:47 vnode2/tsdb/v2f1974ver47.3.data
        overCnt = 0
        for line in lines:
            cols = line.split()
            fileSize = int(cols[4])
            fileName = cols[8]
            #print(f" filesize={fileSize} fileName={fileName}  line={line}")
            if fileSize > maxFileSize:
                tdLog.info(f"error, {fileSize} over max size({maxFileSize}) {fileName}\n")
                overCnt += 1
            else:
                tdLog.info(f"{fileName}({fileSize}) check size passed.")
                
        return overCnt

    def checkUploadToSs(self):
        rootPath = sc.clusterRootPath()
        cmd = f"ls -l {rootPath}/dnode*/data/vnode/vnode*/tsdb/*.data"
        tdLog.info(cmd)
        loop = 0
        rets = []
        overCnt = 0
        while loop < 150:
            time.sleep(2)

            # check upload to ss
            rets = eos.runRetList(cmd)
            cnt = len(rets)
            if cnt == 0:
                overCnt = 0
                tdLog.info("All data file upload to server over.")
                break
            overCnt = self.checkDataFile(rets, self.maxFileSize)
            if overCnt == 0:
                uploadOK = True
                tdLog.info(f"All data files({len(rets)}) size bellow {self.maxFileSize}, check upload to ss ok.")
                break

            tdLog.info(f"loop={loop} no upload {overCnt} data files wait 3s retry ...")
            if loop == 3:
                sc.dnodeStop(1)
                time.sleep(2)
                sc.dnodeStart(1)
            loop += 1
            # migrate
            self.migrateDbSs()
                
        # check can pass
        if overCnt > 0:
            self.exit(f"ss have {overCnt} files over size.")


    def doAction(self):
        tdLog.info(f"do action.")

        self.flushDb(show=True)
        #self.compactDb(show=True)

        # sleep 70s
        self.migrateDbSs()

        # check upload to ss
        self.checkUploadToSs()

    def checkStreamCorrect(self):
        sql = f"select count(*) from {self.db}.stm1"
        count = 0
        for i in range(120):
            tdSql.query(sql)
            count = tdSql.getData(0, 0)
            if count == 100000 or count == 100001:
                return True
            time.sleep(1)
            
        self.exit(f"stream count is not expect . expect = 100000 or 100001 real={count} . sql={sql}")


    def checkCreateDb(self, keepLocal, chunkSize, compact):
        # keyword
        kw1 = kw2 = kw3 = "" 
        if keepLocal is not None:
            kw1 = f"ss_keeplocal {keepLocal}"
        if chunkSize is not None:
            kw2 = f"ss_chunkpages {chunkSize}"
        if compact is not None:
            kw3 = f"ss_compact {compact}"    

        sql = f" create database db1 vgroups 1 duration 1h {kw1} {kw2} {kw3}"
        tdSql.execute(sql, show=True)
        # ss_chunkpages/ss_keeplocal/ss_compact are reserved words (cannot be selected by name),
        # so resolve their indexes from the result set to survive new columns (e.g. keep_time_offset)
        sql = f"select * from information_schema.ins_databases where name='db1';"
        colNames = tdSql.getColNameList(sql)
        tdSql.query(sql)
        if chunkSize is not None:
            tdSql.checkData(0, colNames.index("ss_chunkpages"), chunkSize)
        if keepLocal is not None:
            keepLocalm = keepLocal * 24 * 60
            tdSql.checkData(0, colNames.index("ss_keeplocal"), f"{keepLocalm}m")
        if compact is not None:
            tdSql.checkData(0, colNames.index("ss_compact"), compact)
        sql = "drop database db1"
        tdSql.execute(sql)

    def check_except(self):
        # errors
        sqls = [
            f"create database db2 ss_keeplocal -1",
            f"create database db2 ss_keeplocal 0",
            f"create database db2 ss_keeplocal 365001",
            f"create database db2 ss_chunkpages -1",
            f"create database db2 ss_chunkpages 0",
            f"create database db2 ss_chunkpages 900000000",
            f"create database db2 ss_compact -1",
            f"create database db2 ss_compact 100",
            f"create database db2 duration 1d ss_keeplocal 1d"
        ]
        tdSql.errors(sqls)


    def checkBasic(self):
        # create db
        keeps  = [1, 256, 1024, 365000, None]
        chunks = [131072, 600000, 820000, 1048576, None]
        comps  = [0, 1, None]

        for keep in keeps:
            for chunk in chunks:
                for comp in comps:
                    self.checkCreateDb(keep, chunk, comp)

        
        # --checkss
        idx = 1
        taosd = sc.taosdFile(idx)
        cfg   = sc.dnodeCfgPath(idx)
        cmd = f"{taosd} -c {cfg} --checkss"

        eos.exe(cmd)
        #output, error = eos.run(cmd)
        #print(lines)

        '''
        tips = [
            "put object sstest.txt: success",
            "listing bucket ci-bucket: success",
            "get object sstest.txt: success",
            "delete object sstest.txt: success"
        ]
        pos = 0
        for tip in tips:
            pos = output.find(tip, pos)
            #if pos == -1:
            #    tdLog.exit(f"checkss failed not found {tip}. cmd={cmd} output={output}")
        '''
        
        # except
        self.check_except()

    #
    def preDb(self, vgroups):
        cnt = int(time.time())%2 + 1
        for i in range(cnt):
             vg = eutil.cpuRand(9) + 1
             sql = f"create database predb vgroups {vg}"
             tdSql.execute(sql, show=True)
             sql = "drop database predb"
             tdSql.execute(sql, show=True)

    # history
    def insertHistory(self):
        tdLog.info(f"insert history data.")
        # taosBenchmark run
        json = etool.curFile(__file__, "ssBasic_History.json")
        etool.benchMark(json=json)

        # come from ss_basic.json
        self.insert_rows += self.insert_rows/4
        self.timestamp_step = 50

    # delete
    def checkDelete(self):
        # del 1000 rows
        start = 1600000000000
        drows = 200
        for i in range(1, drows, 2):
            sql = f"from {self.db}.{self.stb} where ts = {start + i*500}"
            tdSql.execute("delete " + sql, show=True)
            tdSql.query("select * " + sql)
            tdSql.checkRows(0)
        
        # delete all 500 step
        self.flushDb()
        self.compactDb()
        self.insert_rows   -= drows/2
        sql = f"select count(*) from {self.db}.{self.stb}"
        tdSql.checkAgg(sql, self.insert_rows * self.childtable_count)

        # delete 10W rows from 100000
        drows = 100000
        sdel = start + 100000 * self.timestamp_step
        edel = start + 100000 * self.timestamp_step + drows * self.timestamp_step
        sql = f"from {self.db}.{self.stb} where ts >= {sdel} and ts < {edel}"
        tdSql.execute("delete " + sql, show=True)
        tdSql.query("select * " + sql)
        tdSql.checkRows(0)

        self.insert_rows   -= drows
        sql = f"select count(*) from {self.db}.{self.stb}"
        tdSql.checkAgg(sql, self.insert_rows * self.childtable_count)
        

    # run
    def test_ss_basic(self):
        """Shared storage (S3) basic scenario

        1. Insert data with ssEnabled=2, ssmigrate, verify local .data files are uploaded
        2. Create and drop a stream on the shared-storage database
        3. History and disorder data with aggregate snapshot checks
        4. ss_keeplocal / ss_chunkpages / ss_compact DDL checks and taosd --checkss
        5. Delete data, then migrate and upload again

        Catalog:
            - Storage

        Since: v3.3.7.0

        Labels: common,ci,integration,functional

        Jira: TD-7135994545

        History:
            - 2026-10-15 Alex Duan Move to cases/02-Databases/010-SS and adapt to the new framework
            - 2026-10-16 Bomin Zhang Fix wrapped migration error handling without skipping upload checks

        """
        self.sname = "stream1"
        if eos.isArm64Cpu():
            tdLog.success(f"{__file__} arm64 ignore executed")
        else:
            
            self.preDb(10)

            # insert data
            self.insertData()

            # snode: required since 3.4, otherwise create stream fails with "No Snode is available"
            tdStream.ensureSnode()

            # creat stream
            self.createStream(self.sname)

            # check insert data correct
            #self.checkInsertCorrect()

            # save
            self.snapshotAgg()

            # do action
            self.doAction()

            # check save agg result correct
            self.checkAggCorrect()

            # check insert correct again
            self.checkInsertCorrect()


            # check stream correct and drop stream
            #self.checkStreamCorrect()

            # drop stream
            self.dropStream(self.sname)

            # insert history  disorder data
            self.insertHistory()

            # checkBasic
            self.checkBasic()

            #self.checkInsertCorrect()
            self.snapshotAgg()
            self.doAction()
            self.checkAggCorrect()
            self.checkInsertCorrect(difCnt=self.childtable_count*1499999)
            self.checkDelete()
            self.doAction()

            # drop database and free ss file
            self.dropDb()

            # release the bucket claimed by this run
            _ss_release_bucket(self.bucketName)
