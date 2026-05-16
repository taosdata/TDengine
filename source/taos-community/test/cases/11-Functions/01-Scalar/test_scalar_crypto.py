import time
from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestCrypto:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_crypto(self):
        """Scalar: Crypto

        Test crypto functions, including sm4, aes encryption/decryption, base64 codec, hashing and masking.

        Catalog:
            - Function:Sclar

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
             - 2025-11-28 Stephen Jin Create crypto functions

        """

        self.smoking()
        tdStream.dropAllStreamsAndDbs()
        self.null()
        tdStream.dropAllStreamsAndDbs()
        self.single()
        tdStream.dropAllStreamsAndDbs()
        self.empty()
        tdStream.dropAllStreamsAndDbs()
        self.error()
        tdStream.dropAllStreamsAndDbs()

    def smoking(self):
        vgroups = 1
        dbName = "db"
        tbName = "tb"

        tdSql.execute(f"create database {dbName} vgroups {vgroups}")

        tdLog.info(f"====> aes")
        f = 'AES_ENCRYPT'
        fi = 'aes_decrypt'
        
        tdSql.query(f"SELECT {fi}({f}('mytext','mykeystring'), 'mykeystring')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'mytext')

        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(32))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('mytext','mykeystring'))")
        tdSql.query(f"SELECT {fi}(f1, 'mykeystring') from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'mytext')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(32))")
        tdSql.execute(f"insert into {dbName}.{tbName} SELECT now, {fi}({f}('mytext','mykeystring'), 'mykeystring')")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'mytext')

        tdLog.info(f"====> sm4")
        f = 'sm4_ENCRYPT'
        fi = 'sm4_decrypt'
        
        tdSql.query(f"SELECT {fi}({f}('mytext','mykeystring'), 'mykeystring')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'mytext')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(32))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('mytext','mykeystring'))")
        tdSql.query(f"SELECT {fi}(f1, 'mykeystring') from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'mytext')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(32))")
        tdSql.execute(f"insert into {dbName}.{tbName} SELECT now, {fi}({f}('mytext','mykeystring'), 'mykeystring')")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'mytext')

        tdLog.info(f"====> base64")
        f = 'to_base64'
        fi = 'from_base64'
        
        tdSql.query(f"SELECT {f}('mytext')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'bXl0ZXh0')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(32))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('mytext'))")
        tdSql.query(f"SELECT {fi}(f1) from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'mytext')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(32))")
        tdSql.execute(f"insert into {dbName}.{tbName} SELECT now, {fi}({f}('mytext'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'mytext')

        tdLog.info(f"====> hashing")
        f = 'md5'
        
        tdSql.query(f"SELECT {f}('mytext')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '947ef8c8db156a568d5974d71f7638f4')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(32))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('mytext'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '947ef8c8db156a568d5974d71f7638f4')

        f = 'sha'
        
        tdSql.query(f"SELECT {f}('mytext')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '65d922aad93c7e165ed888a2ab85befe9841fd39')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('mytext'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '65d922aad93c7e165ed888a2ab85befe9841fd39')

        f = 'sha1'
        
        tdSql.query(f"SELECT {f}('mytext')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '65d922aad93c7e165ed888a2ab85befe9841fd39')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('mytext'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '65d922aad93c7e165ed888a2ab85befe9841fd39')

        f = 'sha2'
        len = 224
        
        tdSql.query(f"SELECT {f}('mytext', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '576e8f2cf59ebc59dd7659c48916f162ae0cf35937563999d5a7800e')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('mytext', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '576e8f2cf59ebc59dd7659c48916f162ae0cf35937563999d5a7800e')

        f = 'sha2'
        len = 256
        
        tdSql.query(f"SELECT {f}('mytext', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'db695168e73ae294e9c4ea90ff593e211aa1b5693f49303a426148433400d23f')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('mytext', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'db695168e73ae294e9c4ea90ff593e211aa1b5693f49303a426148433400d23f')

        f = 'sha2'
        len = 384
        
        tdSql.query(f"SELECT {f}('mytext', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '64f212f07b38912d138eab7c438b38eba1ddd41ab14db497f17594fba92d9ba5cbbea373fa1b8353258d36572b732558')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('mytext', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '64f212f07b38912d138eab7c438b38eba1ddd41ab14db497f17594fba92d9ba5cbbea373fa1b8353258d36572b732558')

        f = 'sha2'
        len = 512
        
        tdSql.query(f"SELECT {f}('mytext', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '7640f40ef57a1c470f88cfb1421434bba1711b5621dff6b2c0d1b64e0d75830f5daf97ee44cf47e168ff7cb67096e69402ab7fd269b66b5e948927f97e12f4e2')

        tdSql.execute(f"drop table {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('mytext', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '7640f40ef57a1c470f88cfb1421434bba1711b5621dff6b2c0d1b64e0d75830f5daf97ee44cf47e168ff7cb67096e69402ab7fd269b66b5e948927f97e12f4e2')

        f = 'mask_none'
        
        tdSql.query(f"SELECT {f}('mytext')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'mytext')

        f = 'mask_full'
        
        tdSql.query(f"SELECT {f}('mytext', '***')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '***')

        f = 'mask_partial'
        
        tdSql.query(f"SELECT {f}('mytext', 1, 2, '*')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*yte**')

    def null(self):
        vgroups = 1
        dbName = "db"
        tbName = "tb"

        tdSql.execute(f"create database {dbName} vgroups {vgroups}")

        tdLog.info(f"====> crypto")

        f = 'aes_encrypt'
        
        tdSql.query(f"SELECT {f}(null, 'mykeystring')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL, 'mykeystring'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'aes_decrypt'
        
        tdSql.query(f"SELECT {f}(null, 'mykeystring')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL, 'mykeystring'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'sm4_encrypt'
        
        tdSql.query(f"SELECT {f}(null, 'mykeystring')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL, 'mykeystring'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'sm4_decrypt'
        
        tdSql.query(f"SELECT {f}(null, 'mykeystring')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL, 'mykeystring'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdLog.info(f"====> masking")

        f = 'mask_none'
        
        tdSql.query(f"SELECT {f}(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'mask_full'
        
        tdSql.query(f"SELECT {f}(null, '***')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'mask_partial'
        
        tdSql.query(f"SELECT {f}(null, 1, 2, '*')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdLog.info(f"====> base64")

        f = 'to_base64'
        
        tdSql.query(f"SELECT {f}(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'from_base64'
        
        tdSql.query(f"SELECT {f}(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdLog.info(f"====> hashing")

        f = 'md5'
        
        tdSql.query(f"SELECT {f}(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'sha'
        
        tdSql.query(f"SELECT {f}(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'sha1'
        
        tdSql.query(f"SELECT {f}(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'sha2'
        len = 224
        
        tdSql.query(f"SELECT {f}(null, {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL, {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'sha2'
        len = 256
        
        tdSql.query(f"SELECT {f}(null, {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL, {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'sha2'
        len = 384
        
        tdSql.query(f"SELECT {f}(null, {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL, {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        f = 'sha2'
        len = 512
        
        tdSql.query(f"SELECT {f}(null, {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(NULL, {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        
    def empty(self):
        vgroups = 1
        dbName = "db"
        tbName = "tb"

        tdSql.execute(f"create database {dbName} vgroups {vgroups}")

        tdLog.info(f"====> masking")

        f = 'mask_full'
        
        tdSql.query(f"SELECT {f}('', '***')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '***')

        f = 'mask_partial'
        
        tdSql.query(f"SELECT {f}('', 1, 2, '*')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')

        f = 'mask_none'
        
        tdSql.query(f"SELECT {f}('')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        tdLog.info(f"====> hashing")

        f = 'md5'
        
        tdSql.query(f"SELECT {f}('')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'd41d8cd98f00b204e9800998ecf8427e')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(''))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'd41d8cd98f00b204e9800998ecf8427e')

        f = 'sha'
        
        tdSql.query(f"SELECT {f}('')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'da39a3ee5e6b4b0d3255bfef95601890afd80709')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(''))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'da39a3ee5e6b4b0d3255bfef95601890afd80709')

        f = 'sha1'
        
        tdSql.query(f"SELECT {f}('')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'da39a3ee5e6b4b0d3255bfef95601890afd80709')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(''))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'da39a3ee5e6b4b0d3255bfef95601890afd80709')

        f = 'sha2'
        len = 224
        
        tdSql.query(f"SELECT {f}('', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'd14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'd14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f')

        f = 'sha2'
        len = 256
        
        tdSql.query(f"SELECT {f}('', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')

        f = 'sha2'
        len = 384
        
        tdSql.query(f"SELECT {f}('', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b')

        f = 'sha2'
        len = 512
        
        tdSql.query(f"SELECT {f}('', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e')
        
        tdLog.info(f"====> base64")

        f = 'to_base64'
        
        tdSql.query(f"SELECT {f}('')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(''))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        f = 'from_base64'
        
        tdSql.query(f"SELECT {f}('')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}(''))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        tdLog.info(f"====> crypto")

        f = 'aes_encrypt'
        fi = 'aes_decrypt'
        
        tdSql.query(f"SELECT {fi}({f}('', 'mykeystring'), 'mykeystring')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('', 'mykeystring'))")
        tdSql.query(f"SELECT {fi}(f1, 'mykeystring') from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        f = 'sm4_encrypt'
        fi = 'sm4_decrypt'
        
        tdSql.query(f"SELECT {fi}({f}('', 'mykeystring'), 'mykeystring')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('', 'mykeystring'))")
        tdSql.query(f"SELECT {fi}(f1, 'mykeystring') from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '')

    def single(self):
        vgroups = 1
        dbName = "db"
        tbName = "tb"

        tdSql.execute(f"create database {dbName} vgroups {vgroups}")

        tdLog.info(f"====> masking")

        f = 'mask_full'
        
        tdSql.query(f"SELECT {f}('a', '***')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '***')

        f = 'mask_partial'
        
        tdSql.query(f"SELECT {f}('a', 1, 2, '*')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '*')

        f = 'mask_none'
        
        tdSql.query(f"SELECT {f}('a')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')

        tdLog.info(f"====> hashing")

        f = 'md5'
        
        tdSql.query(f"SELECT {f}('a')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '0cc175b9c0f1b6a831c399e269772661')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('a'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '0cc175b9c0f1b6a831c399e269772661')

        f = 'sha'
        
        tdSql.query(f"SELECT {f}('a')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '86f7e437faa5a7fce15d1ddcb9eaeaea377667b8')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('a'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '86f7e437faa5a7fce15d1ddcb9eaeaea377667b8')

        f = 'sha1'
        
        tdSql.query(f"SELECT {f}('a')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '86f7e437faa5a7fce15d1ddcb9eaeaea377667b8')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('a'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '86f7e437faa5a7fce15d1ddcb9eaeaea377667b8')

        f = 'sha2'
        len = 224
        
        tdSql.query(f"SELECT {f}('a', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'abd37534c7d9a2efb9465de931cd7055ffdb8879563ae98078d6d6d5')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('a', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'abd37534c7d9a2efb9465de931cd7055ffdb8879563ae98078d6d6d5')

        f = 'sha2'
        len = 256
        
        tdSql.query(f"SELECT {f}('a', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('a', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb')

        f = 'sha2'
        len = 384
        
        tdSql.query(f"SELECT {f}('a', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '54a59b9f22b0b80880d8427e548b7c23abd873486e1f035dce9cd697e85175033caa88e6d57bc35efae0b5afd3145f31')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('a', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '54a59b9f22b0b80880d8427e548b7c23abd873486e1f035dce9cd697e85175033caa88e6d57bc35efae0b5afd3145f31')

        f = 'sha2'
        len = 512
        
        tdSql.query(f"SELECT {f}('a', {len})")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '1f40fc92da241694750979ee6cf582f2d5d7d28e18335de05abc54d0560e0f5302860c652bf08d560252aa5e74210546f369fbbbce8c12cfc7957b2652fe9a75')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('a', {len}))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '1f40fc92da241694750979ee6cf582f2d5d7d28e18335de05abc54d0560e0f5302860c652bf08d560252aa5e74210546f369fbbbce8c12cfc7957b2652fe9a75')
        
        tdLog.info(f"====> base64")

        f = 'to_base64'
        
        tdSql.query(f"SELECT {f}('a')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'YQ==')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('a'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'YQ==')

        f = 'from_base64'
        
        tdSql.query(f"SELECT {f}('a')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('a'))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)

        tdSql.query(f"SELECT {f}('YQ==')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('YQ=='))")
        tdSql.query(f"SELECT f1 from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')

        tdLog.info(f"====> crypto")

        f = 'aes_encrypt'
        fi = 'aes_decrypt'
        
        tdSql.query(f"SELECT {fi}({f}('a', 'mykeystring'), 'mykeystring')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('a', 'mykeystring'))")
        tdSql.query(f"SELECT {fi}(f1, 'mykeystring') from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')

        f = 'sm4_encrypt'
        fi = 'sm4_decrypt'
        
        tdSql.query(f"SELECT {fi}({f}('a', 'mykeystring'), 'mykeystring')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.execute(f"insert into {dbName}.{tbName} values(now, {f}('a', 'mykeystring'))")
        tdSql.query(f"SELECT {fi}(f1, 'mykeystring') from {dbName}.{tbName}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 'a')

    def error(self):
        vgroups = 1
        dbName = "db"
        tbName = "tb"

        tdSql.execute(f"create database {dbName} vgroups {vgroups}")

        tdLog.info(f"====> crypto")

        f = 'aes_encrypt'
        
        tdSql.errors([f"SELECT {f}(null, null)"])

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.errors([f"insert into {dbName}.{tbName} values(now, {f}(NULL, null)"])
    
        tdSql.errors([f"SELECT {f}(null, null, null)"])

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.errors([f"insert into {dbName}.{tbName} values(now, {f}(NULL, null, null)"])
    
        f = 'aes_decrypt'
        
        tdSql.errors([f"SELECT {f}(null, null, null)"])

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.errors([f"insert into {dbName}.{tbName} values(now, {f}(NULL, null, null)"])
    
        f = 'sm4_encrypt'
        
        tdSql.errors([f"SELECT {f}(null, null)"])

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.errors([f"insert into {dbName}.{tbName} values(now, {f}(NULL, null)"])
    
        f = 'sm4_decrypt'
        
        tdSql.errors([f"SELECT {f}(null, null)"])

        tdSql.execute(f"drop table if exists {dbName}.{tbName}")
        tdSql.execute(f"create table {dbName}.{tbName}(ts timestamp, f1 varchar(256))")
        tdSql.errors([f"insert into {dbName}.{tbName} values(now, {f}(NULL, null)"])
