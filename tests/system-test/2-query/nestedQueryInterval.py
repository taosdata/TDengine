import taos
import sys
import time
import socket
import os
import threading

from datetime import timezone, datetime

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *

class TDTestCase:
    hostname = socket.gethostname()

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor())
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file
        
        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf %s/%s.sql" % (self.testcasePath,self.testcaseFilename))


    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def create_tables(self):
        tdSql.execute(f"drop database if exists nested")
        tdSql.execute(f"create database if not exists nested keep 36500  replica 1")
        tdSql.execute(f"use nested")
        tdSql.execute(f"use nested")
        tdSql.execute(f"create stable nested.stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                 q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)                 tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp)")
        tdSql.execute(f"create stable nested.stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                 q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)                 tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp)")
        tdSql.execute(f"create stable nested.stable_null_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                 q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)                 tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp)")
        tdSql.execute(f"create stable nested.stable_null_childtable (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                 q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)                 tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp)")
        tdSql.execute(f"create table nested.stable_1_1 using nested.stable_1 tags('stable_1_1', '1875819221' , '590058475305213213', '27794' , '-9' , 0 , 'binary1.RwUtFTPELUljKAhTomOd' , 'nchar1.jUSliorTipXBPWUFvgmL' , '70702407145428.500000', '3942240905.627770' ,'0')")
        tdSql.execute(f"create table nested.stable_1_2 using nested.stable_1 tags('nested.stable_1_2', '-1129886283' , '8365334906094716813', '-6150' , '57' , 1 , 'binary1.vTmIEcSbytCJAERpWCGd' , 'nchar1.voiUIweoRUDKNWYNjrBV' , '5437.641507', '77141629449.138901' , '1999-09-09 09:09:09.090')")
        tdSql.execute(f"create table nested.stable_1_3 using nested.stable_1 tags('nested.stable_1_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , '2099-09-09 09:09:09.090')")
        tdSql.execute(f"create table nested.stable_1_4 using nested.stable_1 tags('nested.stable_1_4', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')")
        tdSql.execute(f"create table nested.stable_1_5 using nested.stable_1 tags('nested.stable_1_5', '1771737352' , '8519944309455480374', '-8770' , '-7' , 1 , 'binary1.SJUKdVsgafZagrqnYdTh' , 'nchar1.ONFhJIwhhLWJUtLKvKmw' , '31182473.161482', '64.321472' ,'-339837172')")
        tdSql.execute(f"create table nested.stable_1_6 using nested.stable_1 tags('nested.stable_1_6', '987461012' , '-776424343185148156', '-6733' , '-115' , 1 , 'binary1.YQTZHyzKNWJIZrDeOKdP' , 'nchar1.BxnvJVUAFklEphKHWIqP' , '60.387150', '385878886707.695007' ,'-1313802358')")
        tdSql.execute(f"create table nested.stable_2_1 using nested.stable_2 tags('nested.stable_2_1', '1972317427' , '-3820929692060830425', '18206' , '89' , 1 , 'binary2.InbFxJVQLmIseMeYntEH' , 'nchar2.wkGXcuYevFTHgydogliI' , '656538.907187', '11.667827' ,'0')")
        tdSql.execute(f"create table nested.stable_2_2 using nested.stable_2 tags('nested.stable_2_2' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')")
        tdSql.execute(f"create table nested.stable_2_3 using nested.stable_2 tags('nested.stable_2_3', '121025327' , '-5067951734787165505', '-4541' , '39' , 1 , 'binary2.PWqqHvgkGRmSULvNblZM' , 'nchar2.BsSqoSHfdDzTWBIDTxIW' , '722578830492.845947', '-62936658111.557999' ,'1719325653')")
        tdSql.execute(f"create table nested.stable_2_4 using nested.stable_2 tags('nested.stable_2_4', '-1703445602' , '952583276175911157', '-15847' , '-1' , 1 , 'binary2.ZIRXiGPcWsDDYadmVHbC' , 'nchar2.CkMRgnmReMXbRfKiaHos' , '-2.806483', '620882751.119021' ,'1462034599')")
        tdSql.execute(f"create table nested.stable_2_5 using nested.stable_2 tags('nested.stable_2_5', '2022084941' , '-172782721714864542', '-31211' , '-13' , 1 , 'binary2.RgbEIzefrTuDfyRolGgQ' , 'nchar2.ahMsbNznUpydshGChFWR' , '9230.698315', '11569418.757060' ,'1789286591')")
        tdSql.execute(f"create table nested.stable_2_6 using nested.stable_2 tags('nested.stable_2_6', '2139131012' , '-2151050146076640638', '11205' , '98' , 1 , 'binary2.XdcwVjbqUuKEFWdeAOGK' , 'nchar2.adnShgPhNhoFUgVrzrIF' , '4896671.448837', '-663422197.440925' ,'-591925015')")
        tdSql.execute(f"create table nested.stable_null_data_1 using nested.stable_null_data tags('stable_null_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0')")
        tdSql.execute(f"create table nested.regular_table_1                     (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                     q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)")
        tdSql.execute(f"create table nested.regular_table_2                     (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                     q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)")
        tdSql.execute(f"create table nested.regular_table_3                     (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                     q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)")
        tdSql.execute(f"create table nested.regular_table_null                     (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                     q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)")

        return

    def insert_data(self):
        tdLog.debug("start to insert data ............")

        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000000000, -143708439, -6740796029571244151, 15012, -31, -934058894431.915039, -3695933.334631, 0, 'binary.wEvcyPTYhxxKhVFrShTZ', 'nchar.河南省成都市高明邱街y座 294108', 1630000000000)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000000000, -1643794439, -3164709410897301333, -7208, -11, -1306531857188.310059, 632395.211968, 0, 'binary.jrpjGVDqTHJXDbUIxEir', 'nchar.青海省贵阳县牧野邯郸路U座 687168', 1630000000000)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1629999999999, 1048090051, 4753139546176318956, 31490, 65, -8328268324.680830, 74037727.447481, 1, 'binary.QFXcjYxRIgvVcaabbAuo', 'nchar.湖南省畅市璧山石家庄路O座 936681', 1630000000000)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000000000, 1054267597, 332752484638053804, 1388, 112, -125.582297, -76003.954672, 1, 'binary.YNzAxeYGxNpFArjidcOh', 'nchar.江西省雪市六枝特深圳街b座 108494', 1630000000000)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000000001, -115380468, -2294264221485096401, -20382, -90, -3775423439181.850098, -95439449.928655, 1, 'binary.ilLTZcPTGRSwIjACLGlM', 'nchar.台湾省瑜市平山李街D座 331780', 1630000000001)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000000001, -504963560, -8159067702893064842, -5491, -49, 841872.351000, -84748281935263.906250, 1, 'binary.DtKESUYTjuakZGPtBWsn', 'nchar.山东省西宁县房山北镇街k座 852004', 1630000000001)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000000000, 1962198354, 5726511532140217455, 20962, -124, -35534.621562, 655.911379, 0, 'binary.kczEOqXlmYDzQxVrxiri', 'nchar.天津市秀英市锡山成都街c座 934752', 1630000000000)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630015000000, -2044434228, -1228575710134602773, 23424, 71, 6.886844, 71.519941, 0, 'binary.iVmwRqGqAbGImLpoUiKX', 'nchar.山东省兰州市山亭卜路d座 110251', 1630000000001)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630015000000, -1561126129, 4402494144547686327, 19018, -31, 857260.740185, -750.240044, 0, 'binary.qPQkCflOVhWCAnuCCHWY', 'nchar.广东省秀荣市山亭南京路C座 785792', 1630000000001)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630014999999, 1095266727, 2746241631637453300, 7253, 92, -237902303804.821014, 46908.997377, 1, 'binary.NMQGxjbGhMVVbcQEBaEK', 'nchar.台湾省东莞市高坪南京街u座 150902', 1630000000001)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630015000000, 2130829317, 2085871782840505044, 2484, 30, -3.105125, 29879090388.666199, 1, 'binary.yfnegmvETJTxPrIrvxxe', 'nchar.福建省兴安盟县西夏澳门路p座 460995', 1630000000001)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630015000001, -1664825220, -5631789961756669368, -23059, -76, 8441.841532, -6705.596239, 1, 'binary.LZlUlCryyKuhhwGGlSjc', 'nchar.四川省巢湖县长寿西宁街O座 989989', 1630000000002)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630015000001, -1067652799, -5826504339509635297, -390, -15, -66135776826527.101562, -8198121301.465600, 1, 'binary.bppXThGxTXGyhKdEpRgX', 'nchar.四川省桂芳县长寿杭州路M座 546474', 1630000000002)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630015000000, 1596913082, 1646557549281147083, -14492, -72, 33.692574, 72141847615.397003, 0, 'binary.XxVywbRiXEabPDUoMgxT', 'nchar.重庆市凤兰市城北呼和浩特街O座 959006', 1630000000001)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630030000000, 1109067209, 8318147433525574130, 291, 81, 308262.371110, 3017294.523108, 0, 'binary.mJPDrXToNeEldaahHrjD', 'nchar.新疆维吾尔自治区广州市清河沈阳街F座 287894', 1630000000002)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630030000000, -995915290, 3161170011949889743, -3415, -62, -5111655245631.230469, -7257779.688047, 0, 'binary.vzcGfGPzmKOqwWlstVmQ', 'nchar.河北省海燕县怀柔齐齐哈尔路O座 679791', 1630000000002)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630029999999, 1270783858, 3618027349082548235, 25887, 84, -659813588408.829956, -32621042966661.398438, 1, 'binary.GqXavHpdBwhbCDrQnpSF', 'nchar.湖北省永安市金平通辽路o座 143728', 1630000000002)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630030000000, 1739749630, 1610294704102234156, 18492, 117, -24323056593048.300781, 46.116231, 1, 'binary.hzRmZmuYAUsAdQrSEyFm', 'nchar.贵州省杰市南长王街Q座 377635', 1630000000002)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630030000001, -1718628594, -4057665590789803346, -27617, -32, -84372491943.149796, 5698.767553, 1, 'binary.wPOmeglMuNsatgqVOYvh', 'nchar.新疆维吾尔自治区深圳市长寿梁路l座 415101', 1630000000003)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630030000001, -97007549, -1557528468040059015, -6101, -18, -34048.334225, -1657.736116, 1, 'binary.eDYjVVXEPBpiilaVwOrx', 'nchar.贵州省成市六枝特傅街t座 803260', 1630000000003)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630030000000, -2121650398, -1192567467640899873, 27738, 2, -89612476.306992, -5984440.654033, 0, 'binary.BOOyBOTaGGGWppOLcCbQ', 'nchar.天津市峰县朝阳梁街X座 288727', 1630000000002)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630045000000, 1148326681, -6925816541216354608, -31227, 54, 222564.451289, 8431.504529, 0, 'binary.NHZMOcXRwhSPvmIIxdxp', 'nchar.台湾省台北市魏都路街Y座 785205', 1630000000003)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630045000000, 544052556, 66604009301449217, 21345, 113, 765168602.280518, -97989380.717398, 0, 'binary.CwzIKVOzfevwyQikTtty', 'nchar.江苏省郑州县新城骆街E座 897275', 1630000000003)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630044999999, 781814072, 820039380718257377, 16412, 8, 13534073596162.000000, 84710586503.486099, 1, 'binary.LBBuZbsGbCPwrtpgpJUU', 'nchar.西藏自治区西宁县牧野孙路U座 873211', 1630000000003)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630045000000, 2133537352, 8495070517087119217, 650, 4, -99578297.552409, 635328734398.352051, 1, 'binary.NfQiEIznVNaygWdjgrpf', 'nchar.山西省阜新县锡山台北路j座 854425', 1630000000003)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630045000001, -222510400, -1899555459266145413, -1874, -18, -116.302821, -2969179.976125, 1, 'binary.BgnCkJAICiNgxqiaowKU', 'nchar.上海市淑珍市城东上海路v座 534884', 1630000000004)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630045000001, -995678859, -7818673459856301613, -15799, -53, 4590.410976, -289305504318.122009, 1, 'binary.jzxySgQRnfthYtrsClss', 'nchar.香港特别行政区洁市东丽白路X座 942700', 1630000000004)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630045000000, 2039518409, -7951010656620124852, 26555, 97, 317885662.762503, -15671218405798.900391, 0, 'binary.PpJrRRqrlVkrvHDFhGfc', 'nchar.广东省长春县沈河银川街p座 226735', 1630000000003)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630060000000, 723117353, -1590889099228121365, -2895, -118, -9424.662668, 106.531721, 0, 'binary.TublFbdwZUkjDXBnzoCS', 'nchar.内蒙古自治区成都市淄川梁路O座 490394', 1630000000004)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630060000000, -1943729504, 4725330849524291079, 19350, 23, -72996737.774954, 99.647626, 0, 'binary.NaxwyUiYqMDJixXFkrwl', 'nchar.青海省佛山县浔阳辽阳路i座 673884', 1630000000004)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630059999999, 260526220, 1293392929073191769, 195, 84, -889913.874227, -86079294.573690, 1, 'binary.uSifMBsrTaQhLuYRzSNB', 'nchar.河北省台北市梁平石家庄路p座 882177', 1630000000004)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630060000000, 1100804375, 8343686791181684001, 11516, 82, -898520.749607, 8.704132, 1, 'binary.mlUobRUmHCNOOqCgXLAC', 'nchar.上海市潮州县魏都永安街n座 239177', 1630000000004)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630060000001, -1734497021, -6543273238001990224, -9471, -105, 5.215463, -8.406843, 1, 'binary.UnQSdMFNesxovxaQqywB', 'nchar.河北省兰英市高明西安街O座 898407', 1630000000005)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630060000001, -356952133, -5811125050930035045, -10106, -86, -59731.343942, -6.711605, 1, 'binary.BucKdpqIcWfBATWFIUwQ', 'nchar.台湾省梅市华龙葛路f座 841443', 1630000000005)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630060000000, -1448303077, 7581281332610752593, -28597, 65, 42652369.542399, 19027309795.674400, 0, 'binary.kNqlJbVupSKDoutNSkZj', 'nchar.内蒙古自治区梅市龙潭郑街P座 821111', 1630000000004)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630075000000, -1735114764, -910694996111564041, 17973, 73, 210.783307, 78353441914751.406250, 0, 'binary.TCDlFpIyMpXfcghfWlUe', 'nchar.湖南省惠州市浔阳章街y座 769412', 1630000000005)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630075000000, -1569047795, 6154014930112626762, -23107, 71, 7415.321612, -333.790673, 0, 'binary.cQDlPOzuViCfYAjUdgwt', 'nchar.吉林省婷婷市西夏西宁路m座 909021', 1630000000005)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630074999999, 856956546, 6275962241273936020, 9270, 21, -33.848277, 64751237042062.703125, 1, 'binary.ctAKBcRCfQQDgayszuAG', 'nchar.江苏省宁市清河李路g座 724944', 1630000000005)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630075000000, 1448326946, 7972199845302312059, 705, 105, -204.655811, 732152.198035, 1, 'binary.BcZdTPFgPbkxBUcFljtp', 'nchar.宁夏回族自治区峰县沈北新长春街L座 533383', 1630000000005)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630075000001, -2035071163, -7787122277706108419, -7254, -103, 5674.411412, 577.478622, 1, 'binary.evhulVVFzQzmcxSxrhKQ', 'nchar.内蒙古自治区亮市合川长沙路n座 765501', 1630000000006)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630075000001, -306773260, -7374114523451218038, -9388, -82, -62404928835266.203125, 42386299338.397598, 1, 'binary.esbdUcokubRSnOqkJkLB', 'nchar.山东省琴市上街马鞍山路C座 147584', 1630000000006)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630075000000, -400440831, 3437601667225177538, 20276, -39, -16726360314300.500000, -4598873.151255, 0, 'binary.OkLYkfrvFAWiFDpqiaxV', 'nchar.广东省佛山市璧山孟街L座 706570', 1630000000005)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630090000000, 1973623075, -488129617113651385, -13853, 30, 75272169.904987, 27857.549440, 0, 'binary.xkRVpVawmLMQlKCdvZtZ', 'nchar.江西省磊市怀柔太原路K座 851187', 1630000000006)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630090000000, -252137090, -5296831169315465834, -20550, -20, -3.941531, -75700534.472326, 0, 'binary.BpKasTBDYsehOyjPzszU', 'nchar.上海市红市西峰张路E座 415889', 1630000000006)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630089999999, 2024548336, 4139843660031507838, 19827, 13, -2167586.132392, -25417449654.380001, 1, 'binary.HAdhzWKQINYLaYkXNWMA', 'nchar.河南省汕尾市沙市张街w座 551925', 1630000000006)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630090000000, 2107913968, 1289804881586351882, 11892, 64, 5317489029302.500000, -87057162.779656, 1, 'binary.OgWLsotYlsjEVMRdLBuE', 'nchar.安徽省健县西峰太原路Q座 657330', 1630000000006)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630090000001, -1388002413, -8410442498260521376, -32608, -106, 253633520667.924011, 189217.183223, 1, 'binary.uYfWFNKpQafMupBPxsaQ', 'nchar.重庆市宜都县闵行简街i座 191919', 1630000000007)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630090000001, -1299395984, -5371389022375922971, -10699, -3, -77152176732086.500000, 3022372632154.790039, 1, 'binary.suubVqjYFZLbSTtupfJz', 'nchar.上海市桂珍市沈河重庆街m座 297301', 1630000000007)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630090000000, -2027322440, 2494209288710251536, 9722, 36, -5920.224246, -6082294200418.269531, 0, 'binary.AdrnDYFrIaLanobBNtfe', 'nchar.青海省邯郸市新城潮州街n座 431311', 1630000000006)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630105000000, 1693347900, 3148347996562430560, 7984, -125, 851.227689, 36582.261941, 0, 'binary.CRmkBadMfWkcImouLOYC', 'nchar.陕西省澳门市沙湾海口街B座 465135', 1630000000007)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630105000000, -993042639, -3563434487593064918, 25565, 92, 35189008.309030, 636.547745, 0, 'binary.OoULtmPoUYrnyIRmOomq', 'nchar.内蒙古自治区雷县涪城辽阳路M座 278853', 1630000000007)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630104999999, 896614799, 2243073176348662224, 28908, 61, -27224979.660738, -774.713640, 1, 'binary.eLMnpehchKlvWYooNwzT', 'nchar.山西省六盘水县兴山周路E座 606601', 1630000000007)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630105000000, 814428057, 1481917516738425350, 15453, 4, 48966471575006.898438, 963385.769898, 1, 'binary.ihMiqNMlQJJZzmmqQCxd', 'nchar.安徽省南昌县翔安济南路m座 375964', 1630000000007)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630105000001, -449300050, -9120117477577024283, -32402, -33, 10292695658.142700, -34437.176958, 1, 'binary.qKdRIooLiEPovATVrIjv', 'nchar.贵州省淑华县兴山西宁路l座 608705', 1630000000008)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630105000001, -432206150, -2588236186691485163, -23087, -15, 400818.639211, -4.716588, 1, 'binary.eZOuaAtkYBpffobznIIg', 'nchar.湖北省阜新县高港西安街U座 699140', 1630000000008)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630105000000, -1368554069, -7541187960916300774, 14988, -91, 1942225715.555290, -70334.201426, 0, 'binary.HvoKMzXkvOzwerEjbwEu', 'nchar.甘肃省佛山县丰都阜新路h座 248791', 1630000000007)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630120000000, 1041114692, -4272739428405588701, -2465, 88, 21784769476439.500000, -68577664683.573502, 0, 'binary.FplIhrrnZDEqcPpMXcCz', 'nchar.广东省亮市清河嘉禾路Q座 453757', 1630000000008)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630120000000, 176715584, -8079534335031834008, -22423, -105, 2684870082.726260, -8.267891, 0, 'binary.xAOmXPYnrbGdHxHqywad', 'nchar.甘肃省荆门市清浦马鞍山路r座 410741', 1630000000008)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630119999999, 640308875, 3240059255473088756, 31836, 80, -7728815076.892700, 750986226173.490967, 1, 'binary.OzGnekdKxbZxbKgGFJlg', 'nchar.澳门特别行政区台北市白云淮安路B座 375090', 1630000000008)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630120000000, 1092638007, 8935528528767608896, 4337, 23, -372778.775792, 93085.883825, 1, 'binary.vZySFqduHRgMRgGDCHMO', 'nchar.青海省旭县山亭陈路r座 820766', 1630000000008)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630120000001, -1961834598, -4976614618620345763, -15019, -7, -181775367037.890015, -489008.481333, 1, 'binary.FbQxRmecVULCPbyUFBtd', 'nchar.河北省志强县浔阳福州街G座 429013', 1630000000009)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630120000001, -1722034194, -2355585911534022326, -4297, -50, -55.449413, -615331713611.660034, 1, 'binary.JELgidqLYfIKlQGqJxFQ', 'nchar.河南省柳州县花溪田路q座 284391', 1630000000009)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630120000000, -1171799899, 6798705239630104147, 14631, 13, -690512100.797553, 4942216164.922990, 0, 'binary.JZyfPEyhblizmAILYppl', 'nchar.广东省玉英县孝南沈阳街Y座 637681', 1630000000008)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630135000000, -1384036824, 218630290551636514, 30524, -57, -887.357597, 6.720203, 0, 'binary.NpfAGysaedqwHzyjOEhb', 'nchar.澳门特别行政区六盘水市和平长沙街x座 821514', 1630000000009)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630135000000, -1305588265, -8021530421135445279, -32118, -94, -3231868745378.140137, -3384463304.343490, 0, 'binary.yAnQLRdPTIoYFmazSvtN', 'nchar.贵州省宜都县海港童路E座 199160', 1630000000009)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630134999999, 152017229, 5533762214494486905, 29777, 107, -597881444.549760, 60.902207, 1, 'binary.CoERNXcHahySItezBpen', 'nchar.云南省北京县海陵张路A座 568099', 1630000000009)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630135000000, 1236190014, 4653596223295984266, 14748, 21, -5951588996860.620117, 2234250837.617470, 1, 'binary.WwUGpNKIpNfbqTYAZTDT', 'nchar.广东省济南市长寿林街l座 798089', 1630000000009)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630135000001, -873242705, -7271514029553280325, -7516, -61, 66128079.834318, 77571831.128020, 1, 'binary.LLhLPAXTmmXelsILHdQZ', 'nchar.西藏自治区瑜县长寿天津路t座 566335', 1630000000010)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630135000001, -2107043391, -6872223014761334604, -253, -30, -2052472.350812, -816315756.554040, 1, 'binary.wvSnelJAPwZethZNEZqM', 'nchar.上海市兴安盟县徐汇陈街L座 910420', 1630000000010)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630135000000, -283811418, -890001232755261528, -15727, -29, 632302961.122940, 3372672571.351960, 0, 'binary.DNpxbprlJeuvywljvLWV', 'nchar.黑龙江省畅县大东佛山街o座 623470', 1630000000009)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630150000000, 1841425030, 5840114857227443220, 15419, 20, -176685681169.731995, -20366.126795, 0, 'binary.wpRXidgNPOERhEaZlJAM', 'nchar.青海省帅市平山梧州路s座 315192', 1630000000010)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630150000000, 1868258378, -829467537608086421, -15673, 43, -682501443.992492, 564.308967, 0, 'binary.PThYCRdfuhudrwOYOFgy', 'nchar.重庆市欣县丰都唐路B座 999593', 1630000000010)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630149999999, 7371013, 8606554902691526904, 11354, 74, 26482.297818, -6880189394.673160, 1, 'binary.OtbSjvYZOPWbIQOdIrKG', 'nchar.福建省波县璧山嘉禾路X座 190183', 1630000000010)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630150000000, 1574294578, 6360685624760963951, 11117, 71, -272.941943, 29.887578, 1, 'binary.djhaZVhHOWsrnxbiHVTk', 'nchar.西藏自治区南昌县清河拉萨街w座 620924', 1630000000010)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630150000001, -259884599, -3561655319278167435, -1554, -47, -0.233381, 45265486282274.000000, 1, 'binary.ILwvbvFGWLKDelTSCcDm', 'nchar.河北省哈尔滨市海港梧州路m座 889188', 1630000000011)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630150000001, -1515650476, -7802732131723416636, -12941, -64, -809.874644, 6032.826263, 1, 'binary.tAYvdysPqcwaHdGoSNgs', 'nchar.上海市玉兰市高明宁德街v座 296568', 1630000000011)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630150000000, 1110286890, -8373744956439073351, 15041, -53, 4069186274.937290, 2.194559, 0, 'binary.auztDxkjzltBpZNEImEj', 'nchar.贵州省平市璧山乐路a座 747181', 1630000000010)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630165000000, -270068972, 7039763496469502050, -17618, 23, -975020808834.901001, 20433408805144.398438, 0, 'binary.FNcyuyrPVVVfxbzDuGlf', 'nchar.宁夏回族自治区平市白云沈阳街Q座 933408', 1630000000011)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630165000000, 1508766963, -2166535776743943172, -8241, 42, 2.785578, 579582.453552, 0, 'binary.ManjhxWWPYfMDOZgGgDH', 'nchar.宁夏回族自治区石家庄市永川徐路s座 143325', 1630000000011)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630164999999, 1060176497, 5654208159595485509, 24751, 76, -33188910.345760, 3221.783209, 1, 'binary.cZSBdkTCfHzKdaUbRObM', 'nchar.河南省磊市友好方路y座 897409', 1630000000011)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630165000000, 497425977, 354065118101195712, 31551, 8, -615489296.966200, -71357728420.250000, 1, 'binary.yxlWGAHHdFrrqtbkxmms', 'nchar.西藏自治区惠州县安次汕尾街f座 271896', 1630000000011)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630165000001, -1895192712, -4432843295407575817, -20962, -98, -233660222.154913, 56771044.886008, 1, 'binary.CYsQdbzmYnNmadYZeaGH', 'nchar.重庆市阜新县永川惠州街l座 567817', 1630000000012)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630165000001, -716978435, -3103914421806026937, -21749, -50, 700892864.685490, 48303403313.167900, 1, 'binary.BWgPkqjghxVKeRoXrTZu', 'nchar.广西壮族自治区拉萨县萧山海门路k座 599659', 1630000000012)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630165000000, -1947418103, -2239330846377726020, -32039, -98, 56444198375439.101562, 1600824326231.459961, 0, 'binary.MDSyTDtKwgaMZDQPRBnU', 'nchar.宁夏回族自治区广州市蓟州马鞍山街C座 728186', 1630000000011)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630180000000, 418330248, -5409102699471857283, -15785, -3, 6935.516881, 35832047684.500603, 0, 'binary.KevLpYjpMOfugEsBCBlm', 'nchar.广东省宜都市蓟州周街Y座 950736', 1630000000012)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630180000000, 1176493205, 2459630938091775425, -16447, -108, -2248206343057.399902, -6078285.169543, 0, 'binary.hMRUCjOJuPOsbiZnpUAp', 'nchar.台湾省北镇市海港西安街Y座 510871', 1630000000012)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630179999999, 778869650, 849789444462698421, 10597, 31, 7103017.633025, 176806.190116, 1, 'binary.HYiRCyeEqGzFGwoHupCH', 'nchar.上海市杰县安次成都街f座 504387', 1630000000012)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630180000000, 976603072, 3464432716810727627, 31947, 3, -25689733.219437, 8778.999062, 1, 'binary.oIPQmjoWcexSWGXgBAoz', 'nchar.江苏省丽华县闵行广州路s座 381177', 1630000000012)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630180000001, -1070149378, -5440223162019446936, -3467, -31, 39057698787674.796875, -90.367916, 1, 'binary.VZKQmmtwMfYTGzlJOyzv', 'nchar.新疆维吾尔自治区丽丽县高坪乌鲁木齐路P座 898975', 1630000000013)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630180000001, -34074056, -4597641513205145794, -21694, -46, -1647875.388086, 944448733008.215942, 1, 'binary.tJCYGQwLiehXnYdGKEAX', 'nchar.广西壮族自治区巢湖县翔安周街s座 313835', 1630000000013)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630180000000, 1604229550, 1136238301593252013, -13346, 79, 44.613233, -20420040255472.898438, 0, 'binary.dTQiFvIdRuhkakGujxGq', 'nchar.广西壮族自治区冬梅市秀英张路h座 755432', 1630000000012)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630195000000, -1261626278, 8850592003715310781, -3464, -28, -7417877514167.179688, -337072829176.359009, 0, 'binary.DmaGJbQkLmJQaURuaowv', 'nchar.天津市梧州市友好韩路r座 509518', 1630000000013)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630195000000, 779648104, 7349237640085656137, 6427, -18, -8212845456.626070, -382.979362, 0, 'binary.CTvjwpLAPIpvFzGoTwwo', 'nchar.陕西省雷县房山济南街v座 822721', 1630000000013)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630194999999, 2139522991, 329464564820839576, 24696, 37, 29057737.124244, 207289.798705, 1, 'binary.WgIELLWDqNqWvdsblKCB', 'nchar.新疆维吾尔自治区马鞍山县孝南天津路c座 644425', 1630000000013)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630195000000, 1838525411, 7428851921488225564, 29432, 50, -682610786103.390015, -10041.651604, 1, 'binary.KxfGzIGCDurNlCmGKYcc', 'nchar.江苏省建军县淄川唐路A座 669136', 1630000000013)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630195000001, -1450415363, -2829926279480742311, -27438, -88, -9583575812.348600, 273024623.630804, 1, 'binary.THXDnYCLdvuTXRSUsMnH', 'nchar.河北省丽娟县吉区陈街o座 998303', 1630000000014)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630195000001, -886751600, -2534482963926108103, -10707, -39, -617571391893.729980, -67.220665, 1, 'binary.hTLReGUfQIJXEaSINBnV', 'nchar.河南省海口县涪城古路p座 921643', 1630000000014)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630195000000, 1570287998, -344238082251883349, -7319, 81, 90.287960, 88600160.145757, 0, 'binary.dtVTWjArbQtxafOTUJTM', 'nchar.云南省北镇县丰都张路W座 898660', 1630000000013)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630210000000, 665794589, 6004756980835485088, 2092, -123, -627.945609, -7528668680465.099609, 0, 'binary.IKfpbOnsEUqZvDUDXwxE', 'nchar.云南省深圳县高明黄街I座 754805', 1630000000014)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630210000000, 1588394658, -8959712112812621693, 2206, -36, -441691927006.486023, 646726.359195, 0, 'binary.aBvZFciAllhXZWLUFnpr', 'nchar.甘肃省建国县房山李路y座 514187', 1630000000014)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630209999999, 1202083307, 5412703541265854339, 12679, 43, -192710.317268, -669700577.626661, 1, 'binary.jzdrBvoIOBydYtlFDBfZ', 'nchar.吉林省淮安市南湖西安街g座 120691', 1630000000014)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630210000000, 593595969, 819751030048983732, 20912, 107, 9850514448.774269, -5078164092007.000000, 1, 'binary.bJypQCiflteoujptLzMI', 'nchar.湖北省广州县大东华路Q座 390816', 1630000000014)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630210000001, -808169724, -297187932768772011, -24985, -112, -407819568.753060, -79910028504558.000000, 1, 'binary.sUZnozuiMlgMQjgaKLWT', 'nchar.辽宁省太原市南湖刘街o座 982705', 1630000000015)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630210000001, -671630434, -2446424003708709538, -6161, -89, -894616811978.269043, -9662906448.356800, 1, 'binary.xabYsCWCjPMPlKTSVUhg', 'nchar.河北省婷市长寿成都街b座 907850', 1630000000015)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630210000000, 382558478, 5220322206204946702, -11796, -81, 300102736.783901, -1.980706, 0, 'binary.armYvyPdgPaDoNbFvsmU', 'nchar.海南省丹丹市永川戴街g座 757366', 1630000000014)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630225000000, 1556546304, 2632455039606144331, 12588, -78, -9911986.409861, 229879157290.842987, 0, 'binary.GKOAUmTHRsUYNGpJuJXS', 'nchar.西藏自治区浩市龙潭东莞街H座 896444', 1630000000015)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630225000000, -412103084, 7651422716161914550, -8997, -43, -7588824690.400900, 32094429957.228199, 0, 'binary.vHMehvIzcpBBVNWMluzW', 'nchar.安徽省重庆县朝阳兴城街o座 531109', 1630000000015)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630224999999, 202190259, 6158110401345061186, 25612, 56, 99429428549280.703125, 4845580.749217, 1, 'binary.QFvJnTjavOlzsXAEZVwH', 'nchar.重庆市彬县沈河通辽街r座 998791', 1630000000015)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630225000000, 1577801807, 7099278354328143832, 21437, 127, 13218371734602.199219, 161023981083.549988, 1, 'binary.wcmkWTSCZNJlHhnahEQx', 'nchar.广东省彬县永川顾路M座 592892', 1630000000015)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630225000001, -409805763, -6445166879074268180, -22964, -55, -228981.319085, 7949825.592390, 1, 'binary.sbSiqKmjPKvnXbSeKwXG', 'nchar.湖南省沈阳市安次李街u座 919293', 1630000000016)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630225000001, -1355452771, -6578771374830832013, -11963, -31, 7.979511, -2.355683, 1, 'binary.xkUAxXHQaMRjzWXGozUi', 'nchar.黑龙江省莹县龙潭刘街C座 459661', 1630000000016)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630225000000, 651625961, 351888809599313432, 19819, 10, 7236673390283.780273, -9467952650.400270, 0, 'binary.JHSbZaIRcqbSjVqzeLeL', 'nchar.河北省合肥市清河曹街a座 227339', 1630000000015)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630240000000, -635394652, -8268124534001451281, -4907, 93, -64765076507.232002, 8584607.867581, 0, 'binary.UPKoftKIqXYBGkgijulk', 'nchar.天津市永安县怀柔合山街K座 967698', 1630000000016)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630240000000, -213770053, 5635964652235949546, 25274, -124, -5.607006, 535.380902, 0, 'binary.wDdznQCwUKDSARatEpet', 'nchar.四川省北京市翔安佛山街I座 729861', 1630000000016)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630239999999, 238657263, 8869424583833435507, 27884, 75, 7565809000.422980, -416084252676.710022, 1, 'binary.OgrrbsocdxzSAsMKmGFW', 'nchar.辽宁省凤英市魏都李路J座 525191', 1630000000016)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630240000000, 1210124155, 7042734097723621808, 10874, 73, 86.768567, 6463014968.922540, 1, 'binary.oVXdkvYFywXquaOlhtXC', 'nchar.山西省兰州县花溪齐齐哈尔街s座 800225', 1630000000016)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630240000001, -1447960279, -635780358358789864, -5330, -51, -91432.263672, 2275481108331.560059, 1, 'binary.gMIOOqmPvHzbiBiPEVsv', 'nchar.香港特别行政区波市崇文惠州街C座 328563', 1630000000017)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630240000001, -1695106558, -3628563110892589597, -22424, -5, 5.150266, -77282937.851303, 1, 'binary.xJkaFVdbXowwlHYQZaUe', 'nchar.安徽省杭州市西峰邯郸路L座 436694', 1630000000017)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630240000000, 413277553, -2335706071365382890, 32108, -32, 4843.206092, 46356442353609.296875, 0, 'binary.CoOYGsXiwrFxtoGItDBS', 'nchar.广东省太原市崇文石家庄街q座 321264', 1630000000016)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630255000000, 1654794184, -8374474014569215734, -31917, -15, -938770678.951200, -6731352698.717200, 0, 'binary.QHlEvczXvDhrjeXKechh', 'nchar.广西壮族自治区杭州市城北合肥街Q座 560934', 1630000000017)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630255000000, 1566108371, -3808480661071967050, -30863, 42, -89576902.121811, 78224.319157, 0, 'binary.coyrtmcCFEMDoUzvhCay', 'nchar.江西省芳市涪城银川街b座 311496', 1630000000017)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630254999999, 954795210, 5603814446969530884, 969, 18, -5490774.644756, 178.908991, 1, 'binary.VOdZRucmuTuXzTGKuWfA', 'nchar.四川省宁县城东李路i座 842257', 1630000000017)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630255000000, 1152370442, 5925336630497472314, 12541, 125, -719570559.985753, -8.257668, 1, 'binary.huKmkwvJTPCAFDoXOBlW', 'nchar.安徽省柳州县安次昆明路i座 788628', 1630000000017)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630255000001, -185956520, -4103946855252239353, -25507, -108, -7.426104, 4211603.554600, 1, 'binary.LsRcBtiQqicggCLXZoWe', 'nchar.福建省玉华县滨城郭路B座 383017', 1630000000018)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630255000001, -1921474054, -6233300582184415910, -708, -43, -311807806.567927, -658203860771.300049, 1, 'binary.fZeaeMgVywkMCcqxATSP', 'nchar.陕西省杨市沙市吕路y座 145823', 1630000000018)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630255000000, 1570551972, 3518470870097214499, 31701, 67, -937555.256387, 928225.166342, 0, 'binary.JHXTHKFgRbIHElDZDhBC', 'nchar.云南省娜县合川钟街I座 842962', 1630000000017)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630270000000, 41007230, -53608277352743061, 27336, 106, -147300.927027, -84382353.649139, 0, 'binary.wuOctyBeTsCZynkMWYzu', 'nchar.辽宁省惠州市牧野刘路u座 359412', 1630000000018)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630270000000, -781808333, 579199728109057365, -6090, 83, 77577783.578131, -3329626275.966800, 0, 'binary.jVblcXbShCimKsOQPpIm', 'nchar.江西省太原市兴山辛集街G座 996182', 1630000000018)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630269999999, 1365142146, 4845996538283657204, 32039, 114, 75890.665555, 5702245.657774, 1, 'binary.EnWxuBUlakKcHrXrPBOj', 'nchar.重庆市荣县锡山蒋街H座 306410', 1630000000018)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630270000000, 333444667, 7150058468897111437, 14463, 56, 8105784073132.780273, 2424186854.253130, 1, 'binary.hWWfvRLXRhrTBXsFKBhN', 'nchar.海南省广州县长寿贵阳路i座 445153', 1630000000018)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630270000001, -210822579, -5173305434078560312, -32628, -58, -605574731.487583, 8.466106, 1, 'binary.ZKvMAWaDuBEInoXJgQSL', 'nchar.湖北省玲县清城杨街y座 553175', 1630000000019)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630270000001, -99120821, -6443231186326613201, -4035, -59, 967868262277.944946, -3544.231286, 1, 'binary.QBaMOSBKnDsAwBeYphtn', 'nchar.澳门特别行政区春梅县黄浦西安路O座 267015', 1630000000019)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630270000000, 1289868664, -3904737247840537443, -9207, 24, 87052030.454020, -5467838.797992, 0, 'binary.QtwNbVNKonvfueApUYkW', 'nchar.海南省强市花溪太原路L座 680108', 1630000000018)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630285000000, 1387304128, 4228361435380026760, -1238, -38, 819.343099, 3269274838.864710, 0, 'binary.QwtMztxuEtuQdccEPSgc', 'nchar.河北省沈阳县沈北新巢湖街F座 514745', 1630000000019)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630285000000, 1400828058, 7402159199902680827, 4622, -81, 83124160209331.093750, -597482.723308, 0, 'binary.GkCjoKlnQLMdOZQfOaRe', 'nchar.黑龙江省海口市高港北镇路g座 490967', 1630000000019)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630284999999, 952122349, 4226769910009513793, 1618, 46, -82338299659233.500000, -49022614631460.601562, 1, 'binary.lcXghoPWtfUTfSAAJDPS', 'nchar.湖北省巢湖市兴山关街E座 864057', 1630000000019)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630285000000, 749718159, 3908265828578345378, 4939, 2, -3187143413.393930, -63.396907, 1, 'binary.JgknllAiyOFQYSPvLBrT', 'nchar.广西壮族自治区齐齐哈尔市西峰冉街r座 460995', 1630000000019)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630285000001, -1152099753, -8078485240322452190, -23406, -63, 7434279511617.269531, -297650111.818850, 1, 'binary.vtvGwZIEcxVUBWDFVniL', 'nchar.香港特别行政区雷县六枝特卫街O座 501711', 1630000000020)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630285000001, -2025743219, -8360647266536686674, -31408, -127, 3198209717985.200195, -5167244033576.759766, 1, 'binary.EeDihkVVSciCssqEbEgY', 'nchar.湖北省波市高港王街x座 618754', 1630000000020)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630285000000, -651143475, 6089391424113410155, -28724, 72, -6172591503482.719727, 874675419692.713013, 0, 'binary.wAmJChouNetSkLgButdA', 'nchar.内蒙古自治区淑珍县南溪昆明街X座 457011', 1630000000019)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630300000000, -1036308321, 8505935902679868907, -12126, -56, 21041094684126.500000, -31779.452036, 0, 'binary.CHRMHcuiDKIrbquzMWnc', 'nchar.香港特别行政区桂英市徐汇马路y座 894271', 1630000000020)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630300000000, -1691555741, -2294462358761347751, -1794, -1, -4454607780157.769531, -197045759.620307, 0, 'binary.dciaYPKuPwWmZtbTNqAa', 'nchar.宁夏回族自治区沈阳市普陀李街p座 330494', 1630000000020)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630299999999, 1198995002, 3637649461872145723, 16760, 95, -2.986117, 21825715.342398, 1, 'binary.CobYIOXzviHBQmtndNfY', 'nchar.湖南省合肥市高坪天津街w座 641163', 1630000000020)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630300000000, 1641081252, 3497467759234706417, 16909, 61, -156530.187860, -60111517262.459099, 1, 'binary.WegyfVyvBuwzQpvVtxrJ', 'nchar.天津市天津县高明汕尾路L座 748705', 1630000000020)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630300000001, -1498342366, -7248641586651479050, -14472, -49, -8.737031, -916375489.622606, 1, 'binary.qfSLXCSfQexiQHdraTYY', 'nchar.海南省凤兰市普陀王街a座 920512', 1630000000021)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630300000001, -1529997535, -3950829483875051901, -14962, -125, 2566536996.957260, 46289841809.267998, 1, 'binary.xcQvVjJSZRlsYkqJLaqC', 'nchar.黑龙江省济南市永川苏街u座 855053', 1630000000021)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630300000000, 66936834, 6231741803696417438, -26509, 116, -36723525869968.101562, -140577503982.942993, 0, 'binary.JuGZrZpqrsboVygUyAtd', 'nchar.台湾省汕尾县萧山巢湖路n座 479255', 1630000000020)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630315000000, -1768408453, 3912858851624378906, -8457, 115, 63.229176, -5678913267191.299805, 0, 'binary.OaHLnnqkcUEKxOOHCYCJ', 'nchar.甘肃省济南县高明李街w座 328341', 1630000000021)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630315000000, -1676018488, 5929581737688896192, 587, 50, 8974753.896018, 9867720208144.640625, 0, 'binary.kPHIzCFGzzXfjYJPRBPj', 'nchar.广西壮族自治区齐齐哈尔县滨城杜路G座 832544', 1630000000021)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630314999999, 388708472, 3484566086576549522, 24020, 101, -3436412657.609220, 1.526276, 1, 'binary.VWdcyMxbxjRZCcDesjWH', 'nchar.重庆市娜县清浦周路Q座 318835', 1630000000021)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630315000000, 1742645526, 7447737287016433535, 11601, 102, 205650774.133360, -718740206925.855957, 1, 'binary.pLVCvWZQFPGWrURwfLcX', 'nchar.湖北省辽阳县静安李路s座 669158', 1630000000021)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630315000001, -1688761021, -3633609441651034972, -32192, -45, -6589.308664, -96579.525261, 1, 'binary.fsNWygGviERgbHDCTNoU', 'nchar.湖南省合肥市清河马鞍山路g座 155575', 1630000000022)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630315000001, -1972942235, -7595572329950088744, -21058, -64, -652542.999844, -6645809.678817, 1, 'binary.HeHuMgTlvUMAUlpdnygc', 'nchar.西藏自治区郑州县白云东莞路J座 860659', 1630000000022)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630315000000, 34583826, -5677401721211288846, 5759, 4, -922.875171, 41207973.369290, 0, 'binary.FgvACcdWyWjXDQzboRVN', 'nchar.山西省台北市南湖北镇路X座 676913', 1630000000021)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630330000000, 125729034, 5772587033883405659, 31451, 115, 1579351734.492360, 83475.538080, 0, 'binary.YAKBKrKRqlgNweLacnpB', 'nchar.青海省秀华市合川刘街r座 962617', 1630000000022)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630330000000, 1327440015, 5359031926120752338, 13595, -7, -143.262728, -8.120978, 0, 'binary.VJYIDxwcizIsqjyjvTpO', 'nchar.北京市柳县清河淮安路q座 744491', 1630000000022)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630329999999, 1046617386, 1012222098325008150, 22053, 93, 45723435049.348999, -4738289642.121180, 1, 'binary.OmMMfoOWmDphnVXsqVny', 'nchar.澳门特别行政区建华县永川郭路W座 929334', 1630000000022)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630330000000, 1996072991, 2291923905975087108, 25734, 49, -988.744553, 891307.838533, 1, 'binary.RIMwMykwFAdjljWnLgTu', 'nchar.天津市重庆县沙市西宁路T座 441908', 1630000000022)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630330000001, -509016842, -8755338571881630470, -16764, -101, 54206022913.869003, -38597163884.896202, 1, 'binary.miOrdOLIjNjowuqZLMJY', 'nchar.河北省福州县牧野西安街K座 544169', 1630000000023)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630330000001, -1043050284, -4065479627331509328, -30284, -44, -7442000934306.809570, 8404611937.272200, 1, 'binary.FYEOsVKepLPcPqRAMtQf', 'nchar.江苏省凤英市门头沟莫路D座 821625', 1630000000023)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630330000000, 1336266375, -3748199673349739489, -14794, -38, -98329032120.339996, 4354904615.450910, 0, 'binary.byOStgFhklLTIvsltIZw', 'nchar.甘肃省丽娟市安次哈尔滨路m座 552966', 1630000000022)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630345000000, -125236222, -7443235125783230646, -4372, -51, -2592298452.864900, -1806314197.669880, 0, 'binary.UJkpaKsbERScbXdQuhvo', 'nchar.宁夏回族自治区柳州县锡山赵街f座 139786', 1630000000023)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630345000000, -219965012, -4716987119042513689, 24416, -100, -3747315428739.759766, 0.494564, 0, 'binary.NOTNVTroRkOsLgVdkLoJ', 'nchar.重庆市娟县江北哈尔滨街w座 338249', 1630000000023)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630344999999, 1470657859, 2320764016731890530, 27688, 116, 12160748.177215, 83545.714436, 1, 'binary.QLjYhzfGDKcPmHFQjBub', 'nchar.安徽省婷市白云惠州街i座 922795', 1630000000023)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630345000000, 1908975565, 3788319142030902566, 28513, 73, -810951288254.207031, 704115304293.949951, 1, 'binary.WHaxQQMLidfdINYOwQiY', 'nchar.安徽省淑兰县萧山重庆街W座 385558', 1630000000023)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630345000001, -720454315, -3301539443338809381, -23269, -40, -354.491111, 9.482702, 1, 'binary.GaEJJOwaHtRTijYNSKrt', 'nchar.香港特别行政区玲县龙潭南宁街R座 455543', 1630000000024)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630345000001, -1952988609, -8224270172957627373, -12805, -23, -39.405009, 9889.998342, 1, 'binary.nnoIletebLWbHwpqrpjQ', 'nchar.陕西省深圳市长寿兴城路L座 522359', 1630000000024)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630345000000, -523958452, 2997994281167364719, 22138, 13, 8.626707, -2801928136.443440, 0, 'binary.yqSdjVqJlmWCSBRuPeAS', 'nchar.甘肃省惠州县和平田路N座 869008', 1630000000023)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630360000000, -677274672, -7849607379954530156, -24205, 84, -971756295956.161011, -13922706678085.400391, 0, 'binary.jwdoZKFVRIbdNbxvzglZ', 'nchar.北京市帅县金平沈路L座 955700', 1630000000024)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630360000000, 1246868406, 8440235059780981684, 17929, -72, -179883228781.260010, 74775.726422, 0, 'binary.AdgnDiCLdBHvjVsUEUlA', 'nchar.福建省张家港市怀柔唐街k座 206637', 1630000000024)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630359999999, 1874112564, 2471561821001658734, 21353, 86, 60186670.967332, 977025.978653, 1, 'binary.tNjvCgdWlmcOHVbLJRMN', 'nchar.福建省太原市闵行郑路j座 374763', 1630000000024)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630360000000, 1286025365, 9197590639676784477, 7479, 117, -78306895.453520, 16133.105382, 1, 'binary.oyzmHpPVRNdmGjSWoOye', 'nchar.安徽省佳县清河高路t座 131773', 1630000000024)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630360000001, -805620343, -9199526400762315204, -32703, -90, 255348664.922541, 1.265149, 1, 'binary.rMWNLTrygVpKqbvfAPEQ', 'nchar.西藏自治区大冶县沈河马路x座 692129', 1630000000025)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630360000001, -1016979209, -233992014932124431, -3372, -83, 1155.736906, 74.581949, 1, 'binary.wfBaavnTeXgzocUDpJkd', 'nchar.青海省兰英县南溪梁街d座 749974', 1630000000025)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630360000000, 1965870107, -2918841171553952995, -22593, -23, 8305333945954.679688, -3905.740237, 0, 'binary.WijQcImFTfQfgOAyFVuR', 'nchar.贵州省兰英市沙湾广州街R座 380656', 1630000000024)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630375000000, 1859005003, 1104214274917305717, 10798, 94, 850723.403288, -2.625752, 0, 'binary.uDiGDbJBCeWVuQCYGwQU', 'nchar.江西省海燕县西夏天津街h座 925745', 1630000000025)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630375000000, 249965459, 4153812250107690762, 8690, -85, -2.375649, -7556996798575.759766, 0, 'binary.OsRPmbVqrwwmstYOvsFp', 'nchar.宁夏回族自治区丹丹市淄川梧州路M座 486374', 1630000000025)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630374999999, 1624260248, 7015806862250068758, 4350, 97, 5194046419.133550, -8059822602.437020, 1, 'binary.OMehRclQHcJBTynDoScY', 'nchar.江苏省兴安盟县长寿拉萨街L座 799456', 1630000000025)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630375000000, 396319016, 1703630598675774845, 15206, 66, 82212219.419588, -694808505327.339966, 1, 'binary.ThuTfsNgNgkwqdluGnzy', 'nchar.新疆维吾尔自治区重庆市东丽天津街U座 711659', 1630000000025)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630375000001, -321755377, -7381963169677430909, -17059, -104, -50669392403.276001, 24555.641531, 1, 'binary.pYwaFBbMgRmreHVLLqHM', 'nchar.吉林省关岭县西峰王街A座 128393', 1630000000026)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630375000001, -3112247, -2326747004845963522, -32753, -15, -6.479060, -4.885868, 1, 'binary.nyUKFiEfATIOqiQYTGYd', 'nchar.北京市深圳市海陵邓街w座 923149', 1630000000026)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630375000000, 1848873864, -1425149577411932526, -15441, -97, 53983878.838902, 7.604429, 0, 'binary.AebpWwYtnpvpbQojcptM', 'nchar.福建省勇县大东董街D座 925934', 1630000000025)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630390000000, 620402706, -2092872879669844684, 22721, -99, -3.578269, 9.904233, 0, 'binary.kcxyJbYKMUthZCemDZwN', 'nchar.天津市淑英县淄川陶路n座 983382', 1630000000026)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630390000000, 1424324822, -5344921050768394808, -9933, -104, -579352179419.427002, -8535712823374.900391, 0, 'binary.iQVkXSwNBowzrsOuetLF', 'nchar.辽宁省兰州市蓟州长春路b座 767058', 1630000000026)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630389999999, 1338486910, 5553939443389037108, 7465, 21, 57919539684.215698, -1133897.445424, 1, 'binary.PewhoryOLSZsZOExXrsO', 'nchar.云南省东莞县秀英六盘水街f座 870761', 1630000000026)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630390000000, 1377263295, 2287159705001305018, 23692, 108, 839604115054.706055, 4528078.990663, 1, 'binary.NQMLbBaxRGpfdrcfKtci', 'nchar.广东省龙市魏都呼和浩特街X座 339810', 1630000000026)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630390000001, -1972396465, -2625500637464979513, -26565, -22, -441.336819, 4228263266694.529785, 1, 'binary.HCciTUVnuKaUQHOTLmoy', 'nchar.福建省潜江县长寿张街i座 756207', 1630000000027)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630390000001, -1919101583, -1025584328134825800, -12869, -73, 82107948133.142197, 8170338838.340360, 1, 'binary.SdhXWDcuORDtQtHNqinV', 'nchar.山东省淑珍县龙潭王街u座 917673', 1630000000027)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630390000000, -342515504, -850995699758580491, -27592, -104, 9983310041114.599609, -3552213.522875, 0, 'binary.mrZLEcTnUkwltOKwWmMG', 'nchar.陕西省六安县东城汪路a座 741921', 1630000000026)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630405000000, -697189031, -5174916001528455052, -18768, 31, 889278993.213705, -4519776483.921300, 0, 'binary.mjvvuLgBZRyYZvSnmrpW', 'nchar.广东省北京县淄川哈尔滨街k座 473589', 1630000000027)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630405000000, 1399897758, -5070718027080576434, -28441, 81, -6464798967823.299805, 64.959990, 0, 'binary.auzkgdvXEbwhZrzOnCgY', 'nchar.四川省岩市西峰兴安盟街L座 261109', 1630000000027)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630404999999, 586219277, 5261527665322286803, 15053, 17, -7473982.479707, 53758410580.825996, 1, 'binary.ejuJVeKrOYTBIOGtnvwN', 'nchar.黑龙江省英县璧山袁街c座 834323', 1630000000027)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630405000000, 2069647643, 8085374297895048889, 25867, 38, -9114097802.719160, -511.311795, 1, 'binary.hyODFAlyNDYzGBrgfzQx', 'nchar.河北省荆门市高明蒋路p座 794155', 1630000000027)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630405000001, -23773554, -991944997406075040, -17827, -70, -817975598.936861, -59647871.118851, 1, 'binary.HMzLCwXDnCBrSjyvRiij', 'nchar.四川省梅县牧野乌鲁木齐街u座 768713', 1630000000028)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630405000001, -777805682, -6317678604532276560, -15689, -38, -900453816.555716, 384.378368, 1, 'binary.QygspByIGreKmSuckTRN', 'nchar.新疆维吾尔自治区莉县永川巢湖街H座 128070', 1630000000028)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630405000000, 254749120, 1622387876144097468, 8950, 5, -81.624948, -24.366419, 0, 'binary.prUIqqWJFRMgzMaUTmax', 'nchar.新疆维吾尔自治区惠州县长寿南京路y座 661755', 1630000000027)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630420000000, -1223538957, 2286130958857879860, 13823, 69, 40014883538.375099, -65188739203798.500000, 0, 'binary.YhGtRmitXcCooBPlNxlh', 'nchar.北京市西安县兴山罗街c座 992434', 1630000000028)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630420000000, 1037952556, -1750883376714996603, 24124, 25, -182.347721, 547.995492, 0, 'binary.JYQaxIMJeszunPLADRsN', 'nchar.青海省玉珍县大东黄街N座 831217', 1630000000028)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630419999999, 403592186, 7560013542016462366, 168, 55, 9517875.498748, 352.106624, 1, 'binary.KbgXOhPmkagxFsFbQakf', 'nchar.澳门特别行政区郑州县沙市长沙路s座 614203', 1630000000028)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630420000000, 1368705327, 8252423483843671206, 16131, 92, -5515498174334.660156, -3.888975, 1, 'binary.MIPrismzSjiOFMNueQYp', 'nchar.福建省昆明县高港巢湖街t座 950444', 1630000000028)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630420000001, -1017841441, -1591486103588407573, -17251, -3, -2188221503680.879883, -926521.314582, 1, 'binary.BONmREpjVojUyNnkKVDr', 'nchar.辽宁省北京市浔阳潮州路g座 945777', 1630000000029)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630420000001, -1833376251, -9184585089936473286, -3917, -99, -92896209472.994095, -1.820004, 1, 'binary.PVpVjMOTKQwOyrnumWdy', 'nchar.北京市淮安市孝南潘街L座 593813', 1630000000029)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630420000000, 1948244273, -196734016981898350, -12592, 74, 572284.494489, 2864954.350333, 0, 'binary.hfQUVCcsvQaXPEtLGHpD', 'nchar.甘肃省想市翔安通辽街N座 611877', 1630000000028)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630435000000, -838875167, -1361899531005372465, 29323, 99, -1007837437.160790, 3992.134409, 0, 'binary.AXEKjEPNWZzTiqwFpkrg', 'nchar.宁夏回族自治区郑州市徐汇沈阳街X座 928833', 1630000000029)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630435000000, 1388029674, -546879967630233611, 2763, -36, 672919.301731, -18885480.555128, 0, 'binary.wxQQJhYFoSSMpDGXpiTJ', 'nchar.河北省波县沙市喻路d座 714578', 1630000000029)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630434999999, 2123582595, 7410514965760337418, 2706, 2, 62014125.916508, 2008325132.990800, 1, 'binary.JqrmmSVJyyJSphzDfdfF', 'nchar.浙江省浩市怀柔郑路c座 136184', 1630000000029)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630435000000, 1201222250, 7502546531668208416, 9700, 30, 71831491844532.593750, -589.390815, 1, 'binary.QjDXDUKqRtDbRoKaLzPh', 'nchar.重庆市玉梅市高坪大冶街Y座 589155', 1630000000029)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630435000001, -541998051, -1627876155987640389, -18145, -31, -566248120966.409058, 4.742559, 1, 'binary.DAHIBWzujRgGmXHITFdP', 'nchar.宁夏回族自治区博市高坪嘉禾街w座 519572', 1630000000030)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630435000001, -546978867, -2173360923517013860, -19473, -44, 59.410448, 5922.830565, 1, 'binary.RSrVhjmSLyoKdKZvXTIz', 'nchar.福建省海口县金平东莞街o座 455086', 1630000000030)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630435000000, -439057091, 8924713488149522354, 7802, 47, 48376534665906.500000, 45445.194835, 0, 'binary.ZDMaZnUxfgdroXGRliPY', 'nchar.吉林省建市沈河范路n座 318872', 1630000000029)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630450000000, -765134201, 347278210407306547, -999, 101, -9542079.208161, 81311055.831151, 0, 'binary.uytkzHZmorAgiDiISVoG', 'nchar.台湾省潜江县沈河罗街d座 335953', 1630000000030)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630450000000, -883583883, 8388741643434251423, -15135, -94, -68313.913298, -35.800171, 0, 'binary.tzspsEtclAyEBtTUGUVb', 'nchar.安徽省辛集县花溪周路x座 919405', 1630000000030)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630449999999, 863783720, 5196197078377636765, 15104, 13, 51450224872520.898438, 725329.992278, 1, 'binary.iZrgQLIwNNvEhiErAwkt', 'nchar.海南省宇县静安朱街Q座 212815', 1630000000030)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630450000000, 1599585284, 3659927052268251378, 27540, 113, 426035.852478, -9384.374028, 1, 'binary.yjEiWqUVlFokNWyaFBDd', 'nchar.贵州省海门县上街周路L座 539708', 1630000000030)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630450000001, -1507574370, -2553743849779578949, -26885, -73, -54390935620.480400, -8539853135856.200195, 1, 'binary.nVGhmvtMlkPpeaBpAFTM', 'nchar.宁夏回族自治区马鞍山市萧山潘路A座 970545', 1630000000031)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630450000001, -961124591, -9073578603354545587, -23272, -29, 425584796.348519, 190947665.184546, 1, 'binary.kXxbQgCutjoylguhQAdl', 'nchar.天津市旭县房山屈路c座 773300', 1630000000031)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630450000000, -1705974843, -2143380587844176814, -21730, 59, 5199526189.259490, 7705938.503009, 0, 'binary.bxSiEDFvjRcoPXYDTYCh', 'nchar.福建省淮安市和平赵街l座 610790', 1630000000030)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630465000000, -314548498, -5935072584738796094, -27392, -1, -274339.402115, 3511.621351, 0, 'binary.zPUYZbaXGMPJOywgECDZ', 'nchar.福建省兴安盟市上街昆明路j座 373799', 1630000000031)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630465000000, 1966145764, 6102444836691824974, -10115, -19, -5651247.555394, -57863153091.628502, 0, 'binary.ZrFDxoPexxwNZrlMQtvg', 'nchar.西藏自治区凯市长寿邯郸街i座 464310', 1630000000031)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630464999999, 1652311395, 2469620212179292220, 8776, 21, -3689.666358, 7851383246.205440, 1, 'binary.GFGXaNXjaQWBuiuIuflv', 'nchar.台湾省荆门市淄川永安街A座 461064', 1630000000031)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630465000000, 1692550978, 7365080219522096539, 27515, 70, -99952861.539751, 339330736.780068, 1, 'binary.bJznaiQdbEjfsGYBWcoX', 'nchar.新疆维吾尔自治区红梅市上街合肥街V座 387140', 1630000000031)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630465000001, -881848122, -3511404188725458925, -17230, -50, 6717458.476512, -951.668912, 1, 'binary.WKsWUZoBjTlKVGgAJxPx', 'nchar.青海省云县涪城永安街T座 147122', 1630000000032)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630465000001, -1473523142, -4395305224463915000, -3855, -100, 72183.581727, 7932375.897433, 1, 'binary.qNxynPRwuwQrqgyuePBu', 'nchar.安徽省沈阳市双滦张街V座 833089', 1630000000032)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630465000000, -158748589, 6425132610953044127, 18824, 57, -78085392.809785, -521675448.839484, 0, 'binary.TkNybjkEeqjVoGZKWfiU', 'nchar.河南省林县安次陈路l座 523653', 1630000000031)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630480000000, 780205345, -3394022500515803762, -8656, 59, 4353205.758237, 6894995444848.599609, 0, 'binary.dyNGyigdTXCBLJSGKHrJ', 'nchar.天津市秀梅县城北夏街Q座 373284', 1630000000032)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630480000000, 1285010274, 6775476870679767416, -30268, -78, 2376719.820365, -102060.306247, 0, 'binary.gEkadxughuSyhxVlowaj', 'nchar.甘肃省广州县淄川张路u座 694715', 1630000000032)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630479999999, 133942525, 4687491610368814212, 2473, 123, 74.707449, -37825106590154.500000, 1, 'binary.jFkkgXUPSCrZnMUmiDca', 'nchar.江西省海燕市怀柔辽阳街T座 593401', 1630000000032)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630480000000, 1198452303, 2827497746106779583, 15755, 3, -96312992172.283997, -7583.330552, 1, 'binary.OuAJuifTLwDmabcKuPpO', 'nchar.陕西省欢县永川李路U座 124763', 1630000000032)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630480000001, -1161482672, -642941094008198244, -13538, -101, -4415177912288.160156, 57577.407295, 1, 'binary.TMzTATElhmlhyqFvwFKJ', 'nchar.江西省红霞市城东车路h座 867833', 1630000000033)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630480000001, -2145746929, -2828855748044877841, -24180, -68, 3002911.812991, -5.963978, 1, 'binary.IOKXhDrLSveyxAAZcVVV', 'nchar.山东省桂芝市东城长春街f座 739636', 1630000000033)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630480000000, -2128865070, -3361908133638881363, -7698, -124, -2810320306.295400, 42424.469553, 0, 'binary.pRTQbYdbdqTUGzTBJjwW', 'nchar.四川省平县和平刘街G座 530429', 1630000000032)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630495000000, -179608812, -2830103688811366966, 17973, -6, -98267364621.484100, -34541396619.394699, 0, 'binary.VMzHtbnrmjokPDZCMXsk', 'nchar.广西壮族自治区永安市魏都海口街Y座 538635', 1630000000033)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630495000000, 17975727, -9147014879010075409, -29070, 11, 6629237.824116, -644456283208.812988, 0, 'binary.qxIcDfHdizkemidhxBOX', 'nchar.四川省金凤县魏都朱路Y座 167401', 1630000000033)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630494999999, 1748308690, 5607449654092965535, 32130, 79, 662142.922850, -4208712290.375350, 1, 'binary.bsqrWOhUgZliFzqtMCyZ', 'nchar.北京市兴安盟县门头沟夏街W座 853138', 1630000000033)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630495000000, 292621133, 8029177555041414497, 32719, 52, 9.459390, -85.260795, 1, 'binary.SptfDQZmkxZwzQDPUgly', 'nchar.山西省玉市白云合山街d座 653433', 1630000000033)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630495000001, -45395742, -3417033390164130042, -31766, -93, -22336078027069.101562, 68135086530908.703125, 1, 'binary.SEnorgonMIgtUKkNTOxP', 'nchar.澳门特别行政区永安县合川吴路W座 144142', 1630000000034)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630495000001, -1446286464, -2834239883196880371, -9824, -122, 515950931167.439026, 3301610613616.100098, 1, 'binary.pwZptdrFvhLwFfJFLqQh', 'nchar.浙江省桂荣县龙潭徐路e座 191807', 1630000000034)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630495000000, 147020729, 3276148986164408557, 21995, -8, -669813.120570, -66.191763, 0, 'binary.CDlhPrXllKDbKnpPbQCR', 'nchar.江西省桂香市大兴太原路v座 117465', 1630000000033)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630510000000, -138813503, 6315166587841671464, 1355, 40, 9602.262694, 861862121.594895, 0, 'binary.cvjJVfNMFXXyaKDjkvRu', 'nchar.山西省淮安县静安刘路E座 917860', 1630000000034)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630510000000, -1499926587, -5494769986822553194, -7513, -59, 20.566688, 164110090.400241, 0, 'binary.ncYYeVQupIbKXmfuwZoJ', 'nchar.浙江省昆明市滨城深圳街C座 408704', 1630000000034)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630509999999, 59717021, 5309467270318718403, 20281, 80, -690547726621.959961, -279234186984.130005, 1, 'binary.XBJIPnZODnweOOXmzRkO', 'nchar.重庆市南京市滨城罗街R座 920733', 1630000000034)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630510000000, 965932797, 2618901242071795537, 19035, 60, 88667158.776933, 57.714218, 1, 'binary.vPKtTfrDyvkwPapGKBHC', 'nchar.重庆市阳县高坪张路e座 430297', 1630000000034)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630510000001, -369410792, -8341233851372213560, -21268, -53, -68353.245312, 680.211243, 1, 'binary.HBmqhBQjhNYwAaLJWbpE', 'nchar.新疆维吾尔自治区坤市蓟州曾路Y座 389605', 1630000000035)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630510000001, -290124344, -5005783105138836089, -31038, -99, -4125833987.987010, -61865.560889, 1, 'binary.msJuffjbXNohLzDghECt', 'nchar.吉林省辽阳县淄川廖路y座 592416', 1630000000035)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630510000000, 1554805513, 5103394770308878784, -20138, -118, -158087627.334212, -54553.318555, 0, 'binary.QAiiKHDGWEvrjERhDojf', 'nchar.江苏省柳州县江北潜江街R座 415694', 1630000000034)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630525000000, -319429462, 2783408640725418994, -12995, 109, 8059.171767, 34270137051.186001, 0, 'binary.INFGvqMFdDTmdeFePZnQ', 'nchar.青海省南宁市璧山欧街K座 723228', 1630000000035)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630525000000, -2053704222, -387584738711581594, -28738, 100, 321.108570, 174292.288484, 0, 'binary.JEULkwBGXQMTtcrRCcru', 'nchar.重庆市南昌市东丽大冶路r座 919184', 1630000000035)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630524999999, 1042899672, 525664086008023453, 18210, 36, -992.375585, 67404918237736.601562, 1, 'binary.SyRZdWnAthyOUqpOtOos', 'nchar.内蒙古自治区辛集县华龙武汉街U座 797248', 1630000000035)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630525000000, 210832777, 8293861208931311807, 3500, 77, -4.291887, 8353937865.544660, 1, 'binary.JNasTUjHeTYxMpjwHQwp', 'nchar.黑龙江省济南市孝南广州路u座 818270', 1630000000035)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630525000001, -1784712756, -1900282826185833888, -12635, -10, -147742335517.860992, 1892142041.165020, 1, 'binary.FsBnuFMAwIdTjEjYkjBR', 'nchar.天津市丹丹市双滦马鞍山路L座 954700', 1630000000036)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630525000001, -1963762926, -3093406333417399437, -28946, -93, -361883.201813, -7970832707874.200195, 1, 'binary.AzpaKlaPZcHBJgfiMusA', 'nchar.台湾省桂香市兴山罗街I座 411116', 1630000000036)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630525000000, -1594406401, 2911814076154398648, 1593, -40, 94252.505169, -57.127701, 0, 'binary.UieVjNVQaDaURDdvbVxx', 'nchar.河南省建华市锡山马鞍山街d座 922365', 1630000000035)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630540000000, -298407008, -890073803487483026, 19138, 22, -13371145012.763000, 787260475.918045, 0, 'binary.ligoRoVSAHzFhUvIDwlE', 'nchar.上海市淑兰市锡山周街z座 377582', 1630000000036)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630540000000, 1742761629, -3538927150920177969, -26178, -17, -60236333852265.898438, -8776.586953, 0, 'binary.RxvCthLQrpgveOwWJJfo', 'nchar.辽宁省长沙市淄川石路J座 923025', 1630000000036)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630539999999, 656087591, 7671013800748633383, 21136, 114, -9294.519104, 4829386792163.620117, 1, 'binary.vsIjUPiZNVxiFWHzGqkg', 'nchar.湖南省石家庄县城东杨街b座 396607', 1630000000036)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630540000000, 1908331659, 2529406409884626842, 19478, 70, 33286729894380.898438, -3.973702, 1, 'binary.owpOIRLxxXjudcaMtZsk', 'nchar.重庆市彬县金平牛路B座 110982', 1630000000036)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630540000001, -70790307, -4087110708810857496, -6014, -118, -605686122.372439, 26511865.401597, 1, 'binary.GLAJYWukLZueieYRsidQ', 'nchar.湖北省峰县牧野辽阳路N座 528796', 1630000000037)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630540000001, -375748080, -5402179642725069125, -17546, -56, 333.228050, -8211802092271.910156, 1, 'binary.yhkrusrZjfABOqNjyutJ', 'nchar.河南省济南县长寿刘路V座 435013', 1630000000037)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630540000000, 1483404105, -1062308720694573985, 21132, 127, -3034420855288.169922, -4783009296713.500000, 0, 'binary.pkmpVTKNUbqmEYaWIxNI', 'nchar.吉林省石家庄市六枝特张路z座 262916', 1630000000036)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630555000000, 632563376, -2204241930370454289, 20442, -66, 68778435235.149796, 41338715505.798897, 0, 'binary.RsQnCVXQtzoeXuamUiBl', 'nchar.吉林省洁县永川香港街V座 762000', 1630000000037)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630555000000, -472397946, 1038938837241211981, -29204, 47, 3047173131.130830, 9172947943.508301, 0, 'binary.gSxMxJFnpWRALREpkZUQ', 'nchar.云南省辽阳市六枝特汕尾路m座 376809', 1630000000037)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630554999999, 396390446, 1620566595591926682, 11031, 85, -9305855762.999241, 1578928.160116, 1, 'binary.pgBGZOKYSliCxHFmGsyp', 'nchar.浙江省军市清浦许路C座 950185', 1630000000037)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630555000000, 1237088867, 9126129724607826846, 23721, 97, 147.865610, 55260948679.278999, 1, 'binary.EygLnzSPKfjPvserwjwg', 'nchar.青海省济南县普陀太原路P座 240312', 1630000000037)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630555000001, -1965530504, -871830532885341515, -23233, -76, -3.827964, 9.798035, 1, 'binary.ECLVeNvkmqcTHEoSSxYT', 'nchar.西藏自治区北京市长寿昆明路X座 708472', 1630000000038)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630555000001, -1484513338, -792998805013840059, -31538, -71, -8141323055.196050, -7510534665057.400391, 1, 'binary.jmfqESFcasFwykqiXlqj', 'nchar.贵州省重庆市吉区张家港路m座 856450', 1630000000038)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630555000000, 1069828446, 6908596510608104575, 28783, 110, -9162083944.840309, -102.142752, 0, 'binary.gkjpRGJEiQVghWoJbwzo', 'nchar.安徽省潜江县魏都李街r座 766818', 1630000000037)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630570000000, 446510409, -3656476974551396976, 13256, -104, 8171493376646.099609, -530097963872.411987, 0, 'binary.FEewTpfZgngFmPSTSvxJ', 'nchar.内蒙古自治区建华市锡山哈尔滨路U座 278315', 1630000000038)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630570000000, 1541307361, 3859916508801896099, 21134, 123, -1454095130.880070, -793.944907, 0, 'binary.vUtaMtwXoWgIARQjnHHj', 'nchar.广东省秀华县东丽天津街f座 273761', 1630000000038)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630569999999, 554982384, 4634928837125592948, 23064, 110, -16387837.580834, 67731033.476346, 1, 'binary.zpGkiHSJABZuoZlpNXMp', 'nchar.四川省晶县龙潭太原街d座 373693', 1630000000038)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630570000000, 1650730478, 2509551581387664974, 17823, 0, -810284073.820230, 9519108910338.419922, 1, 'binary.MykLktVEEpwPHBiBMoqk', 'nchar.山西省峰市门头沟香港路j座 412357', 1630000000038)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630570000001, -2032946157, -7551742451999725958, -11549, -110, 548121030.233320, 8537.353780, 1, 'binary.lyLVozekCioDlqntmATw', 'nchar.青海省太原市江北宁德街J座 782858', 1630000000039)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630570000001, -563992071, -3064141540636515026, -15744, -120, -430754463567.528015, -3116194745462.259766, 1, 'binary.sHiKXDptOBQSALxyVOWj', 'nchar.湖南省玉兰市龙潭大冶街I座 537334', 1630000000039)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630570000000, -2129764760, -4193554531789104653, -23767, 118, 93.790612, 786694048079.288940, 0, 'binary.WQewsnemfqUAFHeLJnXF', 'nchar.新疆维吾尔自治区东市滨城王街s座 713240', 1630000000038)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630585000000, 640199308, -5655660832137064380, -11147, 77, 16876033.858953, -7485.569430, 0, 'binary.bRHphFiZkeWtyDjXqNaO', 'nchar.天津市杨县高坪梁街B座 525434', 1630000000039)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630585000000, -1686982965, -8299508680322272166, 3619, -126, -64849.198682, -9337.273192, 0, 'binary.IGNxucSArjxWEALkrQRY', 'nchar.安徽省淑兰县城北齐齐哈尔街P座 697435', 1630000000039)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630584999999, 677947760, 1314864805338112674, 15237, 86, -929824456656.410034, -85.764381, 1, 'binary.vfXYvrHXFnDSAHwCndcp', 'nchar.青海省建华县花溪柳州街T座 100213', 1630000000039)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630585000000, 307125580, 5094829418785488158, 19844, 2, -54041115.787217, 6.233947, 1, 'binary.nAMUcXLvZLCQRHFwNvkT', 'nchar.广东省淑华市上街天津路S座 813901', 1630000000039)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630585000001, -494862894, -4253955294640876951, -19423, -20, -37835740177817.796875, 51554911990.855003, 1, 'binary.zffLjgnXcDlwdKAxADMr', 'nchar.河北省玉珍县西峰刁路C座 452461', 1630000000040)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630585000001, -437854754, -8916960191048231333, -6475, -87, 422259.957474, 81115866.296574, 1, 'binary.NlfhDIBFmzxTiVSybgon', 'nchar.新疆维吾尔自治区石家庄县高港大冶街f座 940073', 1630000000040)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630585000000, 895715893, 6676915082558096536, -2843, 80, 3019829720.850890, 45890083973973.703125, 0, 'binary.YtrhJOiBzFYUzUxxGHcB', 'nchar.河北省桂英县海陵重庆路X座 577152', 1630000000039)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630600000000, 790711145, -5535878799111175644, 24130, 71, -864521126.754360, -5257536.250149, 0, 'binary.uCOCqMGewUkNeUhAbNNf', 'nchar.上海市澳门县大兴海门路k座 319118', 1630000000040)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630600000000, -1515729891, 6489509016449838006, -19281, 68, -75090633218986.296875, 43026646570.672997, 0, 'binary.zhgeUDYkrfqFOzepcyTJ', 'nchar.北京市北京市南长拉萨街H座 770300', 1630000000040)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630599999999, 1558178384, 7355756171678259138, 18244, 101, -2195442510.130770, -623135577.309077, 1, 'binary.bKLXWDPaBjWRXgOeesiD', 'nchar.江苏省红市长寿贵阳路T座 962883', 1630000000040)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630600000000, 1292528970, 2464957048738107533, 21085, 23, -97350629.653519, 97632.613057, 1, 'binary.QhAZodfGPuZBebCyHSme', 'nchar.澳门特别行政区建平县山亭邯郸街x座 185831', 1630000000040)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630600000001, -475557451, -6826045973586477294, -7961, -56, 1705823848847.199951, -268959384930.907990, 1, 'binary.wnZulRregqgognSRRYWI', 'nchar.陕西省桂兰市孝南太原路b座 604090', 1630000000041)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630600000001, -1662397849, -6337230624292122601, -10537, -31, -7225617.287194, 9.181853, 1, 'binary.dIBenysHkntfJuvBkgWK', 'nchar.安徽省海口县闵行六安街S座 630253', 1630000000041)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630600000000, 485553847, 7808570548913367950, -25532, 111, 1267936395.925200, 965.329639, 0, 'binary.uUkJRhVNdIFbCCKdSxNu', 'nchar.云南省六盘水市沈河黄街r座 670992', 1630000000040)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630615000000, 1664157316, -3375551224102004419, 200, 88, -745584775385.793945, -699.239448, 0, 'binary.AyZCLhYiMxZgfYXlVciw', 'nchar.吉林省六盘水市清浦贺路O座 851729', 1630000000041)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630615000000, -1198323418, 1449001209366933334, -29356, 113, 970.596178, -8258648600.433700, 0, 'binary.wnEzpmjBgvHDFbCHrxCx', 'nchar.辽宁省淮安市沈北新拉萨街Q座 858646', 1630000000041)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630614999999, 632223220, 7440877353040054077, 26302, 94, -9662701.768525, -9843725.667981, 1, 'binary.JqnabMfPCrzsTaYczTLh', 'nchar.山西省玉市孝南李路i座 331687', 1630000000041)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630615000000, 1054653217, 644785387636789462, 23531, 71, -8451351.603558, -83219909153.306000, 1, 'binary.IPnIZyUyPMYVVcGjGYqM', 'nchar.香港特别行政区济南市秀英巫路v座 383455', 1630000000041)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630615000001, -951785691, -4025456465792961539, -30955, -57, 74739.936019, 23857138186877.699219, 1, 'binary.sKKfLrkShxlGFGromDFZ', 'nchar.江西省宁德市梁平香港街U座 132395', 1630000000042)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630615000001, -1327031516, -9041664297455549079, -6004, -44, -6224.384964, -39.676621, 1, 'binary.amQNMbMfFfKQljlJEvIS', 'nchar.澳门特别行政区兴城县平山张街J座 904644', 1630000000042)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630615000000, -1454876122, -1664191631337826145, 2979, -43, 52777147.724176, 1229194365813.330078, 0, 'binary.IxkIHjiRPnVlBdHQSdLm', 'nchar.广西壮族自治区帆县山亭张街G座 965406', 1630000000041)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630630000000, -1983928412, -7333101339332877122, 3761, 37, -38344519.775088, -65261.718170, 0, 'binary.CaPFXqhDLSOaNmNnOsLh', 'nchar.上海市马鞍山市城北宁德路c座 402187', 1630000000042)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630630000000, -98362180, 3588425829881816071, -9471, -3, -739.807551, -194649953397.838013, 0, 'binary.LQjcSrpQYoEDKvOOlKKI', 'nchar.青海省南宁市孝南宜都路F座 208181', 1630000000042)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630629999999, 826686189, 1712941616400699580, 9250, 116, -92395295608805.296875, -2.541895, 1, 'binary.shGShhPNnWdZQDAdbiHo', 'nchar.辽宁省六盘水市门头沟黄街t座 892638', 1630000000042)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630630000000, 186750484, 8366250288933508150, 15325, 23, -4695500.207880, -64.107600, 1, 'binary.IkdeyRigedSouvHypNnn', 'nchar.福建省倩县怀柔潮州街g座 858358', 1630000000042)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630630000001, -667795153, -2546515577402776696, -6866, -12, -8902370.867967, 90891053949.643005, 1, 'binary.OaUjyowfAGaGXKoVLWZR', 'nchar.河南省长春市城东郑路L座 791122', 1630000000043)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630630000001, -120091173, -4224513340638046957, -14010, -106, 47098343261485.101562, 8407041730175.440430, 1, 'binary.NcaWdgNTvgnnrmOyMSRv', 'nchar.西藏自治区健市南湖北镇路F座 518953', 1630000000043)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630630000000, 1431440158, 8179550750509486230, -9219, -2, 81300.928795, -56.108837, 0, 'binary.EBrNkPXmiwUfZTalWRtd', 'nchar.广西壮族自治区南宁市锡山罗路W座 812195', 1630000000042)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630645000000, -412013965, -7913679721963250806, 10182, 93, -5654939.111803, -846597080580.864990, 0, 'binary.qEwLmiFwCrLZumFHOYjb', 'nchar.西藏自治区合山县滨城贵阳街j座 422034', 1630000000043)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630645000000, 1503721252, -1036748170680619716, 3291, -18, 4.597429, 62188775030.721703, 0, 'binary.XKwsLUCTTzTuOxhWzjkr', 'nchar.新疆维吾尔自治区红市大东邯郸路r座 535699', 1630000000043)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630644999999, 1420828021, 2546086650460850227, 17595, 113, 43.359177, 8941185489440.390625, 1, 'binary.AYcgokdEsFMUOIcUpxnM', 'nchar.云南省桂英县高明张街s座 776931', 1630000000043)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630645000000, 1943840699, 8246807181436486678, 25310, 76, 35456238889763.703125, 642320879.336482, 1, 'binary.RVHWrlKKNsXNwSQINHba', 'nchar.重庆市华市滨城李路l座 828098', 1630000000043)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630645000001, -2009472018, -7027630570263393098, -21696, -67, -8424796538428.150391, -49416.120873, 1, 'binary.JpOZwpoVKCDjiqsFhbaS', 'nchar.安徽省深圳市高明陈街w座 281329', 1630000000044)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630645000001, -177128590, -4886914070408811519, -24807, -23, 170878.982356, -2.287124, 1, 'binary.BMICVHrfygzpAnylwEKr', 'nchar.河北省萍市锡山韩路y座 412263', 1630000000044)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630645000000, 1079184574, 5846412039067228007, -30088, -25, -4212078244.552720, 679329505569.878052, 0, 'binary.gazNmQBKvUAcQQdcOFfh', 'nchar.福建省柳州市翔安石家庄路k座 779547', 1630000000043)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630660000000, -1579595934, -4731667789895780038, 15158, 77, 526489893298.299011, -396.223420, 0, 'binary.xUUoAUpZeJJsquiJOKsO', 'nchar.吉林省兵县闵行乌鲁木齐路Q座 534261', 1630000000044)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630660000000, -223991084, -2619845069313547475, -14196, 65, 9226.169120, 44944134.392266, 0, 'binary.JGVSgWAUVwADbVUvpSiy', 'nchar.天津市太原县六枝特王街g座 766627', 1630000000044)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630659999999, 395324903, 3937705474539369893, 15533, 65, 0.584074, -967418.449977, 1, 'binary.vWVYYMXIWQAEHBTKKWjP', 'nchar.广西壮族自治区上海市南溪淮安路O座 810462', 1630000000044)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630660000000, 762050937, 673715091051246407, 16484, 115, 97643376.627697, 763.832549, 1, 'binary.YaiVXlPHKJSEOWuRsRwS', 'nchar.江苏省东莞县友好彭路R座 578518', 1630000000044)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630660000001, -9754404, -5144925573679303827, -11209, -98, -7.448873, -448115242.603042, 1, 'binary.QNAVDZqImSFgnHpVdWtS', 'nchar.澳门特别行政区沈阳县和平张街H座 176143', 1630000000045)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630660000001, -1866035632, -2427166402076174209, -27881, -22, 7.396055, -698670.261246, 1, 'binary.uZkazUtEEMgPQgeEXPEe', 'nchar.山西省秀珍县房山西宁街g座 249969', 1630000000045)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630660000000, -1051806775, 7747932212903150302, -3555, 33, 136.466491, -14409582572.354000, 0, 'binary.tuGfceNZePXxugZSWrOK', 'nchar.湖南省海燕市朝阳长春街y座 700043', 1630000000044)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630675000000, -98786523, -8235989830378110865, 1159, 12, 539082.659713, -335587651.682570, 0, 'binary.XQfGXHbnwGQPulAcrAPX', 'nchar.澳门特别行政区郑州市沈河张路S座 530101', 1630000000045)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630675000000, -1987879809, 8328244568807450232, -19551, 26, -829281.870267, -2.960861, 0, 'binary.tsChIuGKmCXkiRucjEmc', 'nchar.澳门特别行政区强市长寿银川街V座 830000', 1630000000045)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630674999999, 2068284680, 6162503383056743038, 27733, 48, -35701228967431.296875, -53.652855, 1, 'binary.wNSWJmuVBjpRVVCpksQC', 'nchar.河北省磊县锡山通辽路I座 205908', 1630000000045)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630675000000, 1151350758, 2480050564933255492, 12388, 102, -738078.108110, 37271792.258503, 1, 'binary.HLSVgACKcmyXKMuAaoCr', 'nchar.云南省龙县丰都乌鲁木齐街j座 584869', 1630000000045)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630675000001, -853667209, -5695092490915541765, -2174, -112, 26.293549, 2.242559, 1, 'binary.OgZwwWPZuNLXAAdBcanX', 'nchar.四川省巢湖市门头沟广州路C座 781739', 1630000000046)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630675000001, -1502448521, -720023346921095762, -11220, -47, -4788.467802, -40669738519.174500, 1, 'binary.omeLfsdjbehlFckYepVj', 'nchar.山东省东县城北辛集路e座 898860', 1630000000046)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630675000000, 1648896854, 7223892135855185984, -5070, -5, 1481003953669.600098, 64887.632295, 0, 'binary.TBMjUXWqSdTfxYFZJeBD', 'nchar.北京市博县金平耿路U座 676471', 1630000000045)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630690000000, -287136285, 1361266513134048206, -3947, 6, -94329.350983, 20808636.344494, 0, 'binary.OdsJNUSWeVvTxQdKykmk', 'nchar.陕西省冬梅市魏都王街J座 907521', 1630000000046)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630690000000, -1467609417, -3843627157269427935, -5413, -42, -614746115.104684, 58.540238, 0, 'binary.NtWJpduxaSUSCqzynFYI', 'nchar.内蒙古自治区兰英县梁平合山街l座 424641', 1630000000046)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630689999999, 155983724, 4361653758945923763, 29353, 81, -498.227438, -5998.851784, 1, 'binary.MRJjXYWHBZvzaAjTzrli', 'nchar.安徽省丹丹市江北海门路R座 285157', 1630000000046)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630690000000, 1258506389, 8207419892749460886, 22177, 98, -8132825907524.299805, -80427.275975, 1, 'binary.NKvCFHNYSDBOyLxAkMxr', 'nchar.台湾省荆门市萧山石路c座 773841', 1630000000046)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630690000001, -1760453704, -2551026868258460946, -4549, -24, 80821.980662, 16.963865, 1, 'binary.pARRcJciJQzdzcCZRqZA', 'nchar.江西省关岭县安次阎路W座 514665', 1630000000047)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630690000001, -1840599952, -7633977303259618496, -30566, -98, -54802925.996342, -179096513.804220, 1, 'binary.sXxbWKCIeaTzSmMmYYYY', 'nchar.新疆维吾尔自治区娟市清浦唐街G座 762308', 1630000000047)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630690000000, -1855444632, 2154100108226554299, -9690, 41, -247749.941627, -334792954703.443970, 0, 'binary.zIvNZWbipczRwxcTNujW', 'nchar.黑龙江省帅市和平张路W座 522431', 1630000000046)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630705000000, 1051015641, -2016886488941734520, 25881, 35, -862249.687896, -7581394980.840040, 0, 'binary.HfnUyPAwYxEHQotoCfFT', 'nchar.湖北省郑州县和平哈尔滨街S座 408314', 1630000000047)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630705000000, -510388953, -8064065802993152150, 32481, -96, 7.338368, -983.214115, 0, 'binary.URxUEcHRhmRFuPRLMOfU', 'nchar.北京市海燕县大东安路T座 198225', 1630000000047)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630704999999, 570191599, 5721926649273388978, 16320, 75, 1203.837639, 108022950538.300003, 1, 'binary.VVxoQGPpWrOYSXzSxWlM', 'nchar.重庆市玉梅县城北北镇街j座 561254', 1630000000047)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630705000000, 516742969, 5254851840608530925, 22978, 57, 98.947992, 2.621934, 1, 'binary.qJtFyhoRvILawwiVeHZU', 'nchar.上海市宁德县萧山李路a座 549521', 1630000000047)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630705000001, -1289549437, -4852255743592819912, -21575, -83, 21395693569392.300781, -20107182860.251999, 1, 'binary.mtOMrfEeDMegNqMauPgK', 'nchar.安徽省飞县滨城范街D座 780146', 1630000000048)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630705000001, -530361614, -2932962238369711981, -20256, -101, 1185.387219, -32573778030.118999, 1, 'binary.VFBPpHGESAAGkeRzIbtK', 'nchar.广西壮族自治区金凤县龙潭沈阳路L座 296744', 1630000000048)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630705000000, 726917225, 502661849153334288, -12421, -29, -481018.973281, 685.805197, 0, 'binary.XJeHLodwUXxQGrQUyIzI', 'nchar.河北省彬县萧山海门路s座 748879', 1630000000047)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630720000000, -1161611813, 5817478319014146944, -7727, 6, -378647512.223777, 60632396886.272797, 0, 'binary.jEENQpaYOWcVHUYKPaPn', 'nchar.山东省辽阳市兴山关岭路J座 414020', 1630000000048)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630720000000, 400786735, 1794586209142898723, 29093, -32, 2336.811166, 4.882355, 0, 'binary.xaDzuonslxHsHxVUcTUr', 'nchar.广东省红市南湖兴城路V座 308999', 1630000000048)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630719999999, 1998845359, 296563873066744098, 25562, 88, 543674818246.236023, 589024.805283, 1, 'binary.IROrsLaBEXcULJZvtXDS', 'nchar.河北省婷市南湖奚路F座 111413', 1630000000048)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630720000000, 1792844644, 7461061401252152973, 21237, 38, -26922171.906852, 2426.989224, 1, 'binary.tkXpypBtMzRGUmlEOHzz', 'nchar.上海市齐齐哈尔县涪城呼和浩特路f座 808674', 1630000000048)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630720000001, -1746905381, -2536142587987184204, -30442, -113, -76482733.355534, -14901138135.959801, 1, 'binary.WatcMYFnezOmZtsAGUDK', 'nchar.香港特别行政区杭州县六枝特李路Q座 925367', 1630000000049)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630720000001, -2131665185, -7526157230511290768, -20114, -12, -44.609385, -754455.135801, 1, 'binary.mJjTgToZxAwHTtdlzHCm', 'nchar.湖南省南京市沙湾尤路s座 734395', 1630000000049)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630720000000, 935471992, -1206109439865175665, -13528, 3, 2923999104624.620117, -91367340672254.593750, 0, 'binary.UeJygdaPzMTyLGaYNqVw', 'nchar.上海市佛山县秀英汪街s座 123655', 1630000000048)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630735000000, -1162065386, -3625158475592858359, 15940, -1, -3.678888, 1905.623857, 0, 'binary.ltmEKwzkvDqQIZWAxJtQ', 'nchar.宁夏回族自治区旭县翔安潜江路e座 531775', 1630000000049)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630735000000, 27870294, -1672838010426046661, -271, -67, -36401764.482018, -8776255.622473, 0, 'binary.YpzKzQOXpmQFzkDvKTPZ', 'nchar.贵州省波县和平深圳街H座 685163', 1630000000049)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630734999999, 2123054685, 1637804037910336988, 6220, 95, 70038469196607.601562, 3457393391629.479980, 1, 'binary.cRdRGuitRXDuGAWVDxAE', 'nchar.宁夏回族自治区南昌县高港永安路k座 452965', 1630000000049)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630735000000, 920693621, 6310859245401227510, 303, 79, -60035982.548857, 1543090.996048, 1, 'binary.uTFJycIGGxiHvreuiUWd', 'nchar.黑龙江省重庆市华龙辛集路k座 324574', 1630000000049)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630735000001, -45388705, -6671698571264178710, -1142, -44, 9.623297, -36288182.831700, 1, 'binary.HMSrCuTIHQiHmuAoGqJK', 'nchar.浙江省六盘水市锡山广州街f座 930027', 1630000000050)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630735000001, -1979960230, -5366544476848969489, -9255, -38, 13559526845534.199219, -52598830.800144, 1, 'binary.lwsPLPMBhvmOtIthgnDA', 'nchar.香港特别行政区璐县友好李街I座 225346', 1630000000050)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630735000000, 953316928, 521541905227278512, -7691, 108, 0.929415, 6372.352970, 0, 'binary.FAMTMFGzUQzWeWuWvvCv', 'nchar.青海省浩县吉区李路l座 492272', 1630000000049)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630750000000, 1289374504, -1538126395055742224, -12336, 94, -41.978570, -543035800648.830017, 0, 'binary.SHhbtXRCqxYOqQcWUDtU', 'nchar.台湾省玉英县东丽邵路W座 597939', 1630000000050)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630750000000, 994205413, -8986580971408422193, 23422, -119, -30609466.792492, 844615733.994923, 0, 'binary.cjCTzFJrdzKdAEluupXy', 'nchar.河北省长沙县长寿任街w座 978621', 1630000000050)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630749999999, 422908755, 5457973965996455589, 7028, 70, 40563882.192469, -9235464112.622881, 1, 'binary.xeqmTUMylYPDFkFJGLQY', 'nchar.澳门特别行政区兵县黄浦六安路y座 904155', 1630000000050)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630750000000, 798218536, 664231232740969491, 19717, 18, -63.765636, -5311500109000.320312, 1, 'binary.hiiCPuMjaHFEOXUsUyFs', 'nchar.北京市马鞍山县友好杭州路E座 623925', 1630000000050)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630750000001, -1902203841, -2257246515637886253, -26212, -11, -4386502.688771, 17.396601, 1, 'binary.dEfjmLzekcjvysMUToXa', 'nchar.黑龙江省雪县东城高路T座 703981', 1630000000051)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630750000001, -519516209, -6438472832772944658, -2516, -5, -137.313109, -21.647080, 1, 'binary.XOwaYPgBCUulNhxMuijU', 'nchar.天津市北京市江北宜都路X座 191896', 1630000000051)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630750000000, -1936399460, 5919953889802638962, -6029, 116, -47.500353, 6967.754987, 0, 'binary.TEvmwdpcePbbpPUuCcOL', 'nchar.湖北省帆县西峰邯郸街l座 145632', 1630000000050)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630765000000, -115300759, -5011960136761658883, -1020, 94, 23611.457680, 3419810648081.700195, 0, 'binary.CowDUEaCtAEdafPCLLwM', 'nchar.江苏省沈阳县涪城梧州路g座 537113', 1630000000051)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630765000000, -1632078764, -2531000958759467663, -1282, -120, -44.781583, -983514.899761, 0, 'binary.YgAVHKRKbzDqkjthriDR', 'nchar.宁夏回族自治区桂芳县怀柔辛路h座 764420', 1630000000051)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630764999999, 1335747951, 332059218903242958, 16913, 38, -258359415.692471, -64088.294941, 1, 'binary.ugUzQSUcPjEXMIDrFobB', 'nchar.澳门特别行政区南宁市大东深圳路a座 447398', 1630000000051)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630765000000, 489030717, 421762966583668765, 31429, 94, -93516881483951.796875, -3894133275.320640, 1, 'binary.pMcnfkpKjgIgIQUaEjjG', 'nchar.广西壮族自治区倩县沙市李街q座 504473', 1630000000051)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630765000001, -1552553924, -4861456845426343956, -27864, -39, -47713477064.426201, -5155612964147.650391, 1, 'binary.gZMscjyRXoGyNsFLHpoR', 'nchar.广东省梧州市龙潭蒲路T座 644808', 1630000000052)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630765000001, -915605364, -5868675460644756043, -650, -56, 7433399325297.160156, -52322640586113.500000, 1, 'binary.oopvfRaqoQJMcVsjkWaW', 'nchar.湖北省建平县海陵孙路S座 240665', 1630000000052)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630765000000, -1546478570, 400491964521467219, 12189, -119, -47542.132966, -2297.923262, 0, 'binary.HeXyWwlnWwUQoKSQqlmu', 'nchar.新疆维吾尔自治区拉萨市蓟州李街B座 664953', 1630000000051)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630780000000, -97528818, 8438123946253432917, -2893, 109, -2306175015456.819824, 13390.816579, 0, 'binary.pqfTTMuvVmSCfxCmFTwS', 'nchar.吉林省莉县新城廖街W座 455043', 1630000000052)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630780000000, 1001095126, -3172038662584640805, 31275, -17, -6999060.910240, 2846571851.119890, 0, 'binary.CLiSpqGShMtzcfyVTbqa', 'nchar.四川省长沙县东丽张路i座 936342', 1630000000052)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630779999999, 1092904458, 4456537336906696912, 16315, 4, -13230586.214814, 29375192427.605099, 1, 'binary.IAiAMcGMqeACFMkfgGju', 'nchar.福建省汕尾县丰都海门路R座 239728', 1630000000052)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630780000000, 1949642007, 5173135551369380298, 13811, 118, -59765334.101800, 645915504896.780029, 1, 'binary.KZORRCLafoZgcOOJmook', 'nchar.四川省重庆县新城王街u座 375952', 1630000000052)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630780000001, -437886009, -8023690985345652194, -14116, -22, -41686906094.233299, -2499063538390.600098, 1, 'binary.jWguWEtBAUgcMKtgXieL', 'nchar.台湾省沈阳市山亭牟路X座 126443', 1630000000053)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630780000001, -1594359349, -8856134295356844294, -29431, -58, -5120.713906, 18405344215702.800781, 1, 'binary.eeRKiigMAbjCTZnPHkGn', 'nchar.北京市宁德市合川朱路d座 269386', 1630000000053)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630780000000, -585788677, 3989310696333618920, 29186, 73, -6608.937490, 11.883322, 0, 'binary.lMUQhCelPiyrOIsQUUai', 'nchar.台湾省俊市蓟州淮安街k座 712450', 1630000000052)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630795000000, -738727537, -4908907813359743923, -26386, -105, -841510.274518, -80431949.274122, 0, 'binary.zWAWphEtCuSMACUmkfvN', 'nchar.天津市沈阳县清城杨街X座 500455', 1630000000053)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630795000000, 393352810, -379075072584781331, 9788, 6, 50170524851777.796875, 44763418392.943497, 0, 'binary.lDVSDncOcANkMxzrtUzK', 'nchar.青海省福州市清城蒋路Y座 567996', 1630000000053)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630794999999, 433850999, 5300519222587016898, 9473, 108, -190097.678270, 2904794070549.299805, 1, 'binary.QBIulMvNBsGMfStAKgCQ', 'nchar.重庆市亮市璧山合肥街U座 535443', 1630000000053)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630795000000, 1651231384, 6954565384064882242, 12216, 115, 81102616.344608, 39273.314632, 1, 'binary.mFwsFjuuCpdYWXqSMkgn', 'nchar.西藏自治区武汉市滨城太原路b座 798865', 1630000000053)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630795000001, -1219816305, -7051889644919104807, -28601, -74, 491555.463996, 72211361404034.796875, 1, 'binary.aSnbvFgLheGIahlWoBEf', 'nchar.贵州省倩县大东张街m座 344033', 1630000000054)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630795000001, -2116828534, -5490698997387226161, -12838, -39, 145344193.926365, -463003297.110519, 1, 'binary.wyWFnvMlYxuviQIhtJTb', 'nchar.贵州省鑫县浔阳贵阳街w座 498999', 1630000000054)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630795000000, -717310584, 7305335468389593497, 26992, -11, -33.942630, -4.966136, 0, 'binary.dKiRVdIDouQiceoUupoT', 'nchar.青海省银川市秀英叶路A座 419144', 1630000000053)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630810000000, 1063250813, -5121723296303557180, 3452, -49, 128.722024, 8116.722860, 0, 'binary.XwbrXvujPArWLXYhQRQK', 'nchar.内蒙古自治区玉兰市城东银川街O座 519239', 1630000000054)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630810000000, -433540276, 3265329093020138852, 19837, -63, -8299471626012.559570, -76400754030.522995, 0, 'binary.qUMRRVLolMSqDjJOJGMj', 'nchar.宁夏回族自治区平市高坪广州路D座 168860', 1630000000054)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630809999999, 1771552222, 3276405219271274913, 6311, 75, -9805500055353.400391, -378716.572329, 1, 'binary.YdwgZmlYfeivmxtaWueZ', 'nchar.四川省军市吉区罗街K座 863617', 1630000000054)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630810000000, 1511546600, 5132492378969879970, 21762, 77, -160772007871.860992, 4897001212199.500000, 1, 'binary.HNELJpdiwwcvGwnYiiQt', 'nchar.青海省六安县沈北新李街G座 938908', 1630000000054)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630810000001, -429657495, -726300776399539025, -5426, -71, -906430389.497117, 26283347323689.101562, 1, 'binary.yGIWxOkDtQCQgbPJhHFi', 'nchar.福建省娟市丰都董街I座 482079', 1630000000055)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630810000001, -1918072152, -3958702025992555431, -20124, -100, 325915062.818627, 82697862.465381, 1, 'binary.zNgoJphhqXQOvIxQHzgJ', 'nchar.浙江省秀珍县滨城陈街j座 333330', 1630000000055)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630810000000, 1166488162, 7007631474167031432, -9261, -6, 6932344629.512100, 531610708.460607, 0, 'binary.OwLQNOOkVKVzVtHPJDqC', 'nchar.黑龙江省娜市朝阳李街y座 366075', 1630000000054)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630825000000, 1579885922, 6474658599299815947, 28280, 52, 105208627.487617, 794627163.462063, 0, 'binary.FEXGjykuWeGEfkKImWDw', 'nchar.四川省淮安县长寿海门路T座 448080', 1630000000055)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630825000000, 734586506, 5695938165634694036, -6919, -31, 16653611667.163200, 5495624345.768730, 0, 'binary.YXkQvxGjNZCQDlipuLTw', 'nchar.上海市宜都市滨城田街s座 403680', 1630000000055)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630824999999, 1560886565, 601626973389436785, 25259, 120, -10180.909667, -11157634.936217, 1, 'binary.OlTzMGFNSfSrcixpefQx', 'nchar.四川省太原县沈河申路d座 235380', 1630000000055)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630825000000, 1316061964, 4308983887809754383, 29120, 81, -318524399.639939, -76861841765211.906250, 1, 'binary.BFHoxbNIFRIxSwrutvbT', 'nchar.澳门特别行政区永安市璧山廖街c座 427250', 1630000000055)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630825000001, -2082927804, -1484742345187780268, -6764, -32, -834228179.920564, 182.911517, 1, 'binary.FTNnvEphxFByhzkfmAhb', 'nchar.甘肃省南昌市东丽南昌街v座 686725', 1630000000056)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630825000001, -231751967, -5929805332846772894, -26985, 0, -0.118923, -67637720.762575, 1, 'binary.TYBLuEQrsrxSAJbNcCwe', 'nchar.河南省莹县房山石路a座 319475', 1630000000056)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630825000000, -1500118527, 7742809515581132121, -8755, 59, -47.623223, 4.266104, 0, 'binary.rbQwEMlJmykGPTkOccLp', 'nchar.湖南省辉县和平香港街s座 101830', 1630000000055)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630840000000, -714663691, 5099404450420041478, 27567, 14, 4.945851, -23542476442552.601562, 0, 'binary.tdLqigCoasQnRffCEaDD', 'nchar.江西省太原县华龙何街H座 269392', 1630000000056)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630840000000, -316339515, 6558297131565034564, 10392, 20, -171972.831509, -13296859.696109, 0, 'binary.sBJTgABqvGumXOfORHBt', 'nchar.吉林省昆明县崇文关岭街K座 822911', 1630000000056)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630839999999, 312047911, 6751577130057851873, 24735, 43, 1278880274.608010, -2.634735, 1, 'binary.yaVlkhtTxxgrtZTsFdxa', 'nchar.云南省台北县魏都张路U座 698041', 1630000000056)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630840000000, 364153602, 8378783503904272354, 9837, 123, 617669.431755, 134.118945, 1, 'binary.RPldGyMrCeJhQIJpDaWz', 'nchar.重庆市丽华市淄川昆明路y座 543556', 1630000000056)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630840000001, -1417705247, -1906089795101484200, -3898, -46, 13831918214786.900391, 4384545313581.750000, 1, 'binary.tHnemGaRugDffcmoyWli', 'nchar.青海省建平市清城赵路z座 958026', 1630000000057)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630840000001, -434542286, -3699533697590169746, -21081, -126, 76.275011, -41585.936197, 1, 'binary.XBOSfXHSompdpAuWKgmz', 'nchar.河北省合肥县滨城重庆街k座 644142', 1630000000057)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630840000000, -1785884903, -7685034849720410750, -21661, -99, -3153441121.646620, 502521483208.367004, 0, 'binary.MvIfmuaYUqcNIjJBVEAH', 'nchar.重庆市张家港县东丽孟路X座 295908', 1630000000056)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630855000000, 1437018826, 7400326252504732743, -29033, 112, -53177718.768333, -23404.680449, 0, 'binary.XZcaYfmvLZtxhrmzHWEJ', 'nchar.青海省柳市静安陈路h座 801659', 1630000000057)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630855000000, -869253997, 8132787450326867533, 22924, -123, -902072.768906, 33.572941, 0, 'binary.FjuniySwJpoodedUTAmT', 'nchar.香港特别行政区六盘水县新城太原街W座 631866', 1630000000057)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630854999999, 1667164436, 4152850799576560541, 29745, 75, -450320.964115, 82908.189997, 1, 'binary.NdSzaJxdoQPGYefWPJyi', 'nchar.四川省春梅县璧山刘街a座 898553', 1630000000057)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630855000000, 1667649794, 1088688853312235292, 9455, 97, -3.427779, -66.503636, 1, 'binary.tvFPXcrLOMTMCJFqcwxT', 'nchar.内蒙古自治区柳县璧山齐齐哈尔街T座 380899', 1630000000057)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630855000001, -2145233720, -3109219509113963979, -18812, -108, -981408379333.303955, -684.345037, 1, 'binary.jghuIXEGcUtKikJpzLOh', 'nchar.四川省银川县崇文张街H座 193298', 1630000000058)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630855000001, -1420593489, -5757610227096963226, -16816, -112, 75645.512896, 4840288.738839, 1, 'binary.CCvnOIBQeUgdTvsedOIX', 'nchar.河北省俊市孝南王路V座 596559', 1630000000058)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630855000000, -1569315493, -7039982472137840249, -11039, 43, 396.176284, -6.336007, 0, 'binary.oNPEvbRQaKHZmlKRACCu', 'nchar.江苏省六盘水市锡山杨路Y座 725607', 1630000000057)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630870000000, 1811347552, -8209360489735809987, -12235, -88, 2775714032.345980, -273997.706213, 0, 'binary.riDaPlhuskGRZsNxzooI', 'nchar.辽宁省潮州市东城徐街R座 431820', 1630000000058)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630870000000, 1930606515, -6940503292627563742, 14608, -109, -544236.299151, 18944279791.470501, 0, 'binary.vbFXWFeRlIaxUNtkKXIp', 'nchar.甘肃省红霞市门头沟长沙街A座 350032', 1630000000058)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630869999999, 488072919, 4847709443565724927, 25414, 31, -442295.525846, 654487670258.910034, 1, 'binary.RLQFgpAcQCfhuZExHLkx', 'nchar.江苏省巢湖县秀英翁路z座 536419', 1630000000058)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630870000000, 334934868, 8084257597250217608, 2587, 69, -46356096021.352997, 17315.165561, 1, 'binary.uCbRqUiElxGzVXQYYkmE', 'nchar.云南省太原县黄浦段街q座 675838', 1630000000058)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630870000001, -1943550230, -3503251484175500724, -28810, -67, 4134905907363.700195, -97.850683, 1, 'binary.ozdeiPLKjSgywqNpCBYN', 'nchar.香港特别行政区西宁县城北王街H座 794624', 1630000000059)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630870000001, -970612310, -8553425754362908809, -2144, -72, -99455587437.982300, -97357630030126.500000, 1, 'binary.EcHZioPjEFGIxUhBosdz', 'nchar.辽宁省玉兰市新城拉萨路W座 721716', 1630000000059)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630870000000, -384741102, -7726003315883440776, -2966, -80, -60458.951496, -4975581.359366, 0, 'binary.TZmxmIbpirBCEoTvqUzY', 'nchar.安徽省呼和浩特县六枝特朱路B座 888783', 1630000000058)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630885000000, -2123493348, -4231564094016455736, -31657, 21, 591198324.991950, 794309276463.779053, 0, 'binary.ZKZunxmtjBCRRCZBHTsL', 'nchar.重庆市六盘水市崇文魏街z座 678361', 1630000000059)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630885000000, 766378371, 492367445257699608, -7708, 99, -3.863619, 5004672.630707, 0, 'binary.AmjCwTkBprSgkFpguYng', 'nchar.山东省健市徐汇兴安盟街z座 942291', 1630000000059)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630884999999, 724025582, 7859514857348468608, 2877, 125, -58160.749327, -8273.730579, 1, 'binary.XZmZYlSRZZddcxEOjfEd', 'nchar.新疆维吾尔自治区潜江县清城梧州路m座 660147', 1630000000059)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630885000000, 1075248136, 8396611731005005497, 20388, 107, -7832228.556238, 17032.924250, 1, 'binary.xKdxeqytYTWmncKRyiEZ', 'nchar.吉林省琴县清浦台北路t座 640801', 1630000000059)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630885000001, -2130070769, -5654655793240443105, -30365, -37, 71673.437636, 624958800878.404053, 1, 'binary.qLvSSOSazLTUKvqpCCBL', 'nchar.河南省桂芝市房山韩街x座 576203', 1630000000060)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630885000001, -1953170165, -7336932030848188049, -12255, -67, -1400233209790.189941, 568697943954.404053, 1, 'binary.pQNaWtAWfSUVMWbrhVmk', 'nchar.天津市晶市清河太原路F座 911684', 1630000000060)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630885000000, 281132201, 5123656302965848910, 6859, 69, 6896942.124392, 7059472733608.709961, 0, 'binary.PHwnvqWGOkMIPxdQCkTT', 'nchar.安徽省婷市东城太原街x座 154074', 1630000000059)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630900000000, 2108150002, -3953623224839666424, 26021, -120, -3832507656102.310059, 41875460523837.203125, 0, 'binary.eEaxRyDoGrrYZQfYhDwC', 'nchar.福建省杨市山亭陈街X座 274347', 1630000000060)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630900000000, -95171868, -3159580668189063241, -4074, 47, 82585.410649, -7462199.923442, 0, 'binary.nAHveWPuvkfTISitbBzq', 'nchar.河北省琳县蓟州吴路H座 196552', 1630000000060)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630899999999, 941556630, 7998670511234147715, 15902, 22, -6585.641376, -0.373709, 1, 'binary.WrcojJGdsiZyAaRxYrdw', 'nchar.江苏省东莞市翔安杭州路l座 318171', 1630000000060)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630900000000, 1042736485, 7949270623018065586, 28676, 76, -61426.518584, -889632484.718458, 1, 'binary.MMPKnkKEPUMKxiatOAXs', 'nchar.北京市杭州县永川梧州街L座 989832', 1630000000060)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630900000001, -343938435, -815062415332308272, -26680, -52, 5.498240, 63.248674, 1, 'binary.HoqLkegpacFGqzFWeVXL', 'nchar.山西省辽阳市城北沈阳路u座 992590', 1630000000061)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630900000001, -1788477052, -5188731326210734405, -1324, -57, -2910628601164.899902, 87786.407935, 1, 'binary.LlQNfNmJFPmYIFPwUEzx', 'nchar.重庆市丽华县东丽成都街e座 389910', 1630000000061)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630900000000, 2018195628, 7377417636728033494, 9220, 11, 4207000288325.890137, -618075239621.894043, 0, 'binary.sazxRvzqSdaThdGddmPa', 'nchar.黑龙江省呼和浩特县南长兴安盟路r座 293112', 1630000000060)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630915000000, -1893993866, -4365820244764275930, 31103, 68, -2066117083545.469971, -5037.126794, 0, 'binary.bMuXKucyDJjBggYIIAvd', 'nchar.上海市玉英县合川许街L座 434453', 1630000000061)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630915000000, 176036972, 6462275287504332531, -12663, 9, 7238554.481668, -24.426760, 0, 'binary.nCtwVdottyyUkynsejOK', 'nchar.天津市秀荣市浔阳施路B座 784962', 1630000000061)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630914999999, 462886464, 2194905473450940337, 12828, 98, 3.368086, 9.615829, 1, 'binary.BRImBBEftaaKZeOGRSvW', 'nchar.江西省广州县普陀潜江街J座 122271', 1630000000061)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630915000000, 1703114933, 1832458494225860699, 29866, 118, -3807017124.642100, -912951646003.170044, 1, 'binary.gPctXUuQQLaDZMWiQWzA', 'nchar.陕西省深圳县城北刘路C座 874212', 1630000000061)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630915000001, -674623669, -6380867918231277409, -1843, -119, 435947.604417, 766787648570.537964, 1, 'binary.XevOOqIPqCmaJUcXaaKz', 'nchar.云南省巢湖市丰都柳州街t座 325613', 1630000000062)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630915000001, -1559227469, -7051851803063121646, -27579, -65, -509948.590885, 2838.905231, 1, 'binary.QfqLcJjHqpLoidJREEnX', 'nchar.广西壮族自治区桂芝县清城宋路Q座 713677', 1630000000062)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630915000000, -1205146480, -6555909736905274411, 10866, 11, -917806.522567, 7477874070.574810, 0, 'binary.zDfAtyjfdffSTqbqeobG', 'nchar.北京市春梅县滨城陈路X座 531714', 1630000000061)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630930000000, -1494759877, 7825831993548386780, 3895, -126, -662731502.286191, 540530.198658, 0, 'binary.GGbtLZVCzfmnUdMHExqv', 'nchar.海南省东莞市上街杜街d座 891312', 1630000000062)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630930000000, -1188314039, -5179463148601079589, -727, -86, 3813.243840, 9214.756895, 0, 'binary.uTiBEULehnYGBTLmKywi', 'nchar.河北省勇市东城石街B座 963105', 1630000000062)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630929999999, 1589240480, 6788343323702456012, 17814, 12, -5156431732058.429688, 47771.262506, 1, 'binary.wJyZOXguJRYIbgIuIRye', 'nchar.浙江省桂花市朝阳杨街E座 783650', 1630000000062)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630930000000, 1494846642, 513923720305876961, 29664, 79, 77561611.652663, -742112715702.303955, 1, 'binary.ymFCwOLqQdnnsFafDwkr', 'nchar.上海市六盘水市孝南上海路i座 565965', 1630000000062)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630930000001, -253589870, -4348911754768762667, -11980, -62, -4485569068.944760, 96365.291525, 1, 'binary.CzIxfDKtViliFCPuYkSq', 'nchar.青海省凯县锡山宁德街m座 619840', 1630000000063)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630930000001, -1105799208, -3800641476990756597, -22387, -123, 63290.349790, 171660453137.690002, 1, 'binary.jQRSOxYnBcNuWfRXmBkT', 'nchar.湖北省丽华县城东西安街w座 178969', 1630000000063)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630930000000, -1072430928, -1022000926125289868, 18740, 19, -2339625154017.399902, -5914609622.300700, 0, 'binary.ZbiUjpLNbsjFkdkPPfbM', 'nchar.湖南省杭州县南溪黄路D座 533554', 1630000000062)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630945000000, 2135168110, 1934009776464408479, 6695, 72, 16367931616261.000000, -584177.637447, 0, 'binary.ICfwWUnCEvXrJWGQNYjv', 'nchar.海南省秀梅市丰都重庆路G座 676020', 1630000000063)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630945000000, -1604447094, -5852815210707209114, 17991, 107, 130598712.764873, 289933642.558557, 0, 'binary.yVowiloVQgZDlfKNlbWh', 'nchar.山西省燕市双滦潜江路o座 423049', 1630000000063)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630944999999, 701182235, 666030532933625291, 203, 77, -1.180313, -2809676597535.319824, 1, 'binary.wFVimJeupABVGEvwMiTv', 'nchar.河北省敏市东丽徐街H座 593999', 1630000000063)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630945000000, 1199058915, 6158980904131584989, 19579, 17, 541875543475.742004, -597076.697923, 1, 'binary.FjLluJSaCVzuHbHvJfzo', 'nchar.甘肃省淑兰市南长马路c座 137684', 1630000000063)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630945000001, -1563988450, -9012098528369421000, -14380, -126, -444977.754816, 72.749039, 1, 'binary.OjWhbhEvRlHpqFEHXmfR', 'nchar.浙江省南宁市普陀周街r座 756238', 1630000000064)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630945000001, -135825310, -1557116121902920061, -27295, -123, -9.714553, -1221.739051, 1, 'binary.PoymphzdepeBanAfXkZU', 'nchar.内蒙古自治区颖市花溪吴路Y座 361290', 1630000000064)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630945000000, 220678360, 6988249753807558085, -2581, 63, 44373988506.265701, -776960795071.900024, 0, 'binary.FceJEjYHrZmPNoGcSJYM', 'nchar.香港特别行政区秀华市金平刘街v座 478775', 1630000000063)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630960000000, 15376875, -6664581571745752730, -13465, 6, 16623190903.482500, 48981341.814827, 0, 'binary.lwtmrfdurUFDSkHMZuOe', 'nchar.辽宁省晨市南湖辛集街k座 508374', 1630000000064)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630960000000, 1241969440, -1628740312418081543, 13806, 9, -321375164064.127014, 309512084.295555, 0, 'binary.nxCrfJncdqIIHvFslwbz', 'nchar.重庆市广州县永川太原街S座 529006', 1630000000064)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630959999999, 1631111972, 732723021793120104, 9398, 75, 81.940353, 358.935299, 1, 'binary.QZSHkvsYmRHnGFnyMije', 'nchar.台湾省兴城市孝南兴城街p座 734723', 1630000000064)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630960000000, 1489317687, 5519190191808186905, 7935, 65, -46699875.436843, -3.678916, 1, 'binary.lMozaeGkbfYcTzrxJcoV', 'nchar.广东省辽阳县龙潭侯街q座 888715', 1630000000064)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630960000001, -1081644320, -7901892167113844563, -4059, -78, -12875427.525138, 777315388163.552979, 1, 'binary.TDrOTkriDBpZtXBadhIx', 'nchar.青海省萍县西夏潜江路R座 701876', 1630000000065)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630960000001, -283328976, -5407024306435227732, -18436, -30, -0.910943, 6223301643044.309570, 1, 'binary.bZikexFXrPKScwiyBTtX', 'nchar.台湾省秀华县西峰关岭街Q座 185151', 1630000000065)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630960000000, 667615766, 7516057978804562053, -6908, 82, 8.929773, -91.562693, 0, 'binary.ugfnkKdKlACxwKMCBEbb', 'nchar.海南省成都市沙市孟路Z座 646247', 1630000000064)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630975000000, 1998856189, -4326051996015773570, 17831, 76, -9.863853, -54574439998977.796875, 0, 'binary.JCxsLYijtQSKzNLYAICB', 'nchar.江西省颖县丰都蔡路q座 399088', 1630000000065)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630975000000, -1961052557, -8491150711564049542, 24318, -42, -54126426844007.601562, -2466916.622248, 0, 'binary.XijTExPHHgzfADOYawHs', 'nchar.海南省西宁市丰都齐齐哈尔路m座 331224', 1630000000065)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630974999999, 1021775776, 722158673624256018, 6062, 102, -94606143062457.906250, 35023478748411.500000, 1, 'binary.AstGPfaVxEefogcWzDzq', 'nchar.重庆市潜江市江北李街Y座 700708', 1630000000065)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630975000000, 1449414466, 6336873094660298228, 12438, 74, -790114.216316, -390.420215, 1, 'binary.secMQTXGKVthFXgMEFYl', 'nchar.香港特别行政区上海县西夏谷街n座 477311', 1630000000065)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630975000001, -1715903601, -471065515874324599, -10712, -65, -64736531762581.101562, -96042855.418683, 1, 'binary.lSSCcLFElDWxKZTdHRLN', 'nchar.天津市北镇县淄川禹街B座 570652', 1630000000066)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630975000001, -176995412, -7722430198424559895, -29062, -58, -5301089280.401760, -31441552360.634201, 1, 'binary.ikoqYkfzTrxBhYnfmKUW', 'nchar.新疆维吾尔自治区秀珍县浔阳王路D座 979771', 1630000000066)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630975000000, -65800677, -4755363672240072131, -11875, 34, 67445667.955342, 40434.343296, 0, 'binary.qDCRKCYQnmiDLFcdBAnT', 'nchar.广西壮族自治区北镇县浔阳宜都路W座 236322', 1630000000065)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630990000000, -1190594011, 8137196870545919699, -28040, -9, -68.827873, 52589.409559, 0, 'binary.lTlvbMfWCVccDOCnIDBK', 'nchar.西藏自治区艳市徐汇六安街X座 527915', 1630000000066)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630990000000, 6478249, -2262813485458902331, 28341, 73, -10648.983192, 21.336997, 0, 'binary.vLfZLtfmMGABLIsYcSuG', 'nchar.贵州省倩市朝阳周路Y座 721501', 1630000000066)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630989999999, 1492685945, 2082419493615737887, 1936, 121, 270330631887.458008, 464724.616085, 1, 'binary.esYXdqdUGVFhLCHHciWA', 'nchar.宁夏回族自治区合肥市永川昆明路Z座 321933', 1630000000066)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630990000000, 746488837, 4063291949108736915, 11202, 12, -294206344.943296, -54829.586060, 1, 'binary.ynUOCSQSnPGHiIFNcUlr', 'nchar.河北省慧市朝阳台北街m座 705236', 1630000000066)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630990000001, -457991373, -2036730114286415967, -2136, -105, -5092278572843.200195, 17587773689.161999, 1, 'binary.OCGshioisqgLTUFLRMSc', 'nchar.上海市南宁市沙湾南昌街q座 991276', 1630000000067)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630990000001, -1242447360, -2110971702178298800, -16302, -30, -4130.477367, 8402.793080, 1, 'binary.jHAuHSxbhQHBBbaHCVSe', 'nchar.云南省石家庄县江北程路B座 226188', 1630000000067)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630990000000, 71825038, 5708278874002188089, 4731, -80, 86.310448, -81380752238174.406250, 0, 'binary.WsCFazjKcLJjwrFHPelC', 'nchar.新疆维吾尔自治区永安县白云巢湖路j座 278171', 1630000000066)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631005000000, -925781701, -5868946429108894256, 5048, 19, 6559385936.497240, -9870442107.342270, 0, 'binary.iLESXKenJaWTIeKHFleM', 'nchar.新疆维吾尔自治区浩县高坪田街l座 923007', 1630000000067)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631005000000, -463506149, 8120086957375329125, -6384, 111, -9995250.448939, -736463.811003, 0, 'binary.AcGtBjWRUbXgHysDRMhs', 'nchar.江西省璐县海港惠州街g座 399144', 1630000000067)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631004999999, 306491871, 6281636598236510189, 24086, 67, -639.213146, 35478635363.208801, 1, 'binary.WPbbRgxYIRecrOFSlRYM', 'nchar.西藏自治区宁德市孝南刘路O座 708041', 1630000000067)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631005000000, 1797589305, 162080808454486608, 29046, 34, -54.791716, -71755480989.907196, 1, 'binary.DdiJOQDpLZuOsXbvtmFm', 'nchar.甘肃省台北县长寿高路t座 429482', 1630000000067)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631005000001, -709883988, -950329647663976821, -6274, -38, 64687083.568152, -133880631485.500000, 1, 'binary.cADQdfbZkXfcNPPOPQwp', 'nchar.宁夏回族自治区静市江北覃路u座 523043', 1630000000068)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631005000001, -1863995155, -8908360222222924661, -32472, -104, 82.537309, -89404622537910.703125, 1, 'binary.PBNnqQRGjNfbmaGVYlKD', 'nchar.新疆维吾尔自治区六安县西峰阜新路d座 990661', 1630000000068)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631005000000, -775088092, -151303921680143861, -23850, -72, -89808431301.207199, -646.674122, 0, 'binary.PGaLWNQGDnVSztoNNsSO', 'nchar.贵州省佳市翔安宜都街Q座 487382', 1630000000067)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631020000000, -487874477, 8081110518718693872, 10742, -8, 59233198.443672, -81768146.768733, 0, 'binary.AfIofVnHdvWMCGyeajlC', 'nchar.广东省利市锡山崔街X座 803273', 1630000000068)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631020000000, 822234976, 8508573074776208709, 17893, -96, -82.790723, -288719.773859, 0, 'binary.DoAGpoAKorgskVVmqIgo', 'nchar.河南省淑珍市淄川西宁街S座 903273', 1630000000068)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631019999999, 1639981656, 5471134581418657990, 22743, 76, 6306.116018, -7013891.566539, 1, 'binary.mVTonpWtYFauqLNntbcK', 'nchar.山东省巢湖县门头沟黄路g座 818470', 1630000000068)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631020000000, 1877786780, 1239434978427674122, 3482, 55, 4308520.332734, -7.194133, 1, 'binary.ATBjqErbvWNgzCwEXuVp', 'nchar.辽宁省凯县锡山广州街A座 668121', 1630000000068)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631020000001, -40134567, -5917934079243952151, -8151, -41, -1.160545, -5389824243.111000, 1, 'binary.hSZKVixlyoxpHpdvwYxY', 'nchar.黑龙江省合山市璧山吕路T座 593619', 1630000000069)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631020000001, -2005527285, -6681721755343029867, -25686, -12, -362992022448.400024, -5460308076525.500000, 1, 'binary.oWDnMrJWMhYgnspOVCyk', 'nchar.河南省明市城北阜新路D座 863966', 1630000000069)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631020000000, 24149104, 3385048008347468396, 25731, -51, 8252.733643, -4262287074.133500, 0, 'binary.pwblolLPhJQylCJAhGzn', 'nchar.浙江省柳州县淄川成都路q座 844741', 1630000000068)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631035000000, 212157003, -1467339935461255928, -7186, -34, 7497729511.586900, -98266.367876, 0, 'binary.jDxeCyLAKzKgEHGHAlif', 'nchar.广西壮族自治区桂英县永川张路K座 770340', 1630000000069)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631035000000, -2004196647, -7368122492859630941, 15455, -76, -57468499221.112396, 3727126030.311050, 0, 'binary.FIJWJSaOYEwWZHqhongU', 'nchar.上海市成都市花溪宜都路B座 441493', 1630000000069)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631034999999, 1412945598, 9027606879125343725, 21349, 38, -73686.799340, 3622986817296.899902, 1, 'binary.EPkNzxzaeCAWQetzzzlv', 'nchar.河南省荆门县沙市梧州街X座 162307', 1630000000069)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631035000000, 2125121614, 5109732906858974031, 26301, 81, -52025949738.545998, -0.369444, 1, 'binary.bjABIQwvmToiYBuNZkgo', 'nchar.青海省欢县龙潭荆门路q座 401300', 1630000000069)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631035000001, -517552216, -7052845906951781652, -9720, -120, -259291.640787, -9.749093, 1, 'binary.AmPftzlwURAILqlHhYtv', 'nchar.浙江省凤兰县南湖太原街U座 609741', 1630000000070)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631035000001, -534996716, -6181873037570436300, -10798, -57, -8617907014714.200195, -583022086.598419, 1, 'binary.EhUOJdpDsZWYyyDDATpW', 'nchar.河北省琳市城东六盘水路z座 798939', 1630000000070)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631035000000, 502477321, -3940198187319461410, -590, -122, 11555.763461, 3.474600, 0, 'binary.iufgzJsvjLKSoGJGBiLS', 'nchar.黑龙江省佛山市大东兰州街G座 694559', 1630000000069)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631050000000, -1001974902, 6450834421987986410, 3297, 112, -8092802.381089, -13006311.799326, 0, 'binary.ELgeHxhmjoFyFgkFXZui', 'nchar.贵州省阜新县高明永安路M座 392539', 1630000000070)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631050000000, -1991708456, -864737276653013043, -1035, 104, -621.411878, 358.192500, 0, 'binary.dzcYxzyOZMYgtByHiDTc', 'nchar.重庆市上海县东丽崔街N座 481064', 1630000000070)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631049999999, 1210716085, 5098727175987952663, 14612, 99, 122287663.454320, 1915439.569884, 1, 'binary.xPKETDcCdbWtoYoUCgre', 'nchar.甘肃省晨县上街龚路E座 215933', 1630000000070)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631050000000, 372703643, 5265014540357675070, 11084, 4, 955.287790, -677466762016.219971, 1, 'binary.TyniuJCZqfeldsiTGmnn', 'nchar.四川省济南市六枝特尚路V座 872192', 1630000000070)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631050000001, -1186882087, -6843278012193945664, -21562, -48, -73364783.992379, -8374993150.154530, 1, 'binary.HVKExGKBzdrjjQbKdgim', 'nchar.四川省通辽县西夏秦路J座 522373', 1630000000071)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631050000001, -1513635853, -4943689304527658905, -26256, -17, -157022486.675383, 47593146446.323402, 1, 'binary.xmsafXeSTguAIwMoWsSm', 'nchar.陕西省哈尔滨市大东辛集路G座 719181', 1630000000071)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631050000000, -2096895302, 2527287882879318478, -11906, 39, -2092909636.574400, 832609111532.883057, 0, 'binary.DpAgGGDNkTqeFPziAoZv', 'nchar.重庆市旭县翔安杨街N座 500333', 1630000000070)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631065000000, -607426580, 480106595535647664, -14319, 71, 485500665.146682, -7157.602006, 0, 'binary.NuToHMvRscGxwMENZqOS', 'nchar.贵州省秀荣县璧山申路Y座 936410', 1630000000071)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631065000000, -1266376029, 7399492335983317341, 21772, -21, 3959426326750.160156, 4299575.449645, 0, 'binary.sdhZOekTsIiwOYFEShct', 'nchar.重庆市大冶县友好长沙街T座 325982', 1630000000071)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631064999999, 1209625354, 8611086034262933876, 7159, 57, 66305514.987262, -249576.528082, 1, 'binary.xDqLioOjrUudmVTRaKmP', 'nchar.广东省海门市魏都北京街t座 626317', 1630000000071)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631065000000, 423864530, 7171258036520933259, 5585, 41, -9923192491285.410156, 9.901365, 1, 'binary.JiryCLzxGYRamjkLmMDH', 'nchar.江西省澳门县新城重庆路b座 208813', 1630000000071)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631065000001, -1049869765, -6885754556853202467, -6460, -118, -883111.347273, 654352667.175980, 1, 'binary.ZcJrqAQpBfTRFJupVZDH', 'nchar.陕西省雪县长寿淮安街o座 362739', 1630000000072)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631065000001, -1239790498, -4444805752431922421, -28574, -70, -70077.276971, -76046.699605, 1, 'binary.mCAmZahnycvOkwVDVpgV', 'nchar.辽宁省上海市合川赵路q座 784249', 1630000000072)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631065000000, -1048465520, -2935190573459597102, 21673, 87, -2585566.109742, -2007.606237, 0, 'binary.HNslYqKBtmSuAbsFzwKj', 'nchar.山西省莉市海陵昆明路t座 518218', 1630000000071)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631080000000, 630458653, 711094046704512569, -30447, 17, 18100.228068, -28061872.208530, 0, 'binary.SrRBINnQSvdBTBKNGHFz', 'nchar.吉林省敏市高明尹街t座 876540', 1630000000072)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631080000000, 1587008473, -497820597464002656, -14935, -92, -5426069.379042, -9834.237204, 0, 'binary.qRermCLWFAWnLBFapDsH', 'nchar.陕西省军县璧山石家庄街l座 337307', 1630000000072)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631079999999, 482971664, 4456457973181208346, 29811, 105, -2892240.803873, 28282444.178390, 1, 'binary.ZumfNrRUgHCXCqqjBwgX', 'nchar.安徽省宁德县南湖关岭街K座 983548', 1630000000072)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631080000000, 1255752961, 4617338544672893130, 31384, 45, -3965.493491, -943974751.371172, 1, 'binary.xoiyGCRmWsgMFgfrdONj', 'nchar.内蒙古自治区鹏县南湖石家庄路E座 253529', 1630000000072)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631080000001, -1695698428, -671923235747543675, -26225, -99, -89596996493.199203, 53.155372, 1, 'binary.VzjkizbqoeqlOEtsqzTd', 'nchar.海南省台北市锡山长春街F座 321351', 1630000000073)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631080000001, -470196570, -1577429027012366617, -7829, -48, -39.524752, 51706491389416.898438, 1, 'binary.rOSrdNRJmYutBQkKnIvQ', 'nchar.辽宁省哈尔滨县上街南京街x座 664388', 1630000000073)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631080000000, 752187387, 5814377969189879673, -39, -37, -1240781.998722, -680710584267.488037, 0, 'binary.bFiODlLTjBvFimRGPEZH', 'nchar.上海市桂荣市高明张街U座 936978', 1630000000072)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631095000000, 1299240809, -3372032764175680697, 18966, 71, 284347227.242907, -129822972.670593, 0, 'binary.eqzlYcordfJQQJMBljjF', 'nchar.浙江省台北市大兴邯郸街l座 594497', 1630000000073)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631095000000, -601799850, -773012696448162526, -24255, 69, -85539.695742, 8483599308481.120117, 0, 'binary.fodqpEDidmdgTobhLwVp', 'nchar.江苏省秀荣县大东李路o座 709775', 1630000000073)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631094999999, 369111551, 4483914872861013771, 27035, 120, -61260954640579.898438, 5141985.329967, 1, 'binary.hBVqWjfnRCktdjBpGTYb', 'nchar.宁夏回族自治区六安市秀英哈尔滨街M座 426619', 1630000000073)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631095000000, 1544340960, 8517061698296926738, 16940, 119, -62221249203.984596, 7342075383114.320312, 1, 'binary.EbuTOcRDAAmBGOFvyfZO', 'nchar.宁夏回族自治区武汉县龙潭蔡路M座 210568', 1630000000073)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631095000001, -1324385506, -8127100560658016141, -3090, -49, 1019018870.899400, -86.370275, 1, 'binary.aBZiPEKTvPqdXPsDXgvy', 'nchar.云南省晨县山亭宜都路d座 616893', 1630000000074)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631095000001, -2055898711, -5445111939290776239, -31593, -49, -77345663240.806595, -914.503516, 1, 'binary.BSnRpSkSznADKblDKOiG', 'nchar.河南省凤兰市高明黄路M座 303020', 1630000000074)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631095000000, -943484549, 260761652713813572, -31076, 88, 886963.235930, -38250864.484145, 0, 'binary.qUWErZCBbTHzETMfuRMa', 'nchar.陕西省兴安盟市和平乌鲁木齐街w座 905284', 1630000000073)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631110000000, 1560015019, -6729804845934136099, 352, -115, 9316.130184, 220903.824895, 0, 'binary.fWtsEHhFIrXfqBGYlTNJ', 'nchar.海南省济南县白云李街S座 920870', 1630000000074)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631110000000, 600240137, -4687958607276237783, 31931, 50, 5721.825376, 570753990973.805054, 0, 'binary.utJxZlgPEdcqNAFObuFZ', 'nchar.广东省台北县普陀兴安盟街T座 505320', 1630000000074)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631109999999, 1370148713, 2682283741646434124, 21747, 108, -7336368856593.450195, -86249517.560025, 1, 'binary.ApKvhLsWkpRndmodhPEN', 'nchar.云南省雪梅县安次卢路J座 135754', 1630000000074)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631110000000, 656300451, 4206774300939189669, 24233, 69, 540899878.718127, 5.252156, 1, 'binary.TRvUgqDMiPmpDbTDQJIO', 'nchar.四川省香港县秀英关岭路Y座 839081', 1630000000074)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631110000001, -1440742963, -6981670779291616929, -17863, -118, 9339406547236.320312, -80509839.572484, 1, 'binary.nsuhquhIJtXKINhuhbZm', 'nchar.天津市齐齐哈尔县朝阳西宁路L座 393435', 1630000000075)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631110000001, -1680777424, -3789071286423645612, -29891, -91, -3687858.975654, 28786.604619, 1, 'binary.YfWaoxPFJKJagXhyEOmA', 'nchar.湖北省银川县崇文何路i座 475105', 1630000000075)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631110000000, -1095049119, -2038760289253362613, -4186, -124, 13304836.627667, 77695031812.673798, 0, 'binary.TEZgbSkvAdOPvsssvtOX', 'nchar.四川省鹏市东丽胡路Q座 963536', 1630000000074)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631125000000, -1618319757, 6350170596406303692, -28807, -17, -1265501669.731500, 757504.607632, 0, 'binary.xQXfGqKJJntmbtHJzqHc', 'nchar.吉林省重庆县南湖郑路E座 472846', 1630000000075)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631125000000, -2143880375, -2493359647253702127, 6606, 66, 404444.216865, -7014.675537, 0, 'binary.bPjaavfcVrQcVEYxUtAe', 'nchar.安徽省兰英市涪城王路f座 265166', 1630000000075)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631124999999, 1951461498, 4426546683527923406, 26976, 106, 861491.829361, -5937918253.865560, 1, 'binary.hZDumWlAzVpYfXkEaPlW', 'nchar.云南省重庆县清河陈路P座 522865', 1630000000075)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631125000000, 1219706153, 7536523458446532711, 5155, 120, -675841841.464953, 91755264081.884003, 1, 'binary.KsCtLZBdLUvbbTIKedAw', 'nchar.香港特别行政区永安县高坪南昌街L座 368651', 1630000000075)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631125000001, -1663211983, -3109525234509445099, -21222, -8, -21298956.340869, -9657.318325, 1, 'binary.wBVWJVLFoOdXTzTtRNuT', 'nchar.广西壮族自治区惠州市高明柳州路b座 545369', 1630000000076)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631125000001, -1810125590, -8421898795641820385, -4441, -89, -2734964221.741360, -37493460960.576599, 1, 'binary.yxiYtSxCJYuTTjLuCcLI', 'nchar.辽宁省太原市和平侯街i座 586968', 1630000000076)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631125000000, -1416386308, -7684571277090397009, 22278, -110, -31.162606, -776.531904, 0, 'binary.XYkmkoMHknNtlosmchSh', 'nchar.河北省郑州市花溪周路n座 600734', 1630000000075)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631140000000, 989052773, -5476387551687883714, -11570, 127, -37404372.212454, 42699806.700508, 0, 'binary.SifrNRSsBKcJKDqsnfXN', 'nchar.云南省拉萨县高港张路n座 873322', 1630000000076)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631140000000, 90148890, -9176843805101255090, 7217, -69, 7747.651553, 246706074.781856, 0, 'binary.iuKZktHzJRwchXDkGhbL', 'nchar.河北省娟县高港汕尾路p座 635670', 1630000000076)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631139999999, 2092906691, 5816259342140952837, 27223, 3, -21145.797856, 39546630288.518501, 1, 'binary.hLAAdJsauemmUBKhTtqW', 'nchar.江西省沈阳市海陵周路O座 413415', 1630000000076)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631140000000, 1419713973, 818822617965851283, 32245, 47, 92022664.783976, 3311483.287128, 1, 'binary.yfRnxroKprvGvOyoTlEh', 'nchar.广东省辛集县萧山西安路M座 462571', 1630000000076)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631140000001, -2137737633, -3177008163567761597, -29189, -74, 548693287695.377014, 72371.674202, 1, 'binary.wRfxAxVfEcdfKteGLuaX', 'nchar.内蒙古自治区淮安县普陀杨街A座 734889', 1630000000077)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631140000001, -1023034969, -7642623044713882631, -13299, -59, 3279504.422480, 6716289645.631150, 1, 'binary.vlQPmKsSiqtoGCixEBsV', 'nchar.广西壮族自治区晶市上街宁德街B座 609891', 1630000000077)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631140000000, -30379416, 1958421706180608853, 1892, -29, 4674012.774896, -21398530385.866001, 0, 'binary.pxvGQUheUZkqNsxiAZuT', 'nchar.澳门特别行政区倩县房山杨街U座 114062', 1630000000076)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631155000000, -1117272908, 752098947372767902, -2819, -56, -9085630.993944, -6895774866846.309570, 0, 'binary.HlxBvTuGkChJDLzDaxyu', 'nchar.陕西省昆明县龙潭周街D座 817221', 1630000000077)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631155000000, 2086908228, -5781823409166665769, -3324, 9, -3.671731, 50135208435.527000, 0, 'binary.UWkIRXVdPsZxZFVaWBtS', 'nchar.湖南省郑州县清城古路B座 982490', 1630000000077)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631154999999, 870002160, 4167669984685102889, 30468, 115, 641517032220.147949, 2742.639429, 1, 'binary.JCROwpmJQkPqZkIzrRkU', 'nchar.江西省静市黄浦潜江路B座 249825', 1630000000077)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631155000000, 1396830896, 8133688472099187160, 22272, 75, -4524332410799.129883, -35663261.321445, 1, 'binary.kOEOFvSdtLEFJlgywaSJ', 'nchar.河南省英市长寿汪街E座 114327', 1630000000077)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631155000001, -837033747, -5283148083077494740, -1260, -39, 565321.239314, 275.746366, 1, 'binary.YKouegacQvxoSSgTaPrv', 'nchar.安徽省淑兰市新城李路r座 256922', 1630000000078)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631155000001, -1906954896, -5724786982458123670, -1426, -24, 299922.459639, -1285.219883, 1, 'binary.ZqHGrayEGFBlDHVAObSZ', 'nchar.甘肃省桂芝市金平武街X座 777677', 1630000000078)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631155000000, 745556057, 516983514584470898, -12904, -49, -716006320.556210, -218.138046, 0, 'binary.ZJBaxSKndLZrHVRppYnT', 'nchar.广西壮族自治区梧州县秀英甘街b座 779502', 1630000000077)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631170000000, 2139488417, 6066602372775042425, -1518, 84, -80581942.592809, -37314203203.592003, 0, 'binary.yymGCDKbqZkixkIjIIGT', 'nchar.湖北省荆门市和平淮安路J座 884655', 1630000000078)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631170000000, -374714905, -7736740878366341122, -14276, -79, -64248.970229, -68011884269.584396, 0, 'binary.hMgGBmPqKYkzsArGfbxQ', 'nchar.新疆维吾尔自治区沈阳县城东齐齐哈尔街b座 794676', 1630000000078)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631169999999, 1397781824, 6557161155035163362, 11749, 37, -3.906174, -1.651866, 1, 'binary.hwfhcmJDDTvQaCRmlKCd', 'nchar.江西省玉华市长寿梁路o座 261941', 1630000000078)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631170000000, 1387979869, 2661199544340098486, 9621, 49, 166489205686.515015, -7163639.756925, 1, 'binary.atyzvvxverSTvukiOHRc', 'nchar.澳门特别行政区琳县南长深圳街L座 927560', 1630000000078)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631170000001, -1573224659, -6074816603000302535, -28617, -101, 26624788159.678699, 62.218691, 1, 'binary.QpcyRVyxqqnHSSLxWCdz', 'nchar.台湾省荆门市和平关岭街w座 554552', 1630000000079)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631170000001, -613007188, -2265367830140686655, -30242, -74, 132051246084.664993, 9583075.585374, 1, 'binary.ZwGExMXBaZQnYGUqOZnk', 'nchar.贵州省天津市龙潭吴街P座 514026', 1630000000079)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631170000000, -229811940, -4723474584235949728, -30413, -126, -37.161630, 6566121399.880410, 0, 'binary.QKZaMSaDgNKbuVjVXVZX', 'nchar.湖北省超市友好南宁街b座 387820', 1630000000078)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631185000000, -1462514908, 6815473990049018903, 26765, -60, 8053251416.958930, -94687.447387, 0, 'binary.gMqqCIzQCNGNgbSwolZX', 'nchar.山西省春梅市魏都张街I座 216775', 1630000000079)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631185000000, -1558957537, -3515355671345392932, 32525, 83, -873729.459053, -4948145879174.500000, 0, 'binary.eHTWpPQVAzoRsZXjDHro', 'nchar.内蒙古自治区雪梅市西夏广州路T座 462974', 1630000000079)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631184999999, 1354974803, 4535068117623812145, 15974, 126, -3641262362.474420, -215837.649412, 1, 'binary.mgwWzOiTRoTHUbvzEMSm', 'nchar.宁夏回族自治区欣县东丽南昌街a座 409781', 1630000000079)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631185000000, 909020116, 7166005914556535033, 14716, 26, 50294221.576123, 67159744502236.203125, 1, 'binary.aHafHhYbmjwnjfaDKjaV', 'nchar.天津市马鞍山县南溪宜都路W座 812458', 1630000000079)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631185000001, -1306330996, -7521905733671260635, -1, -23, -651220.365910, 209.745394, 1, 'binary.xazZjdXNjaykPYfDWhos', 'nchar.广西壮族自治区慧县南溪李路n座 595049', 1630000000080)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631185000001, -829584454, -6819959797009261424, -30794, -119, -2545863021084.839844, 48573662965.169998, 1, 'binary.RQfCFnyRhVGAwoZKbfBg', 'nchar.江西省畅市大兴呼和浩特路N座 966282', 1630000000080)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631185000000, 159441443, 3829890051273914786, 22074, -46, -0.126559, -59066.623155, 0, 'binary.sCHQwHDxsHrpfheUsPvS', 'nchar.甘肃省婷婷市和平林街X座 967902', 1630000000079)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631200000000, -1984142506, 918572017792917031, -20270, -51, 7172084337794.419922, -67689618090033.000000, 0, 'binary.pfhSySLjKZbOfIPTmqAB', 'nchar.宁夏回族自治区亮县牧野石家庄路L座 408034', 1630000000080)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631200000000, -1972988665, -3214499273794963694, 3010, 42, -8898.495820, -7139.318526, 0, 'binary.ivdrcXkAeWkscdYzZJEe', 'nchar.浙江省瑞县海港永安街R座 494785', 1630000000080)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631199999999, 1089140558, 5653452583897077783, 8528, 7, -9816158771686.500000, -30.580967, 1, 'binary.tqyfrLsfYAZeSvFCwYXQ', 'nchar.安徽省倩市梁平广州路E座 417291', 1630000000080)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631200000000, 1752602476, 8379644292871336124, 1479, 55, 0.152144, 8340466.258810, 1, 'binary.CaqLbGdnvyvDGmnqAdwA', 'nchar.宁夏回族自治区兰英县滨城董街m座 612134', 1630000000080)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631200000001, -2070609011, -3927356551694226099, -14160, -96, -489939465270.245972, -176.619265, 1, 'binary.HtBovhAaClqmUKDVoPoA', 'nchar.西藏自治区兴城市西峰李街O座 294081', 1630000000081)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631200000001, -507025125, -6769707689103886410, -26670, -62, 7.854748, 91216299872.798492, 1, 'binary.kVTRaSjNPeaJAGYzWqVF', 'nchar.山西省邯郸县六枝特袁街q座 379922', 1630000000081)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631200000000, -91932554, -8911762384559779952, -12134, 114, 8455.888482, 192830806126.287994, 0, 'binary.UmxEgaxmRlsWpDOOjGns', 'nchar.安徽省秀云县金平亢街Z座 413285', 1630000000080)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631215000000, -2106211575, 4343732691913651727, 30695, -65, -1182138766033.570068, -1420143959702.800049, 0, 'binary.lENnnzItGfhHBhuBSKkg', 'nchar.宁夏回族自治区淮安市大东章街Z座 522024', 1630000000081)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631215000000, -1514266386, -1557718503233026241, -10740, -93, -3712.656749, 628.936406, 0, 'binary.KCtPSzikgeatarBhSEXT', 'nchar.安徽省莹县淄川石家庄街i座 428331', 1630000000081)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631214999999, 243460896, 1867078685154320423, 13109, 8, 2562.515572, -8164811617493.440430, 1, 'binary.GwEkcVzBVjqKKvBBYCHl', 'nchar.广西壮族自治区邯郸县门头沟兰州街l座 647959', 1630000000081)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631215000000, 669742703, 3588783546529218834, 10833, 6, -408721127673.909973, 801103747.542088, 1, 'binary.uwFahDnlWanEVobNZkCM', 'nchar.甘肃省张家港县白云罗路f座 137164', 1630000000081)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631215000001, -442417658, -7549129289634916853, -2159, -58, -98911613476.491501, -874.865816, 1, 'binary.aMVpynzemnksvKTWGcDC', 'nchar.吉林省建国市牧野钟街E座 377779', 1630000000082)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631215000001, -1409923779, -8427760215446628026, -18353, -47, -50606823244.468002, -345539443425.945007, 1, 'binary.cugznUahvcPVWaAAckbf', 'nchar.海南省宁市合川宜都街o座 305915', 1630000000082)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631215000000, -1342274642, -8536170080112789400, 30067, 115, 12581450912.783600, -291.972491, 0, 'binary.eOcZzgBgRRIVelvjVeto', 'nchar.澳门特别行政区欢县高坪广州路N座 593765', 1630000000081)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631230000000, -456444500, -653943768841413221, -16654, 115, -74554935958741.203125, 7070430643.419100, 0, 'binary.zrOBefSbOEUgRLtVobMw', 'nchar.北京市璐市东城太原路Z座 165710', 1630000000082)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631230000000, 1112822860, 8809469361686033607, -22243, 84, 718004818.589884, 836.474427, 0, 'binary.cLnnuHpDQIMfMBODNYpW', 'nchar.青海省广州市房山阜新路h座 898094', 1630000000082)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631229999999, 993462171, 7980976104707598817, 22906, 119, -90532577531185.000000, 2883997.534517, 1, 'binary.akkJQvtKVOUMeEJdtLWQ', 'nchar.辽宁省西安市魏都长沙路q座 830446', 1630000000082)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631230000000, 659535811, 6648102677976806439, 14934, 106, 8.999744, -78105262.595693, 1, 'binary.RBWgiabXuPgAeanvsJoE', 'nchar.香港特别行政区倩县长寿王路D座 334836', 1630000000082)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631230000001, -1147877996, -6106212901363852731, -30865, -72, 9390.844553, -7383783838166.849609, 1, 'binary.UsylaHHTjnwUCmWOFRop', 'nchar.新疆维吾尔自治区齐齐哈尔市海港南昌路q座 642772', 1630000000083)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631230000001, -8588768, -7260337404461154045, -21386, -116, 46305041.132235, 6654257695.256100, 1, 'binary.SneLCpCrEbSFqlWIVLwu', 'nchar.陕西省欢县花溪荆门路T座 249790', 1630000000083)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631230000000, 1003481306, 6017067765177374344, -16218, 60, 82.649494, -712.838445, 0, 'binary.qLRxDlzDPjVVbRZHOADr', 'nchar.河北省畅县徐汇大冶街D座 822863', 1630000000082)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631245000000, -294434895, 1924629096591118432, 17988, -44, -63587509.905294, -40747153.224679, 0, 'binary.olALLZcgYbqmLLsQAfvv', 'nchar.云南省强市淄川淮安路c座 262889', 1630000000083)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631245000000, 738129131, 1815388765763244472, -6405, -93, 52.721010, 216.728740, 0, 'binary.yxHFbosrzvHvxaTBUoNe', 'nchar.浙江省福州县梁平嘉禾街i座 170922', 1630000000083)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631244999999, 224419688, 3680471494276801590, 18367, 87, -74383724488933.593750, 0.593016, 1, 'binary.FsYcmhXFIVEwuCmapLFd', 'nchar.青海省合肥市清浦辽阳路I座 165564', 1630000000083)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631245000000, 54658682, 2220380251657075236, 12017, 72, -2409.191361, 16465604801985.900391, 1, 'binary.WVLQqvEMPTlqXptHGBTA', 'nchar.陕西省桂珍县魏都陆路z座 653467', 1630000000083)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631245000001, -111362062, -2079998725394443456, -14043, -83, -8050700.333128, -786.595631, 1, 'binary.wugzMLvmLdkxesgmixOH', 'nchar.安徽省南京县清城柳州路Q座 634494', 1630000000084)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631245000001, -646454407, -7621015329762251852, -32454, -96, 58254987.947866, -86.835965, 1, 'binary.nUZjCcyTPHcAJQjyqiKW', 'nchar.河北省嘉禾市锡山张街F座 809458', 1630000000084)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631245000000, 1630911956, 9008151774496407995, 3422, 97, 1410480123999.199951, -40614657267267.296875, 0, 'binary.kaWyIWeHVgVeCBWtXjos', 'nchar.青海省婷婷市和平黄街G座 571553', 1630000000083)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631260000000, 1229299947, -3222073200735135026, -25377, -68, 76060893505437.296875, 79056750996157.000000, 0, 'binary.TIeullzMmPeJOEDzERVs', 'nchar.内蒙古自治区长春市普陀通辽路R座 260095', 1630000000084)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631260000000, -1829114311, 192679604105772908, 25050, 51, 429584494.456619, -7710.519461, 0, 'binary.sxkAVzIUJGdjrEKTWivG', 'nchar.重庆市帅县南湖长沙街T座 169523', 1630000000084)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631259999999, 2133083813, 7400378387863708089, 15500, 29, 483042885.250631, -4018.354623, 1, 'binary.buDeWRmTqpbBhNKoIWjt', 'nchar.天津市婷县新城呼和浩特街Z座 481098', 1630000000084)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631260000000, 2102189676, 8289210275637280206, 12319, 12, 2360.126260, -0.783536, 1, 'binary.zkocBZqDNaubCdBsYlZK', 'nchar.河南省潜江县吉区邱路R座 988052', 1630000000084)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631260000001, -88622269, -8452436543600128194, -14877, -107, 2.227830, -896.432470, 1, 'binary.fgqZcQmlErAwLKYvDwkA', 'nchar.福建省嘉禾县城北汕尾街S座 162248', 1630000000085)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631260000001, -603730496, -5142204606726944282, -6542, -22, -701.644082, -75915023637.720001, 1, 'binary.JtkfvmLGOlQzBtNKysLt', 'nchar.云南省沈阳县高港李路S座 869342', 1630000000085)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631260000000, 2144837535, 6790333755067852306, 29522, -5, -5292215263549.620117, 9659.226553, 0, 'binary.ZtAHxvlbFZNbmkaMfRDe', 'nchar.台湾省大冶市吉区石家庄街O座 527169', 1630000000084)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631275000000, 1290141025, -2143848369719800423, 7189, 8, -822375762195.462036, 20283811003.944000, 0, 'binary.loHjtwzgyvJksRhJNiQS', 'nchar.河北省佳县徐汇刘路G座 705107', 1630000000085)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631275000000, 801182958, -7572521914234103164, 4604, 107, -859019.418204, -261693048307.811005, 0, 'binary.YAcVYbiCpwSqCdWIgbBo', 'nchar.上海市武汉县闵行杨街a座 347736', 1630000000085)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631274999999, 521859506, 3602105093569999375, 21665, 21, 9019367.551508, 532663957703.719971, 1, 'binary.NgxLslBkOvvWWVcDCUlC', 'nchar.海南省哈尔滨市翔安沈阳路r座 486389', 1630000000085)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631275000000, 1634351470, 1549009549596999035, 26284, 69, -2.961951, -6310787053.197710, 1, 'binary.AERkUPEHGySISfgqulEw', 'nchar.台湾省西安市双滦深圳路R座 706492', 1630000000085)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631275000001, -1577694201, -8482950364465928222, -19741, -105, 351508461253.127991, -28134084860.892399, 1, 'binary.zgfPMXusJGeFxLLImITl', 'nchar.黑龙江省红梅县吉区嘉禾路Y座 870326', 1630000000086)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631275000001, -1817845693, -2298814801521952311, -14645, -83, 223713055.105098, -75532242.499559, 1, 'binary.flJqybaXNWGYZTymaTJG', 'nchar.江西省淑珍市平山周街Y座 953199', 1630000000086)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631275000000, 632609765, 4219748384315739773, -26000, 71, -450862164529.280029, 14560508342304.800781, 0, 'binary.ZHQyoqVmbWNpweggaXDK', 'nchar.宁夏回族自治区辛集市城北辽阳路N座 240842', 1630000000085)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631290000000, -770991939, 5485773724851375125, 23994, -64, 623888353027.609009, -55981854.579338, 0, 'binary.QKhLWpoMPsfwyQYNoPZA', 'nchar.上海市玲县普陀龚街p座 566117', 1630000000086)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631290000000, -793126068, 2224199903529481270, 6843, 106, 99682310978838.093750, 467505798786.474976, 0, 'binary.HmkxkSqBoJJStxYjyvrz', 'nchar.天津市志强市沙市雷路g座 547769', 1630000000086)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631289999999, 896685945, 4158350015273846866, 26730, 53, -6200015073790.200195, 12566017.333818, 1, 'binary.NEkCzZWDoXmvngPlArSM', 'nchar.天津市海口市魏都黄街O座 679546', 1630000000086)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631290000000, 832269201, 4123871706026762985, 13927, 88, 499.930604, -70366099857.880005, 1, 'binary.mADUJbHlBrbziiXvrWeZ', 'nchar.云南省惠州市沙市海口路c座 131412', 1630000000086)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631290000001, -102246541, -3324596924342719960, -3905, -96, -4816.768188, 93915714.748034, 1, 'binary.qiNYdNPnHVKQBpzXATmm', 'nchar.河南省呼和浩特市清浦广州路g座 216441', 1630000000087)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631290000001, -282159644, -7801886583063959893, -15115, -96, -7.823940, -2620.949488, 1, 'binary.GzTEiirhkGvJXbGkiPrq', 'nchar.香港特别行政区玉珍市朝阳刘街r座 939747', 1630000000087)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631290000000, -669399368, 5115472553771177504, 12526, 91, -28942186887.971001, -6.435703, 0, 'binary.XIZFOElmYhlMyQRewDCt', 'nchar.甘肃省乌鲁木齐市西夏贵阳路T座 579190', 1630000000086)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631305000000, 262080824, 8383920960732834455, 13968, -34, -45.211929, -1300.277717, 0, 'binary.hIMCRvrXPAOVpFRmYnUy', 'nchar.江西省北镇市双滦刘街W座 798066', 1630000000087)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631305000000, 2114811730, -1509702226741249727, -13264, 72, 2.150671, 5261316406.519730, 0, 'binary.FtIdbPhtUPjIuTSbpJQq', 'nchar.新疆维吾尔自治区沈阳县锡山林路M座 892641', 1630000000087)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631304999999, 2143525047, 6899945126249773541, 26738, 102, -292.795385, 138207363422.498993, 1, 'binary.eIPLVCyRogBRyzGbrFic', 'nchar.辽宁省福州县长寿杨路z座 941701', 1630000000087)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631305000000, 1073200557, 807774567632061607, 29517, 113, 67931.421812, -1651664.861578, 1, 'binary.UyFmxxBiWhvpNXabSZxN', 'nchar.天津市斌市普陀哈尔滨路P座 502633', 1630000000087)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631305000001, -489200958, -5831038164958816606, -16406, -76, -6392689.485758, 3750666.814934, 1, 'binary.bCLmDBrnkgDcXvcwrBqA', 'nchar.甘肃省哈尔滨市吉区苏街y座 407071', 1630000000088)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631305000001, -2127347634, -4083644771875265580, -20817, -30, -3.785714, 681029605720.448975, 1, 'binary.GcWganOJXCPvFeMQZTYJ', 'nchar.黑龙江省丽丽县高港长春路v座 665579', 1630000000088)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631305000000, 1527621467, -7509002110121261571, -26325, 32, 3.577428, 5008201901846.370117, 0, 'binary.mFsrNoKYqMYXhFEIfVXS', 'nchar.山西省梅市南湖合肥路w座 573200', 1630000000087)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631320000000, -493256377, -8751737434302823435, -27869, -96, 330393978255.500000, 94835651689763.000000, 0, 'binary.PqdASxtDLyeIFzBiaFGV', 'nchar.湖南省关岭市双滦武路o座 527508', 1630000000088)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631320000000, -2087366027, -6303077694275632802, -15261, -89, 43362171018304.000000, -166690.281684, 0, 'binary.zPrdaJdDKwcSHuAxtOlS', 'nchar.澳门特别行政区超市高港刘街N座 420236', 1630000000088)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631319999999, 1129994268, 1552944233564611739, 10242, 108, 562740269.814683, -48475927235174.601562, 1, 'binary.rqJyBwQmuzFVARUJRgKb', 'nchar.山西省巢湖县西夏沈阳街S座 255735', 1630000000088)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631320000000, 2063639050, 2521461528938336565, 2345, 89, -6038709960145.540039, 24451.395881, 1, 'binary.iSmMBujSmVtayVzDoWgG', 'nchar.内蒙古自治区桂香市吉区潘路v座 620160', 1630000000088)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631320000001, -1449143866, -6390394742281036218, -100, -33, -120.220087, 109963155461.466003, 1, 'binary.fPxCvZJXReoaPBmALmPQ', 'nchar.内蒙古自治区婷市兴山冯路h座 570428', 1630000000089)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631320000001, -1895163787, -1867618456195867046, -5071, -105, -1871044419105.310059, 443341008.795331, 1, 'binary.tMibvAJACwmXDYSGDcNH', 'nchar.贵州省兵市静安黄街O座 248304', 1630000000089)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631320000000, -1870734956, 5919383249914923168, 25989, 91, -1.395899, -450627922362.231995, 0, 'binary.rCKozKCilrDzhrinGzhJ', 'nchar.江西省楠县闵行银川路V座 237404', 1630000000088)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631335000000, 408968903, -8367166343934446562, -19738, 15, 854582448833.431030, -9969.161351, 0, 'binary.GLyIXkgZbApVCOoVSqaO', 'nchar.江苏省香港县黄浦淮安街D座 525365', 1630000000089)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631335000000, -1122322355, 8412393600271974612, -23315, 95, -68174012.309009, 15387044.711345, 0, 'binary.MGjySGAlYDujClbZejIQ', 'nchar.安徽省邯郸市城东大冶路G座 360663', 1630000000089)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631334999999, 2026186883, 445653452435547622, 18307, 49, 213343.543679, -4.739704, 1, 'binary.fOSZlDCKdxiQaysyscfx', 'nchar.江西省雷市花溪陈街B座 320678', 1630000000089)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631335000000, 507343304, 7297664243660949047, 12008, 109, -531.925870, 1673114199.854260, 1, 'binary.cTljYtBXIOAsDXqDTlMd', 'nchar.广西壮族自治区帅市崇文王街u座 771522', 1630000000089)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631335000001, -2001883206, -817341878022425389, -2161, -42, 9.708559, -53706854436.695000, 1, 'binary.IUNzHoyqCMDBQckAVEQz', 'nchar.四川省宇县城东淮安街C座 920078', 1630000000090)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631335000001, -1408747533, -4778697158136936011, -27, -101, 47621.171490, 3290528001686.430176, 1, 'binary.vyTXYeaTyzhYwNCQiJrW', 'nchar.广西壮族自治区秀芳县大兴天津路H座 858644', 1630000000090)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631335000000, -1227268662, 4866318703531659868, 8057, -67, 1812966973.800030, 6.738090, 0, 'binary.EMfIIquLTRUJOViCbaXD', 'nchar.河北省长春县翔安禹路f座 740911', 1630000000089)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631350000000, 29364546, 7439453869072957110, 3379, 93, 55692580455.865402, 25451.386688, 0, 'binary.SOQDYnfDtUghMpNrrrvd', 'nchar.吉林省玉梅市清城大冶街w座 657754', 1630000000090)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631350000000, -1512569467, -2170613746493368276, -9226, 86, 94917216138828.500000, 45.344859, 0, 'binary.osdODKgoqrkMjsUQuBxO', 'nchar.湖南省华县山亭赵街o座 343015', 1630000000090)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631349999999, 341004545, 1678452718654659972, 11982, 53, 647268193.946854, -9.321045, 1, 'binary.PlQgjTLDuHwIfIkGmfIq', 'nchar.江苏省长沙市门头沟秦路m座 511307', 1630000000090)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631350000000, 1022164581, 8771702396738754711, 1690, 106, -2055.312055, 363.845057, 1, 'binary.QVYMJaQkcoJnLdYFnwsS', 'nchar.台湾省西宁市南长郑州路f座 795021', 1630000000090)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631350000001, -2139193188, -4423061215457882616, -22771, -31, 492.888005, 22942151260.387100, 1, 'binary.LSzPYUKvvRlNrhoKoYkL', 'nchar.天津市静县浔阳济南路G座 357460', 1630000000091)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631350000001, -1786702987, -5274362232731441209, -16394, -43, 237512315596.303986, -558.984176, 1, 'binary.onBwaQpiFBImnuhjravU', 'nchar.河南省雪市淄川王街Y座 875679', 1630000000091)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631350000000, -1431645289, -6529328401317482981, 9944, -97, 279578438.723650, -79899714.177569, 0, 'binary.ZhbLqmODnxwYrimmIigl', 'nchar.北京市大冶市怀柔刘街t座 692168', 1630000000090)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631365000000, 568429328, -9060392884809893816, 28469, 61, 307437180734.507019, -21602647797.441101, 0, 'binary.IGDZBmjwYmOzJaUbkiKN', 'nchar.福建省兴城市兴山巢湖路P座 743092', 1630000000091)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631365000000, -1550415271, -7558411265476450633, -26998, -58, 1694624.435305, -47865257382.732803, 0, 'binary.JhBLpAPozgqCVUQnONkT', 'nchar.黑龙江省秀云县清城白路q座 555712', 1630000000091)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631364999999, 2050068224, 8869318186483878987, 18135, 56, -324240012.550862, 139208652187.634003, 1, 'binary.fzVzLhgOcjBWiooIbfZy', 'nchar.西藏自治区太原市长寿西宁路h座 862819', 1630000000091)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631365000000, 5839085, 4176280397163068047, 4608, 4, 683.468225, 4674219497.311910, 1, 'binary.YBbSDgHLgpNnIYBAiMnd', 'nchar.广东省欣县沙市惠州街I座 803450', 1630000000091)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631365000001, -1606114187, -8492599008143584602, -21024, -58, 977.510976, -2962257.665275, 1, 'binary.DQLAjLojVsSPrXVqoaFU', 'nchar.江苏省建国市涪城兰州路k座 224858', 1630000000092)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631365000001, -1085786582, -7556940745324501035, -214, 0, 85978773227.734802, 3688136776873.470215, 1, 'binary.LsObIWxTJIHqdNcQniXu', 'nchar.贵州省小红市西夏林路J座 503400', 1630000000092)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631365000000, -486512987, -1848007965784118332, 20641, 39, 7524.727066, 43002.993255, 0, 'binary.JtdWjkAwXwWmNqghjGgR', 'nchar.江苏省福州县萧山李路l座 332686', 1630000000091)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631380000000, 1015255006, -6233642644171313082, -30770, 103, -52871950.517558, -203.631390, 0, 'binary.dcFQefENHBmSbWfsxkNZ', 'nchar.吉林省凤兰县大兴李路j座 146250', 1630000000092)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631380000000, -1944800483, 2662387305551873341, -32553, -7, 49980.865995, -377372259.892400, 0, 'binary.EWzwpfuElPwODlQeSUsg', 'nchar.湖北省荆门县闵行永安街F座 378209', 1630000000092)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631379999999, 869226799, 2896504572324576563, 949, 78, 3067118231.817550, 5.678553, 1, 'binary.thrXsOuUUQEwGkXiZqVD', 'nchar.香港特别行政区荆门市高港天津街K座 250023', 1630000000092)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631380000000, 323027847, 2827349391284089242, 1441, 44, -17278.296831, 78816030.490657, 1, 'binary.YZbYVJvcLQeMTfNdLJhv', 'nchar.贵州省淑珍县永川辽阳街g座 249023', 1630000000092)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631380000001, -2127572203, -4937514564787173908, -63, -76, -123946739.447527, 698804470.690330, 1, 'binary.MtiGMwxbCoGxBrSCfmnb', 'nchar.广东省齐齐哈尔县梁平关岭路t座 761348', 1630000000093)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631380000001, -1306426634, -4986879132935813626, -28229, -23, 39146958063046.203125, 87460954188.246506, 1, 'binary.SZAYkmBTuWBkLyHMsNln', 'nchar.辽宁省荣市浔阳马鞍山街h座 139222', 1630000000093)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631380000000, -24693354, -7179764382339676335, -4810, 42, 49617345.815346, -40955896779.561096, 0, 'binary.AOTdejxaZWZTeWiSQccI', 'nchar.湖南省呼和浩特市南长东莞路Q座 526334', 1630000000092)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631395000000, 110327094, -7171919345203251134, 10385, 50, -88221.924084, -14499963.327697, 0, 'binary.jBWuQXhwybyeWaUeuiCo', 'nchar.河北省深圳县城北昆明路h座 627103', 1630000000093)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631395000000, 1589122997, 4808726552373126087, 13356, -88, -777.180511, -5649.819038, 0, 'binary.PLvgyyFexDVwuXPcgOUJ', 'nchar.广东省璐市梁平石街p座 625686', 1630000000093)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631394999999, 415752279, 5607734497564153556, 28120, 114, -758865393.966240, 2879.590758, 1, 'binary.pNjKEKLwWZUgNZshphnH', 'nchar.新疆维吾尔自治区海门市萧山潜江路N座 450879', 1630000000093)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631395000000, 1725928224, 406982977976053694, 31934, 55, 2365333426653.399902, 52689398.325206, 1, 'binary.tODfUdiMrapEBaRDyjTC', 'nchar.甘肃省旭县六枝特陈路k座 228235', 1630000000093)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631395000001, -1943166726, -461080508996818206, -3563, -103, -984164002.302936, 3328600.852324, 1, 'binary.hSqhMEcJIhZRmIFrQDKl', 'nchar.江苏省桂荣县大东宜都街q座 375062', 1630000000094)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631395000001, -965688414, -4532230014607129716, -32045, -103, -8.400039, -554153162149.166016, 1, 'binary.ZQVXUVCHEOWxAjlnoSEK', 'nchar.吉林省通辽县璧山龙路l座 482360', 1630000000094)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631395000000, -306891211, -3703573295039531167, 6006, 86, 226610060352.789001, -47040211.883215, 0, 'binary.BJYBsZjtDafCchXYbpRr', 'nchar.台湾省哈尔滨县翔安惠州街U座 837583', 1630000000093)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631410000000, 1886325467, -3863586930565866233, 8904, 38, -946832.388950, 55.658536, 0, 'binary.QSJUCkzpJcOiAmsyKmUs', 'nchar.云南省宁德市高港东莞街N座 505827', 1630000000094)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631410000000, 1857465087, -4382581587636034119, 23552, -18, -13.792084, 829139755.682817, 0, 'binary.ljHNBjHCmNlXXVTnknxH', 'nchar.四川省北京县华龙济南街x座 750613', 1630000000094)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631409999999, 1895012884, 2424275775986822787, 22179, 90, -0.394803, -56.575361, 1, 'binary.KBNSdgumJOCWoMmWtLJU', 'nchar.江苏省静县秀英太原路S座 251913', 1630000000094)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631410000000, 1569144121, 5640124213375907439, 8815, 5, 257555.261048, 48254141.237573, 1, 'binary.KKozUPJSMbzhEyunifsH', 'nchar.湖南省伟县萧山萧街k座 474899', 1630000000094)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631410000001, -1384685953, -2320337996509410200, -15795, -10, -98.406889, 42984.674970, 1, 'binary.orjVZeKJsgJsTEqHljkf', 'nchar.海南省建华县沈北新林街g座 586911', 1630000000095)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631410000001, -1012235418, -8338932088221751208, -8979, -127, -25798805.860955, -48091893253761.101562, 1, 'binary.XDTOObdfDUqcGtXWVONC', 'nchar.浙江省柳市双滦鲍路a座 506391', 1630000000095)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631410000000, 726812223, 8571870874380055515, 22786, -83, 942970127.407073, 52.995631, 0, 'binary.xgcSGhNrZOWGKZtOlArO', 'nchar.宁夏回族自治区丽华县蓟州深圳路K座 592080', 1630000000094)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631425000000, 2071766507, 1772340392886850115, -24057, 25, 78.445247, -79637.635931, 0, 'binary.sROneWSFNuXBalpsjyEo', 'nchar.安徽省广州市孝南刘路b座 713687', 1630000000095)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631425000000, 1087816300, 5318228817994763162, -27394, -31, 267905.242469, 8213154732519.320312, 0, 'binary.imZeNtwoLVXCQJPFObQE', 'nchar.山西省沈阳县高港长沙街F座 461428', 1630000000095)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631424999999, 2108909701, 7807744131165483728, 13566, 88, 6.922028, -9.200773, 1, 'binary.vYUalkfScnQyDPeEqCTq', 'nchar.江苏省太原市华龙陈街K座 781219', 1630000000095)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631425000000, 990358809, 3801390332798973674, 31813, 108, 5.517866, 6845.746002, 1, 'binary.DXVimvDjqwoNARHjTXuI', 'nchar.上海市杰市怀柔天津街N座 936198', 1630000000095)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631425000001, -52260486, -2710179305466920974, -16431, -119, -958.607117, -38510688978203.898438, 1, 'binary.erVQJAvbzEahBSRwIflK', 'nchar.江西省桂英县六枝特北镇路A座 788549', 1630000000096)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631425000001, -2093648842, -6938612375617002825, -6697, -12, 4655.180387, 8131192.215618, 1, 'binary.IKRYKZeWgWRTpWFkjzBI', 'nchar.西藏自治区海门市高坪六安路l座 342816', 1630000000096)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631425000000, -1314836279, -7834501286968314160, 15789, -123, 33900643554.123798, -2082127.625636, 0, 'binary.rDTPIDjxHVnobHXBYtXY', 'nchar.北京市昆明市合川董路B座 463423', 1630000000095)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631440000000, -1084304030, 7788263187088917501, -6031, 103, -857.741582, 8339855538945.950195, 0, 'binary.TyxrsRIKPjtzXgKyuAEH', 'nchar.山西省宜都县锡山李路J座 745663', 1630000000096)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631440000000, -1295742585, -5131971234132550528, -2973, 42, 52247.313515, 3926177760.319000, 0, 'binary.vXduhvWXmyRcbKfMRWlx', 'nchar.安徽省兵县高明许街p座 922555', 1630000000096)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631439999999, 20091569, 6781601496202385368, 19493, 34, 430582007336.525024, -2.412247, 1, 'binary.kZJLmYOxtxuJZnMBDufv', 'nchar.内蒙古自治区颖市浔阳王路J座 977519', 1630000000096)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631440000000, 1813973451, 4213442260213870776, 26872, 44, 67809323307135.703125, -88973468.418878, 1, 'binary.LTPEGjxcghSRynZSWwqM', 'nchar.云南省南昌县梁平杨街G座 627457', 1630000000096)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631440000001, -958070866, -691160751112903120, -20406, -41, 2363414966580.939941, 8820731593.777920, 1, 'binary.RZliiclFrDaNImulqJTo', 'nchar.台湾省梧州县沈北新葛街m座 433410', 1630000000097)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631440000001, -1829734602, -7898671321317813289, -22642, -75, 4.101330, 40052.692465, 1, 'binary.KqgvcwBhRIxCNTZaTnLf', 'nchar.福建省金凤市海陵武汉街u座 555612', 1630000000097)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631440000000, 521824259, -8660783264753873829, -29076, -1, 43333588339.556999, -11986.154286, 0, 'binary.GKzBHXETTitGAjMDqHEz', 'nchar.广东省合山县萧山宜都路u座 831011', 1630000000096)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631455000000, 113309185, -5404115880106386219, 26329, -5, 3883055.927579, -91.272110, 0, 'binary.DJuxhSLmckBxGAZrTrdg', 'nchar.黑龙江省丽丽县海陵张路x座 667710', 1630000000097)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631455000000, 1249035464, -1549873989524700903, -1552, 12, 30063970786.732201, 11.623060, 0, 'binary.wGOvxPtoGpoLQkqKqHYd', 'nchar.山东省建国县沙湾南昌路c座 673842', 1630000000097)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631454999999, 420485592, 1118062875777571668, 19952, 48, -13933.583298, -2462443202766.899902, 1, 'binary.PUybFnyERTpaOdCfdUeZ', 'nchar.内蒙古自治区海燕县新城郑州街o座 509072', 1630000000097)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631455000000, 30946503, 7185137291847458233, 19072, 30, -152414.945183, -623547849.652030, 1, 'binary.bPgvDlyyWMQtWdUCTHsf', 'nchar.西藏自治区东莞县上街谢路z座 842631', 1630000000097)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631455000001, -136337675, -6941068854543647771, -30263, -36, 49955993237.460999, 2059.397798, 1, 'binary.AvuVCfvKjWXieILiINcT', 'nchar.台湾省建国县南长刘街Y座 514218', 1630000000098)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631455000001, -1639262737, -5524893491321561875, -14837, -81, 464457921385.609985, -381392530568.276978, 1, 'binary.aDgJHFZdhjaRiLJgewyJ', 'nchar.贵州省建县锡山谢路U座 298544', 1630000000098)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631455000000, 74809487, -2714524097635173095, 20984, 33, 1802605.962684, 77.296856, 0, 'binary.vYEpwfCOmksSKWbrqYDR', 'nchar.香港特别行政区哈尔滨县山亭董街I座 748187', 1630000000097)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631470000000, 222279926, -7912824848487896120, 9941, -30, -29340217214.750000, -19574.321414, 0, 'binary.VsnXIRHAihqVgXYwhHKL', 'nchar.江苏省拉萨县闵行合山街c座 761422', 1630000000098)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631470000000, -557137010, 2878767942030754357, -31366, -47, -4333332356.734030, -1526389473.865540, 0, 'binary.jcMUSJuZgsPjePUNiAdT', 'nchar.海南省东县锡山澳门街q座 268693', 1630000000098)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631469999999, 447814637, 2176882939455175378, 20639, 104, -1356647372.422460, 6.253297, 1, 'binary.fyooIrohcePtHVwJGMuD', 'nchar.河北省关岭市金平香港路p座 886482', 1630000000098)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631470000000, 606893531, 4789787682189143102, 14991, 38, 32272292804541.800781, -943.731667, 1, 'binary.VuPeMxNmIrwVmeDRCPTm', 'nchar.黑龙江省兰州市东城陈街O座 771980', 1630000000098)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631470000001, -994364364, -2126966193042917392, -11261, -99, 7.179842, 9558.331194, 1, 'binary.qpXjZYHYtFFCkEkBnwAb', 'nchar.北京市建国市永川崔路g座 413491', 1630000000099)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631470000001, -1952105143, -3184538982013952257, -9023, -66, 2300257.628379, -83.900999, 1, 'binary.ufpdchwhzdeEoRvElolr', 'nchar.黑龙江省通辽市高明胡街B座 971162', 1630000000099)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631470000000, 1789157808, -942637929512454815, -20552, 22, -2222313287.171970, 295235211456.737000, 0, 'binary.VznwmsilndimcCoBZmyp', 'nchar.澳门特别行政区东莞县静安杭州路m座 966043', 1630000000098)")
        tdSql.execute(f"insert into nested.stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1631485000000, -1420187501, 5901473154726700075, -22562, 122, -8503751910.519820, 2716.822481, 0, 'binary.SlMenUlCCEwyXsFKefLW', 'nchar.海南省马鞍山市兴山余路y座 674434', 1630000000099)")
        tdSql.execute(f"insert into  nested.regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631485000000, 325131812, -7109049890424943791, 905, -37, 431982153775.299011, -415064.781722, 0, 'binary.NdxijvQdCvCbloWQQDtL', 'nchar.台湾省济南市大东郭街V座 658918', 1630000000099)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631484999999, 1572668480, 108340538386194628, 29463, 54, -3679552.122859, -769.652903, 1, 'binary.qWPhdNHKBtjYxVZgdXOh', 'nchar.新疆维吾尔自治区华市翔安赵路R座 753468', 1630000000099)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631485000000, 1226876580, 7653003049181344524, 5255, 97, 2751345.497375, 35183472041.125603, 1, 'binary.qAhAfHMuKshsMmbgIOYK', 'nchar.上海市邯郸县兴山张街x座 375209', 1630000000099)")
        tdSql.execute(f"insert into nested.stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631485000001, -375162227, -4279732710928397934, -19628, -20, 38774493096.818199, 7390201646933.400391, 1, 'binary.yMDiebwbXIHZiaQKPRut', 'nchar.北京市海燕县南长合山街t座 129283', 1630000000100)")
        tdSql.execute(f"insert into nested.regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631485000001, -1188106611, -4426272680272146753, -13643, -51, 40149362212590.203125, 438738.291810, 1, 'binary.CbdGAjuBbirCzXoUVXUG', 'nchar.辽宁省宜都市新城吴街d座 246995', 1630000000100)")
        tdSql.execute(f"insert into nested.stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1631485000000, 698542868, 244085745806715589, -10836, -29, 23705559.480467, 89055463790.105301, 0, 'binary.TCkhaOCjQbElXSecegWh', 'nchar.新疆维吾尔自治区晶县秀英程街a座 168535', 1630000000099)")

        tdLog.debug("insert data ............ [OK]")
        return

    def run(self):
        tdSql.prepare()
        self.create_tables()
        self.insert_data()
        
        self.fun_to_char()
        self.fun_to_timestamp()
        
        tdSql.execute(f"flush database nested")

        wstart_res = ['2021-08-01 00:00:00.000', '2021-09-01 00:00:00.000']
        wend_res = ['2021-09-01 00:00:00.000', '2021-10-01 00:00:00.000']
        twa_res = [280745245.341775954, -162181281.012160718]
        irate_res = [112899.676866667, 46569.524533333]


        ## check twa
        tdSql.query(f"select _wstart,_wend,twa(q_int) from nested.stable_2_1 interval(1n);")
        tdSql.checkCols(3)
        tdSql.checkRows(2)

        for i in range (2):
          tdSql.checkData(i, 0, wstart_res[i]);
          tdSql.checkData(i, 1, wend_res[i]);
          tdSql.checkData(i, 2, twa_res[i]);

        tdSql.query(f"select _wstart,_wend,twa(q_int) from (select * from nested.stable_2_1) interval(1n);")
        tdSql.checkCols(3)
        tdSql.checkRows(2)

        for i in range (2):
          tdSql.checkData(i, 0, wstart_res[i]);
          tdSql.checkData(i, 1, wend_res[i]);
          tdSql.checkData(i, 2, twa_res[i]);

        tdSql.query(f"select _wstart,_wend,twa(q_int) from (select * from nested.stable_2_1 order by ts) interval(1n);")
        tdSql.checkCols(3)
        tdSql.checkRows(2)

        for i in range (2):
          tdSql.checkData(i, 0, wstart_res[i]);
          tdSql.checkData(i, 1, wend_res[i]);
          tdSql.checkData(i, 2, twa_res[i]);

        ## check irate
        tdSql.query(f"select _wstart,_wend,irate(q_int) from nested.stable_2_1 interval(1n);")
        tdSql.checkCols(3)
        tdSql.checkRows(2)

        for i in range (2):
          tdSql.checkData(i, 0, wstart_res[i]);
          tdSql.checkData(i, 1, wend_res[i]);
          tdSql.checkData(i, 2, irate_res[i]);

        tdSql.query(f"select _wstart,_wend,irate(q_int) from (select * from nested.stable_2_1) interval(1n);")
        tdSql.checkCols(3)
        tdSql.checkRows(2)

        for i in range (2):
          tdSql.checkData(i, 0, wstart_res[i]);
          tdSql.checkData(i, 1, wend_res[i]);
          tdSql.checkData(i, 2, irate_res[i]);

        tdSql.query(f"select _wstart,_wend,irate(q_int) from (select * from nested.stable_2_1 order by ts) interval(1n);")
        tdSql.checkCols(3)
        tdSql.checkRows(2)

        for i in range (2):
          tdSql.checkData(i, 0, wstart_res[i]);
          tdSql.checkData(i, 1, wend_res[i]);
          tdSql.checkData(i, 2, irate_res[i]);
          
        self.fun_to_char()
        self.fun_to_timestamp()
        self.TS_3932()
        tdSql.query(f"flush database nested;")
        self.TS_3932_flushdb()
        
    def fun_to_char(self):
        tdLog.debug("test to_char ............ [OK]")
        
        sql = "select ts,to_char(ts, 'yyyy-YYYY-MON-D-DD-DDD hh24:mi:ss.msa.m.TZH DY') from nested.stable_1 limit 2"
        tdSql.query(sql)
        tdSql.checkData(0, 1, '2021-2021-AUG-6-27-239 01:46:40.000a.m.+08 FRI');    
        tdSql.query(f"select * from (%s)"%sql)
        tdSql.checkData(0, 1, '2021-2021-AUG-6-27-239 01:46:40.000a.m.+08 FRI');      
        
        sql = "select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.msa.m.TZH DY') from nested.stable_1 limit 2"
        tdSql.query(sql)
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.m.+08 FRI');
        tdSql.query(f"select * from (%s)"%sql)
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.m.+08 FRI');
               
        sql = "select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.msa.m.tzh DY') from nested.stable_1 limit 2"
        tdSql.query(sql)
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.m.+08 FRI');
        tdSql.query(f"select * from (%s)"%sql)
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.m.+08 FRI');
        
        sql = "select ts,to_char(ts, 'yyyy-MON-D hh24:mi:ss.msa.m.TZH DY') from nested.stable_1 limit 2"
        tdSql.query(sql)
        tdSql.checkData(0, 1, '2021-AUG-6 01:46:40.000a.m.+08 FRI');
        tdSql.query(f"select * from (%s)"%sql)
        tdSql.checkData(0, 1, '2021-AUG-6 01:46:40.000a.m.+08 FRI');
        
        sql = "select ts,to_char(ts, 'yyyy-MON-DD hh24:mi:ss.msa.m.TZH DY') from nested.stable_1 limit 2"
        tdSql.query(sql)
        tdSql.checkData(0, 1, '2021-AUG-27 01:46:40.000a.m.+08 FRI');
        tdSql.query(f"select * from (%s)"%sql)
        tdSql.checkData(0, 1, '2021-AUG-27 01:46:40.000a.m.+08 FRI');
        
        sql = "select ts,to_char(ts, 'yyyy-MON-DDD hh24:mi:ss.msa.m.TZH DY') from nested.stable_1 limit 2"
        tdSql.query(sql)
        tdSql.checkData(0, 1, '2021-AUG-239 01:46:40.000a.m.+08 FRI');
        tdSql.query(f"select * from (%s)"%sql)
        tdSql.checkData(0, 1, '2021-AUG-239 01:46:40.000a.m.+08 FRI');
        
        sql = "select ts,to_char(ts, 'yyyy-MON-DDD HH24:mi:ss.msa.m.TZH DY') from nested.stable_1 limit 2"
        tdSql.query(sql)
        tdSql.checkData(0, 1, '2021-AUG-239 01:46:40.000a.m.+08 FRI');
        tdSql.query(f"select * from (%s)"%sql)
        tdSql.checkData(0, 1, '2021-AUG-239 01:46:40.000a.m.+08 FRI');
        
        sql = "select ts,to_char(ts, 'yyyy') from nested.stable_1 limit 2"
        tdSql.query(sql)
        tdSql.checkData(0, 1, '2021');
        tdSql.query(f"select * from (%s)"%sql)
        tdSql.checkData(0, 1, '2021');
        
        tdSql.query(f"select ts,to_char(ts, 'YYYY') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021');
        tdSql.query(f"select ts,to_char(ts, 'YYY') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '021');
        tdSql.query(f"select ts,to_char(ts, 'yyy') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '021');
        tdSql.query(f"select ts,to_char(ts, 'YY') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '21');
        tdSql.query(f"select ts,to_char(ts, 'yy') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '21');
        tdSql.query(f"select ts,to_char(ts, 'Y') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '1');
        tdSql.query(f"select ts,to_char(ts, 'y') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '1');
        tdSql.query(f"select * from (select ts,to_char(ts, 'y') from nested.stable_1 limit 2);")
        tdSql.checkData(0, 1, '1');
        
        
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG');
        tdSql.query(f"select * from (select ts,to_char(ts, 'yyyy-MON') from nested.stable_1 limit 2);")
        tdSql.checkData(0, 1, '2021-AUG');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-Mon') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-Aug');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-mon') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-aug');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MONTH') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUGUST   ');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-Month') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-August   ');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-month') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-august   ');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-mm') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-08');
        tdSql.query(f"select * from (select ts,to_char(ts, 'yyyy-mm') from nested.stable_1 limit 2);")
        tdSql.checkData(0, 1, '2021-08');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MM') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-08');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-DAY-mm') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-FRIDAY   -08');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-Day-mm') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-Friday   -08');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-day-mm') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-friday   -08');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-DY-MM') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-FRI-08');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-Dy-MM') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-Fri-08');
        tdSql.query(f"select * from (select ts,to_char(ts, 'yyyy-Dy-MM') from nested.stable_1 limit 2);")
        tdSql.checkData(0, 1, '2021-Fri-08');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-dy-MM') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-fri-08');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-DD-DDD') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-27-239');
        tdSql.query(f"select ts,to_char(ts, 'D-DD-DDD DY') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '6-27-239 FRI');
        tdSql.query(f"select ts,to_char(ts, 'D-DD-DDD dy') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '6-27-239 fri');
        tdSql.query(f"select ts,to_char(ts, 'D-DD-DDD DYdyDY') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '6-27-239 FRIfriFRI');
        tdSql.query(f"select * from (select ts,to_char(ts, 'D-DD-DDD DYdyDY') from nested.stable_1 limit 2);")
        tdSql.checkData(0, 1, '6-27-239 FRIfriFRI');
        tdSql.query(f"select ts,to_char(ts, '') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '');
        tdSql.query(f"select * from (select ts,to_char(ts, '') from nested.stable_1 limit 2);")
        tdSql.checkData(0, 1, '');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.msa.m') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.m');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON-D-DD-DDD HH24:mi:ss.msa.m') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.m');
        tdSql.query(f"select * from (select ts,to_char(ts, 'yyyy-MON-D-DD-DDD HH24:mi:ss.msa.m') from nested.stable_1 limit 2);")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.m');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh12:mi:ss.msa.m') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.m');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON-D-DD-DDD HH12:mi:ss.msa.m') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.m');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh:mi:ss.msa.m') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.m');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON-D-DD-DDD HH:mi:ss.msa.m') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.m');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.msp.m') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000p.m');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.msa.mp.m') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000a.mp.m');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.ms') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.us') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000000');
        tdSql.query(f"select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.ns') from nested.stable_1 limit 2;")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000000000');
        tdSql.query(f"select * from (select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.ms') from nested.stable_1 limit 2);")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000');
        tdSql.query(f"select * from (select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.us') from nested.stable_1 limit 2);")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000000');
        tdSql.query(f"select * from (select ts,to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.ns') from nested.stable_1 limit 2);")
        tdSql.checkData(0, 1, '2021-AUG-6-27-239 01:46:40.000000000');

        
    def sql_data_check(self,sql):   
        tdLog.info("\n=============sql:(%s)====================\n" %(sql)) 
        tdSql.query(sql) 
        sql1_value = tdSql.getData(0,0)
        sql2_value = tdSql.getData(0,1)
        self.value_check(sql1_value,sql2_value)       
                      
    def value_check(self,base_value,check_value):
        if base_value==check_value:
            tdLog.info(f"checkEqual success, base_value={base_value},check_value={check_value}") 
        else :
            tdLog.exit(f"checkEqual error, base_value={base_value},check_value={check_value}") 
            
    def fun_to_timestamp(self):
        tdLog.debug("test to_timestamp ............ [OK]")
        
        sql = "select ts,to_timestamp(to_char(ts, 'yyyy-YYYY-MON-D-DD-DDD hh24:mi:ss.msa.m.TZH DY'),'yyyy-YYYY-MON-D-DD-DDD hh24:mi:ss.msa.m.TZH DY') from nested.stable_1 limit 2"
        self.sql_data_check(sql)    
        sql = "select * from (%s)"%sql
        self.sql_data_check(sql)            
        
        sql = "select ts,to_timestamp(to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.msa.m.TZH DY'), 'yyyy-MON-D-DD-DDD hh24:mi:ss.msa.m.TZH DY') from nested.stable_1 limit 2"
        self.sql_data_check(sql)  
        sql = "select * from (%s)"%sql
        self.sql_data_check(sql)
               
        sql = "select ts,to_timestamp(to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.msa.m.tzh DY'), 'yyyy-MON-D-DD-DDD hh24:mi:ss.msa.m.tzh DY') from nested.stable_1 limit 2"
        self.sql_data_check(sql)  
        sql = "select * from (%s)"%sql
        self.sql_data_check(sql)
        
        sql = "select ts,to_timestamp(to_char(ts, 'yyyy-MON-DD hh24:mi:ss.msa.m.TZH DY'), 'yyyy-MON-DD hh24:mi:ss.msa.m.TZH DY') from nested.stable_1 limit 2"
        self.sql_data_check(sql)  
        sql = "select * from (%s)"%sql
        self.sql_data_check(sql)
        
        sql = "select ts,to_timestamp(to_char(ts, 'yyyy-MON-DD hh24:mi:ss.msa.m.TZH DY') , 'yyyy-MON-DD hh24:mi:ss.msa.m.TZH DY')from nested.stable_1 limit 2"
        self.sql_data_check(sql)  
        sql = "select * from (%s)"%sql
        self.sql_data_check(sql)
        
        sql = "select ts,to_timestamp(to_char(ts, 'yyyy-MON-DDD hh24:mi:ss.msa.m.TZH DY'), 'yyyy-MON-DDD hh24:mi:ss.msa.m.TZH DY') from nested.stable_1 limit 2"
        tdSql.error(sql)  
        sql = "select * from (%s)"%sql
        tdSql.error(sql)
        
        sql = "select ts,to_timestamp(to_char(ts, 'yyyy-MON-DDD HH24:mi:ss.msa.m.TZH DY'), 'yyyy-MON-DDD HH24:mi:ss.msa.m.TZH DY') from nested.stable_1 limit 2"
        tdSql.error(sql)  
        sql = "select * from (%s)"%sql
        tdSql.error(sql)
        
        sql = "select to_timestamp(to_char(ts, 'YYYY') , 'yyyy'),to_timestamp(to_char(ts, 'yyyy') , 'yyyy')from nested.stable_1 limit 2"
        self.sql_data_check(sql)  
        sql = "select * from (%s)"%sql
        self.sql_data_check(sql)
        
        sql = "select to_timestamp(to_char(ts, 'YYY') , 'yyy'),to_timestamp(to_char(ts, 'yyy') , 'yyy')from nested.stable_1 limit 2"
        self.sql_data_check(sql)  
        sql = "select * from (%s)"%sql
        self.sql_data_check(sql)
        sql = "select to_timestamp(to_char(ts, 'YY') , 'yy'),to_timestamp(to_char(ts, 'yy') , 'yy')from nested.stable_1 limit 2"
        self.sql_data_check(sql)  
        sql = "select * from (%s)"%sql
        self.sql_data_check(sql)
        sql = "select to_timestamp(to_char(ts, 'Y') , 'y'),to_timestamp(to_char(ts, 'y') , 'y')from nested.stable_1 limit 2"
        self.sql_data_check(sql)  
        sql = "select * from (%s)"%sql
        self.sql_data_check(sql)
        
        sql = "select to_timestamp(to_char(ts, 'yyyy-MONTH'), 'yyyy-Month'),to_timestamp(to_char(ts, 'yyyy-month'), 'yyyy-month') from nested.stable_1 limit 2;"
        self.sql_data_check(sql)
        sql = "select to_timestamp(to_char(ts, 'yyyy-MON'), 'yyyy-Mon'),to_timestamp(to_char(ts, 'yyyy-mon'), 'yyyy-MON') from nested.stable_1 limit 2;"
        self.sql_data_check(sql)
        
        sql = "select to_timestamp(to_char(ts, 'yyyy-MM'), 'yyyy-MM'),to_timestamp(to_char(ts, 'yyyy-mm'), 'yyyy-mm') from nested.stable_1 limit 2;"      
        self.sql_data_check(sql)
        
        sql = "select to_timestamp(to_char(ts, 'yyyy-day-mm'), 'yyyy-DAY-mm'),to_timestamp(to_char(ts, 'yyyy-DAY-mm'), 'yyyy-Day-mm') from nested.stable_1 limit 2;"      
        self.sql_data_check(sql)
        
        sql = "select to_timestamp(to_char(ts, 'yyyy-DY-MM'), 'yyyy-Dy-MM'),to_timestamp(to_char(ts, 'yyyy-DY-MM'), 'yyyy-Dy-mm') from nested.stable_1 limit 2;"      
        self.sql_data_check(sql)
        
        sql = "select * from (select to_timestamp(to_char(ts, 'D-DD-DDD DY'),'D-DD-DDD DY'),to_timestamp(to_char(ts, 'D-DD-DDD dy'),'D-DD-DDD dy') from nested.stable_1 limit 2);"
        self.sql_data_check(sql)        
        sql = "select * from (select to_timestamp(to_char(ts, 'D-DD-DDD DY'),'D-DD-DDD DY'),to_timestamp(to_char(ts, 'D-DD-DDD'),'D-DD-DDD') from nested.stable_1 limit 2);"
        self.sql_data_check(sql)        
        sql = "select * from (select to_timestamp(to_char(ts, 'D-DD-DDD DY'),'D-DD-DDD DY'),to_timestamp(to_char(ts, 'D-DD-DDD DYdyDY'),'D-DD-DDD DYdyDY') from nested.stable_1 limit 2);"
        self.sql_data_check(sql)
        sql = "select * from (select to_timestamp(to_char(ts, ''),''),to_timestamp(to_char(ts, ''),'') from nested.stable_1 limit 2);"
        self.sql_data_check(sql)
        
        sql = "select to_timestamp(to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.msa.m'), 'yyyy-MON-D-DD-DDD HH24:mi:ss.msa.m') ,\
          to_timestamp(to_char(ts, 'yyyy-MON-D-DD-DDD hh12:mi:ss.msa.m'), 'yyyy-MON-D-DD-DDD HH12:mi:ss.msa.m') from nested.stable_1 limit 2;"
        self.sql_data_check(sql)
        sql = "select to_timestamp(to_char(ts, 'yyyy-MON-D-DD-DDD hh:mi:ss.msa.m'), 'yyyy-MON-D-DD-DDD HH:mi:ss.msa.m') ,\
          to_timestamp(to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.msp.m'), 'yyyy-MON-D-DD-DDD hh24:mi:ss.msa.mp.m') from nested.stable_1 limit 2;"
        self.sql_data_check(sql)
        
        sql = "select * from (select to_timestamp(to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.ms'), 'yyyy-MON-D-DD-DDD hh24:mi:ss.ms'),\
          to_timestamp(to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.us'), 'yyyy-MON-D-DD-DDD hh24:mi:ss.us') from nested.stable_1 limit 2);"
        self.sql_data_check(sql)
        sql = "select * from (select to_timestamp(to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.ms'), 'yyyy-MON-D-DD-DDD hh24:mi:ss.ms'),\
          to_timestamp(to_char(ts, 'yyyy-MON-D-DD-DDD hh24:mi:ss.ns'), 'yyyy-MON-D-DD-DDD hh24:mi:ss.ns') from nested.stable_1 limit 2);"
        self.sql_data_check(sql)
          
    def TS_3932(self):
        tdLog.debug("test insert data into stable")
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname having count(*)>0 order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 100)
        tdSql.checkData(1, 1, 200)
                
        tdSql.query(f"insert into nested.stable_1 (ts,tbname) values(now,'stable_1_1');")
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 101)
        tdSql.checkData(1, 1, 200)
               
        qlist = ['q_int', 'q_bigint', 'q_smallint', 'q_tinyint', 'q_float', 'q_double', 'q_bool', 'q_binary', 'q_nchar', 'q_ts']
        for i in range(10):
            coulmn_name =  qlist[i]
            tdSql.execute(f"insert into nested.stable_1 (ts, tbname, {coulmn_name}) values(now+{i}s,'stable_1_1',1);")
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;",queryTimes=5)
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 111)
        tdSql.checkData(1, 1, 200)

        q_null_list = ['q_int_null', 'q_bigint_null', 'q_smallint_null', 'q_tinyint_null', 'q_float_null', 'q_double_null', 'q_bool_null', 'q_binary_null', 'q_nchar_null', 'q_ts_null']
        for i in range(10):
            coulmn_name =  q_null_list[i]
            tdSql.execute(f"insert into nested.stable_1 (ts, tbname, {coulmn_name}) values(now+{i}s,'stable_1_1',1);")       
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;",queryTimes=5)
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 121)
        tdSql.checkData(1, 1, 200)
                       
                       
        tdSql.query(f"insert into nested.stable_null_data (ts,tbname) values(now,'stable_null_data_1');")
        tdSql.query(f"select tbname,count(*) from nested.stable_null_data_1 group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        for i in range(10):
            coulmn_name =  qlist[i]
            tdSql.execute(f"insert into nested.stable_null_data (ts, tbname, {coulmn_name}) values(now+{i}s,'stable_null_data_1',1);")                        

        tdSql.query(f"select tbname,count(*) from nested.stable_null_data group by tbname order by tbname;",queryTimes=5)
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 11)

        for i in range(10):
            coulmn_name =  q_null_list[i]
            tdSql.execute(f"insert into nested.stable_null_data (ts, tbname, {coulmn_name}) values(now+{i}s,'stable_null_data_1',1);")                       

        tdSql.query(f"select tbname,count(*) from nested.stable_null_data group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 21)
        
        tdSql.query(f"insert into nested.stable_null_childtable (ts,tbname) values(now,'stable_null_childtable_1');")
        tdSql.query(f"select tbname,count(*) from nested.stable_null_childtable group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        for i in range(10):
            coulmn_name =  qlist[i]
            tdSql.execute(f"insert into nested.stable_null_childtable (ts, tbname, {coulmn_name}) values(now+{i}s,'stable_null_childtable_1',1);")    
        tdSql.query(f"select tbname,count(*) from nested.stable_null_childtable group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 11)

        for i in range(10):
            coulmn_name =  q_null_list[i]
            tdSql.execute(f"insert into nested.stable_null_childtable (ts, tbname, {coulmn_name}) values(now+{i}s,'stable_null_childtable_1',1);")           
        tdSql.query(f"select tbname,count(*) from nested.stable_null_childtable group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 21)
        
    def TS_3932_flushdb(self):
        tdLog.debug("test flush db and insert data into stable")
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 121)
        tdSql.checkData(1, 1, 200)

        qlist = ['q_int', 'q_bigint', 'q_smallint', 'q_tinyint', 'q_float', 'q_double', 'q_bool', 'q_binary', 'q_nchar', 'q_ts']
        q_null_list = ['q_int_null', 'q_bigint_null', 'q_smallint_null', 'q_tinyint_null', 'q_float_null', 'q_double_null', 'q_bool_null', 'q_binary_null', 'q_nchar_null', 'q_ts_null']                        
        tdSql.query(f"insert into nested.stable_1 (ts,tbname) values(now,'stable_1_1');")
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 122)
        tdSql.checkData(1, 1, 200)

        pd = datetime.datetime.now()
        ts = int(datetime.datetime.timestamp(pd)*1000 - 10000)
        print(f"start time {ts}")

        for i in range(10):
            coulmn_name =  qlist[i]
            tdSql.execute(f"insert into nested.stable_1 (ts, tbname, {coulmn_name}) values({ts+i},'stable_1_1',1);")
        ts = ts + 20
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 132)
        tdSql.checkData(1, 1, 200)

        for i in range(10):
            coulmn_name =  q_null_list[i]
            tdSql.execute(f"insert into nested.stable_1 (ts, tbname, {coulmn_name}) values({ts+i},'stable_1_1',1);")
        ts = ts + 20
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 142)
        tdSql.checkData(1, 1, 200)
                       
        tdSql.execute(f"insert into nested.stable_1 (ts,tbname,q_int) values({ts},'stable_1_1',1) \
                      nested.stable_1 (ts,tbname,q_bigint) values({ts+1},'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_smallint) values({ts+2},'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_tinyint) values({ts+3},'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_float) values({ts+4},'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_double) values({ts+5},'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_bool) values({ts+6},'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_binary) values({ts+7},'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_nchar) values({ts+8},'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_ts) values({ts+9},'stable_1_1',1);")
        ts = ts + 20
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 152);
        tdSql.checkData(1, 1, 200);
                       
        tdSql.query(f"insert into nested.stable_null_data (ts,tbname) values({ts},'stable_null_data_1');")
        ts = ts + 1
        tdSql.query(f"select tbname,count(*) from nested.stable_null_data_1 group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 22);

        for i in range(10):
            coulmn_name =  qlist[i]
            tdSql.execute(f"insert into nested.stable_null_data (ts, tbname, {coulmn_name}) values({ts+i},'stable_null_data_1',1);")
        ts = ts + 20
        tdSql.query(f"select tbname,count(*) from nested.stable_null_data group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 32)

        for i in range(10):
            coulmn_name =  q_null_list[i]
            tdSql.execute(f"insert into nested.stable_null_data (ts, tbname, {coulmn_name}) values({ts+i},'stable_null_data_1',1);")                
        ts = ts + 20
        tdSql.query(f"select tbname,count(*) from nested.stable_null_data group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 42)
         
        tdSql.query(f"insert into nested.stable_null_data (ts,tbname,q_int) values({ts},'stable_null_data_1',1) \
                      nested.stable_null_data (ts,tbname,q_bigint) values({ts+1},'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_smallint) values({ts+2},'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_tinyint) values({ts+3},'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_float) values({ts+4},'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_double) values({ts+5},'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_bool) values({ts+6},'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_binary) values({ts+7},'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_nchar) values({ts+8},'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_ts) values({ts+9},'stable_null_data_1',1);")
        ts = ts + 20
        tdSql.query(f"select tbname,count(*) from nested.stable_null_data group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 52);
        
        
        
        tdSql.query(f"insert into nested.stable_null_childtable (ts,tbname) values({ts},'stable_null_childtable_1');")
        ts = ts + 1
        tdSql.query(f"select tbname,count(*) from nested.stable_null_childtable group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 22)

        for i in range(10):
            coulmn_name =  qlist[i]
            tdSql.execute(f"insert into nested.stable_null_childtable (ts, tbname, {coulmn_name}) values({ts+i},'stable_null_childtable_1',1);")
        ts = ts + 20
        tdSql.query(f"select tbname,count(*) from nested.stable_null_childtable group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 32)

        for i in range(10):
            coulmn_name =  q_null_list[i]
            tdSql.execute(f"insert into nested.stable_null_childtable (ts, tbname, {coulmn_name}) values({ts+i},'stable_null_childtable_1',1);")
        ts = ts + 20
        tdSql.query(f"select tbname,count(*) from nested.stable_null_childtable group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 42);
         
        tdSql.query(f"insert into nested.stable_null_childtable (ts,tbname,q_int) values({ts},'stable_null_childtable_1',1) \
                      nested.stable_null_childtable (ts,tbname,q_bigint) values({ts+1},'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_smallint) values({ts+2},'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_tinyint) values({ts+3},'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_float) values({ts+4},'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_double) values({ts+5},'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_bool) values({ts+6},'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_binary) values({ts+7},'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_nchar) values({ts+8},'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_ts) values({ts+9},'stable_null_childtable_1',1);")
        ts = ts + 20
        tdSql.query(f"select tbname,count(*) from nested.stable_null_childtable group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 52)
        
        
        #stables
        tdSql.query(f"insert into nested.stable_1 (ts,tbname,q_int) values({ts},'stable_1_1',1) \
                      nested.stable_1 (ts,tbname,q_bigint) values({ts}+1a,'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_smallint) values({ts}+2a,'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_tinyint) values({ts}+3a,'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_float) values({ts}+4a,'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_double) values({ts}+5a,'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_bool) values({ts}+6a,'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_binary) values({ts}+7a,'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_nchar) values({ts}+8a,'stable_1_1',1)\
                      nested.stable_1 (ts,tbname,q_ts) values({ts}+9a,'stable_1_1',1)\
                      nested.stable_null_data (ts,tbname,q_int) values({ts},'stable_null_data_1',1) \
                      nested.stable_null_data (ts,tbname,q_bigint) values({ts}+1a,'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_smallint) values({ts}+2a,'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_tinyint) values({ts}+3a,'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_float) values({ts}+4a,'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_double) values({ts}+5a,'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_bool) values({ts}+6a,'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_binary) values({ts}+7a,'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_nchar) values({ts}+8a,'stable_null_data_1',1)\
                      nested.stable_null_data (ts,tbname,q_ts) values({ts}+9a,'stable_null_data_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_int) values({ts},'stable_null_childtable_1',1) \
                      nested.stable_null_childtable (ts,tbname,q_bigint) values({ts}+1a,'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_smallint) values({ts}+2a,'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_tinyint) values({ts}+3a,'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_float) values({ts}+4a,'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_double) values({ts}+5a,'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_bool) values({ts}+6a,'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_binary) values({ts}+7a,'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_nchar) values({ts}+8a,'stable_null_childtable_1',1)\
                      nested.stable_null_childtable (ts,tbname,q_ts) values({ts}+9a,'stable_null_childtable_1',1);")
        
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 1, 162)
        tdSql.checkData(1, 1, 200)
        
        tdSql.query(f"select tbname,count(*) from nested.stable_null_data group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 62)
        
        tdSql.query(f"select tbname,count(*) from nested.stable_null_childtable group by tbname order by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 62)
        
        #test special character

        tdSql.query(f"insert into nested.stable_1 (ts,tbname,q_int) values({ts}+10a,'!@!@$$^$',1) \
                      nested.stable_null_data (ts,tbname,q_int) values({ts}+10a,'%^$^&^&',1) \
                      nested.stable_null_childtable (ts,tbname,q_int) values({ts}+10a,'$^%$%^&',1);")
        
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;")
        tdSql.checkRows(7)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 162)
        tdSql.checkData(2, 1, 200)
        tdSql.query(f"select count(*) from nested.stable_1 where tbname ='!@!@$$^$' ;")
        tdSql.checkData(0, 0, 1)
        
        tdSql.query(f"select tbname,count(*) from nested.stable_null_data group by tbname order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 62)
        tdSql.query(f"select count(*) from nested.stable_null_data where tbname ='%^$^&^&' ;")
        tdSql.checkData(0, 0, 1)
        
        tdSql.query(f"select tbname,count(*) from nested.stable_null_childtable group by tbname order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 62)
        tdSql.query(f"select count(*) from nested.stable_null_childtable where tbname ='$^%$%^&' ;")
        tdSql.checkData(0, 0, 1)
        
        #test csv
        sql1 = "select tbname,ts,q_int,q_binary from nested.stable_1 >>'%s/stable_1.csv';" %self.testcasePath
        sql2 = "select tbname,ts,q_int,q_binary from nested.stable_null_data >>%s/stable_null_data.csv;" %self.testcasePath
        sql3 = "select tbname,ts,q_int,q_binary from nested.stable_null_childtable >>%s/stable_null_childtable.csv;" %self.testcasePath
        
        os.system("taos -s '%s'" %sql1)       
        os.system("taos -s '%s'" %sql2)
        os.system("taos -s '%s'" %sql3)
        
        tdSql.query(f"delete from nested.stable_1;")
        tdSql.query(f"delete from nested.stable_null_data;")
        tdSql.query(f"delete from nested.stable_null_childtable;")
        tdSql.query(f"insert into nested.stable_1(tbname,ts,q_int,q_binary) file '{self.testcasePath}/stable_1.csv'\
                      nested.stable_null_data(tbname,ts,q_int,q_binary) file '{self.testcasePath}/stable_null_data.csv'\
                      nested.stable_null_childtable(tbname,ts,q_int,q_binary) file '{self.testcasePath}/stable_null_childtable.csv';")
        
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;")
        tdSql.checkRows(7)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 162)
        tdSql.checkData(2, 1, 200)
        tdSql.query(f"select count(*) from nested.stable_1 where tbname ='!@!@$$^$' ;")
        tdSql.checkData(0, 0, 1)
        tdSql.query(f"select count(q_bool) from nested.stable_1;")
        tdSql.checkData(0, 0, 0)
        
        tdSql.query(f"select tbname,count(*) from nested.stable_null_data group by tbname order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 62)
        tdSql.query(f"select count(*) from nested.stable_null_data where tbname ='%^$^&^&' ;")
        tdSql.checkData(0, 0, 1)
        tdSql.query(f"select count(q_bool) from nested.stable_null_data;")
        tdSql.checkData(0, 0, 0)
        
        tdSql.query(f"select tbname,count(*) from nested.stable_null_childtable group by tbname order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 62)
        tdSql.query(f"select count(*) from nested.stable_null_childtable where tbname ='$^%$%^&' ;")
        tdSql.checkData(0, 0, 1)
        tdSql.query(f"select count(q_bool) from nested.stable_null_childtable;")
        tdSql.checkData(0, 0, 0)
        
        
        tdSql.query(f"delete from nested.stable_1;")
        tdSql.query(f"delete from nested.stable_null_data;")
        tdSql.query(f"delete from nested.stable_null_childtable;")
        tdSql.query(f"insert into nested.stable_1(tbname,ts,q_int,q_binary) file '{self.testcasePath}/stable_1.csv';")
        tdSql.query(f"insert into nested.stable_null_data(tbname,ts,q_int,q_binary) file '{self.testcasePath}/stable_null_data.csv';")
        tdSql.query(f"insert into nested.stable_null_childtable(tbname,ts,q_int,q_binary) file '{self.testcasePath}/stable_null_childtable.csv';")
        
        tdSql.query(f"select tbname,count(*) from nested.stable_1 group by tbname order by tbname;")
        tdSql.checkRows(7)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 162)
        tdSql.checkData(2, 1, 200)
        tdSql.query(f"select count(*) from nested.stable_1 where tbname ='!@!@$$^$' ;")
        tdSql.checkData(0, 0, 1)
        tdSql.query(f"select count(q_bool) from nested.stable_1;")
        tdSql.checkData(0, 0, 0)
        
        tdSql.query(f"select tbname,count(*) from nested.stable_null_data group by tbname order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 62)
        tdSql.query(f"select count(*) from nested.stable_null_data where tbname ='%^$^&^&' ;")
        tdSql.checkData(0, 0, 1)
        tdSql.query(f"select count(q_bool) from nested.stable_null_data;")
        tdSql.checkData(0, 0, 0)
        
        tdSql.query(f"select tbname,count(*) from nested.stable_null_childtable group by tbname order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 62)
        tdSql.query(f"select count(*) from nested.stable_null_childtable where tbname ='$^%$%^&' ;")
        tdSql.checkData(0, 0, 1)
        tdSql.query(f"select count(q_bool) from nested.stable_null_childtable;")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select tbname,count(*) from nested.stable_1 where ts is not null group by tbname order by tbname;")
        tdSql.checkRows(7)
        tdSql.query(f"select tbname,count(*) from nested.stable_1 where ts is null group by tbname order by tbname;")
        tdSql.checkRows(7)
        tdSql.query(f"select tbname,last(*) from nested.stable_1 where ts is not null group by tbname order by tbname;")
        tdSql.checkRows(3)
        tdSql.query(f"select tbname,last(*) from nested.stable_1 where ts is null group by tbname order by tbname;")
        tdSql.checkRows(0)
        
        #test stable 

        tdSql.error(f"insert into nested.stable_1 (ts,tbname,q_int) values({ts},'stable_1',1) \
                      nested.stable_null_data (ts,tbname,q_int) values({ts},'stable_null_data',1) \
                      nested.stable_null_childtable (ts,tbname,q_int) values({ts},'stable_null_childtable',1);")
        
    
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
