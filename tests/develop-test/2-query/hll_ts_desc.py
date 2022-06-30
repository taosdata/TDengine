import sys 
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf

class TDTestCase:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TD-15259] state window order by ts desc 
        ''' 
        return
    
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn
        
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use hll_ts_desc")

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists hll_ts_desc")
        tdSql.execute("create database if not exists hll_ts_desc")
        tdSql.execute('use hll_ts_desc')
        tdSql.execute('create stable stable_1 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                 q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)                 tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);')

        tdSql.execute('create stable stable_2 (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                 q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)                 tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);')

        tdSql.execute('create stable stable_null_data (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                 q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)                 tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);')

        tdSql.execute('create stable stable_null_childtable (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                 q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp)                 tags(loc nchar(100) , t_int int , t_bigint bigint , t_smallint smallint , t_tinyint tinyint, t_bool bool , t_binary binary(100) , t_nchar nchar(100) ,t_float float , t_double double , t_ts timestamp);')

        tdSql.execute("create table stable_1_1 using stable_1 tags('stable_1_1', '0' , '0' , '0' , '0' , 0 , 'binary1' , 'nchar1' , '0' , '0' ,'0') ;")

        tdSql.execute("create table stable_1_2 using stable_1 tags('stable_1_2', '2147483647' , '9223372036854775807' , '32767' , '127' , 1 , 'binary2' , 'nchar2' , '2' , '22' , '1999-09-09 09:09:09.090') ;")

        tdSql.execute("create table stable_1_3 using stable_1 tags('stable_1_3', '-2147483647' , '-9223372036854775807' , '-32767' , '-127' , false , 'binary3' , 'nchar3nchar3' , '-3.3' , '-33.33' , '2099-09-09 09:09:09.090') ;")

        tdSql.execute("create table stable_1_4 using stable_1 tags('stable_1_4', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;")

        tdSql.execute("create table stable_2_1 using stable_2 tags('stable_2_1' , '0' , '0' , '0' , '0' , 0 , 'binary21' , 'nchar21' , '0' , '0' ,'0') ;")

        tdSql.execute("create table stable_2_2 using stable_2 tags('stable_2_2' , '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;")

        tdSql.execute("create table stable_null_data_1 using stable_null_data tags('stable_null_data_1', '0' , '0' , '0' , '0' , 0 , '0' , '0' , '0' , '0' ,'0') ;")

        tdSql.execute('create table regular_table_1                     (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                     q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;')

        tdSql.execute('create table regular_table_2                     (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                     q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;')

        tdSql.execute('create table regular_table_3                     (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                     q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;')

        tdSql.execute('create table regular_table_null                     (ts timestamp , q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , q_bool bool , q_binary binary(100) , q_nchar nchar(100) , q_ts timestamp ,                     q_int_null int , q_bigint_null bigint , q_smallint_null smallint , q_tinyint_null tinyint, q_float_null float , q_double_null double , q_bool_null bool , q_binary_null binary(20) , q_nchar_null nchar(20) , q_ts_null timestamp) ;')

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000000000, 1838292771, -1436446954495651682, -27924, 43, 62578.538002, -4714655.396591, 0, 'binary.DqeXfMBjSMNUUUfCrZfG', 'nchar.北京市东市怀柔沈阳街N座 179042', 1630000000000) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000000000, -826619451, 9181809009278567933, -16684, 61, 0.236378, -114369.856622, 0, 'binary.bWBbbkcDlRqncMhiwBYK', 'nchar.香港特别行政区天津市翔安重庆路E座 315317', 1630000000000) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000000000, 260956358, 1052364796672509236, 31819, 111, 31067.458123, -116677.622733, 1, 'binary.KVsHFEIVLmKzfxdIhUZJ', 'nchar.香港特别行政区潜江市龙潭太原街K座 687258', 1630000000000) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000000000, 372344514, 4157755888120390423, 24142, 75, -27570730970922.101562, -7630281557.405780, 1, 'binary.msVgDYgjidqgEnqVzReU', 'nchar.海南省北京县璧山拉萨街d座 794889', 1630000000000) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000000001, -1801676948, -2126817846347164667, -23892, -96, -365976443802.140015, 58277248.742673, 1, 'binary.eqYHSWoLPxVKTJFRttEt', 'nchar.北京市合肥市南长刘路e座 228866', 1630000000001) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000000001, -569538618, -6021773662758445336, -25027, -127, -3152824259748.759766, 29708624719738.500000, 1, 'binary.OIjfILerjtFxcsnHraJx', 'nchar.安徽省成都县江北武汉街x座 898605', 1630000000001) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000000000, 606708567, -4981939489236458511, -20084, -9, 756910800.101722, 8534441.920926, 0, 'binary.aKccCxuWwqSkyixYhNTp', 'nchar.海南省丹市孝南张家港路N座 743181', 1630000000000) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000001000, -1939455920, -5033466346269591844, -22907, 109, -860.723329, -88833934229611.500000, 0, 'binary.eGwhetxOrBPpppPHseSp', 'nchar.贵州省俊县友好赵街x座 614294', 1630000000001) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000001000, -1810269728, 6973688127696505144, 22941, -91, -52684129623692.398438, -7946160.598174, 0, 'binary.yWfYAUYaYSYxnUYBrbwF', 'nchar.河北省兰州市蓟州福州街n座 536731', 1630000000001) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000001000, 1472672843, 5756167348843965720, 20406, 114, 87186576.859021, -6318.622398, 1, 'binary.aXmuvDUJMZIlWqCJdBzX', 'nchar.福建省西宁县蓟州嘉禾街y座 621451', 1630000000001) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000001000, 1154349808, 3243625422411806826, 17919, 11, 189.594838, -8977061888290.900391, 1, 'binary.BATIWjDNckwdldNisrLx', 'nchar.广西壮族自治区宜都市东丽施路n座 503014', 1630000000001) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000001001, -1737177100, -7137625062669171268, -11019, -112, -5263119.207533, 11706.409767, 1, 'binary.MehpLLalRPRaaKnKsXZe', 'nchar.天津市刚市城东杭州街k座 904482', 1630000000002) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000001001, -636550367, -4654057644900281081, -30451, -107, 584346571.352422, -9192269.158940, 1, 'binary.AUUKjMePFgCJYGmmgRtr', 'nchar.吉林省巢湖市璧山罗路f座 841971', 1630000000002) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000001000, 1498397170, -8716247451614136246, 14654, -25, 977.952902, -52812.755788, 0, 'binary.TeyPqABQnocliqgeeEBo', 'nchar.重庆市汕尾市永川杨街O座 527893', 1630000000001) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000002000, -492569513, -131231180062243872, 18670, 71, 690185611707.464966, -209437269965.459991, 0, 'binary.MuowZpwbMxtEjpYuPBUK', 'nchar.天津市西安县房山潜江街u座 642663', 1630000000002) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000002000, 1281523919, -7635613027006605768, 6953, 102, 87.588982, 8217489340933.730469, 0, 'binary.dKBxwMRxScEyhUxhlqvl', 'nchar.海南省莉县梁平乌鲁木齐路y座 810318', 1630000000002) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000002000, 783955626, 1927364805357976734, 17706, 69, -5056259617517.320312, -73486242922592.906250, 1, 'binary.fIsnTHTFQBEPmRQnUAUt', 'nchar.内蒙古自治区沈阳县秀英关岭街v座 999519', 1630000000002) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000002000, 2086836442, 6181136079795961343, 5101, 3, -3.305533, 98569707838955.703125, 1, 'binary.cqzlthAWOUpUcPacwArn', 'nchar.西藏自治区荆门县平山澳门街A座 438618', 1630000000002) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000002001, -1617231229, -8650590129984301895, -1065, -95, 279.997018, 1.660429, 1, 'binary.pWGmBHCnuGloOHTgVPJk', 'nchar.天津市淑兰县长寿惠州街b座 890926', 1630000000003) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000002001, -687771611, -5737906531498081610, -10890, -38, -27536249453354.898438, -2518159239934.520020, 1, 'binary.AXJGPViEmsAOLwvNTutk', 'nchar.宁夏回族自治区志强市六枝特南昌路d座 955322', 1630000000003) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000002000, 539695015, 8972029075123402435, 2389, 0, -796858267675.152954, 810.513574, 0, 'binary.LjKYwdWnfbpVtTspvztV', 'nchar.福建省北镇县吉区邢路k座 461631', 1630000000002) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000003000, -1658222497, 7647963269999057690, 4036, -92, 28672058.268912, 43478320606413.398438, 0, 'binary.zeTbWjANfcKnvVTQuOTz', 'nchar.甘肃省倩市海港吴街l座 775142', 1630000000003) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000003000, -2022561375, 245132469384557666, 4366, -6, -1099.499755, 6458.437826, 0, 'binary.zcsIBNYdozDmTLgwjDPP', 'nchar.天津市帆县海陵严街u座 829723', 1630000000003) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000003000, 970847584, 7420877040204393665, 9281, 94, -964519485916.422974, -252.341910, 1, 'binary.ZEveuoOJgocbtMwzhypX', 'nchar.贵州省通辽县海陵荆门街z座 754782', 1630000000003) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000003000, 1054240308, 3276196953787147368, 5850, 52, 1.530455, -51913387.255388, 1, 'binary.gJhFHNnTGsdRqPJfBPaC', 'nchar.重庆市桂兰县丰都成都街p座 353574', 1630000000003) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000003001, -1803292448, -6799524517938422788, -7283, -88, 331.944869, 9.284399, 1, 'binary.mvjmgpjGSYKrlmMTBymm', 'nchar.辽宁省潜江县璧山兴安盟街v座 926951', 1630000000004) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000003001, -1500364803, -7549808624027348898, -28481, -76, -8540563.903054, -790980018.406773, 1, 'binary.UPoGPmgQbfsCeQrQASaT', 'nchar.香港特别行政区荣市白云石街h座 912669', 1630000000004) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000003000, 2049166237, -8246587740307975912, 27620, -65, -68424427.395031, 8351661455.260000, 0, 'binary.YOOcElJqSOyVodVgOwIS', 'nchar.安徽省天津市沈北新朱街a座 965085', 1630000000003) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000004000, 711729882, 1211262563740740101, -11472, 106, 26335.210294, 62467.767506, 0, 'binary.rfHqHNZTWDvPjChSKiSj', 'nchar.贵州省凤英县普陀石家庄街U座 737126', 1630000000004) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000004000, -1719122186, -468614713244790696, 28225, 27, -65899275756.352997, -270957963.116336, 0, 'binary.spjrFlpPXUBIZpAudfYN', 'nchar.重庆市慧县浔阳谢街g座 309829', 1630000000004) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000004000, 780970014, 6941952758523959930, 11537, 121, -56.225718, -56.986864, 1, 'binary.TlRLsUeGNtxdLvOxzxgz', 'nchar.福建省梧州县魏都白街H座 438478', 1630000000004) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000004000, 1190531055, 4782535958786739061, 28925, 93, 266.400366, 35694014265.612396, 1, 'binary.YGQnnSTPqFfqjRarbaFA', 'nchar.四川省淮安市南长齐齐哈尔路j座 805409', 1630000000004) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000004001, -1917324885, -713176828635674325, -20527, -14, 892328348.978370, 969028.940242, 1, 'binary.SArHajFHsAmEtCNfmlKG', 'nchar.河南省淑珍县南湖王路B座 564233', 1630000000005) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000004001, -943696389, -8958130008417416167, -10931, -65, -2437011398757.890137, 1190.438000, 1, 'binary.GzirRNAvqOuzqChOcYGe', 'nchar.吉林省大冶市花溪姚路A座 604583', 1630000000005) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000004000, -924083242, -4958999442504210517, -26676, 91, 947472439.845608, -58271.112609, 0, 'binary.svaGwDdEltVkFlJoVNnt', 'nchar.澳门特别行政区云县永川齐齐哈尔街e座 412894', 1630000000004) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000005000, -1402429144, 6445963121697899534, -4925, -102, -5653809997177.490234, 99845133562.445801, 0, 'binary.xkGKpOqbOtagXpTmXFJA', 'nchar.山西省哈尔滨市静安重庆路G座 673198', 1630000000005) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000005000, 2058582005, 394021709635751129, -10257, 35, 5.983187, 8220272997.347670, 0, 'binary.tWTCFTyzGpfNbhGFkDAE', 'nchar.天津市汕尾市蓟州阜新路a座 443668', 1630000000005) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000005000, 574102632, 4346447808884744436, 13973, 40, 4.622582, -598777.875349, 1, 'binary.mETmhXplABNknqjcQKjI', 'nchar.广西壮族自治区六安市普陀北镇路l座 853012', 1630000000005) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000005000, 537120771, 5940564582509694821, 25602, 118, -3752256.856203, -2555756841436.479980, 1, 'binary.QWkeDHUZmxpfdRQLmzmq', 'nchar.河北省欢市永川阜新街z座 556902', 1630000000005) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000005001, -1826063371, -4482492149225470413, -27522, -112, 73878181.125833, -281459.312793, 1, 'binary.ACsstFQTkVKxCjTxkVlJ', 'nchar.浙江省雷县淄川郑路c座 975893', 1630000000006) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000005001, -247255143, -4077602445976948207, -15808, -32, -575985933.271966, -550027099175.355957, 1, 'binary.lEMpgZRBZwciyBqlRtnp', 'nchar.河南省帆县东丽汤路f座 762185', 1630000000006) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000005000, -206271078, 6608258443449533766, 16905, 56, 7.397869, 369239.190702, 0, 'binary.QmJldHhwclgxcSwiwlwU', 'nchar.内蒙古自治区台北市沙市汕尾路t座 617190', 1630000000005) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000006000, 702751120, -3054109820716502632, 6815, 11, 11.159470, 349513236.940497, 0, 'binary.vBzihTMyxtEWjjOikTIR', 'nchar.辽宁省萍县金平邯郸街u座 954293', 1630000000006) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000006000, -555735421, 8359786394427387680, 901, -19, 9122039779.699900, -398341626.862251, 0, 'binary.njZddhtOpIEdEyENHeyx', 'nchar.安徽省磊市大东汪路h座 722007', 1630000000006) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000006000, 1523354450, 8588975648975026627, 19935, 110, 16830828.924155, 9592403.407070, 1, 'binary.obhtnWjvbEgpLwsEnBUW', 'nchar.甘肃省邯郸县涪城福州路V座 311893', 1630000000006) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000006000, 2118276328, 6930827244806523318, 21205, 87, 78655680247661.203125, 8341963089891.459961, 1, 'binary.vOClPIdvakMPixtANyxP', 'nchar.新疆维吾尔自治区玲县大东徐街C座 398308', 1630000000006) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000006001, -1573875804, -1696905991292164866, -17463, -111, 2010239.747114, 7.526200, 1, 'binary.qqRjmLydfmJooJfspKTP', 'nchar.陕西省石家庄市平山昆明街b座 543268', 1630000000007) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000006001, -1211609231, -1321473527175162626, -4808, -127, -9820.207254, -716761600.933264, 1, 'binary.bHiLbfAsSZsBexsJIUCe', 'nchar.河北省欣市兴山汕尾街v座 578957', 1630000000007) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000006000, 204466945, -2329637195935268731, -7385, -76, -58626491.222563, 2.135034, 0, 'binary.pAdydjnsMLECMCXvFZuN', 'nchar.甘肃省荆门市东城杨街i座 820397', 1630000000006) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000007000, -737461969, 1061198755171560900, -22347, 45, -26152.376048, -3009.304726, 0, 'binary.rEDYJKNGMSytOYJdGfNm', 'nchar.黑龙江省红霞市萧山吴街d座 698442', 1630000000007) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000007000, 648860226, 7599547578401789754, -28804, -48, 5.144602, 867514141436.572998, 0, 'binary.JkzuTYiQUgtJozTobazr', 'nchar.台湾省济南市牧野赵街C座 479057', 1630000000007) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000007000, 536827311, 3725559326145593607, 14057, 89, 38395463934836.000000, -4833944259955.169922, 1, 'binary.ngWFPEqxFJnPkSCQnsqt', 'nchar.四川省乌鲁木齐县沙湾长春路v座 418846', 1630000000007) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000007000, 348899162, 3017684102110887359, 32765, 3, 21.993553, -54565470131.376099, 1, 'binary.gvZCYnUzEcKfAVGzwNzy', 'nchar.香港特别行政区红霞市滨城余街Y座 326550', 1630000000007) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000007001, -231415558, -2607043220158644759, -5074, -96, 57027.968587, -141.494817, 1, 'binary.KPqgmnUvqVDklSIKYsCI', 'nchar.重庆市汕尾市山亭邯郸路E座 410204', 1630000000008) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000007001, -1946870514, -7327923975037763177, -11533, -111, 17486925.201162, 6118013578.713690, 1, 'binary.SaavIaDtJJTCaOmDfoDj', 'nchar.湖南省济南市大兴佛山路G座 737248', 1630000000008) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000007000, 2039746262, -5934313200840780037, -4979, -103, 5.479286, -39281614.439066, 0, 'binary.dwxLqGJnSPCPSTtjduIR', 'nchar.澳门特别行政区郑州县白云阜新路Y座 206776', 1630000000007) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000008000, 1218822064, -2828166007717509981, 10606, -71, -71.167301, -77121006.691649, 0, 'binary.ZORawqaKMCRDSSAIIOEg', 'nchar.黑龙江省邯郸市浔阳惠州街c座 116296', 1630000000008) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000008000, 893801163, -5199547311937685839, 3675, 98, -420387.505673, 92.866850, 0, 'binary.YVMJlbHeioCuahXDmACF', 'nchar.青海省佛山县朝阳合肥街s座 933976', 1630000000008) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000008000, 1756918974, 6424846466828790828, 2961, 104, 55409969.702847, 4416604.779920, 1, 'binary.HMVyDLtxwbHzsYDOqdKU', 'nchar.澳门特别行政区上海县清城呼和浩特路i座 770301', 1630000000008) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000008000, 711229222, 1819200463431512256, 19131, 114, -32826420216464.101562, -86474.150428, 1, 'binary.GWnxeZgeUAvkxyKbeQYA', 'nchar.福建省晶市牧野汕尾街W座 959273', 1630000000008) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000008001, -242517182, -7605954271881835278, -26326, -87, 490667781659.344971, -8851.548402, 1, 'binary.PsqlqiUtlLxIDEzixIuh', 'nchar.福建省通辽县高坪曲路o座 368800', 1630000000009) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000008001, -2003794371, -8710567649853252432, -7636, -85, -239.646330, 4924171367084.299805, 1, 'binary.GRXdzHEjataJbzaJvcrQ', 'nchar.江苏省永安县翔安关岭路q座 242147', 1630000000009) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000008000, 1676252847, 7682335554097376186, -7249, -123, -122056.493308, -317718990.604781, 0, 'binary.wzORsvBGDMvhenRTtlvU', 'nchar.辽宁省金凤市江北福州路k座 964138', 1630000000008) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000009000, -1251131687, 3695350831871215911, 15650, 14, -369780185793.552979, 7854845634770.700195, 0, 'binary.rdQnkoayYkknXwmKzWnH', 'nchar.甘肃省玉珍县西峰银川街x座 116835', 1630000000009) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000009000, -922630313, 1522261834894023521, -14070, 6, 184232.798782, 16054.678519, 0, 'binary.XCZDopMuoVzRnizXhwzy', 'nchar.湖北省张家港市城北王路Z座 659506', 1630000000009) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000009000, 1565359867, 3903153455790361791, 28308, 23, 42275426.319100, -145549786902.829010, 1, 'binary.aQHdNMauCwRjFxlWVNKY', 'nchar.黑龙江省六盘水县龙潭深圳路C座 356671', 1630000000009) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000009000, 2067816943, 4873679448427818931, 11460, 22, 417.394522, -47682.136013, 1, 'binary.CkyGGQAkByAFrcRyMIEq', 'nchar.台湾省成都市江北孙路c座 788104', 1630000000009) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000009001, -1949786335, -1291297326647067044, -10920, -52, 1454357924348.959961, -726.634673, 1, 'binary.gMcKadDAUnLGwxhlRAAx', 'nchar.天津市婷婷市丰都台北路x座 890385', 1630000000010) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000009001, -1702438409, -7913252316307543245, -23, -118, 199064.950141, -7174.295854, 1, 'binary.jmwCXMaaTDNvRowdjuFd', 'nchar.新疆维吾尔自治区芳县沈北新蒋街y座 962963', 1630000000010) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000009000, 419570483, -3235176134274873949, 7365, -95, 285057897765.927979, -6130437.723892, 0, 'binary.uJBkwcAkQYGkbWGhtvaS', 'nchar.河北省深圳市六枝特欧路m座 700980', 1630000000009) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000010000, -68163813, 7649468357177711556, -28878, 81, 22249721391119.398438, -20398.305126, 0, 'binary.uQwANfVwGGfGQEIoKvIC', 'nchar.青海省太原市友好许街v座 156681', 1630000000010) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000010000, 1561968852, -3625919016436330584, 3286, -97, -298.972258, -6862.478887, 0, 'binary.BDbwOAWHeVZtjQPRDGXk', 'nchar.河南省荆门市秀英上海街n座 751697', 1630000000010) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000010000, 1451429872, 6469039797993059466, 10640, 72, -1901102076.268450, 651233512.314323, 1, 'binary.obXlZctaaxoXDTrjAMsj', 'nchar.浙江省娜市华龙佘路i座 202792', 1630000000010) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000010000, 775373195, 4005708816307982233, 32637, 16, 57160258761867.601562, -51974270.953215, 1, 'binary.ZTACImTKRBVuBqcoQgOj', 'nchar.上海市天津市金平宜都街k座 478001', 1630000000010) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000010001, -2069200474, -3221876319410927698, -27106, -47, 37890.495225, 25.727565, 1, 'binary.DVYoFaHDoAGEEUjVCGiw', 'nchar.青海省帆县城东大冶路x座 496843', 1630000000011) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000010001, -901855481, -1772151262257768387, -13244, -103, 59069485000747.898438, -9.927202, 1, 'binary.UClOImuNyQtwcjMbdpsl', 'nchar.江苏省南京市崇文沈阳街t座 775087', 1630000000011) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000010000, 1108499774, 8902072342664095347, 6492, -58, -9265834.767399, -47249231544941.500000, 0, 'binary.nxQPIjFCOQQfwboVOJxy', 'nchar.宁夏回族自治区张家港县合川佘街Q座 575171', 1630000000010) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000011000, -2033003445, -7257770607653513270, 18152, 113, -178.443647, -109864451279.177994, 0, 'binary.xWXpUHAgAMKlXOZwKeCf', 'nchar.四川省沈阳县平山申街z座 274255', 1630000000011) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000011000, -1458313151, -1013483962855274441, -21524, -54, 315741.636940, 1396879354590.310059, 0, 'binary.senyOcawzaxkywdsIfwO', 'nchar.上海市秀英市普陀阮路h座 863935', 1630000000011) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000011000, 681131679, 1063707749330056742, 32599, 44, 680253231.493274, 801450480.909562, 1, 'binary.aCMLVFHcMVbTFCXLccqa', 'nchar.陕西省海门县兴山梧州路h座 914884', 1630000000011) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000011000, 599506113, 2713540172238088036, 9274, 39, 969018190.551371, -3.632101, 1, 'binary.YqoCjHibzHDjwefvvDvH', 'nchar.重庆市建平市朝阳杨街G座 335461', 1630000000011) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000011001, -2091786340, -4348640913684479593, -17221, -114, 8517916.202428, -2833572905635.299805, 1, 'binary.crjpOmtveANanEKWROdS', 'nchar.四川省志强县高明南昌街a座 564134', 1630000000012) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000011001, -167803971, -1360164950216795369, -7025, -120, -733099668463.995972, 35109159097988.500000, 1, 'binary.ERfJryluWxUIYTXKIeEA', 'nchar.山东省香港县黄浦宁德路A座 692846', 1630000000012) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000011000, 9913871, -9136393438206684463, -7347, 49, 1466998516.605100, 1913075114.359370, 0, 'binary.oRAFzWtfrkhssZEEOrqN', 'nchar.宁夏回族自治区潜江市高坪吕街P座 439485', 1630000000011) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000012000, 416303023, -613629662333559454, 25067, -15, -4952.883864, 849954.839321, 0, 'binary.iGIoowYNSiiSFeYKuxJg', 'nchar.湖北省西安市门头沟柳州路f座 643353', 1630000000012) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000012000, -1639678202, -7867748417352735741, 24559, -37, -63388.890871, 909794.904377, 0, 'binary.NOpkUwIvmmOopzxMGTuX', 'nchar.山西省冬梅县西峰谢街q座 664078', 1630000000012) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000012000, 1186628950, 2518491556660537746, 16799, 123, 8772367.653705, -27.628098, 1, 'binary.bdFBnFxLlRSAHMrejdwv', 'nchar.浙江省宁德市高港关岭街S座 736622', 1630000000012) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000012000, 360716219, 8828198435834403278, 22324, 108, 41.387771, -7917033.597233, 1, 'binary.bgROcZQGRHAREAeeTMgc', 'nchar.天津市鑫县淄川长春路F座 690264', 1630000000012) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000012001, -20473598, -23194205436567961, -7748, -113, -971446.813015, -885116.584093, 1, 'binary.YyzqsezWzruMHULKtePa', 'nchar.四川省拉萨市房山陆街O座 240590', 1630000000013) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000012001, -335837537, -1983136728890195810, -23448, -32, -0.833659, 56008906.886809, 1, 'binary.zozYDkQFIjYeNmxPkUgP', 'nchar.西藏自治区海燕县清城齐街Q座 931848', 1630000000013) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000012000, -1750287662, -3443386191653972207, -14337, 125, 40819712.184339, 3605.645431, 0, 'binary.uBkMCGjzZqxFlwGyTMMJ', 'nchar.黑龙江省瑞县南溪台北街B座 994637', 1630000000012) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000013000, -1751193786, 7295744812305382792, -12854, -26, -6.303262, 79793499.338968, 0, 'binary.gLgYRiMrwltCQuoTczvH', 'nchar.山西省汕尾县和平齐齐哈尔街Z座 435861', 1630000000013) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000013000, 627910412, -916994908362685934, 6894, 48, 99290605942330.593750, 10666.904004, 0, 'binary.MeqexsDUgMDIytXsUvZB', 'nchar.辽宁省关岭县南湖罗路v座 404782', 1630000000013) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000013000, 995622841, 7729841355848153387, 3640, 112, -2.835485, -353.406022, 1, 'binary.GaAskYnjslPlLsfmxdxo', 'nchar.山西省冬梅县高明王街P座 264151', 1630000000013) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000013000, 1515092917, 7731544838420120172, 12835, 1, 63155151164574.703125, 251364801.414645, 1, 'binary.MosiGCZmvofWpwAthGhO', 'nchar.上海市瑜县东城王路q座 572907', 1630000000013) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000013001, -413664785, -3812750994754065209, -27935, -102, 2575627295.964500, 2513496.174095, 1, 'binary.bWQBWBTPSzlRgcUhAdZk', 'nchar.贵州省西宁县兴山王路r座 354861', 1630000000014) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000013001, -549420623, -1408637008917498027, -10543, -66, -5095928986110.669922, 6853699.857183, 1, 'binary.tlJBIuwNqJscpcgHVHnB', 'nchar.浙江省沈阳县滨城汕尾路y座 918983', 1630000000014) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000013000, -1552219824, 6003631807619158016, -2306, 5, 233515.507300, 629994050006.702026, 0, 'binary.IOBjWQeleAklqjvqpfkt', 'nchar.甘肃省北镇市普陀徐街Y座 983106', 1630000000013) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000014000, 669963265, -5298601844839927726, 27873, -14, 280.874967, -34781432847.365997, 0, 'binary.ahCfyiQuXDgENrKmzvFI', 'nchar.浙江省杨县西峰林路G座 112509', 1630000000014) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000014000, 724200400, 1219145980738941308, 31916, 68, -287392.760528, 968435631500.145020, 0, 'binary.TyDbPqcsqVHZJpsYwyLI', 'nchar.浙江省林县兴山杨路T座 780922', 1630000000014) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000014000, 1162122076, 4549349420603527307, 27625, 92, -2659.171241, 34540911.194729, 1, 'binary.KrRdAkKbbFmkrVptcnza', 'nchar.辽宁省合肥市长寿薛路k座 195670', 1630000000014) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000014000, 515481926, 6951355543832722922, 11559, 114, -42550294265861.703125, 845410553351.197021, 1, 'binary.TYttKMLVVMzZLVrGhRXM', 'nchar.山西省雪梅市崇文成都街Q座 340632', 1630000000014) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000014001, -448491322, -4248086042963935, -30303, -52, -67.229599, 6985670564398.219727, 1, 'binary.VTbXgulJzGAXbhoQWFSQ', 'nchar.青海省平县房山哈尔滨路n座 899870', 1630000000015) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000014001, -811813563, -4899573317499228447, -4893, -92, 343.414666, -6.805581, 1, 'binary.PtTeusDZXyyUxAcbajyn', 'nchar.内蒙古自治区北镇市徐汇龚街z座 597427', 1630000000015) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000014000, 690007708, -4039741156082968181, 16407, -5, 1232661612531.770020, 5595883222.184000, 0, 'binary.DpgCuTJvrPgkkQMTAMJk', 'nchar.海南省辉县怀柔郑州路T座 284627', 1630000000014) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000015000, -293845942, -2730930821801375956, -7107, -1, -68623162.427154, 31837346985.650200, 0, 'binary.yNQBZqgtaTUjzJtQQCfs', 'nchar.江苏省慧市海陵邢街x座 924699', 1630000000015) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000015000, -2071603007, -8510966882971776839, 1602, -6, -3.843508, 14902120.435251, 0, 'binary.uyCGTxWMPCOrmKeFTOZw', 'nchar.海南省丽华市永川永安街d座 438520', 1630000000015) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000015000, 1133879141, 7235035511076555349, 4894, 115, -9702072.244509, -4573835.922409, 1, 'binary.qHbEpllFkcalnNUeQpdB', 'nchar.内蒙古自治区桂花县友好关岭路O座 682944', 1630000000015) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000015000, 706012728, 2845330380881701141, 15024, 3, -1678.543920, 89672777.517070, 1, 'binary.ZpbDhQBRpVxGqySNefZr', 'nchar.安徽省杭州县南湖魏街f座 214240', 1630000000015) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000015001, -951923455, -8763208484466472066, -4624, -76, 8357946832.966400, -23991.538509, 1, 'binary.eYJWsocwinphlBhCqlpQ', 'nchar.广东省广州市门头沟台北街E座 123145', 1630000000016) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000015001, -62257373, -2412311976423850741, -31869, -77, 47570582268.541000, 8110544.150476, 1, 'binary.YIkDeinwwZtkVmPPwILe', 'nchar.海南省乌鲁木齐县浔阳六盘水街A座 797576', 1630000000016) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000015000, -1200449132, -6243798639259319268, 3910, 41, 782790.248787, 78.466672, 0, 'binary.aPkysCPpBqWEhiUbycYf', 'nchar.山东省秀云县蓟州邱街b座 935199', 1630000000015) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000016000, -1507490148, -718102051582246155, -10951, -15, -4079.733653, -50430403.638327, 0, 'binary.trFjVGiMBykCJuZKeTlC', 'nchar.山西省邯郸县永川刘街f座 726006', 1630000000016) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000016000, 712101303, 8987304891216927449, 8625, -1, -74958873445540.000000, -5.857812, 0, 'binary.tBdPKIcDgTtjAFIJzBSo', 'nchar.吉林省太原市城北任路h座 471952', 1630000000016) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000016000, 762924705, 5376420654515324201, 30404, 42, -87.428626, 918.930022, 1, 'binary.qBLGTpOryvLgOhircttV', 'nchar.澳门特别行政区拉萨县六枝特南昌路g座 715241', 1630000000016) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000016000, 996700360, 7022036380848842632, 5352, 75, 372483.318803, -10533621309841.699219, 1, 'binary.GgemzLzJTraBvUgfzVVV', 'nchar.西藏自治区六安县花溪梁路q座 499733', 1630000000016) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000016001, -757685210, -5843269199730092475, -32722, -104, -752513253.596235, -758541780814.250977, 1, 'binary.XDhQkCAimgBenyhysPGu', 'nchar.浙江省深圳市翔安太原街b座 867059', 1630000000017) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000016001, -508055467, -2766131915824835247, -28072, -38, 68682581299.321800, -48264.430322, 1, 'binary.KJrclXlChjgurubgGdFX', 'nchar.浙江省旭县西夏谭街N座 102402', 1630000000017) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000016000, 680349561, -1959918178954166440, -26196, -46, -746.257726, -86812926800.553207, 0, 'binary.llgqVQNbcZBXxnUfOKQu', 'nchar.山西省丽县城北沈阳街D座 471101', 1630000000016) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000017000, 1287168518, -8671547421882131441, 19942, -85, 53445.175918, -84319.372609, 0, 'binary.SgtpseRexunITYgKGIYI', 'nchar.甘肃省倩县萧山香港街J座 451322', 1630000000017) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000017000, -76819500, 6455836859720959305, -6060, -69, 10.373982, -570339621019.558960, 0, 'binary.uufHabfQDmCXUZZBPQaW', 'nchar.甘肃省合山县崇文谢街i座 369621', 1630000000017) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000017000, 566245419, 3786365723204063340, 30409, 17, -44199.553829, 45746.488298, 1, 'binary.hCceLyYMVbLRtCUZApre', 'nchar.陕西省宜都市南长杨路H座 136225', 1630000000017) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000017000, 945614539, 630379595824549980, 9697, 64, 556294.771409, -5356339743.673330, 1, 'binary.ijzUeFRybnrcosnGjkCW', 'nchar.北京市宜都市沙湾惠州路C座 127996', 1630000000017) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000017001, -1504795238, -6216854896491867157, -26610, -58, -6626051.451338, -307415643.484121, 1, 'binary.ccbPwjthurPMsNlrcviw', 'nchar.台湾省岩县南湖石家庄街K座 602668', 1630000000018) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000017001, -2144192754, -8199223655847136650, -11650, -66, 78505.197538, 268.736598, 1, 'binary.xRcsSsRlmoQEqaTpeIlc', 'nchar.山西省磊县秀英沈阳街F座 309176', 1630000000018) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000017000, 2055126807, 7889519696890356069, 32187, 78, 93905.772558, -25.301046, 0, 'binary.dOqAqHYgdwIYjWhJgSEq', 'nchar.广西壮族自治区永安市海港陈街H座 916495', 1630000000017) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000018000, 1077673997, 7331150122111224590, 31707, 3, -763369761.766568, 10532863.623429, 0, 'binary.oWOVEddCKzJcUJdoIOJG', 'nchar.台湾省军县沙市潜江街V座 113138', 1630000000018) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000018000, -321232328, -3705474260133015870, 13505, -69, -988.403784, -3533.336395, 0, 'binary.SmQUqsyqtEwZqHnLFJzR', 'nchar.四川省秀芳市璧山李路v座 809361', 1630000000018) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000018000, 1483880986, 1239663741988408128, 29655, 93, -6.531768, -548921.291885, 1, 'binary.cvoCdcukdkaXKXBdOjZg', 'nchar.台湾省海门县西夏合山街u座 672665', 1630000000018) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000018000, 59783479, 8665833087827610450, 12599, 102, 30714.330175, 25171157926.599998, 1, 'binary.BuWFTMTgKcpsZwHmKiYg', 'nchar.甘肃省西宁市长寿辽阳街q座 505675', 1630000000018) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000018001, -1496518878, -3618500489622680106, -15013, -28, 41.354893, -5348.819753, 1, 'binary.FCAclVQNDQpqdVpXjkrm', 'nchar.新疆维吾尔自治区秀梅县长寿深圳街i座 989564', 1630000000019) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000018001, -1863339066, -1782652098028811598, -7270, -35, 996062039563.717041, 6587115.619320, 1, 'binary.oXdZlqeFvVOjUMOJNBgJ', 'nchar.宁夏回族自治区哈尔滨市华龙南宁街e座 401888', 1630000000019) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000018000, -1136224926, 3322792242353090194, -16741, 107, 56710563408.959099, 52354.223834, 0, 'binary.NEBMJGcijZDuVOYKrcSK', 'nchar.吉林省南京县永川荆门路i座 515086', 1630000000018) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000019000, 556135688, -1183098634825101579, 22185, -84, -452325.928975, 66508.789959, 0, 'binary.GoTJpCrRqjKcqSGLTqje', 'nchar.宁夏回族自治区北镇县闵行陈街V座 393922', 1630000000019) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000019000, -244514559, 3761321437479613120, -27479, -101, -4646799105.158700, -3329.260579, 0, 'binary.yjvPUBJMBeskuzxlupnQ', 'nchar.重庆市建军市丰都长春路k座 239780', 1630000000019) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000019000, 991011035, 4860085153614729774, 12761, 127, 91410741603.691101, 943373964750.319946, 1, 'binary.dTtRyCCFJUcAAQEtjfJp', 'nchar.山东省宁县沈河宁德街q座 341869', 1630000000019) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000019000, 1670587657, 5310189072518850284, 28123, 114, -820385.159544, 42094947.826999, 1, 'binary.wZIxlnrvkstQHrFEQYaY', 'nchar.江西省巢湖市海港赵路d座 161168', 1630000000019) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000019001, -194998091, -4152700310769347354, -28072, -77, -786014404848.599976, 9467093224240.289062, 1, 'binary.JTnCMdzJOauswkGAinjw', 'nchar.湖北省凯市滨城彭街H座 518533', 1630000000020) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000019001, -355481020, -5000273039623540368, -28607, -108, 51726791080.816002, 7393671141918.150391, 1, 'binary.nlWZMVJGFyiWuZhqruHH', 'nchar.天津市杰市安次柳州路a座 697877', 1630000000020) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000019000, -310862641, -4635343943523666683, -7785, 73, -414865.372619, 1613.708968, 0, 'binary.PwinfzUyFHrKFFzirzaC', 'nchar.湖北省刚县南溪张家港路H座 631845', 1630000000019) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000020000, 1399899619, 1490075378383677224, -10547, 46, 29451573.184514, -392055803.766604, 0, 'binary.dVNOcuBGdsvsUWsmpyoQ', 'nchar.福建省太原县孝南邓街W座 523011', 1630000000020) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000020000, 1445562995, -1684358190638627310, -14000, -74, 8.853163, -2779729.261598, 0, 'binary.piIcelPaPTpwVcDCQuxg', 'nchar.澳门特别行政区汕尾市清河长春街M座 339840', 1630000000020) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000020000, 1837601742, 7214867730063279232, 22780, 90, 183216089.256240, -676661682398.145996, 1, 'binary.TaCiYvkXlziNpCkpmMve', 'nchar.云南省阳市涪城张路X座 578836', 1630000000020) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000020000, 1833751442, 1303183584491610962, 1278, 115, 869758406.608992, -7772455.504376, 1, 'binary.nsnVnUOsnKGHwKGPdmtO', 'nchar.四川省通辽市沈河澳门街q座 837957', 1630000000020) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000020001, -1739594263, -303350905845853465, -27444, -41, -253.207125, 5199.720702, 1, 'binary.gtvOPqYYCDrCOSuaGJzj', 'nchar.北京市桂芝市门头沟贵阳路M座 433133', 1630000000021) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000020001, -85873572, -5230790373695382219, -26827, -86, -477426548080.922974, 70144508078.448593, 1, 'binary.wLeNmDNvlJCvwsrPbsST', 'nchar.安徽省长沙县涪城周街L座 140728', 1630000000021) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000020000, 1016679288, -6954532194842818416, -31593, -63, -31.428504, -79059968690.281601, 0, 'binary.jaafEdnahXmlQLUumxBw', 'nchar.江西省六安县江北宜都路j座 695786', 1630000000020) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000021000, 1422949792, -1701105490399360384, 2657, -8, -3401799977.602670, -5473.655796, 0, 'binary.UPCqqqXzPkhNsaCtBEec', 'nchar.江苏省合肥市高坪张街G座 286253', 1630000000021) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000021000, -1526730961, -3780746003253122588, -17177, -125, 96382.116180, 340026818769.229980, 0, 'binary.JjiXBqGqAtHIPQYcVfUr', 'nchar.台湾省慧市浔阳韦路g座 466114', 1630000000021) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000021000, 1730894469, 7972830046558188857, 25732, 36, 2350138015.891730, 925019534530.680054, 1, 'binary.ySiYRzwHSGafFbDwhFUz', 'nchar.江苏省齐齐哈尔市浔阳马街m座 591527', 1630000000021) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000021000, 828827429, 3607005081035448261, 4861, 96, 9968954041.601990, -3562528.177925, 1, 'binary.smMtwQEcRhnBXWnQgZDG', 'nchar.陕西省北镇县牧野荆门路V座 154847', 1630000000021) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000021001, -2132227686, -3809623853118181381, -5947, -23, 9144044.452890, -1762377915.170110, 1, 'binary.EteNZIiYLxLwoiqARvMa', 'nchar.湖南省武汉市蓟州长沙路o座 266308', 1630000000022) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000021001, -811467543, -4604895620226623093, -13650, -64, -78.119629, -628.771575, 1, 'binary.HFQbahFWUukCIshhKFSi', 'nchar.海南省成县城东萧街x座 195004', 1630000000022) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000021000, -102249004, 7202285073851674596, -28660, 1, 5861.435362, -70279053.347416, 0, 'binary.ZIJKevdPfxPTFDfQtUvE', 'nchar.香港特别行政区潜江市璧山胡街G座 710995', 1630000000021) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000022000, -526408040, -1151427707242144044, 5889, 116, -555570109455.625977, 192354.146665, 0, 'binary.NyzRRzxSQXzCUnNZFOOZ', 'nchar.山东省合肥县永川罗路C座 388683', 1630000000022) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000022000, 348240648, 6898365047550298693, 18256, 28, -870418928824.099976, 7.261402, 0, 'binary.owvsMLUCGksEeCmZEqSy', 'nchar.西藏自治区涛市双滦宁街f座 248726', 1630000000022) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000022000, 2020568313, 609556296676157237, 9038, 30, -4383912586003.879883, 34914227.623877, 1, 'binary.eabcMyGVWuJvzKtQmOVL', 'nchar.湖北省关岭县白云徐街K座 925710', 1630000000022) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000022000, 377412049, 2389447590348067957, 28600, 117, 30131.938632, -9552088933.833349, 1, 'binary.qvaHAymWVWvuRLNgvLjZ', 'nchar.香港特别行政区红霞县兴山张街l座 363356', 1630000000022) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000022001, -1964427620, -2504748862640405601, -31428, -14, -13171759967.844000, 347.466967, 1, 'binary.KJkuuwXEheMEjLLOhpjl', 'nchar.海南省宁市房山永安路t座 178308', 1630000000023) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000022001, -778860465, -8985230197200956034, -9309, -51, 33801527.150173, 277915326052.640015, 1, 'binary.YYibYezoQFXAnsiPQWbu', 'nchar.青海省太原市兴山长春街w座 843911', 1630000000023) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000022000, -717284516, 1691370520866824117, -22064, -77, 280691233.772134, -31299055239224.398438, 0, 'binary.CTiRgUPqClzzNmRcdSVQ', 'nchar.云南省贵阳市长寿凌街J座 426065', 1630000000022) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000023000, 485979266, 290181879041879473, -20032, 33, 5036342.494735, 36284.330432, 0, 'binary.wwCkcauZdyjGrYGzdqeV', 'nchar.贵州省燕县萧山东莞街h座 852764', 1630000000023) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000023000, 2095115012, 8573008902668514656, -14849, 108, -24157569.885902, 325.328968, 0, 'binary.AbBHSAsryfuSZixkKANf', 'nchar.湖南省大冶县萧山陆街r座 588369', 1630000000023) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000023000, 1496961049, 6877498423231087416, 28081, 102, 96332.890813, 157543096.864735, 1, 'binary.LkCzKMpfEgWnkOEJwoPl', 'nchar.辽宁省红霞县沈河宁德街o座 972970', 1630000000023) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000023000, 403042292, 8554596509509080987, 627, 111, -45862310558669.601562, -911399688.545020, 1, 'binary.UfANyfRziyIkNdnDsBUu', 'nchar.广西壮族自治区梧州市双滦惠州街r座 105680', 1630000000023) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000023001, -935043633, -8456354041963621957, -22856, 0, -534.446619, -8.817055, 1, 'binary.REQnvPAMmvMdEaMGkTRJ', 'nchar.天津市飞县江北西安街D座 931328', 1630000000024) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000023001, -374822463, -2559308893527062138, -28000, -53, 97234355.758780, 5.612709, 1, 'binary.UlMwzwGIHzXRHvlNwFaA', 'nchar.重庆市香港市魏都钟路G座 432481', 1630000000024) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000023000, -92575776, -4133402340894190690, 7802, -108, 315363.759554, -6890.679988, 0, 'binary.GyjbPifVkLDJIviwxvZC', 'nchar.广西壮族自治区辛集县六枝特潮州街x座 854046', 1630000000023) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000024000, 1912188224, 3599181797691427779, 12822, -21, -4127834.794509, 4559027605392.889648, 0, 'binary.cUgFHZargryLNpGTKAYL', 'nchar.江苏省长春县西夏费路W座 993626', 1630000000024) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000024000, -236643262, 3759934940472939705, -3993, -12, 774.336157, 6831640814.149160, 0, 'binary.PGebEJfixoJbbmWSGCzA', 'nchar.湖北省丹市大东邹街q座 204658', 1630000000024) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000024000, 328871652, 5347050514166485396, 7510, 32, -6328297191299.440430, 53357933.682145, 1, 'binary.nyXCHqmoDchNelXjIQWk', 'nchar.北京市兰州县普陀六盘水街o座 623621', 1630000000024) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000024000, 503307483, 5891987367035416040, 1484, 48, -165.143784, -50832453.820328, 1, 'binary.CRLmSSbKnhIvLgLNIDDQ', 'nchar.江苏省丹县锡山南京街b座 638448', 1630000000024) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000024001, -1533155696, -6531781294446716867, -32547, -125, -26781644.782225, -758.753750, 1, 'binary.YzEjuBAgLRrJLqebkLlf', 'nchar.江苏省晨县沈河银川街C座 461670', 1630000000025) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000024001, -1908865540, -7131121815496300043, -17233, -101, 33.590404, 6815.396916, 1, 'binary.CgqIOpTntRtChEErjaqd', 'nchar.江苏省璐市崇文朱路h座 471501', 1630000000025) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000024000, -309377908, 2628099457337897085, 7468, -66, 9879231788.887260, -25030417922.870899, 0, 'binary.zhjbrvTwhJeyUvWjWEsA', 'nchar.西藏自治区六盘水市华龙王路w座 463796', 1630000000024) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000025000, -1946329561, 207915259351633394, 11407, 41, 1602681419.567880, 7085.458968, 0, 'binary.eEggUFsokwUwANAhJwSE', 'nchar.广西壮族自治区慧市闵行靳路a座 157406', 1630000000025) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000025000, 351148910, 7405789729843676638, -22387, 55, 91812.548654, 62587905606.598198, 0, 'binary.yapFPOxLCzVjJsCeRiCD', 'nchar.甘肃省楠县南溪成都街q座 408088', 1630000000025) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000025000, 1984286682, 3011961045537268032, 30440, 30, 20.217560, -2344951725058.700195, 1, 'binary.xbHnAEfThWUqdzDEnjii', 'nchar.山西省璐市龙潭西安街V座 218642', 1630000000025) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000025000, 2142051736, 8213719388000931267, 8779, 2, -964.770542, -5022848.149211, 1, 'binary.JvPdLBzFFXzAChxCtybo', 'nchar.北京市倩市白云马鞍山街N座 333025', 1630000000025) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000025001, -1067809037, -3613051703193604568, -6581, -15, 179248377.884200, -20963418.437613, 1, 'binary.iXNzqWftcQNAJhLnIqXG', 'nchar.贵州省南京市徐汇龙路a座 774905', 1630000000026) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000025001, -884920414, -1693830437004471103, -16182, -3, 5109650898.239070, -65822444132461.601562, 1, 'binary.BJXKKLqccXQZEpNpZoUT', 'nchar.重庆市成都县大东石家庄街j座 224193', 1630000000026) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000025000, -670035158, -788082685585968933, -20572, -45, -35194235375114.398438, -66.618308, 0, 'binary.JxJZiPGAFjnlFFwIKPtx', 'nchar.辽宁省畅市蓟州太原街R座 486311', 1630000000025) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000026000, -1238224288, 2333987911735180771, 7397, -17, 52712965.969128, 20.212454, 0, 'binary.ARiFeuIBSUuntOSjMNCU', 'nchar.福建省北京市兴山合肥街y座 310001', 1630000000026) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000026000, 1510986689, -817271663217162258, -14463, -16, 5.546845, 96206813.722661, 0, 'binary.CuMteuOHVssYXuzjTSKv', 'nchar.湖北省邯郸县孝南郑街t座 110664', 1630000000026) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000026000, 1894812376, 1548508625720447521, 14927, 57, -396602063578.578979, -72.886768, 1, 'binary.ulBsLcJcNrbqhPgezzsZ', 'nchar.台湾省凤英市平山彭路U座 771726', 1630000000026) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000026000, 219014867, 2093476165236746979, 3047, 44, -391335843.881393, 172.790581, 1, 'binary.gNPTkvpqmfbANIKbMAjg', 'nchar.四川省香港县翔安上海路g座 814182', 1630000000026) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000026001, -553823230, -8496979350701622896, -10774, -5, 2336.121914, 32107861310.353500, 1, 'binary.uDnzSRQjYbTBriNiIxiO', 'nchar.贵州省倩县锡山徐街k座 824450', 1630000000027) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000026001, -554235555, -5018342227767291256, -25650, -1, 49.214070, -7700487977.489990, 1, 'binary.mLrlWeJzEbkvBaDdGHNz', 'nchar.天津市通辽市房山谷街l座 415007', 1630000000027) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000026000, -1243623673, 1341280833428328115, -23237, 17, -69465778.807510, -65901059665.967300, 0, 'binary.QiEaGiVKqNCvdGFseuRb', 'nchar.江西省香港县白云王街m座 349492', 1630000000026) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000027000, 1981641270, 8853568470177242450, -11537, 120, -64567105.564388, -271928128.398841, 0, 'binary.qlUXUTDiCSwFpnjxNxrS', 'nchar.重庆市倩市徐汇孙街S座 921838', 1630000000027) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000027000, 1039565891, -6552726341747104306, -14248, 74, 8.413983, 849.126034, 0, 'binary.cOvkZZunXXspDBuHouTF', 'nchar.海南省杭州市清河钟街B座 394931', 1630000000027) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000027000, 231192324, 8472378443447775725, 20768, 22, 29902097389.612099, -684546.655790, 1, 'binary.FEADbWHNgPkhqNEQkbQd', 'nchar.吉林省北京市华龙彭街y座 527988', 1630000000027) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000027000, 850117049, 306142192336894108, 5578, 63, -20953166225.813999, 4967.485789, 1, 'binary.wlpcQkHqzqwcsvpGFYns', 'nchar.河南省玉华市西夏钱街O座 134499', 1630000000027) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000027001, -912101482, -1699023559338901083, -16393, -34, -7945.321196, -53566624835739.898438, 1, 'binary.oBHizMHsOWchJEuSlxrW', 'nchar.上海市玲县牧野梧州路z座 474514', 1630000000028) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000027001, -262772919, -7388809716187305654, -25510, -38, -421754.950928, 858524.467388, 1, 'binary.zNAcCmcSXwrQxEFHDkfT', 'nchar.西藏自治区潜江县安次黄路d座 979010', 1630000000028) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000027000, -1714459806, 5914143461898067466, -27962, -65, 9203.979003, -76177748674423.406250, 0, 'binary.QbCTPymSAtPUYZbmvQvI', 'nchar.广西壮族自治区兴城市沙市覃路h座 152611', 1630000000027) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000028000, -320679403, 8441219075405184915, -22243, -43, -3804410.550534, 35848.615104, 0, 'binary.aDTsUTAkhCiZpRrhBgmW', 'nchar.广西壮族自治区玉珍县锡山韩路U座 577347', 1630000000028) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000028000, 1971027251, -6088730739864392244, -25087, 6, 2400118.255717, 14053310503.000000, 0, 'binary.NJmlfukaikSExRdoXCSB', 'nchar.湖北省琳县南溪太原街Q座 225245', 1630000000028) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000028000, 570055749, 2438070604505206593, 10340, 45, -66910387.453444, -3126735713.670510, 1, 'binary.YjKnRCfiosbBezWhNLLF', 'nchar.澳门特别行政区涛市东城李街m座 559662', 1630000000028) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000028000, 34977704, 604808017527204950, 22515, 102, -6486.262022, 7.594417, 1, 'binary.FUWIeEKpQVlnNbenHray', 'nchar.内蒙古自治区雪市西夏侯路L座 792671', 1630000000028) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000028001, -1739142563, -6342433396004356814, -10252, -23, 3275.236686, 854013312853.920044, 1, 'binary.qSZSpHEJuJUzeGutummH', 'nchar.宁夏回族自治区小红市大兴潮州路H座 169908', 1630000000029) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000028001, -1793036513, -6400048367013083682, -3044, -27, -311325518105.450012, -8.161867, 1, 'binary.LxQNpelajhpZMZggtOol', 'nchar.上海市楠市永川陈街R座 975342', 1630000000029) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000028000, -1544798582, 1963465178644929054, 25956, 104, 46337112221248.101562, 3.438979, 0, 'binary.PXlpyCOicUVokaEOndMR', 'nchar.青海省澳门县黄浦鞠街w座 932985', 1630000000028) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000029000, 2108556126, -165724005696787805, -12701, 93, 36899.322949, 4815765.715915, 0, 'binary.wbOSuRCMlrscktZVIXvU', 'nchar.新疆维吾尔自治区晨市孝南六安路H座 199556', 1630000000029) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000029000, 1585440451, -2922347878004558648, 21857, -103, -87716692818893.593750, -6.433454, 0, 'binary.kZFsqzIPEeUECwHQTbkq', 'nchar.河南省汕尾县锡山冯街w座 709513', 1630000000029) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000029000, 2052031912, 4924385213069716982, 32450, 88, -4.206189, 727780291.796142, 1, 'binary.pYxEJRrHLjoKyxPnDZhv', 'nchar.重庆市兴安盟市东丽张街s座 361293', 1630000000029) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000029000, 809895360, 2893675187953035726, 139, 1, -94418229932.742706, 7404.320626, 1, 'binary.WQoRmNhVZmabMDxtwxMQ', 'nchar.西藏自治区凤兰市梁平罗街d座 976776', 1630000000029) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000029001, -936333994, -5850182451187714977, -30733, -96, 72340485648.831497, 7623203255366.719727, 1, 'binary.opKIXiFRxQHhIUqJuXcG', 'nchar.江西省辉县和平合肥街W座 150361', 1630000000030) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000029001, -1456202209, -3141862884084967632, -12131, -82, -50176.235296, -43176714.393587, 1, 'binary.YqeUnGwvyjujeJLCnxvL', 'nchar.宁夏回族自治区峰县长寿陈路w座 425293', 1630000000030) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000029000, 244723847, 5591188506061745354, 10532, 103, 5575248659.891600, 16957558103314.699219, 0, 'binary.sJnBmIKpmugipxJQFFfd', 'nchar.江西省玉珍市怀柔兴城街Z座 698350', 1630000000029) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000030000, 675019438, -4652702963343301091, -11496, -50, -4087.433149, 9346004691253.250000, 0, 'binary.ERJbuiTCqKDdjRBhgyHe', 'nchar.天津市南宁市翔安许路S座 720372', 1630000000030) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000030000, 525675523, 8224309868478629335, 10309, 24, 5545.666054, 45537436905.114998, 0, 'binary.IrEIKhJVXmiGpUwTPXHc', 'nchar.湖北省强市锡山黄街R座 902867', 1630000000030) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000030000, 628311234, 7967898932765670549, 32502, 100, 33.751814, -833814.987392, 1, 'binary.uemUuwZEkEBbhwdeqckh', 'nchar.安徽省涛市清浦詹路c座 556336', 1630000000030) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000030000, 1632012033, 4023411317251621689, 19909, 61, -97040.619749, 5.883499, 1, 'binary.jEWPXZNXVzUgfVtaFKHG', 'nchar.北京市齐齐哈尔县牧野张街x座 871261', 1630000000030) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000030001, -645471416, -2453485275355685878, -12285, -123, -405555584.802136, -86249959060.101395, 1, 'binary.vjLUBFNOwUXwIQQlVHVR', 'nchar.广东省拉萨市双滦阮街X座 449848', 1630000000031) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000030001, -495791040, -7349295787104132151, -2530, -35, 1230643683793.699951, 7.745764, 1, 'binary.BMsjVucajoGkuPpQjzOG', 'nchar.福建省磊市翔安南京街A座 798360', 1630000000031) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000030000, -1254144656, -6059676078297966674, -158, -66, 4159482415577.939941, 8673.502989, 0, 'binary.sQSJzjjLCOijXAkugxdg', 'nchar.山东省长春县白云王路x座 707654', 1630000000030) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000031000, 1793917597, -8021979651638851665, -30668, -121, -782786208.493367, 40312973243.419403, 0, 'binary.gUgXghMHjyHDGBCiAais', 'nchar.青海省成县沙湾王街r座 393677', 1630000000031) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000031000, 1945911009, -3343118051415122826, 1441, -45, -45152.657086, -6775.964382, 0, 'binary.GUvMtaBLAMSnfThNWFzr', 'nchar.甘肃省张家港县崇文佛山街m座 705342', 1630000000031) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000031000, 166947244, 7326908937559434269, 3619, 46, -60206237812.774002, 58.395956, 1, 'binary.jYqZWAJuPBzFTiAMeUvL', 'nchar.宁夏回族自治区永安县沙市彭路e座 428787', 1630000000031) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000031000, 606069290, 6459556586635400233, 19456, 24, -1055958507979.680054, -194812024888.614990, 1, 'binary.BaiLODwMocQacwHKLqyT', 'nchar.广东省博市沙市长沙街H座 708920', 1630000000031) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000031001, -333091845, -5277748216878183357, -27160, -79, 415660105.947690, 12.670555, 1, 'binary.rTuAtNWjhlitACrMAHSz', 'nchar.天津市秀华市沈北新马鞍山路t座 265450', 1630000000032) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000031001, -1167842045, -2558685580583725544, -31927, -43, 1.804973, 5.975681, 1, 'binary.XrcoGPUmdILLwzJpPFKW', 'nchar.河北省合肥县山亭太原路Y座 339451', 1630000000032) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000031000, 1883704446, 4030272473288201390, 27822, -8, -9.330243, 89720617910.360596, 0, 'binary.troVimZNjcOQLyMrbgHj', 'nchar.安徽省芳县涪城南昌路N座 244641', 1630000000031) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000032000, -1458213837, -3772696596256099485, -13767, -105, -973619269.253165, -4815846.308422, 0, 'binary.BsSwkexINTKSSfbVuNKi', 'nchar.安徽省丹丹县黄浦刘街D座 140777', 1630000000032) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000032000, 685702640, 1226088595269874817, 17569, 30, 20534121980.924801, -8607.154417, 0, 'binary.RXtiZGImNnbhZlNiDJlQ', 'nchar.吉林省海口市南溪胡路T座 390534', 1630000000032) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000032000, 291455294, 7241727087327856932, 10930, 117, -42782124968908.000000, -627546.301229, 1, 'binary.GyFlrIYQEwfVIkuyghzK', 'nchar.贵州省哈尔滨县秀英南京路k座 436698', 1630000000032) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000032000, 2142470788, 8670286468030934323, 16908, 41, -1659.796272, 878407513330.574951, 1, 'binary.WEVRPKLGpVEWIGrptEjC', 'nchar.海南省娜县门头沟北京路L座 625950', 1630000000032) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000032001, -623237853, -2649569238353939284, -10412, -14, -66674065071.461899, -6031.178937, 1, 'binary.IaQpAyZKcvBqRucucEta', 'nchar.甘肃省大冶市长寿齐齐哈尔街E座 974104', 1630000000033) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000032001, -476936442, -604090572155097453, -17350, -112, -950892171281.917969, -6522.558809, 1, 'binary.diOYjKXfBPMuFQXofSqz', 'nchar.甘肃省佛山市华龙长春街f座 656560', 1630000000033) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000032000, -1611285860, 5806172561027099275, 4188, -40, -720080580526.719971, -29.421133, 0, 'binary.iFRyjkpedAgYYHLiePcn', 'nchar.湖北省济南市闵行梁街C座 423704', 1630000000032) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000033000, -1092503824, 4426934682192019732, -6038, 44, -4302128757869.609863, 187196585201.200012, 0, 'binary.mPLLajMfdeSoQglTfglq', 'nchar.安徽省广州市高坪永安街i座 778954', 1630000000033) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000033000, 1799921137, -3236551821678609536, 27198, 30, 52308192.658098, -592081383790.857056, 0, 'binary.egqcgbeBCCXmDxtENSyp', 'nchar.浙江省邯郸县魏都关岭路O座 528060', 1630000000033) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000033000, 986060027, 8349386494291768741, 29901, 110, 40046354190111.398438, -638.968879, 1, 'binary.VhMXxlACvPeENcnbSZkq', 'nchar.北京市兴安盟县永川苏路T座 679608', 1630000000033) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000033000, 309049044, 8494926295493780218, 28193, 17, 950.590035, 185154.948903, 1, 'binary.CcjXsvDJVeMShuJOskiO', 'nchar.广西壮族自治区邯郸市清浦赵街s座 521478', 1630000000033) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000033001, -1490826188, -2152973351078199517, -17165, -33, -37350321335482.296875, -11209025670846.099609, 1, 'binary.dyNbwZOpmsTLrmoMGNru', 'nchar.河北省海门县平山梧州路S座 580926', 1630000000034) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000033001, -1865158728, -3635359925315237347, -16399, -125, 969824.255212, -5419690581018.509766, 1, 'binary.qmufBbmvQyjlzcWsODpQ', 'nchar.广东省邯郸县东城重庆街R座 243855', 1630000000034) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000033000, -1782804291, 8233364802535794131, 19896, -68, -57014211104.609802, 6079786605936.709961, 0, 'binary.wCGvmPHXIBbOYaiXZklm', 'nchar.重庆市永安县永川邯郸街a座 803706', 1630000000033) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000034000, 418328468, 3082578137195896657, -28324, -41, 311.185821, -665455651397.157959, 0, 'binary.JgFnQFmwgbxpTJrbajgO', 'nchar.山西省长沙县上街东莞街m座 880561', 1630000000034) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000034000, 566781774, 17318229927991601, -22869, 117, 543.393560, -2076851169901.810059, 0, 'binary.QBeBfLWUmeXRoOEpknPG', 'nchar.黑龙江省邯郸市闵行姚街W座 963507', 1630000000034) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000034000, 1455589036, 252196545905077922, 6641, 86, -3461.471067, -5262854.628909, 1, 'binary.qORKUfdQJmdGhbCkmXmc', 'nchar.福建省玉英县普陀沈阳路a座 419015', 1630000000034) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000034000, 1424428794, 2957762133941353881, 13770, 23, -527198807.833370, 8.152931, 1, 'binary.hmdLloDZMtXtxxNUAZRS', 'nchar.河南省秀梅市清浦汕尾路K座 434573', 1630000000034) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000034001, -1648540042, -7897498994007700849, -9996, -108, -0.813070, 726438230.392081, 1, 'binary.bovZKZAUrsMsaMyRZkcy', 'nchar.甘肃省柳州市闵行陈路O座 963426', 1630000000035) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000034001, -244451547, -1093400903072656216, -13615, -15, -178311.550393, -51934734664932.101562, 1, 'binary.XddGcJdFZmHKawbUZgeZ', 'nchar.浙江省明县涪城上海路r座 324865', 1630000000035) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000034000, 895121444, -8631266587050785594, -24212, 93, 1656266012517.919922, 689051.650862, 0, 'binary.KawpDgMhSxiDMUThCGxH', 'nchar.吉林省桂兰县东丽东莞路V座 252353', 1630000000034) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000035000, 1995046162, -4043831105695016557, -26965, 33, -75690747408.954102, -5463796776.931930, 0, 'binary.mcNWaarunvQZjPATEIdN', 'nchar.河南省文县城北王路J座 107042', 1630000000035) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000035000, 669087221, 6399726771687680200, 5771, -47, -4235.462010, -794.307173, 0, 'binary.xHxfoZzcDvBnbZxkCteM', 'nchar.江西省西安市山亭潜江路w座 828905', 1630000000035) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000035000, 1888841486, 4605265043820857479, 22820, 77, -6540380890816.370117, 8614253.948077, 1, 'binary.yAvWvdqkMjNbZbeQoBgi', 'nchar.内蒙古自治区六安市城北杨街I座 349547', 1630000000035) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000035000, 1791335931, 1807768913758680255, 20398, 102, 1316.918146, -71249980808.278000, 1, 'binary.UNCALzmRPDkyGyHvEwkR', 'nchar.江西省杭州市翔安辽阳路L座 955114', 1630000000035) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000035001, -86085105, -7194632614584945063, -32666, -87, -92493.486865, 21851596643.681400, 1, 'binary.DJCjtCVCsCPGWzOejtAc', 'nchar.重庆市军市西峰宁德路I座 908424', 1630000000036) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000035001, -1042235205, -6413393490691094334, -18311, -59, -1906057050.952690, -912002219.410335, 1, 'binary.UDfSPGHZdyIAUGGyUJeW', 'nchar.山东省太原县萧山罗街q座 594388', 1630000000036) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000035000, 1888592357, -8300704347628523775, -14858, -29, -259178062706.348999, 323808.778285, 0, 'binary.robGNwjrzFvyStJgZfUm', 'nchar.内蒙古自治区合山县龙潭卢路d座 762798', 1630000000035) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000036000, 295922971, -2058642506833746888, 29737, 96, 25210.900950, -80541845779.418198, 0, 'binary.LhNNjYjrXJCZmoNriozu', 'nchar.浙江省成都市友好应街r座 639530', 1630000000036) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000036000, 712088902, 827278089461537963, -26175, 30, 7070284.817533, -84.295595, 0, 'binary.CguxxFKFnXLBssDDbHbm', 'nchar.江西省郑州县涪城潮州路e座 790009', 1630000000036) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000036000, 2033793834, 8248869647507481620, 8054, 26, -27.136380, 53.656514, 1, 'binary.OKLTmLxcIMKaFCzUyiAh', 'nchar.陕西省东莞县兴山刘路a座 223426', 1630000000036) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000036000, 785671486, 5340398831604327327, 6263, 121, -465560597.665370, -748386293.735279, 1, 'binary.TmxxACQrnUKXKNSIiqiT', 'nchar.台湾省强市西峰闵路r座 548781', 1630000000036) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000036001, -2036663911, -651652174198075862, -11102, -20, -345558578.894910, 1809372.305891, 1, 'binary.QJvYDecioaxXcotxJOph', 'nchar.云南省淑英市白云龚路m座 471043', 1630000000037) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000036001, -1896960707, -8123410862678314900, -31226, -96, 8332368919149.599609, 66840586.588556, 1, 'binary.UrpjvtIBDtEwtKwBLLHy', 'nchar.江苏省宇县高港宁德路X座 985681', 1630000000037) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000036000, -1748811428, -3662927079565752033, -32214, 13, 6355798.568022, 94028.677974, 0, 'binary.wERghNcnmmnDQuZnzKfY', 'nchar.辽宁省红梅县东丽拉萨街F座 794906', 1630000000036) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000037000, -595016139, -4580131277148620995, 29580, 105, -9456923.361187, -7673619.792012, 0, 'binary.taEiPkKOqawmHMzrSJMA', 'nchar.香港特别行政区宜都市魏都大冶街w座 474515', 1630000000037) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000037000, 1871866725, -6132597418496101173, -31191, 103, -3584162.524396, 3.964078, 0, 'binary.ErrNltadPCtbxFOygBTV', 'nchar.澳门特别行政区齐齐哈尔市沈河陈街I座 154146', 1630000000037) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000037000, 1184900481, 7751301334545259374, 20617, 105, 0.716871, -5859245846291.900391, 1, 'binary.nSIHKcctTxnKqijjRTBn', 'nchar.贵州省玲市金平温街y座 288602', 1630000000037) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000037000, 1965680838, 2310421124765665064, 8454, 29, 8004023464145.240234, 4414372860.831530, 1, 'binary.hoJpxVLxtcVCAXFWIktu', 'nchar.陕西省潮州市东丽关岭路j座 519224', 1630000000037) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000037001, -867322728, -2216454597779395076, -27957, -11, -4.919477, -28300700106.134800, 1, 'binary.pgTmFlqnYCRrDjiqRGlB', 'nchar.安徽省燕市长寿甘街G座 398538', 1630000000038) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000037001, -1004996196, -5190729260548762520, -479, -1, -64235795118.474998, 98126209.734064, 1, 'binary.wcqzmolTjwiptFAJoBDc', 'nchar.澳门特别行政区建军县秀英冯路D座 321534', 1630000000038) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000037000, -1229399524, -6046595499970005814, -1210, 29, 62697615862.290001, -91411082528558.906250, 0, 'binary.RzocNHZJbREfinRuvRzd', 'nchar.安徽省哈尔滨市山亭南宁街e座 407848', 1630000000037) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000038000, 1775840436, 3635975747322829210, -1487, 38, 293441873.224942, 4710.478142, 0, 'binary.AoyEZpVzxQeDbfnhShie', 'nchar.湖南省香港市东城杜街T座 581061', 1630000000038) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000038000, -567532886, 8854407552390897820, -26589, -14, -8352722.282594, -87617.183612, 0, 'binary.fLnRHfvrgRlTUTqxnGWq', 'nchar.山西省瑞县安次潮州街M座 373318', 1630000000038) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000038000, 77799360, 5476829675849109770, 10488, 1, 71.275540, 197839301.650685, 1, 'binary.BxvxSsnVDbwcamHkIYyM', 'nchar.四川省惠州市海港刘路O座 249869', 1630000000038) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000038000, 1353442279, 1386451198864315669, 21354, 40, 69175.762852, 5246.905662, 1, 'binary.PEhOXfqxbHmaByEIpSpl', 'nchar.上海市桂珍市西夏郭路n座 175074', 1630000000038) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000038001, -2002149887, -6983119915141409445, -24673, -65, -908728751.694875, 326523694.952220, 1, 'binary.evUYqyPhqVPbIshHOILU', 'nchar.重庆市兴安盟市静安严街h座 198129', 1630000000039) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000038001, -2083472213, -2587462033717738519, -21520, -90, -166276.944103, 670.659211, 1, 'binary.NsPfDMxwhPEOeQtQvxEN', 'nchar.河南省齐齐哈尔市沙市廖街y座 644285', 1630000000039) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000038000, -1190641768, -5412627199180425536, -14766, 76, 3.888315, -94643076.768437, 0, 'binary.KalWdOnoPEqWmypGRavW', 'nchar.新疆维吾尔自治区西宁县沈北新梧州路y座 765634', 1630000000038) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000039000, -2088060485, 3803967948736193025, 29873, 13, 9817.832005, 664894932.892036, 0, 'binary.JdPgcNWpGihhCodTivAb', 'nchar.广西壮族自治区勇市沙市阎街V座 178541', 1630000000039) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000039000, 570227789, 2047254590607033434, 19653, -107, 7678094158367.259766, 532896.787082, 0, 'binary.XqKroWKbnrwwJttMoKHr', 'nchar.四川省秀云市清河马街R座 197819', 1630000000039) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000039000, 1895326204, 181547469598517091, 12836, 17, -9043710902.598000, -4866666287.818780, 1, 'binary.jRrjQwelopynokefcykN', 'nchar.吉林省秀英市清河邓街Y座 377840', 1630000000039) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000039000, 108863201, 8859640351067556479, 16977, 67, -838302199470.109009, 33748.276961, 1, 'binary.ywQcDBZHYkCFXAVhmqEO', 'nchar.北京市平县六枝特崔路B座 227098', 1630000000039) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000039001, -394476994, -5928463586586304689, -10400, -92, -9551260552466.250000, 7271.690587, 1, 'binary.aAAxnwysJdKlEYweFHjd', 'nchar.台湾省南宁县崇文重庆街o座 498105', 1630000000040) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000039001, -1713233992, -3624308608989422655, -14063, -63, -141946221.608975, -3068768419.882630, 1, 'binary.vRDtxjOqQEQNXUNiwviT', 'nchar.福建省汕尾市门头沟海口街B座 131201', 1630000000040) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000039000, -1306715174, 4562365744380404688, 27109, -22, -36352.503689, -7934.303469, 0, 'binary.CpaHPBFrvkIiMWaxPJLi', 'nchar.陕西省荣县东城贵阳街a座 695095', 1630000000039) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000040000, 865271066, -9148181509160265671, 27249, 18, 2.222764, -280070739046.601013, 0, 'binary.egpsbKtcofHqXTLLPGRn', 'nchar.吉林省天津县友好周路Y座 353431', 1630000000040) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000040000, -1450376439, -4081106315180070049, -2576, -51, -74.457163, 6.505777, 0, 'binary.dJmbiHyuKOSVIIiIjwSS', 'nchar.上海市北镇县金平韩街D座 375248', 1630000000040) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000040000, 1450234898, 293705683748016183, 6494, 79, -3817693.681251, 165.534392, 1, 'binary.eTKJHrWAXJQhWdAGLujy', 'nchar.贵州省梧州县江北合肥街d座 669258', 1630000000040) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000040000, 1957955925, 7342571197361824614, 7886, 9, -0.661068, -8166235884.930000, 1, 'binary.INkGFJfRkjHqmzHaxLnf', 'nchar.山东省宁县城东深圳路o座 586367', 1630000000040) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000040001, -2026357343, -2032915622801652136, -15106, -95, -15486853513.681999, -3.247198, 1, 'binary.aXmytLLuKbEnWjKdQuat', 'nchar.四川省秀珍市翔安张街K座 327972', 1630000000041) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000040001, -959711900, -378413733372374586, -17348, -42, -204318.960991, 7245683489.564970, 1, 'binary.gmapmoTtXcruGPPbSuCR', 'nchar.浙江省关岭市牧野王街v座 360980', 1630000000041) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000040000, 1374164876, 3493211258284368659, -5200, -38, 27715741.924351, 91381621238039.906250, 0, 'binary.rujRCbAOydEYwjLMpMzC', 'nchar.新疆维吾尔自治区海口县涪城潮州街Q座 555329', 1630000000040) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000041000, -664413676, 884889951921171564, -7342, -25, 5028362.728369, 66621.951894, 0, 'binary.PqbeQZPwDhZtpGKvtRXB', 'nchar.天津市柳市秀英钱街a座 567286', 1630000000041) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000041000, 1034659759, 8107820373364808671, -911, -99, -64067.156517, -493936756060.739990, 0, 'binary.gffPNWtvZmyytXwwWVFh', 'nchar.重庆市合山县崇文呼和浩特路i座 516567', 1630000000041) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000041000, 2135360553, 7046734888375148506, 4350, 124, -23.119426, -68973.863143, 1, 'binary.msXUHgXBgtNVrEZynzbk', 'nchar.上海市瑞市合川孟街u座 758417', 1630000000041) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000041000, 1839044826, 2022958645969540451, 3369, 92, -91010.154826, -402673904.321380, 1, 'binary.eoWVlAvsGRKyNdoMgvha', 'nchar.吉林省文市丰都王路q座 651334', 1630000000041) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000041001, -690520239, -1199711515986420954, -26604, -121, -7518647047754.769531, 29.598467, 1, 'binary.rGwjadKsTjSWUcsvUsQi', 'nchar.上海市阳市门头沟贵阳路h座 326936', 1630000000042) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000041001, -256727407, -7227188198913271135, -3089, -115, 8.731515, -16.104279, 1, 'binary.reMICBmdKahmLpFhgbwc', 'nchar.北京市西宁市蓟州陈路L座 963644', 1630000000042) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000041000, 1772009726, 8838083164456833027, -149, -96, 31020897881818.898438, -983159424293.770020, 0, 'binary.DSwRbhIfHuaSJIKWjRBe', 'nchar.河南省长沙县新城陈街i座 767529', 1630000000041) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000042000, 865547496, 2966320895100610826, 8904, 24, 712230.779635, -27561756442.319901, 0, 'binary.JwyzlvlNeCvGKvDAHqOU', 'nchar.吉林省澳门市华龙拉萨街F座 442779', 1630000000042) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000042000, 1790904469, -7815140405234993748, -15614, 2, -2895132723.715220, 5665282000.355180, 0, 'binary.JAVdmTHMvmRZaUAZkkFD', 'nchar.海南省波县高港刘街r座 889215', 1630000000042) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000042000, 1343812351, 1671687364893071059, 5375, 1, -54587360010.559998, -91899701453061.093750, 1, 'binary.JoIMSICnhxeqbTHMgLaM', 'nchar.四川省北镇县沙市胡街l座 201950', 1630000000042) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000042000, 2143068355, 1438963834054733745, 19277, 52, 98473183331.623199, -815326243626.189941, 1, 'binary.qwKBWCfmWdNdtHbgZCvb', 'nchar.江西省柳州县徐汇哈尔滨路T座 740211', 1630000000042) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000042001, -1996625528, -353925787248976239, -7401, -70, 9182.454080, -0.543037, 1, 'binary.zZskLlUAHbfJexUXDjvV', 'nchar.福建省西宁县六枝特刘街Z座 889278', 1630000000043) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000042001, -1687869027, -7347875632438377096, -11297, -96, 50127920937.892899, -4.470659, 1, 'binary.skKKCPDSCdBbAYaElExa', 'nchar.广西壮族自治区东莞县沙市南昌路v座 951687', 1630000000043) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000042000, 810133767, -8186354850748335358, -19676, 86, -36259.368352, 60112758392.482399, 0, 'binary.RdlWGNsMfaWnOfNnUogD', 'nchar.广东省慧市和平北镇路E座 657348', 1630000000042) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000043000, -785735158, -5953722007270542152, 7034, -113, 62074544938085.898438, -3.476161, 0, 'binary.LIuoIkzdOxgAOduhzzLa', 'nchar.宁夏回族自治区丽华县涪城呼和浩特街z座 400298', 1630000000043) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000043000, 598941211, 5701828312990238709, 10927, -61, -28646557523815.699219, 106277552769.371002, 0, 'binary.qwVxRzwwxaSEbZYGHofH', 'nchar.江西省广州市江北柳州路m座 189871', 1630000000043) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000043000, 195155040, 2079631739679620300, 22071, 14, -90573582.366355, 605061081.970000, 1, 'binary.qksxaVLUyAjxljXdhKyC', 'nchar.黑龙江省淑华市双滦贵阳街y座 588239', 1630000000043) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000043000, 479912450, 8064117606888647277, 12842, 51, -153320.689936, 5977760.668092, 1, 'binary.uoTgCioiSqJyGbIcbgxU', 'nchar.新疆维吾尔自治区太原县上街福州路I座 105053', 1630000000043) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000043001, -1756271027, -6857822739162374799, -24644, -103, -71.717422, -40878.956491, 1, 'binary.oHceECvdRTLLfWkuwinR', 'nchar.黑龙江省斌市闵行惠州路r座 906537', 1630000000044) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000043001, -1479042625, -1760464133173460063, -22603, -41, -489652.181115, 513.627699, 1, 'binary.sZYPtssjWOseRilNVJSI', 'nchar.香港特别行政区建华县城北林路l座 329216', 1630000000044) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000043000, 1249549254, -3099410062091493579, 4184, 18, -6.305976, -8880835187.385349, 0, 'binary.YGnZBFGfTJxIolKkmxZO', 'nchar.广西壮族自治区洁市牧野兴城街v座 987578', 1630000000043) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000044000, -1518342625, -5235006011982343212, -4865, 75, -353076966791.942017, -45300009482889.101562, 0, 'binary.TjfmwHbYGDFrcsvKUhpw', 'nchar.重庆市东莞市新城全路z座 737324', 1630000000044) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000044000, 308927821, 2190577094771159990, 14034, -110, -8243302514174.570312, 14.494177, 0, 'binary.tScTFHjqpTlevaVrdqjb', 'nchar.上海市张家港市东丽南宁街z座 261562', 1630000000044) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000044000, 92284433, 6808464989634591395, 1010, 117, 90414951.828124, -29.172915, 1, 'binary.KtIDfCniDdvNVelHcszv', 'nchar.广西壮族自治区重庆县大兴辛集街I座 210885', 1630000000044) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000044000, 966572398, 386222127573181604, 4461, 61, 85895151014737.796875, 73.871197, 1, 'binary.FFZpTXNXDsskYeQNHkOC', 'nchar.山东省福州县锡山潮州街B座 858214', 1630000000044) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000044001, -1925824027, -1766312606810948210, -14264, -93, -7364377.452126, 88914448.724263, 1, 'binary.vrhiASOGkeyCVXZeLYka', 'nchar.甘肃省太原市浔阳黄街P座 958421', 1630000000045) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000044001, -907738171, -8175453757416532600, -25306, -34, -25130904.339176, 16790244922400.699219, 1, 'binary.pnurmnqPTxagvGDPOUSa', 'nchar.台湾省西安县双滦钟路P座 629120', 1630000000045) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000044000, 481279354, 2093629047939728324, 3389, -90, 78580636.720625, 3160.934550, 0, 'binary.JzXOoeVWtmgUVWZsuobd', 'nchar.安徽省玉珍县普陀张路P座 742434', 1630000000044) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000045000, -1567447823, 7186000735980763176, -31797, 45, -541.709323, 477914159.914460, 0, 'binary.igJKGTpPMVyaHRATFthW', 'nchar.青海省合山市徐汇乔街b座 689079', 1630000000045) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000045000, 275161253, 1095874572360838591, -14326, -124, 598.111646, 73720935296687.500000, 0, 'binary.JRZcGFizCSBmvcqczNNr', 'nchar.四川省贵阳市新城杨街A座 990458', 1630000000045) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000045000, 1036676894, 4016575713453945757, 19735, 51, 6491.268075, -3724427.159848, 1, 'binary.WQdIWMMJivSOHCIUSeZr', 'nchar.贵州省丹丹市丰都拉萨路j座 310363', 1630000000045) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000045000, 1836204768, 6413722951001635239, 16488, 79, 19.214637, 68508.516815, 1, 'binary.YrNBKjOHPnlGiNwIiPlV', 'nchar.云南省齐齐哈尔县涪城西安路M座 773092', 1630000000045) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000045001, -265361197, -5208952292564203612, -5399, -64, -8426.390212, 13.873634, 1, 'binary.zGgEVpyjupVjbyeSHiqI', 'nchar.云南省利市高坪许街i座 175376', 1630000000046) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000045001, -2083007722, -1688416890301571319, -1145, -116, 68.313684, -37354.166596, 1, 'binary.NAiMixiXRcOjUNeytRoM', 'nchar.云南省广州县梁平大冶街e座 534538', 1630000000046) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000045000, 1745320839, -1075812631194515755, -23483, 114, 6926394160.240650, -483651062706.370972, 0, 'binary.JcSqSfcgGnctPirAMlCE', 'nchar.黑龙江省深圳市沈北新太原街p座 803587', 1630000000045) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000046000, 249052813, -1615693826961148608, -15972, 7, -8.466074, 9515406.800452, 0, 'binary.kNbgrFbuALWlaCwzoMxo', 'nchar.广西壮族自治区琴市江北合山街B座 218874', 1630000000046) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000046000, 1735317837, -858745481842661891, -15193, -84, 14142026.769700, -26408079078444.800781, 0, 'binary.UYWDlWCaiqsjaEJdYiCl', 'nchar.宁夏回族自治区玲县蓟州徐街c座 396855', 1630000000046) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000046000, 713089120, 2298566616929610147, 27441, 10, -881.506652, 460586.924445, 1, 'binary.oeGaUaPlyllAxTWSKYfr', 'nchar.重庆市惠州市涪城巢湖街N座 762167', 1630000000046) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000046000, 733794086, 4876841040341786343, 23468, 22, 3516380.758568, -158277.601742, 1, 'binary.bvXlkBzbBGcKTCswqNYg', 'nchar.内蒙古自治区楠县沈河孙街Y座 831091', 1630000000046) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000046001, -1460253286, -2898170360094821082, -30610, -45, 74276.519774, 83.999091, 1, 'binary.RfUSnuxsXRnugDfcGiCm', 'nchar.香港特别行政区宜都市朝阳阜新街e座 795551', 1630000000047) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000046001, -18767216, -8622998374791248987, -10351, -81, 791655134183.812012, 3147594608549.799805, 1, 'binary.xbSXtVNWbXzYNSzRudGa', 'nchar.辽宁省志强县金平沈阳路j座 189054', 1630000000047) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000046000, 677385788, -401213451257153248, -21251, -39, 6.476141, -2980441593592.410156, 0, 'binary.DObvKidDFhQQRenzJyPH', 'nchar.湖北省建国县锡山宁街w座 242856', 1630000000046) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000047000, -1228778749, -7322503814963714978, 16054, -94, 12446.761526, 91.758896, 0, 'binary.zwffexiOQAuZjXFBZIaz', 'nchar.山东省志强县萧山刘路M座 315061', 1630000000047) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000047000, -1102860642, -1791714937739862347, -26812, 81, 728566.197997, 9911062067566.609375, 0, 'binary.WQYfHWeWoldpwnkkeLPZ', 'nchar.黑龙江省沈阳县永川西安街i座 197572', 1630000000047) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000047000, 1805224034, 1509462018265572405, 7623, 18, 89.424583, -6361764257.361300, 1, 'binary.FfkIziCgEzTaaWTIEfzh', 'nchar.四川省洋市城北银川街L座 295985', 1630000000047) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000047000, 1390362856, 2354075834187781752, 29923, 80, 951.101689, 6860.785575, 1, 'binary.ebcJhpTqiqfWYWALduhx', 'nchar.湖北省红县牧野长春路f座 434587', 1630000000047) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000047001, -1676907853, -3229641935140943374, -2879, -77, 898.995490, 2862339606.495200, 1, 'binary.DQLVRgEgwPYFHrdfCUGF', 'nchar.台湾省桂香市高坪杭州街j座 665523', 1630000000048) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000047001, -1337362153, -3661479465653440299, -5338, -11, -422.677226, 1.427939, 1, 'binary.VKtirtBWxflxOZkrmnsD', 'nchar.重庆市金凤县高明北镇路d座 159709', 1630000000048) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000047000, -1124852779, -5435782868019640637, -19418, 28, 24814666.725112, -9569212.206002, 0, 'binary.MLGMEBaqfSNRBWgGINvn', 'nchar.江西省成市长寿游街S座 720420', 1630000000047) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000048000, -853100330, -4695600377366196970, -8338, 4, 416999.519067, -892099850179.979004, 0, 'binary.DzkHtVHGVMAdcYbicfst', 'nchar.西藏自治区秀芳县江北台北路E座 877940', 1630000000048) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000048000, -1994419117, 5847457726823987251, 27299, 87, 91.421049, -172447.781008, 0, 'binary.iMIpQvQtaOwrpuoBGCug', 'nchar.湖南省文市孝南深圳街d座 534313', 1630000000048) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000048000, 368067690, 5513682680191497301, 17738, 66, -58.948289, 24281555589449.601562, 1, 'binary.xnMlWDcmsrnYypIyFxmE', 'nchar.山东省太原县牧野兰州街h座 762914', 1630000000048) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000048000, 1923435181, 4085137771782092589, 4106, 6, 363076.738289, 499.940714, 1, 'binary.TUPYxRqNsLANfKyHpaWG', 'nchar.西藏自治区沈阳市东丽兴安盟路b座 636395', 1630000000048) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000048001, -1273943525, -3459479971987145760, -4139, -51, 11651.536664, -1582655561.379670, 1, 'binary.tdxHsJtUJpZUPMMdFluT', 'nchar.天津市银川市朝阳西安路D座 479286', 1630000000049) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000048001, -1481736376, -8536522828169497965, -4912, -110, 66149873.559775, -87197408.329774, 1, 'binary.ngesAkzZfURjqgJmwLch', 'nchar.澳门特别行政区邯郸县南长辽阳街C座 675169', 1630000000049) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000048000, -1743519885, -2826904057221181638, -31447, -50, -101336007299.233002, 13.596907, 0, 'binary.xOOIZhYygGsZRBbORdWF', 'nchar.黑龙江省杭州市山亭银川路c座 544132', 1630000000048) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000049000, -995739889, -1374534569954984206, -32757, 39, -6656205215.135380, 150922935.612060, 0, 'binary.apUxFjbojAamRNyJgcsZ', 'nchar.江西省红梅县友好辛集路i座 672896', 1630000000049) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000049000, 1459513125, -1710726964996646557, -20272, -93, -914586636.165287, -98073.933825, 0, 'binary.cAIBdBYPjuaHOmUGMKzF', 'nchar.陕西省伟市淄川南京街Y座 119671', 1630000000049) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000049000, 1005904895, 6585379756903859279, 32451, 65, -0.547343, 634788897296.239990, 1, 'binary.bthdLjOWPkOkRVobNtsp', 'nchar.山西省北京市长寿辽阳街J座 663010', 1630000000049) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000049000, 515937491, 913692191756991359, 32285, 10, -5530079338.785460, -484.884875, 1, 'binary.CHbgzPLokudrGOkoKcfT', 'nchar.贵州省西宁市沙湾郑州路q座 434912', 1630000000049) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000049001, -1670406885, -7171780790080131942, -5949, -16, 577960797984.390015, -7192.693231, 1, 'binary.PTIoBjHKMqaczHYloLCd', 'nchar.江苏省鑫市高坪姜街s座 814992', 1630000000050) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000049001, -1471647875, -1377858689161644713, -19450, -11, -12.508195, -2534786.198108, 1, 'binary.VLUyRPhvUBjvQpqBAaoo', 'nchar.安徽省英市和平沈阳路d座 452052', 1630000000050) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000049000, 940664787, -1228654123442576156, 12871, -53, 9.228169, -36167843929753.101562, 0, 'binary.tDCbLkMJypvPAGNXdyAE', 'nchar.陕西省彬县璧山宁德街K座 250145', 1630000000049) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000050000, 1482262859, -8350569332255509337, -6420, 36, 410.674167, -23507674.360876, 0, 'binary.gEpViZnIdajSXnkkQCHn', 'nchar.天津市丽市清城辛集街s座 683525', 1630000000050) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000050000, -1265982867, 1955447146376580319, 30777, -32, -86462.295473, -16378.484916, 0, 'binary.aVcnsEudfzdiWtVbAVsr', 'nchar.浙江省兴安盟市海陵汤路g座 517292', 1630000000050) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000050000, 1860002619, 8597028723931298905, 22327, 109, -34001.463149, 6111229078.430500, 1, 'binary.QwDxnmcYLNYKEaBnxwlR', 'nchar.北京市萍市牧野兰州路q座 437460', 1630000000050) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000050000, 186069545, 9222137837329764599, 25037, 111, -263316697.985656, -2487453659.300350, 1, 'binary.eospRiqUFIFOIHPUSjkd', 'nchar.上海市雪市沙市荆门路G座 576150', 1630000000050) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000050001, -1964754789, -2635592817294856391, -31662, -87, 14332895424333.000000, 1560.455947, 1, 'binary.gDnEOuJCfhLPnkzsiwAc', 'nchar.云南省雪市萧山彭路Z座 659995', 1630000000051) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000050001, -6105780, -2958289076249476266, -5717, -71, 300.801520, 53025289763749.398438, 1, 'binary.WWBiNaLjIhumOzobeCIA', 'nchar.吉林省婷婷市南湖香港街u座 905925', 1630000000051) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000050000, -1732980439, 9006144128884670804, 436, -34, -611682123340.636963, -56693503.469843, 0, 'binary.DjWIoyzbiDrFKiCcXQCx', 'nchar.海南省秀兰县东丽张街K座 394966', 1630000000050) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000051000, -1278208700, -2600004501424127438, -28534, -50, -91217576.429108, -32.418646, 0, 'binary.qpqUzMofkFQNKcgEQQXS', 'nchar.陕西省岩县怀柔台北路d座 382000', 1630000000051) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000051000, -1478684630, 4446155390995823328, 24593, 42, -40863922436779.203125, -56871.279928, 0, 'binary.WYxADULbfYvkoVLOdaOC', 'nchar.新疆维吾尔自治区婷婷县璧山梁路H座 408869', 1630000000051) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000051000, 1518477407, 7835432937287723043, 11506, 44, -212756.145629, -819941664.657950, 1, 'binary.KVvzFCFCjOWspwhVdbCt', 'nchar.江西省龙县浔阳李路k座 614321', 1630000000051) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000051000, 1202059615, 6054812189681551064, 5411, 13, 818.305865, 710947.997541, 1, 'binary.lrIkSdLTpoozkFnXLvwy', 'nchar.江西省玉英县翔安陈路R座 814132', 1630000000051) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000051001, -1783065785, -8814357349788973390, -27725, -37, 40172559.431487, -593409.636997, 1, 'binary.rsiKmPsvEazEhbalNyCb', 'nchar.新疆维吾尔自治区六盘水市上街陈路q座 584080', 1630000000052) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000051001, -1217651512, -8044827569042913394, -6993, -52, -9385338379.532600, 58971228158.170998, 1, 'binary.upepPgIiuPCcQMqSLKsx', 'nchar.内蒙古自治区斌市南长杨街R座 982702', 1630000000052) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000051000, -289271686, -5003880583579490509, -13214, 120, 6170.999613, 9440371317687.630859, 0, 'binary.nfNmSvkLJmdMdpwveaxL', 'nchar.湖南省上海县新城马鞍山街l座 726730', 1630000000051) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000052000, -1959828938, -3283798903708480229, 13873, 119, 6274.172088, 8539349976.700520, 0, 'binary.QQtOXuzlykzGVRKWkkmY', 'nchar.重庆市兴城市华龙石家庄街y座 260304', 1630000000052) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000052000, -1425390302, -7798392127348520535, -13196, -118, -611039328.750825, 8472997.582840, 0, 'binary.fuKNTzkdTZsJWjdLGZrw', 'nchar.山东省马鞍山市翔安海门街I座 711199', 1630000000052) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000052000, 1740770504, 1249685128805241681, 9951, 59, 3769215387762.000000, 68533.467179, 1, 'binary.iEIkdmkXDCLBgwCClykF', 'nchar.辽宁省永安县丰都张路S座 919656', 1630000000052) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000052000, 1666431048, 3178122972801968574, 32068, 81, 81368037450.259003, -8.118395, 1, 'binary.NMyHrpCbrWQwyIyVLJDg', 'nchar.黑龙江省志强县南溪史路q座 254255', 1630000000052) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000052001, -200932980, -4386534032529273055, -8186, -110, 7826133156132.959961, -76419331116118.296875, 1, 'binary.PzqsbinrXVsZciHuCIhE', 'nchar.甘肃省凤兰县花溪赵路g座 215241', 1630000000053) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000052001, -613795404, -1513658646896349045, -32411, -92, 78264086668181.500000, 544232242.446523, 1, 'binary.aLCxdMDWmsYulapHDAjj', 'nchar.贵州省刚市兴山沈阳街p座 945944', 1630000000053) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000052000, -1553403591, -2500648260013987124, -15338, -37, 41947962.980060, -790541171225.983032, 0, 'binary.wIwXoshjUHyCsHUFvAPH', 'nchar.山东省广州县海陵来路Y座 269076', 1630000000052) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000053000, 232627094, -8420137884013071115, -4562, -97, -970965744.274358, -2289655354.717830, 0, 'binary.mnOGDoHgtORdeXPsHJhg', 'nchar.湖北省拉萨市六枝特俞路x座 416522', 1630000000053) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000053000, -2134441629, 6602255067959725837, 9829, -17, -6637.939059, 43.589160, 0, 'binary.KrwAJjOGmhsYVtyQPntW', 'nchar.台湾省坤县清河尹路f座 694936', 1630000000053) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000053000, 39446242, 4596305411365775021, 26740, 71, -3948372.747999, 96454.750373, 1, 'binary.nPpXSLuqqtvroVynIXQk', 'nchar.吉林省燕市怀柔任路g座 537458', 1630000000053) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000053000, 1933454596, 6640171771480227608, 7740, 59, 912104.772269, -46233.503856, 1, 'binary.djObiGLgORTmxKYWJyVu', 'nchar.海南省西宁市蓟州香港街p座 381291', 1630000000053) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000053001, -703782622, -7297221347660966855, -18846, -64, 33.137983, -970.531614, 1, 'binary.sCFHWMEygDxjjPjKdhqN', 'nchar.西藏自治区建军县秀英杨街B座 302599', 1630000000054) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000053001, -1215139111, -6947746948140620115, -28397, -34, -704765042406.939941, 588.161016, 1, 'binary.aLnUwsEkpNKjyukKjcuw', 'nchar.黑龙江省洁市高港陆路A座 327393', 1630000000054) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000053000, 1899110934, 8997045207900637460, -17487, -1, -941.476290, 9218133069.527731, 0, 'binary.buhqUKDVaBudkSNzTOYj', 'nchar.江苏省辛集县静安蒙街w座 677885', 1630000000053) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000054000, -2083198322, -8510258236044526569, 32698, -26, -2882051986.901400, -40825154.132085, 0, 'binary.nrLiaalsTjLwNlxPNSNM', 'nchar.上海市荆门县龙潭任街n座 292155', 1630000000054) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000054000, 463679906, -2461398756686640916, 631, 7, -8836078.870692, 461.814429, 0, 'binary.BkSqxioJoWOTxJzGiAqH', 'nchar.安徽省亮县房山阎街q座 143863', 1630000000054) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000054000, 499650317, 14377317276834330, 10220, 0, 69.171850, -39165503262605.500000, 1, 'binary.eGhgotKydPoAVWohFkgw', 'nchar.浙江省瑜县吉区敬路Y座 337195', 1630000000054) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000054000, 1751460595, 3015251412183577274, 3392, 125, 50498967780.786400, -8.776229, 1, 'binary.NfRElcGdJvtSGrYbzQPH', 'nchar.四川省萍市和平南昌街P座 892327', 1630000000054) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000054001, -611544, -5336110590286200763, -11575, -16, -805937633854.949951, 31241056.907792, 1, 'binary.xUJfgEzSUuVXfuLrSALN', 'nchar.贵州省秀珍市六枝特杨路k座 588107', 1630000000055) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000054001, -1885052667, -2829775126820323796, -2181, -81, 708536411383.946045, 907.148162, 1, 'binary.VSqPERgmbKpicSFIleyX', 'nchar.宁夏回族自治区洁县璧山吴街D座 164250', 1630000000055) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000054000, -1344815896, -6803903580337010257, 24613, 95, 4041087.745068, 3478106.347905, 0, 'binary.ftJBlmDwwvBLJXRRfokN', 'nchar.四川省兰英县萧山大冶街G座 678643', 1630000000054) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000055000, -828294634, 3386027034188373225, 6702, -13, 16839819170710.699219, 394060788.145837, 0, 'binary.ERlRrQAuOIBzFseBXkWe', 'nchar.江西省拉萨市友好贵阳路P座 583198', 1630000000055) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000055000, 1481441759, -8837361133999569211, -8114, 64, -65152789039.197998, 800461586869.649048, 0, 'binary.usKpSrIsPpVzPKxyLiPz', 'nchar.上海市南昌市合川石街g座 829461', 1630000000055) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000055000, 25610612, 9182983328623760604, 11624, 119, -5721788248.634950, -580.766417, 1, 'binary.SrXLVBqqSkAbvvrpHBFH', 'nchar.山西省军市高明曹路d座 795843', 1630000000055) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000055000, 728812448, 4501800112768130253, 26835, 118, 903416544.225530, 639673.218903, 1, 'binary.ihctcvRmRkRdGAVgvVFN', 'nchar.重庆市峰县黄浦昆明街R座 695125', 1630000000055) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000055001, -567294959, -6701833587027065403, -17136, -6, 4727.373320, -1887129297112.110107, 1, 'binary.sOcAlcwjwzSuBWBmYuMK', 'nchar.湖南省颖市清城齐路x座 779429', 1630000000056) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000055001, -430353559, -5574076684212958757, -29472, -5, -3.101457, -466.906137, 1, 'binary.VfhXhfUSBLmrNXxaFlLG', 'nchar.宁夏回族自治区兴城市高坪吕街Y座 359858', 1630000000056) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000055000, 2011356099, -6586118782611219366, 7383, -1, -310476992.706190, 68058.394574, 0, 'binary.UvDoPUopROOnIaKXSkAm', 'nchar.山西省红县淄川孙街N座 254061', 1630000000055) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000056000, 636020009, 3211319867537903583, -29350, 125, 6495182876342.839844, 44044.150616, 0, 'binary.gwrVcdilnnfkOXcrORsV', 'nchar.青海省芳市秀英上海路C座 267230', 1630000000056) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000056000, -895338423, -8647731512395671915, -1920, -48, 8323.937848, 44303848474.731201, 0, 'binary.XWVuLiXkPhxhZwgGfTgW', 'nchar.福建省宁德县涪城吴街N座 985543', 1630000000056) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000056000, 44316289, 1699119409317956475, 17935, 89, -615.417861, 51420814074.252403, 1, 'binary.SaOgEmgHCEHAemtKRGfR', 'nchar.福建省柳州县黄浦崔街a座 439298', 1630000000056) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000056000, 1806782002, 133047412739316547, 10583, 113, -1.196259, -94757297.801538, 1, 'binary.zsOnZwIjyPYmaPMgwMkl', 'nchar.海南省畅市花溪梧州路a座 248861', 1630000000056) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000056001, -132932903, -1139223406185328403, -29729, -24, 176130.865825, -7110983724786.099609, 1, 'binary.aoyWWxWyrbtLpcjjgqug', 'nchar.河南省东莞县南长应路F座 350919', 1630000000057) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000056001, -646901505, -4456106849463981718, -14702, -74, 8180722.314159, -0.541177, 1, 'binary.ErtRnmqlWRLXwEcYvxBT', 'nchar.福建省福州市沙市刘路m座 482142', 1630000000057) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000056000, -1844652574, 5783521695696020592, -3381, 1, -9674.440571, -54128.398637, 0, 'binary.eMUkmxmDsQyGAXynIkRH', 'nchar.云南省刚市永川深圳路U座 740312', 1630000000056) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000057000, 1992291237, 6351539803223426521, -31278, -93, 97270557077.834198, -229.200477, 0, 'binary.dNLopvLctboiNlXyJCpb', 'nchar.山西省洁县闵行朱街B座 938536', 1630000000057) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000057000, -1940346627, 6527090220566290375, 3344, -22, -70524.328759, 5567166.588688, 0, 'binary.lVCafNbVZBjETalWhKQQ', 'nchar.海南省刚县海港朱街k座 158943', 1630000000057) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000057000, 1849010054, 8346105246136797686, 14579, 122, -2862394415.241180, 37986858.323703, 1, 'binary.GpthqcvLvtQVrgvMdnfC', 'nchar.辽宁省刚市西峰樊路X座 766106', 1630000000057) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000057000, 1400614879, 178325118149084722, 18493, 45, -9126526.478846, 6.521004, 1, 'binary.rbrTpvPTHaMGBujnQdkd', 'nchar.湖南省婷市西夏许街H座 790083', 1630000000057) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000057001, -518905642, -5262831042812134490, -31010, -41, 31228306428.686001, 76215369031.838806, 1, 'binary.EjBAblbjKIDOciBYSySh', 'nchar.陕西省勇县上街王街l座 970244', 1630000000058) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000057001, -772907726, -3506655960168392677, -7748, -25, 5571758509265.099609, 2396.244701, 1, 'binary.YEYoYiZOvyejtzNtRNjd', 'nchar.天津市桂芝县沈河关岭街X座 899744', 1630000000058) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000057000, -958246995, -3881282576516133167, 19690, 35, 5859736148561.000000, -320211072552.122009, 0, 'binary.VjtttdqnmbKrmLUNrcXp', 'nchar.北京市东莞市萧山东莞路z座 795540', 1630000000057) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000058000, 1204386698, -6181406095770820717, 24347, 13, -58556648146375.000000, -11472960583492.699219, 0, 'binary.kZAIavohFBYBkJFnqTHc', 'nchar.福建省辛集市六枝特吴路J座 537287', 1630000000058) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000058000, 13731968, 4387230766860207212, -7937, 86, 5.150356, 418929519367.440979, 0, 'binary.vfXuFJRghRCrQcFenvYY', 'nchar.江西省欣市牧野贵阳街Y座 633623', 1630000000058) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000058000, 932023458, 6755095996942339567, 3614, 103, 864802310313.574951, -44346700.745799, 1, 'binary.HcnUaFpFngUDDmwyuHxq', 'nchar.澳门特别行政区海门市门头沟兴城街H座 184195', 1630000000058) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000058000, 1793336391, 4899818299592561290, 25823, 12, 715119.967742, 62462286.777519, 1, 'binary.GEDiJARnhgsPjDvswPZn', 'nchar.黑龙江省惠州市沈河周街Y座 223151', 1630000000058) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000058001, -892047565, -477854359833776406, -5998, -87, -1593.237735, -11056744.566035, 1, 'binary.zwtswHmuOQQMWcUuHPYm', 'nchar.四川省晶市安次香港路z座 754698', 1630000000059) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000058001, -759763235, -1807064577499934909, -28129, -100, 864.524682, -4.426938, 1, 'binary.kJXjaJQdsmkcRclNMawj', 'nchar.内蒙古自治区莉县孝南台北路s座 319413', 1630000000059) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000058000, -5748286, -2694117545074118317, -19591, 33, -331.696059, 4361.251740, 0, 'binary.rDSIGeSmpxSjKJiiaZkN', 'nchar.贵州省冬梅县南长兴安盟街X座 390657', 1630000000058) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000059000, -1690717892, -6656059074355963238, 3989, -80, -28630.740931, 97666.315643, 0, 'binary.SQFOVgcCEUrJaSxGdFQT', 'nchar.四川省关岭县沙市沈阳路T座 283005', 1630000000059) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000059000, -97573422, -3822521836241481296, -25956, 22, -9280346877.357700, -898.982977, 0, 'binary.IBYzRgYniEHPIkVEASjQ', 'nchar.内蒙古自治区亮市孝南辛集路D座 966951', 1630000000059) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000059000, 1534206781, 2842216018578819736, 1518, 66, -72.659545, 50530.364778, 1, 'binary.SlNrjDQLcrVozpdDbORw', 'nchar.湖北省淑兰县西峰惠州街a座 564180', 1630000000059) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000059000, 1929526168, 8244906804401657015, 32158, 108, 148703036631.673004, -19153813282.525600, 1, 'binary.EEVJehJZqZKyYDHySWkl', 'nchar.陕西省桂英市海陵张街D座 128432', 1630000000059) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000059001, -1443355328, -2258808833381791199, -2886, -58, 86736960.269058, 830803754.865938, 1, 'binary.oULTSTulSQiUVGnVTYev', 'nchar.台湾省龙市海陵太原路o座 246055', 1630000000060) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000059001, -784075921, -2479350571615855019, -10102, -122, -7.953394, 4.907129, 1, 'binary.zskLcbQIGeQhWfWxoqnm', 'nchar.湖北省芳县永川宁德路k座 879778', 1630000000060) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000059000, 1123527311, 2680922019678888919, -21606, -123, 973.988093, -546657060494.234985, 0, 'binary.DvTCDISqEbcrZkTijxIa', 'nchar.山西省淮安市双滦齐齐哈尔街v座 234288', 1630000000059) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000060000, -1113802659, 1518946417732471284, -28279, 2, -12703202.532180, 5153.415693, 0, 'binary.rkmHVDTAkINpYrMUfNyD', 'nchar.上海市合肥县海陵王街j座 466838', 1630000000060) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000060000, 451148809, -7593046671168193049, 30539, -85, -655034873.124780, -495.880712, 0, 'binary.vAocfIpEklTOZOHNNBab', 'nchar.吉林省太原县城东北镇街T座 695519', 1630000000060) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000060000, 1034902351, 1424106797035494281, 27908, 67, 947022271.557340, -2308.605533, 1, 'binary.DLSkoWyOVwbihdWeWeWp', 'nchar.河南省斌县静安杨街A座 279120', 1630000000060) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000060000, 1652147781, 2345787816105959256, 25082, 102, 508.651104, -929.829077, 1, 'binary.QOiUHsCBbfhuCrhahVSc', 'nchar.天津市瑜市安次赵路e座 701757', 1630000000060) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000060001, -1638692853, -4077362993074023560, -6575, -69, -3584529566.952130, -8.445422, 1, 'binary.XZjRyqbZlinaiICtyCBd', 'nchar.黑龙江省柳市合川昆明街g座 386560', 1630000000061) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000060001, -1712804121, -2949547304787254092, -18134, -67, -60908400.412081, 881163991.666826, 1, 'binary.YkuJIMxzOzVpBHdmqYfN', 'nchar.河北省建国市江北乌鲁木齐街T座 520265', 1630000000061) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000060000, 974320241, -7385003098431258437, -12763, 104, 8748493978.795971, 695584985.744512, 0, 'binary.ffKjYeqXGkeieqsmFODd', 'nchar.海南省东莞县永川韦路a座 421999', 1630000000060) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000061000, -1052389972, -2947614565784414957, 18664, 11, 750.462740, -3.716174, 0, 'binary.qCDGSUxZgaScUOjpCnLz', 'nchar.新疆维吾尔自治区淮安县沈河永安街q座 827112', 1630000000061) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000061000, 626327331, 8286894989069398039, 19046, 71, 32421950435954.601562, 1560.933754, 0, 'binary.OEwaOZmExxqqXPLVgmFP', 'nchar.青海省云市金平潮州街k座 192732', 1630000000061) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000061000, 1687154456, 737988625690779155, 6194, 117, 42163.621312, -803349.854026, 1, 'binary.jyRZrZirWjjEnUvQVlUw', 'nchar.辽宁省上海县梁平李街G座 574127', 1630000000061) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000061000, 1147213953, 7975861762555630012, 4177, 62, 75107416.928603, -20792.197369, 1, 'binary.BySfZSuBuCSjSLzOiysJ', 'nchar.广东省嘉禾市高坪兰州路G座 421632', 1630000000061) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000061001, -378384871, -8469662688010341681, -15780, -8, -6733463666.918200, -3769710556.245850, 1, 'binary.aprfXsAWFoazHuFvxRty', 'nchar.云南省冬梅市兴山东莞路Q座 366002', 1630000000062) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000061001, -488319399, -5464927958442119284, -19822, -61, -94089.623470, -59.983792, 1, 'binary.NPVAiGHopwcLOenxVxGr', 'nchar.北京市张家港县清浦黄街p座 904425', 1630000000062) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000061000, 361004507, 1549504818268190622, 8062, 24, 96764778215.457306, 894742115.943975, 0, 'binary.hEckPcqApuJOBmoKPeEJ', 'nchar.陕西省哈尔滨市长寿沈阳街y座 412179', 1630000000061) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000062000, 1423932757, -2437695714611395190, 23663, -70, -9765079287.101950, -1967203.892031, 0, 'binary.bsiWESyZMVNpVtIyQQGR', 'nchar.福建省福州市沙湾张路P座 840731', 1630000000062) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000062000, -570448441, 1265712901028829434, 769, -64, -67193.328935, 815089573496.550049, 0, 'binary.IiQbGqXtieySwOahhVsE', 'nchar.辽宁省兴安盟县沙市六盘水路V座 624759', 1630000000062) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000062000, 1106271912, 5823728036607073838, 20410, 118, -625.988194, -5470595032.293700, 1, 'binary.jqzirQuWyASMhIUAvdWS', 'nchar.湖北省桂珍县秀英唐街z座 299849', 1630000000062) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000062000, 1336424214, 543663359368581478, 265, 121, 6.659131, 88.363385, 1, 'binary.xEnFnNtvdmHBQsvecCGG', 'nchar.澳门特别行政区长春县大东杨街N座 747529', 1630000000062) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000062001, -686235293, -8977175016793945762, -23148, -72, 30323306252353.101562, 6.930008, 1, 'binary.XZtpZsuwXeckudVHmaYM', 'nchar.上海市广州市沙市兴安盟路l座 252588', 1630000000063) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000062001, -234283410, -5207080455634661553, -24117, -94, -278788543535.950012, 591.550849, 1, 'binary.wkyOeokpXCavWsChrwQo', 'nchar.海南省太原县秀英王街y座 237892', 1630000000063) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000062000, -646718152, -7889155994611487249, 6631, -58, 6446.718887, 5.755302, 0, 'binary.RzoabBQDzCmVzWvJXiKg', 'nchar.云南省凤英县山亭深圳路R座 183762', 1630000000062) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000063000, -243432140, 1632699542312240307, -22888, -75, -7755.697234, -34143119912.363701, 0, 'binary.VRadpmknrAaOmzyQYTZV', 'nchar.香港特别行政区长沙县门头沟南京街I座 519865', 1630000000063) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000063000, -558613075, -1896542331371569929, -20797, 121, -9756.522387, -98.525862, 0, 'binary.qBNozKTMvCHZNPpzIWOr', 'nchar.江西省桂珍市南长高街U座 278579', 1630000000063) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000063000, 479146911, 6415704663657053679, 17875, 111, -171366.381633, -82466707.507588, 1, 'binary.lBcaduCgCHYOJrUaTBRE', 'nchar.上海市利县孝南北京街x座 895388', 1630000000063) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000063000, 1258108966, 1095159657308995586, 13341, 65, 186874.871714, 1.597096, 1, 'binary.ZWLFDHRguJhaLNGqGOir', 'nchar.新疆维吾尔自治区丽娟市沈河刘路K座 217669', 1630000000063) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000063001, -1566440684, -1461317369815609520, -7755, -74, 522374534.135510, 748.541493, 1, 'binary.MIOlRZtdGltDGRxSuCfS', 'nchar.吉林省阜新县萧山大冶街s座 582955', 1630000000064) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000063001, -1332865892, -988702317899602489, -8673, -63, -79169922619053.500000, 270220.296414, 1, 'binary.gvKnTiCwlhjzyuQGXApt', 'nchar.内蒙古自治区淑珍县清城辛集路D座 729429', 1630000000064) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000063000, 1935748085, 5761327750300938493, 5814, 77, -6222.940727, 53627768221954.898438, 0, 'binary.FDcPwlQOwnJFGxOrcrug', 'nchar.贵州省秀梅市清浦北京路e座 321169', 1630000000063) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000064000, -1109193509, -5597329064374793487, -18546, 21, -50.699735, 2.292258, 0, 'binary.xucjdFyIbjKISKeVcUYv', 'nchar.浙江省洋县闵行杨路A座 591617', 1630000000064) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000064000, -1236725562, 3743020746103591155, -30961, -91, -655830314578.645996, -6581518371.926000, 0, 'binary.BAzfrYSSrBJDhJSxgNdI', 'nchar.重庆市永安县蓟州太原街z座 530295', 1630000000064) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000064000, 1595135011, 5633409861382920123, 12422, 2, 52943338.282638, 637857544636.817993, 1, 'binary.jzMjrlaZzphgpAtLUDlR', 'nchar.安徽省永安县翔安梧州街l座 365456', 1630000000064) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000064000, 1337000797, 3909416693831641857, 24167, 106, -4157.417235, 62.736918, 1, 'binary.HBWYnKkWdNmKdaqoDhCp', 'nchar.辽宁省涛市丰都黄街p座 391233', 1630000000064) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000064001, -1276071398, -9188404903044042775, -17811, -16, 71599983.296493, 914635834281.147949, 1, 'binary.yFPbHbRWQYMKUAmcuLAi', 'nchar.宁夏回族自治区建平县西峰潮州街u座 153906', 1630000000065) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000064001, -1708205232, -6622059120865853013, -9093, -15, 48.376899, -876513223.845230, 1, 'binary.zkKwMYcLcMWIyCnWtfaS', 'nchar.吉林省哈尔滨县华龙澳门路Y座 929531', 1630000000065) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000064000, 1045787817, -8074287637905228617, 20131, -47, -8327329.276497, -200175817707.766998, 0, 'binary.qcfDJoLCpaFzTpVXGGwI', 'nchar.重庆市芳市白云长沙街m座 105040', 1630000000064) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000065000, 1162381755, 2755018038806689717, -31937, 11, 128.614599, 7730024.689169, 0, 'binary.MhDqVILMhrnWlNMDtmtU', 'nchar.上海市齐齐哈尔市高坪李路M座 495226', 1630000000065) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000065000, 1285402482, -204837352138015951, -14457, -88, 2961793239.722420, 509211419872.280029, 0, 'binary.guYBPHXeSQmQHZeVJphL', 'nchar.江西省哈尔滨市城北重庆路I座 353925', 1630000000065) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000065000, 1576407682, 8317383827977796751, 8304, 63, -8161.746658, 7669981844.565600, 1, 'binary.iVwPxaTLyUsHAUPLgZFv', 'nchar.北京市俊市南溪刘街e座 612833', 1630000000065) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000065000, 1737968344, 1003518393386379677, 9235, 117, 7.665069, 7442.886175, 1, 'binary.QkSpXBxQJtewpLZxMUgf', 'nchar.辽宁省马鞍山市合川朱街k座 750187', 1630000000065) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000065001, -199563945, -3156765759719552607, -29985, -28, -791926943.145214, -841087665166.713013, 1, 'binary.dPKSUSEfWXjzqnbXICcf', 'nchar.河北省嘉禾市崇文黄街o座 477104', 1630000000066) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000065001, -361689094, -357447704144964698, -4410, -47, 20937.257424, 4425255635.687630, 1, 'binary.eHmxGPKNVGADsGjAhtpD', 'nchar.内蒙古自治区六盘水县梁平邯郸街d座 848088', 1630000000066) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000065000, 1553343325, -7689645402176625801, -29432, 105, -9298.296283, 961551854.483106, 0, 'binary.hWCKQdLLHIHuMFOGSOeg', 'nchar.重庆市梧州县安次六盘水路L座 876682', 1630000000065) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000066000, 22753433, 950157378432755366, 2891, 37, -46.621336, -6893.902043, 0, 'binary.wxxmIGmYVJxQLYtQJyEy', 'nchar.吉林省贵阳县静安六盘水路k座 101713', 1630000000066) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000066000, 74894539, -7730269481364071405, -26501, -45, -10056711055636.400391, -5.904729, 0, 'binary.qtjOqdtoQdqhonODsVqw', 'nchar.内蒙古自治区杭州县长寿冯路Z座 569017', 1630000000066) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000066000, 1198027384, 7126468628002412922, 30740, 21, 1961242.981343, -231.862226, 1, 'binary.XYAmrnqUiNrHJLKqDXam', 'nchar.宁夏回族自治区丽娟县浔阳长沙路X座 948148', 1630000000066) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000066000, 1870269708, 2428769665461730971, 17587, 103, 62510290113183.101562, 326.603229, 1, 'binary.mLdaxoXPFPMcrOCkrMrA', 'nchar.山西省明县安次兰州街w座 881660', 1630000000066) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000066001, -1162455785, -236811753362608830, -14255, -27, 9600058700950.519531, -29406.975155, 1, 'binary.zEKyWaOSxQddjYagnBur', 'nchar.浙江省娜县滨城拉萨路w座 524334', 1630000000067) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000066001, -2018364624, -957376051216282453, -32242, -122, 5837922714009.929688, -12.644803, 1, 'binary.MzCMjFvzGiQkUvLLHhla', 'nchar.江西省潜江市南溪大冶街c座 305942', 1630000000067) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000066000, -226519151, -2994977347356329014, 4920, 113, 86655.627631, -78654825177.440796, 0, 'binary.BnguFwbUPlkixVkeyLnY', 'nchar.广东省重庆县房山杭州街n座 800849', 1630000000066) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000067000, 50625735, -6004345554418338200, 24116, 126, -297.930306, 803542.911688, 0, 'binary.sNAhmpzVkBZfYYPpqvcX', 'nchar.重庆市秀珍市丰都宜都街b座 730464', 1630000000067) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000067000, 1000416344, -2229956311192277910, -18659, 23, 148629935.737780, -7.507768, 0, 'binary.AgMGTCUBfTTEihrfHhXe', 'nchar.广东省丽市双滦余路t座 600784', 1630000000067) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000067000, 918688524, 4741476405821807958, 1830, 101, -5442952263078.469727, 9871.520902, 1, 'binary.hjOMuvzIDVvgfxUxZosb', 'nchar.广西壮族自治区佳县大东王街W座 173168', 1630000000067) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000067000, 1569607985, 1841808013517006471, 25500, 71, 3831.863174, -41742.221077, 1, 'binary.VlOxqTJWqadAAjVgWhZQ', 'nchar.安徽省晨市和平哈尔滨街F座 423778', 1630000000067) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000067001, -587642328, -3645008058702102924, -32672, -94, -8986730638.607349, 503320056.870371, 1, 'binary.wzQtOnXurpQrSThmQzMX', 'nchar.广西壮族自治区成都县沙市张家港街s座 616506', 1630000000068) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000067001, -589923563, -5757540808163534424, -19192, -69, 868.257456, -94970.706181, 1, 'binary.zGvfcpKMgMoSGKWUySxc', 'nchar.山西省秀云市沙湾刘街f座 413179', 1630000000068) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000067000, -215240946, 6897590240602792989, -3018, -37, -3.160609, 11168679068716.900391, 0, 'binary.TproLfzaXVsgYFjtHLlU', 'nchar.山东省佛山县长寿郑州路J座 528020', 1630000000067) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000068000, 1322505308, 6462679592095687381, -27396, 0, -46049.552350, -49.968349, 0, 'binary.cwLZJcmlIQvgujOSMycl', 'nchar.台湾省柳州县江北永安路E座 304868', 1630000000068) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000068000, 1011709238, 2895565966608641613, -17437, 47, 8985796140.823601, -9852769038.755739, 0, 'binary.awjvTXIJezJlZAvfsvbX', 'nchar.内蒙古自治区南京市上街李路l座 181472', 1630000000068) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000068000, 74008350, 2360072838816149446, 18474, 108, 1905699497.606700, -6682.292696, 1, 'binary.qPtjrxlEIMwrGTsKPLci', 'nchar.四川省关岭县永川洪街X座 240508', 1630000000068) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000068000, 2047616217, 3932416364241535419, 424, 20, 6297.498979, -17516.432271, 1, 'binary.LRaWHykyKAJifYlIlcKb', 'nchar.辽宁省红县城北潮州路P座 750623', 1630000000068) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000068001, -488325123, -3003434930490368735, -10259, -12, 41674.494267, 150648007916.222992, 1, 'binary.ebOMtzwLosZnfgpckEIQ', 'nchar.北京市太原市沈河天津街S座 568474', 1630000000069) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000068001, -988064537, -4352531870573192723, -18415, -93, 0.585916, 1114258.360253, 1, 'binary.JELUyJVDwqFPIaAbWGMn', 'nchar.青海省邯郸市长寿郭路E座 631294', 1630000000069) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000068000, 375348914, -4530650117969706451, -10421, 62, -459775960.116535, 70220.944583, 0, 'binary.qzYuogJxwYsMEOjUnbLP', 'nchar.贵州省马鞍山市合川鞠路R座 761181', 1630000000068) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000069000, 1159755099, 3457522269873995889, 28981, 80, -115931.667815, -58278530.589955, 0, 'binary.SpZvicGtVKImUEylTJHd', 'nchar.浙江省欣县秀英通辽街V座 355455', 1630000000069) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000069000, -1713864051, -3553057624718918664, -13663, 6, -5914609671918.740234, 8.195514, 0, 'binary.eXfLALlgaImEwouQeRrY', 'nchar.江苏省宜都县闵行天津路D座 858395', 1630000000069) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000069000, 528117506, 3968061210546090186, 28096, 15, -52.312135, 308.824866, 1, 'binary.ftXElgrGFSarpVYphHFu', 'nchar.福建省丽市长寿贺街i座 989710', 1630000000069) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000069000, 880816360, 6681491908134513799, 5808, 30, 40222231.992629, 53025601245.445801, 1, 'binary.NzdlEHiXENPROmwHZubS', 'nchar.河南省贵阳市丰都海门路x座 989766', 1630000000069) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000069001, -224211534, -4862036308989276456, -24015, -17, -85525.324877, -924148.640335, 1, 'binary.ZpGMxICpekpoUpIvTkPp', 'nchar.河南省哈尔滨县吉区何街Y座 865328', 1630000000070) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000069001, -741548054, -2276708117263869490, -25325, -76, 7053402.521474, 44616.515514, 1, 'binary.opkikmdEdFXOZfKulRIV', 'nchar.河北省明县静安殷路N座 139642', 1630000000070) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000069000, 1021399554, -6894625893096202487, -31546, 118, -25420202911235.199219, 1819905350.840480, 0, 'binary.cGWbSzxlZtlmWIYhjdBO', 'nchar.台湾省静县平山汕尾街S座 687483', 1630000000069) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000070000, -517030242, 8077818329591098640, 10864, -15, -8899257.432976, -20.867384, 0, 'binary.PmIMmHMvzsESfQUCrcRZ', 'nchar.青海省岩市六枝特庄路B座 816293', 1630000000070) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000070000, 1871759418, 5128414299720474216, 16771, -52, -5790933.856475, 99.731413, 0, 'binary.qJfXmVmSkUARMzAOPxxX', 'nchar.湖北省秀珍县静安荆门街Y座 562829', 1630000000070) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000070000, 1241442263, 227546009676022739, 19914, 16, -39138582925.112297, -9.899012, 1, 'binary.nbtcXBcWpsAVhcECGgGQ', 'nchar.陕西省兰州县长寿西安路K座 598025', 1630000000070) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000070000, 443797761, 3970391486074147101, 31951, 74, 274600.467876, 0.544314, 1, 'binary.XJplSmDIOXjqIaVrfyhW', 'nchar.云南省凯县长寿黄街s座 973723', 1630000000070) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000070001, -990573409, -5542318622824904557, -28815, -8, -31464.403078, -3370.700071, 1, 'binary.NtkiepEhrzDGDjSArwOe', 'nchar.江西省柳市朝阳王街h座 952088', 1630000000071) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000070001, -139995673, -7270963680355594845, -1006, -86, 3366151526.976270, -8.810384, 1, 'binary.uNXefkOUxtYXYbJUZVnh', 'nchar.江苏省瑜县山亭兴城路B座 944924', 1630000000071) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000070000, -1633857650, -5697927022083353607, 26548, -63, -3804736861.544000, 74.579297, 0, 'binary.YBXtKxRjuWSkeyZoPccv', 'nchar.江苏省旭县新城刘路r座 807873', 1630000000070) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000071000, -476453959, 1123505280674030226, 11153, -123, -2935292436.368880, -45895.747845, 0, 'binary.lyqsRraRYRjDlPqawTHl', 'nchar.新疆维吾尔自治区志强县长寿刘街D座 420843', 1630000000071) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000071000, 1841136284, 5410191290497441868, -27449, -9, -717426339854.988037, 906400.586812, 0, 'binary.sUrQzZklrfCpaliFHlgk', 'nchar.江苏省荆门市静安陈街n座 719388', 1630000000071) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000071000, 1771774235, 8723753978089004205, 31556, 54, -1332433.383767, -749672767.510400, 1, 'binary.ptwVYnlHxuRjtzadRxlW', 'nchar.北京市丹市秀英杭州街Z座 357216', 1630000000071) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000071000, 1839715616, 5750279426783013425, 6219, 87, 38.501117, -6850081179.947280, 1, 'binary.jDbxakCIVittusbPTvjT', 'nchar.江西省武汉市清城徐街W座 807061', 1630000000071) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000071001, -800280559, -5742544748584517116, -8185, -82, -306157131353.690002, 7039189428572.000000, 1, 'binary.pDJwKCbgxlzxDTtRXPFH', 'nchar.江苏省杨县徐汇李街S座 980290', 1630000000072) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000071001, -823284120, -3827095211449757314, -10098, -96, -2557.686021, -89809031749287.296875, 1, 'binary.JxFQTJwcwWaSQzJEOrLh', 'nchar.重庆市惠州市永川刘路C座 630307', 1630000000072) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000071000, 304736268, -4794724730571887520, 716, 22, 5590654.786819, 4879.560233, 0, 'binary.DEIGZBadmbjDRtlKkqTK', 'nchar.新疆维吾尔自治区六盘水县新城何街z座 711529', 1630000000071) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000072000, 1989384774, 9056659496813433706, -22261, 77, 4.796316, 81715593104.476395, 0, 'binary.KmjdpyISdTDHGRttqsTT', 'nchar.陕西省沈阳市海港海门街J座 150192', 1630000000072) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000072000, 1080725820, 4848623249547548677, -12737, 18, -3729429.287555, -9.544489, 0, 'binary.oqeOJNoCcyiQjnKcobvH', 'nchar.宁夏回族自治区柳州县怀柔淮安路B座 525472', 1630000000072) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000072000, 942086334, 9196167660077537643, 7834, 122, 8152762120.525060, -37612025878.864098, 1, 'binary.NIqrUamvTOhyBUwBWOnV', 'nchar.湖南省冬梅县南湖常街u座 227357', 1630000000072) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000072000, 1405051565, 8383657139136401854, 17889, 25, -759516264.993981, -61.507457, 1, 'binary.RyzcNyoSNcyJTPTifkuL', 'nchar.辽宁省凯县海陵郑路U座 681928', 1630000000072) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000072001, -861396505, -3291447233862829169, -26017, -69, 633.347718, 68739677.362451, 1, 'binary.SWjVSepsIqpKBpMDihOG', 'nchar.湖南省玉华县蓟州苏街a座 168934', 1630000000073) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000072001, -1158883157, -2230367025390900167, -27930, -51, 33179906584.328201, -785888.974224, 1, 'binary.lixVawCGBsQOwgJzrxxq', 'nchar.河南省张家港市安次重庆街L座 983822', 1630000000073) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000072000, 430453615, 8565399960036689618, 435, 68, 6717985850671.450195, -784.675762, 0, 'binary.cazaRRkRnClrtWDsmBaL', 'nchar.西藏自治区长沙市吉区杭州路P座 868155', 1630000000072) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000073000, 1556264203, -191955577347466108, 3896, 19, -90885273.939782, 403026865321.888977, 0, 'binary.nGvjRJVVCaPBYcjjNXhx', 'nchar.湖北省成市滨城东莞路i座 871347', 1630000000073) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000073000, 561661717, 7151513707858276450, 3085, 86, 92.356064, -4157920.895487, 0, 'binary.KZGNFrrqrfYmykLiHypG', 'nchar.甘肃省玉珍县大东伍路R座 660141', 1630000000073) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000073000, 771513875, 8655724217083510613, 6385, 11, -433116.605385, 98871051368.562698, 1, 'binary.BTgiMJdFrvuhlhCiRBMv', 'nchar.北京市成都市房山吴街K座 966975', 1630000000073) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000073000, 281947328, 3501798281113152178, 17701, 121, -64038461748.894997, -120615625.612714, 1, 'binary.PlBZBAftewtkGJgpGUzS', 'nchar.广西壮族自治区小红县城北郑州街y座 518902', 1630000000073) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000073001, -162392860, -7369338236199723237, -25818, -45, -282.649417, 91885.720351, 1, 'binary.iSnnWWYgbSqRhBKWLpSx', 'nchar.安徽省澳门县东丽张路X座 822302', 1630000000074) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000073001, -97687437, -4764317191750108979, -6500, -125, -68123447388112.601562, -56.381858, 1, 'binary.fqAHtPWeYFfqAKrhNcrD', 'nchar.江西省深圳市清河永安街A座 447643', 1630000000074) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000073000, -1708904699, -5482252920937947408, -26150, -38, -82.700727, -31327637412888.300781, 0, 'binary.IsYdOPUlWKKsyEfOfLIg', 'nchar.江西省兵县六枝特李路R座 755202', 1630000000073) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000074000, 82185917, -3202338003377021246, 2368, 109, -31851681459172.199219, 9648761407.328449, 0, 'binary.MzKgnNAWBBiyJuhrENNy', 'nchar.广东省台北市璧山昆明街x座 674834', 1630000000074) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000074000, -1208229455, -5275134413756648702, 21480, 62, -4.120974, -540677737.994960, 0, 'binary.YBhfFwXutsPUNRsCxayl', 'nchar.海南省杨县东丽杨路L座 645025', 1630000000074) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000074000, 206023665, 5739356074305206841, 31830, 69, 32063.164495, -1.800686, 1, 'binary.uXyMYMwyykpWPBUluVba', 'nchar.江苏省昆明市黄浦长春路O座 606362', 1630000000074) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000074000, 658729187, 4564468369555780682, 15052, 97, -3.452813, -345.563960, 1, 'binary.QBotvAxRynerIkzPLCwB', 'nchar.安徽省丽丽县友好彭街p座 409620', 1630000000074) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000074001, -563659842, -3626654484405779149, -24308, -101, 928087.868603, 89.571375, 1, 'binary.rwUenoRyBLKEtsSccaHi', 'nchar.湖北省欣市山亭重庆街l座 130407', 1630000000075) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000074001, -304139859, -4230166196874408928, -16592, -58, -5807372.410308, -90.599590, 1, 'binary.dAXSVzjXPplEqFyGLDgj', 'nchar.广西壮族自治区斌市清城潮州街E座 542607', 1630000000075) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000074000, 1627932138, 1526293736416215135, -3218, 52, -42867048.970697, -5029423.726692, 0, 'binary.jBTNzsszHahbvMYCmVKh', 'nchar.山西省海门县璧山杨路h座 216349', 1630000000074) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000075000, 799577068, -4116560142480588696, 2788, 5, -7376300.885877, 53932988206.921204, 0, 'binary.mXOneZzmKUhrmobHragN', 'nchar.海南省六安市合川李街E座 733551', 1630000000075) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000075000, -1300108310, 5177643618673112015, -14576, 6, -34250244886637.500000, 28885350940223.000000, 0, 'binary.SQeywXYWFNnBozOVFbYr', 'nchar.安徽省东莞市涪城巢湖街X座 681109', 1630000000075) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000075000, 2101211242, 6144996173316409040, 28559, 43, -2046929556.842270, 13494.936491, 1, 'binary.RBVMKccPCNoDiTTnAeXA', 'nchar.安徽省关岭县牧野曾街h座 667941', 1630000000075) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000075000, 974668006, 7320231034785952818, 32717, 85, -8645182671200.280273, 7.403464, 1, 'binary.LRaZvjGUuNULqTvvpjKc', 'nchar.四川省福州市清浦惠州路U座 748352', 1630000000075) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000075001, -612833343, -2714607530754462043, -11140, -82, 11482093110349.500000, -32268.270222, 1, 'binary.GxhKcDKsUPOfiIgmWxTG', 'nchar.澳门特别行政区英县普陀呼和浩特路A座 869225', 1630000000076) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000075001, -800331784, -2337160769466657110, -8762, -69, 471941.635437, 52739.939233, 1, 'binary.ejILBAIhPnRMJobDzDEi', 'nchar.江西省娟市高港南京街p座 367086', 1630000000076) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000075000, -1832317649, -2938639539192648383, -10772, -83, 0.969012, 12272237398150.699219, 0, 'binary.RyfIhLxFIikuqpWcBYBJ', 'nchar.西藏自治区秀云市孝南董街I座 861094', 1630000000075) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000076000, -1393579322, -847093322834171794, -27064, -75, -67569679051676.703125, 2.737645, 0, 'binary.KZgOsOyeUShLjQVzGeJa', 'nchar.广西壮族自治区秀云市沙市陈路F座 931915', 1630000000076) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000076000, -1890223697, -100467649364484070, 21992, -96, -5076725.185118, -3.763708, 0, 'binary.NZLaprzWpqkjXqgkbwTZ', 'nchar.宁夏回族自治区贵阳市龙潭高街m座 713512', 1630000000076) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000076000, 42817502, 3944526004769267362, 31538, 36, -66406.977895, -99717073153.745102, 1, 'binary.VttBVEOguazQoIMxWQGh', 'nchar.江西省涛县新城陶路H座 245549', 1630000000076) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000076000, 947113384, 1621634289552530147, 22068, 88, 8759994638435.589844, 3116971.555185, 1, 'binary.ByigtfpCBYQMletStAMY', 'nchar.湖北省哈尔滨县东丽张街l座 110183', 1630000000076) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000076001, -1340643456, -1656794861322831604, -12935, -39, 6192853102.339050, 14871740217249.800781, 1, 'binary.FoJWDVWARpNCGcvdrBrP', 'nchar.福建省兵县山亭武汉街N座 691441', 1630000000077) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000076001, -1290634020, -8918445716963549339, -31264, -28, -61.424882, -0.833202, 1, 'binary.mOtZioApcJHHItfThewX', 'nchar.海南省石家庄县高坪惠州路f座 814630', 1630000000077) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000076000, -178802145, 4884292240602898344, 14008, 80, 296962.774155, 2336903473.500210, 0, 'binary.nKFPEtRGNPqHPoYjPqZG', 'nchar.海南省淑华县吉区齐齐哈尔路J座 523346', 1630000000076) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000077000, 1798756808, 8288796835161053501, 25515, -106, -9929009.483989, 95610999.380609, 0, 'binary.ZeoEAxXEwxNFkAfAbqrt', 'nchar.福建省北京县江北淮安路k座 602195', 1630000000077) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000077000, -881909000, 7713426345431572737, 9572, 53, 66.608932, -65874870698.617599, 0, 'binary.lIdTNWYuuBMbXxiMhFmM', 'nchar.云南省平市海港詹街U座 639815', 1630000000077) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000077000, 230614204, 5916543849409855959, 5909, 46, 157218956.938300, -388.410936, 1, 'binary.fHxEYcnGCASQcessJxoF', 'nchar.河北省明县蓟州太原路Y座 188245', 1630000000077) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000077000, 1321947422, 5814348985684042374, 31212, 79, 93191924219.118698, -2439308706848.810059, 1, 'binary.HJQTFfkxnNYHoNnZcKho', 'nchar.香港特别行政区淮安市龙潭拉萨街n座 403807', 1630000000077) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000077001, -2076469797, -3082865114413740433, -8409, -120, 62.368250, 67604.956604, 1, 'binary.PtcynyOmhDJTBLiTjtHG', 'nchar.广东省晨市兴山大冶街H座 531188', 1630000000078) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000077001, -1859676229, -2357735115267036029, -28823, -76, -8734923.418781, 54766.127524, 1, 'binary.nrRGxnupgKQCjYZVCjEK', 'nchar.江西省北京县滨城海门街c座 633047', 1630000000078) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000077000, 754873753, 2726735586426943681, 15941, -92, 895703968345.112061, 793460.677016, 0, 'binary.QLWPlSFrINEaNeyrDRJr', 'nchar.广西壮族自治区宜都市房山西安路l座 491535', 1630000000077) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000078000, -975608419, 5991209692626951689, 6793, 16, -4795069.486132, 1.602892, 0, 'binary.ERkKOvIFZJvNWFKilkOv', 'nchar.台湾省北镇市孝南张家港街f座 718790', 1630000000078) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000078000, -529527924, -1742182588693022186, 2944, -96, -220.893553, 4046159.371957, 0, 'binary.tvgKgbIgXzXkxvgkPfJD', 'nchar.台湾省平县淄川马路C座 311709', 1630000000078) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000078000, 578651663, 3565388302742868226, 4029, 52, 7368120396.281010, 483.583728, 1, 'binary.UoAKfqBYsgYLzQnxRrJi', 'nchar.黑龙江省沈阳县兴山张路j座 157337', 1630000000078) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000078000, 2009123145, 4413024412982602810, 1399, 96, 4616949.303994, 5429891960.956910, 1, 'binary.sANpVwObRhfkiSRxDCJl', 'nchar.北京市红县花溪汤街Z座 271011', 1630000000078) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000078001, -1192683152, -783788380172054939, -3276, -50, -541677207.305038, -25858.904282, 1, 'binary.XKqlrKpMIiibHTGUmSiB', 'nchar.重庆市梧州市高明吴街u座 560740', 1630000000079) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000078001, -307543572, -8353913754242261650, -24370, -120, 22284.249436, -777.471368, 1, 'binary.wHaTbHQmWvardnRbOcNm', 'nchar.上海市兰州市华龙陈路K座 565531', 1630000000079) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000078000, -1778166590, -579102817206191520, -11785, -104, -239061795.321072, 327649.263759, 0, 'binary.KoiLRMpNTxmdJihGtdyV', 'nchar.四川省玉英县南湖通辽街L座 877217', 1630000000078) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000079000, -2009202781, -486318740286646296, -26142, 127, -801809419.196269, -7093.371842, 0, 'binary.ubuVBBDZMxcrKZeSUddq', 'nchar.澳门特别行政区帆县西峰贺路r座 638230', 1630000000079) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000079000, -1049516274, -8378140259306218774, 28275, -49, 8063.297680, -96.150486, 0, 'binary.nCdzGmdYftErBzbOEXIM', 'nchar.重庆市六安县高明刘路j座 809636', 1630000000079) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000079000, 1804824233, 5016565669476320857, 32592, 28, -25644.844632, -618555548.385723, 1, 'binary.YSAJHcGDLZZKfXMfsSLH', 'nchar.四川省潜江县清浦乔路x座 532820', 1630000000079) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000079000, 1321113125, 718833033531626717, 25071, 62, 14.163006, -68362.446855, 1, 'binary.HnVGKxSINgFRxqWscDpK', 'nchar.广西壮族自治区丽娟市永川欧街x座 849378', 1630000000079) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000079001, -990565065, -6547037869179610986, -20553, -10, 53693.235524, -352516478.268321, 1, 'binary.aaxGuMbZuCtGKhFzIKNm', 'nchar.北京市建华县沙湾六安街X座 686788', 1630000000080) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000079001, -182484733, -884951031589311267, -24321, -106, -58.608190, -8725.267417, 1, 'binary.WOfKJsIZtMgQayzTHtpw', 'nchar.吉林省畅县崇文银川街M座 401454', 1630000000080) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000079000, 849743661, -4955882118213401211, -24292, 34, 781842530.495915, -734018452755.577026, 0, 'binary.myWNRMThivGUhKvbNVwq', 'nchar.陕西省福州县大东黄街o座 364444', 1630000000079) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000080000, 1802723334, 2659344139696358529, 2859, 78, -4872771.735881, 10758021456.609501, 0, 'binary.LFuzYaesPtILtBdTcNyf', 'nchar.江西省石家庄市锡山贺街A座 164762', 1630000000080) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000080000, -835845104, 7289439998620681632, -15010, 46, 5.969999, 5269824.875681, 0, 'binary.YFxbdYZeGYYzGILOXvXt', 'nchar.江西省敏市孝南石路F座 908966', 1630000000080) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000080000, 904828568, 8975276574557857016, 29584, 112, 11431981413.306299, -7977529590.000000, 1, 'binary.VDHkiAxtjOPSvKdfNvce', 'nchar.吉林省金凤市吉区兴城路Z座 829528', 1630000000080) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000080000, 1854585637, 468309985265992640, 26278, 81, -54273481887.697998, -72214314576261.703125, 1, 'binary.FaUwJeTtYWUpmNCzGZgI', 'nchar.山西省龙县魏都莫街r座 795410', 1630000000080) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000080001, -393039252, -740869741891940674, -449, -118, 96402.305064, 51878131031054.203125, 1, 'binary.AOsJgVBxQsQDToCRAkJu', 'nchar.吉林省银川县沙湾龚路A座 847471', 1630000000081) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000080001, -185869324, -7584421867265739562, -21602, -122, 9681739078.384581, 57.562036, 1, 'binary.mOFIQXrksJgHTJeoiHLL', 'nchar.甘肃省佛山市秀英梁路X座 807733', 1630000000081) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000080000, -1237901977, 8567571441107349839, 23551, 75, -66069.541754, -542582118484.520996, 0, 'binary.vvRAyhkyTvXFACKaWnDB', 'nchar.广东省重庆市高港樊街S座 418223', 1630000000080) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000081000, -186028543, -5249508308892268889, 10177, -118, 103871876.982690, 4572530.594965, 0, 'binary.PzswHDraphUTziLHRNif', 'nchar.河北省浩市沈河罗路O座 886151', 1630000000081) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000081000, 1753750477, -3283135636059602533, -25172, 37, 52795010606878.296875, 99549507.163262, 0, 'binary.FDIJGaZJrmveWbikSQiO', 'nchar.福建省淑华市黄浦袁街H座 684763', 1630000000081) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000081000, 406524950, 4403956757074585763, 31019, 72, -3360997.656582, -59.798407, 1, 'binary.FOzCEswooIQOBUBnFGIc', 'nchar.陕西省娜县海陵邯郸路J座 434770', 1630000000081) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000081000, 1485244961, 685628401174738277, 16263, 89, 5122943666002.589844, -3616507666.602380, 1, 'binary.AvJOEEAcggUtaiTrwIYs', 'nchar.吉林省柳州市兴山上海路D座 953278', 1630000000081) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000081001, -1577646478, -6736287500893918168, -9471, -119, 429272746329.687988, -186110.996492, 1, 'binary.BjEFJBqUGtELsbcMsXJF', 'nchar.青海省刚县花溪福州街W座 432797', 1630000000082) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000081001, -392187526, -9021253419836322207, -29067, -124, -737078813679.146973, 31302316.434474, 1, 'binary.yfeRGlXvqSbYNDpmcpZN', 'nchar.西藏自治区广州市淄川郑州街R座 649046', 1630000000082) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000081000, 1570721448, -198324049238498060, -4689, -116, -1007462.173777, 85505352.900985, 0, 'binary.DvdjkcpIUuOCVTTffsvl', 'nchar.北京市秀芳县魏都王路h座 517334', 1630000000081) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000082000, 1759723230, -2814168970262797611, 9339, 89, -1.399593, -60591645158240.398438, 0, 'binary.IhsDdpwsqvgdSJSSfzzr', 'nchar.新疆维吾尔自治区宁市朝阳佘街T座 543103', 1630000000082) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000082000, 1910594800, 1020709854043124561, 26699, 86, 97723089281456.406250, 4.806501, 0, 'binary.LbBLbkpIymepIhZsaqlU', 'nchar.海南省娜市和平梁路p座 109843', 1630000000082) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000082000, 1711919480, 1981052580869028726, 12788, 61, -369837097.837250, 506063.942730, 1, 'binary.dFnBcFBwDSriviDCviRp', 'nchar.天津市荆门县大兴林街l座 143725', 1630000000082) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000082000, 1702990244, 587723026105241365, 1492, 116, -54135370860.335999, -53.255253, 1, 'binary.CNOaksxMNcofwmLEICHX', 'nchar.内蒙古自治区哈尔滨县怀柔陈街W座 935334', 1630000000082) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000082001, -1580759275, -7730721032690066356, -9384, -110, 656.196732, 27748.306586, 1, 'binary.JBNJGvxYVxCfSEPHWQju', 'nchar.天津市静县海陵周街d座 328753', 1630000000083) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000082001, -443679347, -9161906023691535600, -14196, -30, -7251.173838, 7961.744113, 1, 'binary.CFxisxfqMUZEsuXacWrS', 'nchar.江苏省嘉禾市大兴沈阳路k座 326561', 1630000000083) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000082000, -628237232, 7702139492599837712, 4142, -81, -9081423203629.220703, 592320930764.859985, 0, 'binary.ngTqKyLkBUBOPNeIGWvP', 'nchar.内蒙古自治区萍县山亭东莞街v座 916572', 1630000000082) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000083000, -473605749, -197230074021981967, 1542, -27, 24823.230643, 4877430.658278, 0, 'binary.KMhXKfxvxYdsFMaODPjS', 'nchar.香港特别行政区通辽县梁平潜江街v座 321908', 1630000000083) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000083000, 2030467875, 8177969690310229737, 8742, -32, -5152304826.416460, -4650.350030, 0, 'binary.JnBzFNibbfFypCvNWRAD', 'nchar.江西省丽市静安刘街q座 139213', 1630000000083) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000083000, 253264809, 3051294629425100709, 15814, 29, -4795907704903.459961, -93562502081.819199, 1, 'binary.JWVaNdPRUufGeOInmvtM', 'nchar.海南省合山县房山南京路Z座 964056', 1630000000083) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000083000, 1494605832, 3605530437318645669, 11168, 43, -82289335047508.593750, 1773382797834.489990, 1, 'binary.QwJgmgOKqiahtQDqOZYh', 'nchar.吉林省太原市浔阳吴路b座 961865', 1630000000083) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000083001, -594654456, -4875532806485561895, -1192, -16, -1426168111.428340, -4958218.317080, 1, 'binary.WQcvMVaIxIJbkyQpqIJZ', 'nchar.重庆市永安县合川通辽路g座 891290', 1630000000084) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000083001, -2146815756, -5646004571309281997, -1912, -119, -197818.865735, 396.668855, 1, 'binary.vzyHgjSVQNdjqVMhtlJF', 'nchar.江西省兰州市房山王路p座 952411', 1630000000084) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000083000, -333236323, -6555514943145999322, -7991, 88, 818.815474, -6.436563, 0, 'binary.ZVvTBAvgxttHSWtnfDiJ', 'nchar.广东省欢市长寿海门路B座 193633', 1630000000083) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000084000, 925133141, 8784234561939618509, 16380, 123, 2469040534766.290039, 4756113.726930, 0, 'binary.QoYeJeDqGTLqlhJCnNOm', 'nchar.新疆维吾尔自治区小红市崇文兰州路Z座 361363', 1630000000084) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000084000, -574319606, 8095548887802185095, -23962, 87, -1041566520.629920, 873.833811, 0, 'binary.eeHQZEDBMMiDodUcaAEs', 'nchar.新疆维吾尔自治区北京市花溪陈街f座 629554', 1630000000084) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000084000, 353124249, 1050314308397632613, 12991, 1, -7.380011, -0.697234, 1, 'binary.OrPRcBPjTfCDyLYWlSSb', 'nchar.辽宁省宜都市魏都兴安盟街j座 824734', 1630000000084) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000084000, 680112260, 3230900354155397413, 28085, 118, -9.285944, -968836486.515199, 1, 'binary.bmMNZzVAvUfORcuFDUTD', 'nchar.河南省秀兰市海港贵阳街n座 576252', 1630000000084) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000084001, -1583564857, -5178535289588070136, -8221, -63, 4149.303081, -541527957.655130, 1, 'binary.FhGEdqzGCRbnqkqTafXv', 'nchar.天津市凤兰市长寿黄街o座 799727', 1630000000085) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000084001, -1905961356, -4444334516210101051, -5090, -102, -768565.519108, -519543652.624613, 1, 'binary.nkjOVOxAchHzctYWSefT', 'nchar.四川省玉兰市东丽张街I座 793364', 1630000000085) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000084000, 1708855967, -1431568107133204650, -22932, 32, -94100.241818, -26120.646049, 0, 'binary.HarhQwVYoSmQkHTbiWtc', 'nchar.黑龙江省台北市沙湾孙街Y座 984584', 1630000000084) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000085000, -1576092506, -8756723467201469581, 25039, -39, 261.610164, 503062006.257981, 0, 'binary.FHltQIQHdCPiEuYhMJab', 'nchar.广东省乌鲁木齐县西夏成都路j座 369783', 1630000000085) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000085000, -2087321228, -8738579161573533686, 29576, -102, -45.768436, 99284418.291775, 0, 'binary.WWgyxhgMAUvkiOZIsuXG', 'nchar.黑龙江省玉兰县新城鞠路H座 923667', 1630000000085) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000085000, 804809509, 2209269910022272600, 11040, 115, 23017185.783178, 3637637029.667120, 1, 'binary.PHbHAyhukPupkbVJCnAe', 'nchar.福建省玉梅市海陵太原街Z座 974745', 1630000000085) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000085000, 1804876445, 4424992658949318579, 22134, 65, 560557849.361273, 92880167354.590805, 1, 'binary.woaaHVUVZZrhcmQlwtwj', 'nchar.四川省冬梅县双滦澳门路l座 620087', 1630000000085) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000085001, -860413272, -1719091540206495014, -26088, -17, 3426.967015, -134020080473.893997, 1, 'binary.uUhCGKdkYfjmxvWIqdEG', 'nchar.山东省南宁市沙湾阜新路G座 565726', 1630000000086) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000085001, -1478372290, -3315651909758695929, -21586, -29, -13.545503, 94730798100.779999, 1, 'binary.EqkarkwHbdPYSBYwrdbn', 'nchar.安徽省拉萨市清河赵路y座 473198', 1630000000086) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000085000, 1423603693, 7042789288902781056, -8679, 1, -96337.690305, -76956048.707545, 0, 'binary.gDwyszUzJtCEIkTYNmfm', 'nchar.北京市惠州市清城马鞍山街F座 416939', 1630000000085) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000086000, -896949707, -4232911764443317544, -23799, -39, 929703.617469, 673677428456.895996, 0, 'binary.tQwSigSSRfuXdwpHOQkK', 'nchar.甘肃省荆门县南湖合肥街h座 111897', 1630000000086) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000086000, -288635812, 2318829552565473866, 8141, -29, 8674.399204, 31456396.985450, 0, 'binary.hnaqFzoYJymqnMmlToDM', 'nchar.安徽省桂兰县璧山郑州街r座 789491', 1630000000086) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000086000, 1800425211, 3573773513314489932, 29918, 16, 24941782.349805, -8445618.984039, 1, 'binary.uxHnqqqNhuQUCMOymWme', 'nchar.新疆维吾尔自治区彬县和平兴城街k座 904716', 1630000000086) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000086000, 67496906, 5166186520788762179, 21578, 118, -2770.665558, 73.769863, 1, 'binary.CbBkYClkypyjPjAHEqSK', 'nchar.辽宁省红霞县海陵合肥路a座 543888', 1630000000086) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000086001, -1178662160, -3916579564049974065, -26795, -83, 7037427222373.780273, -213.878554, 1, 'binary.wJSgLxRkoNjCrDYcETqN', 'nchar.甘肃省雪市山亭张家港街s座 816783', 1630000000087) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000086001, -177971056, -2454234864768218923, -8878, -42, 20829631944345.101562, 672434.560753, 1, 'binary.QRZLQQisXZhygoJVWBHV', 'nchar.河北省杭州县沈河呼和浩特路p座 333070', 1630000000087) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000086000, -1677806201, 7047154639654143248, 5350, -116, 6316749285280.870117, 634.478482, 0, 'binary.BXkSmsQeHCIAWhnJtCPi', 'nchar.黑龙江省辛集市西夏东莞街x座 718466', 1630000000086) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000087000, 1235731155, 8779559424493357605, 26602, 75, 2.734506, 9651189.964586, 0, 'binary.uIOGdsPKkbJfMcpYAPdm', 'nchar.河南省畅县大东沈阳街L座 753867', 1630000000087) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000087000, 2091243331, -7825416574945298694, -2716, 45, 14309039.377341, -811585.279152, 0, 'binary.aOtmdGYaoSHxRseaemnj', 'nchar.吉林省涛县大东阜新街K座 302143', 1630000000087) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000087000, 1493986147, 3444863277556903182, 19692, 74, -278.587284, -8.644536, 1, 'binary.THZCwvUpWNmCxpUvJwtD', 'nchar.湖南省合肥县清河吴街n座 898062', 1630000000087) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000087000, 1828513409, 5110507708799780350, 17644, 73, -13510373.146401, 66489005520.509003, 1, 'binary.NnBKZdPuRmVvEKIOpoZP', 'nchar.湖北省阜新县西夏北镇街e座 913596', 1630000000087) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000087001, -837441883, -3136751908814601021, -7067, -109, -54304697605.497200, 76513175457.428299, 1, 'binary.SOaIOqLHgQMxQLwjwdyC', 'nchar.福建省六盘水市清城周路y座 998328', 1630000000088) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000087001, -1941853524, -1725999780614411198, -20967, -36, -1532111874202.699951, -753143.326433, 1, 'binary.PzbRfeKbovaauEVfmvAL', 'nchar.湖南省帅县安次王路O座 749002', 1630000000088) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000087000, 469278015, -7482130495529867825, 31206, -62, 5871124058.918950, -50.898068, 0, 'binary.hhlLnLYhCyOpsLlDJigN', 'nchar.甘肃省辛集县清城任街Q座 145058', 1630000000087) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000088000, 1429480144, -5083549478027541565, -11560, -14, 36793205910.635300, 50904.884961, 0, 'binary.ZfMjTKHbfGyJDrdVYVPx', 'nchar.江苏省惠州县朝阳兴安盟街D座 827139', 1630000000088) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000088000, -201007158, 8055259366712684046, 3697, 51, 430448.420579, -315854.564703, 0, 'binary.LjjAUFCEGQkmxnxLEGAG', 'nchar.上海市沈阳市淄川周街j座 547889', 1630000000088) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000088000, 1443709130, 1574741008083980112, 17351, 95, 6060739804960.669922, -1532528.579958, 1, 'binary.qJJIoAaJbDGxoOoLrQtR', 'nchar.四川省勇县东城王街m座 770065', 1630000000088) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000088000, 982486428, 941285056220156086, 23634, 117, 348.513054, 169372813996.626007, 1, 'binary.UOTTXHXjGWMFcFhOBOGq', 'nchar.内蒙古自治区海口县和平刘街b座 528287', 1630000000088) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000088001, -150578974, -3969767378914249107, -10392, -110, 974.135498, -628885.160028, 1, 'binary.EquCXLihqWQsONDXRINa', 'nchar.甘肃省志强县孝南西宁路e座 165539', 1630000000089) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000088001, -1520979505, -4362639601846318368, -8635, -106, -16.903616, 948.881217, 1, 'binary.QTCRCxmEYXIAPtHEQspK', 'nchar.贵州省汕尾市翔安西宁路O座 317183', 1630000000089) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000088000, 405734093, 584483594573898891, 3127, -86, -71337722714.251999, -550632204.851496, 0, 'binary.snYWpsXskBDvYmdynmQI', 'nchar.香港特别行政区秀荣县沙市荆门街B座 799024', 1630000000088) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000089000, -1943783192, 6468757188659105346, -23784, -127, -36810463808662.796875, -1112590797395.360107, 0, 'binary.FsJwOhGuXhjGzMqQmsrh', 'nchar.澳门特别行政区南宁县南长郑州路K座 145568', 1630000000089) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000089000, 52831756, -2220435958599850680, -15655, 28, -6.681313, 4877914.986545, 0, 'binary.VZPjjgNdRjGylUgfpyNI', 'nchar.浙江省合山市牧野周路V座 485436', 1630000000089) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000089000, 16345797, 6999874801883372620, 9930, 33, 20526736.640306, -2.983233, 1, 'binary.KqpaAlcKbmwYePHCZZWd', 'nchar.广东省呼和浩特县和平刘路b座 503590', 1630000000089) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000089000, 1084753049, 7320863269402178063, 25949, 98, 128367.460307, -61.606980, 1, 'binary.XZCOzaGneOpLenHFMkVu', 'nchar.内蒙古自治区玉梅市普陀李街p座 871045', 1630000000089) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000089001, -722424113, -1395101658815266670, -32742, -104, 11.386013, -37.104428, 1, 'binary.numDWIFutcffotbwgHLm', 'nchar.香港特别行政区银川县上街辛集街x座 208873', 1630000000090) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000089001, -2120291117, -1583149460933102636, -9734, -69, 8447928654.959480, 6412.679016, 1, 'binary.hGuFToHzymqFGUdxfyVY', 'nchar.香港特别行政区凯县沈河南宁街K座 315345', 1630000000090) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000089000, -2019337864, 6746422161605199872, 32325, 23, 871.734249, 82184806.693967, 0, 'binary.HtyYoYbhItoXiaWUmvuS', 'nchar.福建省阳市东丽王路m座 153871', 1630000000089) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000090000, 1660958375, 5194392744956739766, 12848, 5, -70356436387.373398, -51864694264125.000000, 0, 'binary.NPcQWEWRHXQFtBjqstTz', 'nchar.陕西省通辽市吉区巢湖路v座 855528', 1630000000090) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000090000, -1541816139, -5636830477586609693, 3526, 57, 93813077.171739, 922892661911.844971, 0, 'binary.xDdNGBwybdpoxcgCpomk', 'nchar.广东省岩市平山拉萨街I座 780804', 1630000000090) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000090000, 826148, 8894584491949146100, 9551, 63, 99815617876401.203125, 221632549.126595, 1, 'binary.iNYmTBNiDNdOkoYYMTwQ', 'nchar.山东省广州市怀柔武汉路K座 264127', 1630000000090) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000090000, 602556307, 1228967701397183483, 7621, 117, -43893559833203.601562, 14.614910, 1, 'binary.ppzEdpKslelYyWIwPGej', 'nchar.宁夏回族自治区静市静安姜路v座 157499', 1630000000090) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000090001, -1155921092, -7072012080357136127, -1656, -19, -2476976874.709860, -669115.723608, 1, 'binary.bBxqDupBdanmzyPsRKLr', 'nchar.西藏自治区桂荣县牧野王街D座 146488', 1630000000091) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000090001, -1916219496, -8357710603305156625, -22253, -34, 647.783704, 46870558.437746, 1, 'binary.TOlAMlrvtsrhjFMmYuwr', 'nchar.重庆市丹县永川王路r座 683926', 1630000000091) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000090000, -1671552433, -3807362262726944832, 31741, -49, 244.147795, -51253352.699052, 0, 'binary.LjqWxDTOHZJCcNVooyFC', 'nchar.广西壮族自治区强市萧山周路e座 445322', 1630000000090) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000091000, 336141443, 4828109794924441156, -17826, 12, 781.783243, -43.929993, 0, 'binary.pXRJMAygsvYEgxYcqPOX', 'nchar.海南省邯郸县高坪南京路V座 567448', 1630000000091) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000091000, 2003010271, 986890702561353354, -28716, -50, -6914343.605896, -803565238249.427979, 0, 'binary.vSsKtTugjXyajkaQrsMt', 'nchar.广东省关岭市丰都柳州路f座 293217', 1630000000091) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000091000, 1963333404, 108184139978098657, 15764, 6, -959879.249888, 19848665.656708, 1, 'binary.GPUNrOLMbICePaFAoVXI', 'nchar.吉林省淮安县高港汕尾街W座 388403', 1630000000091) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000091000, 445310240, 5019697679335576072, 13633, 88, -13136635369.665300, 1155115854335.879883, 1, 'binary.rxJLHlitwmoEyvJmttYY', 'nchar.台湾省北镇市山亭上海街X座 677279', 1630000000091) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000091001, -185602692, -1326618088735038542, -16985, -86, -4.363816, 4043.592395, 1, 'binary.OzaMcUzfyWmuqdHrSLTu', 'nchar.广东省拉萨市白云福州路T座 757169', 1630000000092) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000091001, -1158845107, -8280668490410175712, -12685, -116, -2782930.851364, -997920128.518610, 1, 'binary.mxzRbULuDixdSYPKyFMe', 'nchar.西藏自治区张家港县丰都佛山街J座 727779', 1630000000092) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000091000, 149089442, -4278022706591027455, 22915, -41, 5.466152, -9238429.705321, 0, 'binary.stceFFfhKRNKKpQBUIMj', 'nchar.西藏自治区云市城北陈路c座 928531', 1630000000091) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000092000, -235439207, -9033560473339872815, -25826, -84, -5102108.637301, 70363.899002, 0, 'binary.aGukQYwgkOcMulErvAfi', 'nchar.辽宁省潮州县和平陈街V座 576850', 1630000000092) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000092000, -244131034, 7533096354101663716, 21552, -23, 90450866.244366, 5266.780535, 0, 'binary.wcSAHtRYzhvkguoFUrWS', 'nchar.广东省洁市华龙兰州街G座 859468', 1630000000092) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000092000, 238591693, 169450934040138746, 19699, 121, -8720856665138.000000, -539938.826556, 1, 'binary.NTRxweZggDiWMuiTXtBX', 'nchar.湖北省帆县闵行济南街k座 748709', 1630000000092) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000092000, 567776990, 5422530468642091172, 1445, 3, 99678581160679.093750, -10453818.893428, 1, 'binary.LaTXmpjwrumbUogucfit', 'nchar.重庆市峰市新城唐路m座 487170', 1630000000092) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000092001, -2050289409, -2907555945177881048, -26000, -88, 805739837.567640, -722441405416.305054, 1, 'binary.mQmPCaNIrkaxguhBQNqT', 'nchar.广东省郑州市上街南宁路Y座 230612', 1630000000093) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000092001, -22000103, -5996113223016225094, -21282, -27, -6.108455, 2193770839513.979980, 1, 'binary.yaCoRzLlbNrLWrOWvcZP', 'nchar.山西省柳州县大兴北镇街l座 692624', 1630000000093) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000092000, 1028007961, 5396249736315566830, 23574, -91, 3.593189, 849905852016.988037, 0, 'binary.PIvsrFiLeXvYkaAjAEnc', 'nchar.浙江省岩县花溪六安路F座 273811', 1630000000092) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000093000, -1296634261, -8483768956335823364, -5978, 79, 64940604998923.703125, 91858.142247, 0, 'binary.JdhDUeMiSJBOuRcbvYLD', 'nchar.北京市南京县海港刘街s座 942380', 1630000000093) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000093000, -1169167509, 7431328764161398049, -25730, 7, -2867.711112, 6.476135, 0, 'binary.wcZIBYeLJnCgLbioHPIV', 'nchar.重庆市雪梅市牧野台北路Y座 385567', 1630000000093) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000093000, 1371012896, 4949441178779865970, 21938, 127, 8616202858517.639648, 59.177162, 1, 'binary.IMvfzwWIWPEbMeremGAg', 'nchar.吉林省俊县龙潭南京路b座 312035', 1630000000093) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000093000, 1757281003, 2472467961639479874, 1503, 89, -28559.200610, 4173880454165.209961, 1, 'binary.TDsswYcxTUxTrgmHAETA', 'nchar.黑龙江省武汉市璧山六安街A座 270026', 1630000000093) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000093001, -711231405, -2996817387397925002, -12643, -100, 305.332560, 6850387821978.740234, 1, 'binary.eywGoTIBxhMtOycRzIXX', 'nchar.江西省拉萨县华龙廖街U座 825601', 1630000000094) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000093001, -1912927556, -9030166560372820943, -15055, -67, -111.643452, -6979.659019, 1, 'binary.VTeXcjZvmteXLoJRXPUt', 'nchar.香港特别行政区静市锡山关岭路r座 877824', 1630000000094) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000093000, 394476492, 5765009410379809422, 31047, -81, 59188464224641.898438, 7025.680199, 0, 'binary.ZEOTbbASFhheoFinuwqn', 'nchar.海南省沈阳县魏都裴街Z座 596006', 1630000000093) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000094000, 380495824, 1265203587731691149, 7209, -37, 8562.348342, -3948.190479, 0, 'binary.iAASCgdrMbADnmABnGWN', 'nchar.河北省春梅市浔阳呼和浩特路p座 124495', 1630000000094) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000094000, -1080679103, 1114555908742318054, -29766, -117, -9338.981980, 6375.362463, 0, 'binary.cJypJkJRLxkMUighCFqn', 'nchar.浙江省晶市蓟州香港街O座 535191', 1630000000094) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000094000, 823466308, 2660573759077875956, 29909, 106, -82207.548275, -22188673065884.199219, 1, 'binary.zCuvYPkobqfcxAWauKIg', 'nchar.内蒙古自治区深圳县滨城崔路v座 779641', 1630000000094) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000094000, 1112625721, 8513911397651588080, 5730, 38, 73890.778568, -22.990146, 1, 'binary.SsmtnsRsHPgvIhHrYqyO', 'nchar.海南省涛市吉区史街q座 108712', 1630000000094) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000094001, -140973401, -5123140165105748262, -16365, -73, -96876100872181.296875, -40423.619081, 1, 'binary.FsDucYnXWFsWeULdYWOy', 'nchar.江西省台北市大东王路C座 228618', 1630000000095) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000094001, -972503780, -5335135036468864350, -30876, -11, -9126000108657.949219, -539915872.980128, 1, 'binary.cFIyYCLpOcnVVGpmYsvW', 'nchar.天津市佳市永川上海路f座 391083', 1630000000095) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000094000, 750938596, 8996152659385333115, 4520, -82, 177630561037.372986, -700.442519, 0, 'binary.PKxHAAjJDUmuZdsVlSdn', 'nchar.甘肃省倩市淄川沈路O座 565139', 1630000000094) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000095000, 1497231979, -3308043702122804898, -21789, 19, 83018510637330.703125, 2.799482, 0, 'binary.NFRgOkQCsTNqXTXqrMam', 'nchar.新疆维吾尔自治区哈尔滨县吉区黄路r座 249374', 1630000000095) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000095000, -361400303, -3298055192272530115, -32487, 41, -715.143790, 96906340957.879501, 0, 'binary.ggrflOzPricGBCfwlllA', 'nchar.台湾省志强市城北翟街b座 213548', 1630000000095) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000095000, 329723052, 3345203638509855816, 13663, 10, -48588556157.981499, 35378691883.154999, 1, 'binary.ZjlsCSUFfSDxnZWHogfP', 'nchar.湖北省南昌县沈河王路i座 348377', 1630000000095) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000095000, 864146472, 5666121126378699571, 9548, 104, -2252746.209748, -93578145344651.500000, 1, 'binary.AVHKcFLrUcFPZquZNeOh', 'nchar.河南省淑英市长寿宜都街s座 122342', 1630000000095) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000095001, -942796464, -8296022352836675674, -31332, -12, 4210888769158.810059, -85512980.263986, 1, 'binary.fsrayptWCCYFcKdceOdC', 'nchar.北京市伟县六枝特李路z座 635640', 1630000000096) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000095001, -1888423447, -2294188447530858110, -17095, -122, -1318.540758, -6224487226.178020, 1, 'binary.SxXqWpzogtuJZXbDQRvf', 'nchar.云南省阳市璧山颜街I座 671632', 1630000000096) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000095000, 1640346419, -4655634826659777565, 6231, -90, -105.664501, -476938.550746, 0, 'binary.CroOTgZNEcbInZTTbzyK', 'nchar.甘肃省太原县花溪刘路q座 932901', 1630000000095) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000096000, 875450040, -2086854791032037580, 24188, -18, -33341375.338635, -593.775281, 0, 'binary.TltjuwBTsHhccgFwueec', 'nchar.湖南省玉梅县沙市阜新路t座 548392', 1630000000096) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000096000, -2059537912, 7530061005422328124, 12882, -90, 107.982288, 15144545603.722601, 0, 'binary.giDypqHwYyPANLOoIssh', 'nchar.北京市合山市六枝特太原路B座 126582', 1630000000096) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000096000, 762599698, 787649402587182542, 25192, 4, 18.885219, 58824196350.280998, 1, 'binary.IKmxRBJGrpilqXuFIxZu', 'nchar.广东省邯郸市上街永安街a座 992428', 1630000000096) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000096000, 392085822, 1972377793126888439, 2697, 72, 37431081878001.898438, -54.407402, 1, 'binary.IygJhdkhFKEgkDVuDWiP', 'nchar.江苏省欢县平山张路z座 163203', 1630000000096) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000096001, -1507903796, -5637372946592285656, -27012, -50, -595312309.209642, -66711877.983604, 1, 'binary.dRvEgzLBoslJilYbrLHl', 'nchar.吉林省郑州县大东张街w座 280633', 1630000000097) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000096001, -1753536013, -1035237687503078825, -10057, -83, -42477533116173.601562, 583.594117, 1, 'binary.fNAyfIPVXXgXaceJEnAt', 'nchar.广东省上海市长寿马路S座 246812', 1630000000097) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000096000, -1559826116, -7359164034562072414, -2530, 99, 86349.103142, 25149.389500, 0, 'binary.ErXMHRyCpXVRxJVdjVHm', 'nchar.云南省欢市沈北新辛集街G座 764426', 1630000000096) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000097000, 1567878510, 3083107009125447, 32726, 1, -42.947442, -3005936149.318940, 0, 'binary.ZxNzXTtLyonMRGHoLkZG', 'nchar.澳门特别行政区重庆县魏都齐齐哈尔路f座 391606', 1630000000097) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000097000, -983043510, -2251649685057793980, 23028, 75, 401.146359, -269944403.901722, 0, 'binary.ZAGwlgJeRysaUwtAnkNZ', 'nchar.辽宁省博县龙潭李街g座 901703', 1630000000097) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000097000, 411305177, 7485680019685488040, 15248, 108, 4808038.107839, -6267700553450.580078, 1, 'binary.UjTANlNMVFKmSFebeYpJ', 'nchar.云南省六安市蓟州香港街z座 950917', 1630000000097) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000097000, 1141015297, 6951675438179506710, 12772, 77, 78005983.975238, 65455663119.913300, 1, 'binary.wlHJUoADdqNFWPFGLrCw', 'nchar.辽宁省惠州县山亭昆明路f座 816936', 1630000000097) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000097001, -1475235475, -4275766069381626256, -26284, -126, -449435826231.403992, 17754498.847360, 1, 'binary.njyjZXiYlkeWsmfgIxxn', 'nchar.云南省六安县新城澳门路b座 322831', 1630000000098) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000097001, -1898465579, -8548878959611227078, -30552, -6, -29041976.824253, 2444037833260.700195, 1, 'binary.cGvsJYbpoSUvEAYLvTGf', 'nchar.辽宁省健县东城王路a座 189980', 1630000000098) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000097000, -859676205, 8669088172759819155, 1056, 106, -43.864525, 7851310.326290, 0, 'binary.JKpbBamEgwUBBbLfesfe', 'nchar.安徽省峰县吉区南宁路k座 316430', 1630000000097) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000098000, 939221461, -3732554921929467269, 1323, 82, 6427474568786.700195, -499628590.846885, 0, 'binary.xqdgEAvsxtcBAeOdTigh', 'nchar.山东省畅县沈河田街Q座 754725', 1630000000098) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000098000, -1237982251, -48417695454496564, 7581, -26, -87.308286, 688667.769241, 0, 'binary.gNRWfPsAQkeMSJfXqaAR', 'nchar.江苏省明县东丽黎路v座 731371', 1630000000098) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000098000, 1155953615, 8350656021706486176, 31391, 91, 37825814.797034, -9908887329897.919922, 1, 'binary.FoxGwMJtUHTIpRHpDgGT', 'nchar.甘肃省玉华市沙湾惠州路e座 848473', 1630000000098) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000098000, 2044466802, 9048343097762912848, 21278, 93, 92.670973, 541962.504630, 1, 'binary.qzKFQCIvBjWCxMbgAyAL', 'nchar.河北省宇县永川沈阳街D座 973307', 1630000000098) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000098001, -1633023943, -1961394684312695300, -19064, -50, -1845722.315086, -696993904372.139038, 1, 'binary.BBBTYILqbBKsBqJxxSwj', 'nchar.山西省志强市花溪刘街R座 216246', 1630000000099) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000098001, -81189563, -9114410960726781868, -9902, -76, 133322.195036, 2679.572143, 1, 'binary.oLmLhSxbSMeEjhmfyFUy', 'nchar.香港特别行政区明市高坪梁街Z座 837862', 1630000000099) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000098000, 1231242332, 8911299464172143101, 23470, 54, 9698.810809, 728.687012, 0, 'binary.AifGmmbdIfTPQSGzUkrJ', 'nchar.新疆维吾尔自治区红霞县南溪大冶路G座 828834', 1630000000098) ;")

        tdSql.execute("insert into stable_1_1  (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double , q_bool , q_binary , q_nchar, q_ts) values(1630000099000, 1644534011, 3125802916837644510, -26106, -10, -834358151032.780029, -57474849406.678001, 0, 'binary.FWVctHTUcZbCSKaKgIhC', 'nchar.陕西省桂兰市南长施街V座 796885', 1630000000099) ;")

        tdSql.execute("insert into  regular_table_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000099000, 381539499, 867213000144832412, 11082, 42, 57220.336572, 209844.635055, 0, 'binary.QGXxKQPDjUUcvXpPWnjc', 'nchar.甘肃省想市兴山李街R座 989501', 1630000000099) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000099000, 1462175451, 8692156614590641458, 14266, 99, 23163851923.519299, 6998526168681.620117, 1, 'binary.oqpqHRblTLCiJvIlnDfM', 'nchar.陕西省梅县江北夏路F座 512130', 1630000000099) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000099000, 1209330517, 8180579910238165547, 7863, 37, 64598433327.966797, -6551304300.792400, 1, 'binary.gNCATCVHpgxNokfZTaPj', 'nchar.西藏自治区欣县东丽吕街p座 674253', 1630000000099) ;")

        tdSql.execute("insert into stable_1_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000099001, -1194410621, -6267737311898067321, -13953, -78, 455904882694.192993, 93991410230.119995, 1, 'binary.USarBghLFHYRFGeLIQRZ', 'nchar.天津市天津县牧野重庆街H座 405910', 1630000000100) ;")

        tdSql.execute("insert into regular_table_2 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000099001, -1144816999, -8569406363517077776, -22031, -14, 3486675.847699, 423303671.483930, 1, 'binary.gMadLXmKInSrhkWwMJjk', 'nchar.河北省西安市梁平包路A座 574270', 1630000000100) ;")

        tdSql.execute("insert into stable_2_1 (ts , q_int , q_bigint , q_smallint , q_tinyint , q_float , q_double, q_bool , q_binary , q_nchar, q_ts) values(1630000099000, 199204759, -4620028387247442955, 2200, 126, 2456606795499.439941, 16.357118, 0, 'binary.GaTAvbDtTWGZbWyxprht', 'nchar.广西壮族自治区俊市崇文济南街u座 179856', 1630000000099) ;")

        tdSql.query('select count(*) from stable_2_1 ;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)

        tdSql.query('select HYPERLOGLOG(q_bool_null) from stable_2_1 STATE_WINDOW(q_bool) order by ts ;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select HYPERLOGLOG(q_bool_null) from stable_2_1 STATE_WINDOW(q_bool) order by ts ;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select HYPERLOGLOG(q_bool_null) from stable_2_1 STATE_WINDOW(q_bool) order by ts ;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select HYPERLOGLOG(q_bool_null) from stable_2_1 STATE_WINDOW(q_bool) order by ts ;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select HYPERLOGLOG(q_bool_null) from stable_2_1 STATE_WINDOW(q_bool) order by ts ;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select HYPERLOGLOG(q_bool_null) from stable_2_1 STATE_WINDOW(q_bool) order by ts desc;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select HYPERLOGLOG(q_bool_null) from stable_2_1 STATE_WINDOW(q_bool) order by ts desc;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select HYPERLOGLOG(q_bool_null) from stable_2_1 STATE_WINDOW(q_bool) order by ts desc;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select HYPERLOGLOG(q_bool_null) from stable_2_1 STATE_WINDOW(q_bool) order by ts desc;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query('select HYPERLOGLOG(q_bool_null) from stable_2_1 STATE_WINDOW(q_bool) order by ts desc;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.execute('drop database hll_ts_desc')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
