using System;
using Utils;

namespace Cases
{

    class Program
    {
        static void Main(string[] args)
        {
            IntPtr conn = IntPtr.Zero;
            Console.WriteLine("===================JsonTagTest====================");
            conn = conn = UtilsTools.TDConnection("127.0.0.1", "root", "taosdata", "", 0);
            UtilsTools.ExecuteUpdate(conn, "create database if not exists csharp keep 3650");
            UtilsTools.ExecuteUpdate(conn, "use csharp");
            JsonTagSample jsonTagSample = new JsonTagSample();
            jsonTagSample.Test(conn);
        }

    }
    
    public class JsonTagSample
    {
        public void Test(IntPtr conn)
        {
            Console.WriteLine("STEP 1 prepare data & validate json string===== ");
            UtilsTools.ExecuteQuery(conn, "create table if not exists jsons1(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)");
            UtilsTools.ExecuteQuery(conn, "insert into jsons1_1 using jsons1 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 1, false, 'json1', '涛思数据') (1591060608000, 23, true, '涛思数据', 'json')");
            UtilsTools.ExecuteQuery(conn, "insert into jsons1_2 using jsons1 tags('{\"tag1\":5,\"tag2\":\"beijing\"}') values (1591060628000, 2, true, 'json2', 'sss')");
            UtilsTools.ExecuteQuery(conn, "insert into jsons1_3 using jsons1 tags('{\"tag1\":false,\"tag2\":\"beijing\"}') values (1591060668000, 3, false, 'json3', 'efwe')");
            UtilsTools.ExecuteQuery(conn, "insert into jsons1_4 using jsons1 tags('{\"tag1\":null,\"tag2\":\"shanghai\",\"tag3\":\"hello\"}') values (1591060728000, 4, true, 'json4', '323sd')");
            UtilsTools.ExecuteQuery(conn, "insert into jsons1_5 using jsons1 tags('{\"tag1\":1.232, \"tag2\":null}') values(1591060928000, 1, false, '涛思数据', 'ewe')");
            UtilsTools.ExecuteQuery(conn, "insert into jsons1_6 using jsons1 tags('{\"tag1\":11,\"tag2\":\"\",\"tag2\":null}') values(1591061628000, 11, false, '涛思数据','')");
            UtilsTools.ExecuteQuery(conn, "insert into jsons1_7 using jsons1 tags('{\"tag1\":\"涛思数据\",\"tag2\":\"\",\"tag3\":null}') values(1591062628000, 2, NULL, '涛思数据', 'dws')");
            Console.WriteLine("");

            Console.WriteLine("test duplicate key using the first one. elimate empty key======== ");
            UtilsTools.ExecuteQuery(conn, "CREATE TABLE if not exists jsons1_8 using jsons1 tags('{\"tag1\":null, \"tag1\":true, \"tag1\":45, \"1tag$\":2, \" \":90}')");
            Console.WriteLine("");

            Console.WriteLine("test empty json string, save as jtag is NULL========== ");
            UtilsTools.ExecuteQuery(conn, "insert into jsons1_9  using jsons1 tags('\t') values (1591062328000, 24, NULL, '涛思数据', '2sdw')");
            UtilsTools.ExecuteQuery(conn, "CREATE TABLE if not exists jsons1_10 using jsons1 tags('')");
            UtilsTools.ExecuteQuery(conn, "CREATE TABLE if not exists jsons1_11 using jsons1 tags(' ')");
            UtilsTools.ExecuteQuery(conn, "CREATE TABLE if not exists jsons1_12 using jsons1 tags('{}')");
            UtilsTools.ExecuteQuery(conn, "CREATE TABLE if not exists jsons1_13 using jsons1 tags('null')");
            Console.WriteLine("");

            Console.WriteLine("test invalidate json==================== ");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('\"efwewf\"')");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('3333')");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('33.33')");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('false')");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('[1,true]')");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('{222}')");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"fe\"}')");
            Console.WriteLine("");

            Console.WriteLine("test invalidate json key, key must can be printed assic char========== ");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"tag1\":[1,true]}')");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"tag1\":{}}')");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"。loc\":\"fff\"}')");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"\":\"fff\"}')");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"\t\":\"fff\"}')");
            UtilsTools.ExecuteErrorQuery(conn, "CREATE TABLE if not exists jsons1_14 using jsons1 tags('{\"试试\":\"fff\"}')");
            Console.WriteLine("");

            Console.WriteLine("STEP 2 alter table json tag============");
            UtilsTools.ExecuteErrorQuery(conn, "ALTER STABLE jsons1 add tag tag2 nchar(20)");
            UtilsTools.ExecuteErrorQuery(conn, "ALTER STABLE jsons1 drop tag jtag");
            UtilsTools.ExecuteErrorQuery(conn, "ALTER TABLE jsons1_1 SET TAG jtag=4");
            UtilsTools.ExecuteQuery(conn, "ALTER TABLE jsons1_1 SET TAG jtag='{\"tag1\":\"femail\",\"tag2\":35,\"tag3\":true}'");
            Console.WriteLine("");

            Console.WriteLine("STEP 3 query table============");
            Console.WriteLine("test error syntax============");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag->tag1='beijing'");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag->'location'");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag->''");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag->''=9");
            UtilsTools.ExecuteErrorQuery(conn, "select -> from jsons1");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where contains");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag->");
            UtilsTools.ExecuteErrorQuery(conn, "select jtag->location from jsons1");
            UtilsTools.ExecuteErrorQuery(conn, "select jtag contains location from jsons1");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag contains location");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag contains''");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag contains 'location'='beijing'");
            Console.WriteLine("");

            Console.WriteLine("test select normal column===========");
            IntPtr res = IntPtr.Zero;
            res = UtilsTools.ExecuteQuery(conn, "select dataint from jsons1");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test select json tag===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select jtag from jsons1");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select jtag from jsons1 where jtag is null");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select jtag from jsons1 where jtag is not null");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test #line 41===========");
            res = UtilsTools.ExecuteQuery(conn, "select jtag from jsons1_8");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test #line 72===========");
            res = UtilsTools.ExecuteQuery(conn, "select jtag from jsons1_1");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test jtag is NULL===========");
            res = UtilsTools.ExecuteQuery(conn, "select jtag from jsons1_9");
            UtilsTools.DisplayRes(res);


            Console.WriteLine("test select json tag->'key', value is string ===========");
            res = UtilsTools.ExecuteQuery(conn, "select jtag->'tag1' from jsons1_1");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select jtag->'tag2' from jsons1_6");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test select json tag->'key', value is int===========");
            res = UtilsTools.ExecuteQuery(conn, "select jtag->'tag2' from jsons1_1");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test select json tag->'key', value is bool===========");
            res = UtilsTools.ExecuteQuery(conn, "select jtag->'tag3' from jsons1_1");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test select json tag->'key', value is null===========");
            res = UtilsTools.ExecuteQuery(conn, "select jtag->'tag1' from jsons1_4");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test select json tag->'key', value is double===========");
            res = UtilsTools.ExecuteQuery(conn, "select jtag->'tag1' from jsons1_5");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test select json tag->'key', key is not exist===========");
            res = UtilsTools.ExecuteQuery(conn, "select jtag->'tag10' from jsons1_4");
            UtilsTools.DisplayRes(res);

            res = UtilsTools.ExecuteQuery(conn, "select jtag->'tag1' from jsons1");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test header name===========");
            res = UtilsTools.ExecuteQuery(conn, "select jtag->'tag1' from jsons1");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test where with json tag===========");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1_1 where jtag is not null");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag='{\"tag1\":11,\"tag2\":\"\"}'");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag->'tag1'={}");

            Console.WriteLine("where json value is string===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag2'='beijing'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select dataint,tbname,jtag->'tag1',jtag from jsons1 where jtag->'tag2'='beijing'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'='beijing'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'='涛思数据'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag2'>'beijing'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag2'>='beijing'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag2'<'beijing'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag2'<='beijing'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag2'!='beijing'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag2'=''");
            UtilsTools.DisplayRes(res);


            Console.WriteLine("where json value is int===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=5");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=10");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'<54");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'<=11");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'>4");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'>=5");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'!=5");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'!=55");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("where json value is double===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=1.232");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'<1.232");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'<=1.232");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'>1.23");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'>=1.232");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'!=1.232");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'!=3.232");
            UtilsTools.DisplayRes(res);
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag->'tag1'/0=3");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag->'tag1'/5=1");

            Console.WriteLine("where json value is bool===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=true");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=false");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'!=false");
            UtilsTools.DisplayRes(res);
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag->'tag1'>false");

            Console.WriteLine("where json value is null===========");
            Console.WriteLine("only json suport =null. This synatx will change later.===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=null");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("where json is null===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag is null");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag is not null");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("where json key is null===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag_no_exist'=3");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("where json value is not exist===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1' is null");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag4' is null");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag3' is not null");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test contains===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag contains 'tag1'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag contains 'tag3'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag contains 'tag_no_exist'");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test json tag in where condition with and/or===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=false and jtag->'tag2'='beijing'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=false or jtag->'tag2'='beijing'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=false and jtag->'tag2'='shanghai'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=false and jtag->'tag2'='shanghai'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=13 or jtag->'tag2'>35");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'=13 or jtag->'tag2'>35");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1' is not null and jtag contains 'tag3'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1'='femail' and jtag contains 'tag3'");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test with tbname/normal column===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where tbname = 'jsons1_1'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3' and dataint=3");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where tbname = 'jsons1_1' and jtag contains 'tag3' and dataint=23");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test where condition like===========");
            res = UtilsTools.ExecuteQuery(conn, "select *,tbname from jsons1 where jtag->'tag2' like 'bei%'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select *,tbname from jsons1 where jtag->'tag1' like 'fe%' and jtag->'tag2' is not null");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test where condition in  no support in===========");
            UtilsTools.ExecuteErrorQuery(conn, "select * from jsons1 where jtag->'tag1' in ('beijing')");

            Console.WriteLine("test where condition match===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1' match 'ma'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1' match 'ma$'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag2' match 'jing$'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select * from jsons1 where jtag->'tag1' match '收到'");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test distinct===========");
            UtilsTools.ExecuteQuery(conn, "insert into jsons1_14 using jsons1 tags('{\"tag1\":\"涛思数据\",\"tag2\":\"\",\"tag3\":null}') values(1591062628000, 2, NULL, '涛思数据', 'dws')");
            res = UtilsTools.ExecuteQuery(conn, "select distinct jtag->'tag1' from jsons1");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select distinct jtag from jsons1");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test dumplicate key with normal colomn===========");
            UtilsTools.ExecuteQuery(conn, "INSERT INTO jsons1_15 using jsons1 tags('{\"tbname\":\"tt\",\"databool\":true,\"datastr\":\"涛思数据\"}') values(1591060828000, 4, false, 'jjsf', \"你就会\")");
            res = UtilsTools.ExecuteQuery(conn, "select *,tbname,jtag from jsons1 where jtag->'datastr' match '涛思数据' and datastr match 'js'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select tbname,jtag->'tbname' from jsons1 where jtag->'tbname'='tt' and tbname='jsons1_14'");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test join===========");
            UtilsTools.ExecuteQuery(conn, "create table if not exists jsons2(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)");
            UtilsTools.ExecuteQuery(conn, "insert into jsons2_1 using jsons2 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 2, false, 'json2', '涛思数据2')");
            UtilsTools.ExecuteQuery(conn, "insert into jsons2_2 using jsons2 tags('{\"tag1\":5,\"tag2\":null}') values (1591060628000, 2, true, 'json2', 'sss')");
            UtilsTools.ExecuteQuery(conn, "create table if not exists jsons3(ts timestamp, dataInt int, dataBool bool, dataStr nchar(50), dataStrBin binary(150)) tags(jtag json)");
            UtilsTools.ExecuteQuery(conn, "insert into jsons3_1 using jsons3 tags('{\"tag1\":\"fff\",\"tag2\":5, \"tag3\":true}') values(1591060618000, 3, false, 'json3', '涛思数据3')");
            UtilsTools.ExecuteQuery(conn, "insert into jsons3_2 using jsons3 tags('{\"tag1\":5,\"tag2\":\"beijing\"}') values (1591060638000, 2, true, 'json3', 'sss')");

            res = UtilsTools.ExecuteQuery(conn, "select 'sss',33,a.jtag->'tag3' from jsons2 a,jsons3 b where a.ts=b.ts and a.jtag->'tag1'=b.jtag->'tag1'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select 'sss',33,a.jtag->'tag3' from jsons2 a,jsons3 b where a.ts=b.ts and a.jtag->'tag1'=b.jtag->'tag1'");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test group by & order by  json tag===========");
            res = UtilsTools.ExecuteQuery(conn, "select count(*) from jsons1 group by jtag->'tag1' order by jtag->'tag1' desc");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select count(*) from jsons1 group by jtag->'tag1' order by jtag->'tag1' asc");
            UtilsTools.DisplayRes(res);


            Console.WriteLine("test stddev with group by json tag===========");
            res = UtilsTools.ExecuteQuery(conn, "select stddev(dataint) from jsons1 group by jtag->'tag1'");
            UtilsTools.DisplayRes(res);
            res = UtilsTools.ExecuteQuery(conn, "select stddev(dataint) from jsons1 group by jsons1.jtag->'tag1'");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("test top/bottom with group by json tag===========");
            res = UtilsTools.ExecuteQuery(conn, "select top(dataint,100) from jsons1 group by jtag->'tag1'");
            UtilsTools.DisplayRes(res);

            Console.WriteLine("subquery with json tag===========");
            res = UtilsTools.ExecuteQuery(conn, "select * from (select jtag, dataint from jsons1)");
            UtilsTools.DisplayRes(res);


            res = UtilsTools.ExecuteQuery(conn, "select jtag->'tag1' from (select jtag->'tag1', dataint from jsons1)");
            UtilsTools.DisplayRes(res);

            res = UtilsTools.ExecuteQuery(conn, "select jtag->'tag1' from (select jtag->'tag1', dataint from jsons1)");
            UtilsTools.DisplayRes(res);

            res = UtilsTools.ExecuteQuery(conn, "select ts,tbname,jtag->'tag1' from (select jtag->'tag1',tbname,ts from jsons1 order by ts)");
            UtilsTools.DisplayRes(res);
            Console.WriteLine("");


        }
    }
}