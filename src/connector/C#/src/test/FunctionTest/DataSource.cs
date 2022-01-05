using System;
using Test.UtilsTools;
using TDengineDriver;
using System.Collections.Generic;
namespace Test.UtilsTools.DataSource
{
    public class DataSource
    {
        public static long[] tsArr = new long[5] { 1637064040000, 1637064041000, 1637064042000, 1637064043000, 1637064044000 };
        public static bool?[] boolArr = new bool?[5] { true, false, null, true, true };
        public static sbyte?[] tinyIntArr = new sbyte?[5] { -127, 0, null, 8, 127 };
        public static short?[] shortArr = new short?[5] { short.MinValue + 1, -200, null, 100, short.MaxValue };
        public static int?[] intArr = new int?[5] { -200, -100, null, 0, 300 };
        public static long?[] longArr = new long?[5] { long.MinValue + 1, -2000, null, 1000, long.MaxValue };
        public static float?[] floatArr = new float?[5] { float.MinValue + 1, -12.1F, null, 0F, float.MaxValue };
        public static double?[] doubleArr = new double?[5] { double.MinValue + 1, -19.112D, null, 0D, double.MaxValue };
        public static byte?[] uTinyIntArr = new byte?[5] { byte.MinValue, 12, null, 89, byte.MaxValue - 1 };
        public static ushort?[] uShortArr = new ushort?[5] { ushort.MinValue, 200, null, 400, ushort.MaxValue - 1 };
        public static uint?[] uIntArr = new uint?[5] { uint.MinValue, 100, null, 2, uint.MaxValue - 1 };
        public static ulong?[] uLongArr = new ulong?[5] { ulong.MinValue, 2000, null, 1000, long.MaxValue - 1 };
        public static string[] binaryArr = new string[5] { "1234567890~!@#$%^&*()_+=-`[]{}:,./<>?", String.Empty, null, "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM", "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890~!@#$%^&*()_+=-`[]{}:,./<>?" };
        public static string[] ncharArr = new string[5] { "1234567890~!@#$%^&*()_+=-`[]{}:,./<>?", null, "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM", "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890~!@#$%^&*()_+=-`[]{}:,./<>?", string.Empty };

        public static string[] binaryArrCn = new string[5] { "涛思数据", String.Empty, null, "taosdata涛思数据", "涛思数据TDengine" };
        public static string[] NcharArrCn = new string[5] { "涛思数据", null, "taosdata涛思数据", "涛思数据TDengine", String.Empty };

        // Construct a TAOS_BIND array which contains normal character.
        // For stmt bind tags,this will be used as tag info
        public static TAOS_BIND[] GetTags()
        {
            TAOS_BIND[] binds = new TAOS_BIND[13];
            binds[0] = TaosBind.BindBool(true);
            binds[1] = TaosBind.BindTinyInt(-2);
            binds[2] = TaosBind.BindSmallInt(short.MaxValue);
            binds[3] = TaosBind.BindInt(int.MaxValue);
            binds[4] = TaosBind.BindBigInt(Int64.MaxValue);
            binds[5] = TaosBind.BindUTinyInt(byte.MaxValue - 1);
            binds[6] = TaosBind.BindUSmallInt(UInt16.MaxValue - 1);
            binds[7] = TaosBind.BindUInt(uint.MinValue + 1);
            binds[8] = TaosBind.BindUBigInt(UInt64.MinValue + 1);
            binds[9] = TaosBind.BindFloat(11.11F);
            binds[10] = TaosBind.BindDouble(22.22D);
            binds[11] = TaosBind.BindBinary("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKZXCVBNM`1234567890-=+_)(*&^%$#@!~[];,./<>?:{}");
            binds[12] = TaosBind.BindNchar("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKZXCVBNM`1234567890-=+_)(*&^%$#@!~[];,./<>?:{}");
            return binds;
        }
        // Get the tag data within and string list 
        // Which will be retrieved as a string List
        private static List<String> GetTagData()
        {
            List<String> tagData = new List<String>();
            tagData.Add(true.ToString());
            tagData.Add((-2).ToString());
            tagData.Add((short.MaxValue).ToString());
            tagData.Add((int.MaxValue).ToString());
            tagData.Add((Int64.MaxValue).ToString());
            tagData.Add((byte.MaxValue - 1).ToString());
            tagData.Add((UInt16.MaxValue - 1).ToString());
            tagData.Add((uint.MinValue + 1).ToString());
            tagData.Add((UInt64.MinValue + 1).ToString());
            tagData.Add((11.11F).ToString());
            tagData.Add((22.22D).ToString());
            tagData.Add("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKZXCVBNM`1234567890-=+_)(*&^%$#@!~[];,./<>?:{}");
            tagData.Add("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKZXCVBNM`1234567890-=+_)(*&^%$#@!~[];,./<>?:{}");
            return tagData;
        }

        public static List<string> GetMultiBindStableRowData()
        {
            List<string> rowData = new List<String>();
            List<string> tagData = GetTagData();
            for (int i = 0; i < tsArr.Length; i++)
            {
                rowData.Add(tsArr[i].ToString());
                rowData.Add(boolArr[i].Equals(null) ? "NULL" : boolArr[i].ToString());
                rowData.Add(tinyIntArr[i].Equals(null) ? "NULL" : tinyIntArr[i].ToString());
                rowData.Add(shortArr[i].Equals(null) ? "NULL" : shortArr[i].ToString());
                rowData.Add(intArr[i].Equals(null) ? "NULL" : intArr[i].ToString());
                rowData.Add(longArr[i].Equals(null) ? "NULL" : longArr[i].ToString());
                rowData.Add(floatArr[i].Equals(null) ? "NULL" : floatArr[i].ToString());
                rowData.Add(doubleArr[i].Equals(null) ? "NULL" : doubleArr[i].ToString());
                rowData.Add(uTinyIntArr[i].Equals(null) ? "NULL" : uTinyIntArr[i].ToString());
                rowData.Add(uShortArr[i].Equals(null) ? "NULL" : uShortArr[i].ToString());
                rowData.Add(uIntArr[i].Equals(null) ? "NULL" : uIntArr[i].ToString());
                rowData.Add(uLongArr[i].Equals(null) ? "NULL" : uLongArr[i].ToString());
                rowData.Add(String.IsNullOrEmpty(binaryArr[i]) ? "NULL" : binaryArr[i]);
                rowData.Add(String.IsNullOrEmpty(ncharArr[i]) ? "NULL" : ncharArr[i]);
                rowData.AddRange(tagData);
                // Console.WriteLine("binaryArrCn[{0}]:{1},ncharArr[{0}]:{2}",i,String.IsNullOrEmpty(binaryArrCn[i]) ? "NULL" : binaryArrCn[i],String.IsNullOrEmpty(ncharArr[i]) ? "NULL" : NcharArrCn[i]);
                // Console.WriteLine("binaryArrCn[{0}]:{1},ncharArr[{0}]:{2}",i,String.IsNullOrEmpty(binaryArrCn[i]) ? 0 :binaryArrCn[i].Length, String.IsNullOrEmpty(ncharArr[i]) ? 0 : NcharArrCn[i].Length);
                // Console.WriteLine("========");

            }
            return rowData;

        }
        // Construct a TAOS_BIND array which contains chinese character.
        // For stmt bind tags,this will be used as tag info
        public static TAOS_BIND[] GetCNTags()
        {
            TAOS_BIND[] binds = new TAOS_BIND[13];
            binds[0] = TaosBind.BindBool(true);
            binds[1] = TaosBind.BindTinyInt(-2);
            binds[2] = TaosBind.BindSmallInt(short.MaxValue - 1);
            binds[3] = TaosBind.BindInt(int.MaxValue - 1);
            binds[4] = TaosBind.BindBigInt(Int64.MaxValue - 1);
            binds[5] = TaosBind.BindUTinyInt(byte.MaxValue - 1);
            binds[6] = TaosBind.BindUSmallInt(UInt16.MaxValue - 1);
            binds[7] = TaosBind.BindUInt(uint.MinValue + 1);
            binds[8] = TaosBind.BindUBigInt(UInt64.MinValue + 1);
            binds[9] = TaosBind.BindFloat(11.11F);
            binds[10] = TaosBind.BindDouble(22.22D);
            binds[11] = TaosBind.BindBinary("TDengine涛思数据");
            binds[12] = TaosBind.BindNchar("涛思数据taos");
            return binds;
        }
        // Get the tag data within and string list 
        // Which will be retrieved as a string List
        private static List<String> GetTagCnData()
        {
            List<String> tagData = new List<String>();
            tagData.Add(true.ToString());
            tagData.Add((-2).ToString());
            tagData.Add((short.MaxValue - 1).ToString());
            tagData.Add((int.MaxValue - 1).ToString());
            tagData.Add((Int64.MaxValue - 1).ToString());
            tagData.Add((byte.MaxValue - 1).ToString());
            tagData.Add((UInt16.MaxValue - 1).ToString());
            tagData.Add((uint.MinValue + 1).ToString());
            tagData.Add((UInt64.MinValue + 1).ToString());
            tagData.Add((11.11F).ToString());
            tagData.Add((22.22D).ToString());
            tagData.Add("TDengine涛思数据");
            tagData.Add("涛思数据taos");
            return tagData;
        }
        // A line of data that's without CN character.
        // Which is construct as an TAOS_BIND array
        public static TAOS_BIND[] GetNtableCNRow()
        {
            TAOS_BIND[] binds = new TAOS_BIND[15];
            binds[0] = TaosBind.BindTimestamp(1637064040000);
            binds[1] = TaosBind.BindTinyInt(-2);
            binds[2] = TaosBind.BindSmallInt(short.MaxValue);
            binds[3] = TaosBind.BindInt(int.MaxValue);
            binds[4] = TaosBind.BindBigInt(Int64.MaxValue);
            binds[5] = TaosBind.BindUTinyInt(byte.MaxValue - 1);
            binds[6] = TaosBind.BindUSmallInt(UInt16.MaxValue - 1);
            binds[7] = TaosBind.BindUInt(uint.MinValue + 1);
            binds[8] = TaosBind.BindUBigInt(UInt64.MinValue + 1);
            binds[9] = TaosBind.BindFloat(11.11F);
            binds[10] = TaosBind.BindDouble(22.22D);
            binds[11] = TaosBind.BindBinary("TDengine数据");
            binds[12] = TaosBind.BindNchar("taosdata涛思数据");
            binds[13] = TaosBind.BindBool(true);
            binds[14] = TaosBind.BindNil();
            return binds;
        }
        //Get and list data that will be insert into table
        public static List<String> GetNtableCNRowData()
        {
            var data = new List<string>{
                "1637064040000",
                "-2",
                short.MaxValue.ToString(),
                int.MaxValue.ToString(),
                Int64.MaxValue.ToString(),
                (byte.MaxValue - 1).ToString(),
                (UInt16.MaxValue - 1).ToString(),
                (uint.MinValue + 1).ToString(),
                (UInt64.MinValue + 1).ToString(),
                (11.11F).ToString(),
                (22.22D).ToString(),
                "TDengine数据",
                "taosdata涛思数据",
                "True",
                "NULL"
            };
            return data;
        }
        // Get the data value and tag values which have chinese characters
        // And retrieved as a string list.This is single Line.
        public static List<String> GetStableCNRowData()
        {
            List<String> columnData = GetNtableCNRowData();
            List<String> tagData = GetTagCnData();
            columnData.AddRange(tagData);
            return columnData;
        }

        // A line of data that's without CN character
        public static TAOS_BIND[] GetNtableRow()
        {
            TAOS_BIND[] binds = new TAOS_BIND[15];
            binds[0] = TaosBind.BindTimestamp(1637064040000);
            binds[1] = TaosBind.BindTinyInt(-2);
            binds[2] = TaosBind.BindSmallInt(short.MaxValue);
            binds[3] = TaosBind.BindInt(int.MaxValue);
            binds[4] = TaosBind.BindBigInt(Int64.MaxValue);
            binds[5] = TaosBind.BindUTinyInt(byte.MaxValue - 1);
            binds[6] = TaosBind.BindUSmallInt(UInt16.MaxValue - 1);
            binds[7] = TaosBind.BindUInt(uint.MinValue + 1);
            binds[8] = TaosBind.BindUBigInt(UInt64.MinValue + 1);
            binds[9] = TaosBind.BindFloat(11.11F);
            binds[10] = TaosBind.BindDouble(22.22D);
            binds[11] = TaosBind.BindBinary("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKZXCVBNM`1234567890-=+_)(*&^%$#@!~[];,./<>?:{}");
            binds[12] = TaosBind.BindNchar("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKZXCVBNM`1234567890-=+_)(*&^%$#@!~[];,./<>?:{}");
            binds[13] = TaosBind.BindBool(true);
            binds[14] = TaosBind.BindNil();
            return binds;
        }
        // A List of data ,use as expectResData. The value is equal to getNtableRow()
        public static List<String> GetNtableRowData()
        {
            var data = new List<string>{
            "1637064040000",
            "-2",
            short.MaxValue.ToString(),
            int.MaxValue.ToString(),
            (Int64.MaxValue).ToString(),
            (byte.MaxValue - 1).ToString(),
            (UInt16.MaxValue - 1).ToString(),
            (uint.MinValue + 1).ToString(),
            (UInt64.MinValue + 1).ToString(),
            (11.11F).ToString(),
            (22.22D).ToString(),
            "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKZXCVBNM`1234567890-=+_)(*&^%$#@!~[];,./<>?:{}",
            "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKZXCVBNM`1234567890-=+_)(*&^%$#@!~[];,./<>?:{}",
            true.ToString(),
            "NULL"
            };
            return data;
        }

        // Five lines of data, that is construct as taos_mutli_bind array. 
        // There aren't any CN character
        public static TAOS_MULTI_BIND[] GetMultiBindArr()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[14];
            mBinds[0] = TaosMultiBind.MultiBindTimestamp(tsArr);
            mBinds[1] = TaosMultiBind.MultiBindBool(boolArr);
            mBinds[2] = TaosMultiBind.MultiBindTinyInt(tinyIntArr);
            mBinds[3] = TaosMultiBind.MultiBindSmallInt(shortArr);
            mBinds[4] = TaosMultiBind.MultiBindInt(intArr);
            mBinds[5] = TaosMultiBind.MultiBindBigint(longArr);
            mBinds[6] = TaosMultiBind.MultiBindFloat(floatArr);
            mBinds[7] = TaosMultiBind.MultiBindDouble(doubleArr);
            mBinds[8] = TaosMultiBind.MultiBindUTinyInt(uTinyIntArr);
            mBinds[9] = TaosMultiBind.MultiBindUSmallInt(uShortArr);
            mBinds[10] = TaosMultiBind.MultiBindUInt(uIntArr);
            mBinds[11] = TaosMultiBind.MultiBindUBigInt(uLongArr);
            mBinds[12] = TaosMultiBind.MultiBindBinary(binaryArr);
            mBinds[13] = TaosMultiBind.MultiBindNchar(ncharArr);
            return mBinds;
        }
        // A List of data ,use as expectResData. The value is equal to GetMultiBindCNArr()
        public static List<string> GetMultiBindResData()
        {
            var rowData = new List<string>();
            for (int i = 0; i < tsArr.Length; i++)
            {
                rowData.Add(tsArr[i].ToString());
                rowData.Add(boolArr[i].Equals(null) ? "NULL" : boolArr[i].ToString());
                rowData.Add(tinyIntArr[i].Equals(null) ? "NULL" : tinyIntArr[i].ToString());
                rowData.Add(shortArr[i].Equals(null) ? "NULL" : shortArr[i].ToString());
                rowData.Add(intArr[i].Equals(null) ? "NULL" : intArr[i].ToString());
                rowData.Add(longArr[i].Equals(null) ? "NULL" : longArr[i].ToString());
                rowData.Add(floatArr[i].Equals(null) ? "NULL" : floatArr[i].ToString());
                rowData.Add(doubleArr[i].Equals(null) ? "NULL" : doubleArr[i].ToString());
                rowData.Add(uTinyIntArr[i].Equals(null) ? "NULL" : uTinyIntArr[i].ToString());
                rowData.Add(uShortArr[i].Equals(null) ? "NULL" : uShortArr[i].ToString());
                rowData.Add(uIntArr[i].Equals(null) ? "NULL" : uIntArr[i].ToString());
                rowData.Add(uLongArr[i].Equals(null) ? "NULL" : uLongArr[i].ToString());
                rowData.Add(String.IsNullOrEmpty(binaryArr[i]) ? "NULL" : binaryArr[i]);
                rowData.Add(String.IsNullOrEmpty(ncharArr[i]) ? "NULL" : ncharArr[i]);
                // Console.WriteLine("binaryArrCn[{0}]:{1},NcharArrCn[{0}]:{2}",i,String.IsNullOrEmpty(binaryArrCn[i]) ? "NULL" : binaryArrCn[i],String.IsNullOrEmpty(NcharArrCn[i]) ? "NULL" : NcharArrCn[i]);
                // Console.WriteLine("binaryArrCn[{0}]:{1},NcharArrCn[{0}]:{2}",i,String.IsNullOrEmpty(binaryArrCn[i]) ? 0 :binaryArrCn[i].Length, String.IsNullOrEmpty(NcharArrCn[i]) ? 0 : NcharArrCn[i].Length);
                // Console.WriteLine("========");

            }
            return rowData;
        }
        // Five lines of data, that is construct as taos_mutli_bind array. 
        // There aren some CN characters and letters.
        public static TAOS_MULTI_BIND[] GetMultiBindCNArr()
        {
            TAOS_MULTI_BIND[] mBinds = new TAOS_MULTI_BIND[14];
            mBinds[0] = TaosMultiBind.MultiBindTimestamp(tsArr);
            mBinds[1] = TaosMultiBind.MultiBindBool(boolArr);
            mBinds[2] = TaosMultiBind.MultiBindTinyInt(tinyIntArr);
            mBinds[3] = TaosMultiBind.MultiBindSmallInt(shortArr);
            mBinds[4] = TaosMultiBind.MultiBindInt(intArr);
            mBinds[5] = TaosMultiBind.MultiBindBigint(longArr);
            mBinds[6] = TaosMultiBind.MultiBindFloat(floatArr);
            mBinds[7] = TaosMultiBind.MultiBindDouble(doubleArr);
            mBinds[8] = TaosMultiBind.MultiBindUTinyInt(uTinyIntArr);
            mBinds[9] = TaosMultiBind.MultiBindUSmallInt(uShortArr);
            mBinds[10] = TaosMultiBind.MultiBindUInt(uIntArr);
            mBinds[11] = TaosMultiBind.MultiBindUBigInt(uLongArr);
            mBinds[12] = TaosMultiBind.MultiBindBinary(binaryArrCn);
            mBinds[13] = TaosMultiBind.MultiBindNchar(NcharArrCn);
            return mBinds;
        }
        // A List of data ,use as expectResData. The value is equal to GetMultiBindCNArr()
        public static List<string> GetMultiBindCNRowData()
        {
            var rowData = new List<string>();
            for (int i = 0; i < tsArr.Length; i++)
            {
                rowData.Add(tsArr[i].ToString());
                rowData.Add(boolArr[i].Equals(null) ? "NULL" : boolArr[i].ToString());
                rowData.Add(tinyIntArr[i].Equals(null) ? "NULL" : tinyIntArr[i].ToString());
                rowData.Add(shortArr[i].Equals(null) ? "NULL" : shortArr[i].ToString());
                rowData.Add(intArr[i].Equals(null) ? "NULL" : intArr[i].ToString());
                rowData.Add(longArr[i].Equals(null) ? "NULL" : longArr[i].ToString());
                rowData.Add(floatArr[i].Equals(null) ? "NULL" : floatArr[i].ToString());
                rowData.Add(doubleArr[i].Equals(null) ? "NULL" : doubleArr[i].ToString());
                rowData.Add(uTinyIntArr[i].Equals(null) ? "NULL" : uTinyIntArr[i].ToString());
                rowData.Add(uShortArr[i].Equals(null) ? "NULL" : uShortArr[i].ToString());
                rowData.Add(uIntArr[i].Equals(null) ? "NULL" : uIntArr[i].ToString());
                rowData.Add(uLongArr[i].Equals(null) ? "NULL" : uLongArr[i].ToString());
                rowData.Add(String.IsNullOrEmpty(binaryArrCn[i]) ? "NULL" : binaryArrCn[i]);
                rowData.Add(String.IsNullOrEmpty(NcharArrCn[i]) ? "NULL" : NcharArrCn[i]);
                // Console.WriteLine("binaryArrCn[{0}]:{1},NcharArrCn[{0}]:{2}",i,String.IsNullOrEmpty(binaryArrCn[i]) ? "NULL" : binaryArrCn[i],String.IsNullOrEmpty(NcharArrCn[i]) ? "NULL" : NcharArrCn[i]);
                // Console.WriteLine("binaryArrCn[{0}]:{1},NcharArrCn[{0}]:{2}",i,String.IsNullOrEmpty(binaryArrCn[i]) ? 0 :binaryArrCn[i].Length, String.IsNullOrEmpty(NcharArrCn[i]) ? 0 : NcharArrCn[i].Length);
                // Console.WriteLine("========");

            }
            return rowData;
        }

        public static List<String> GetMultiBindStableCNRowData()
        {
            List<String> columnData = new List<string>();
            List<String> tagData = GetTagCnData();
            for (int i = 0; i < tsArr.Length; i++)
            {
                columnData.Add(tsArr[i].ToString());
                columnData.Add(boolArr[i].Equals(null) ? "NULL" : boolArr[i].ToString());
                columnData.Add(tinyIntArr[i].Equals(null) ? "NULL" : tinyIntArr[i].ToString());
                columnData.Add(shortArr[i].Equals(null) ? "NULL" : shortArr[i].ToString());
                columnData.Add(intArr[i].Equals(null) ? "NULL" : intArr[i].ToString());
                columnData.Add(longArr[i].Equals(null) ? "NULL" : longArr[i].ToString());
                columnData.Add(floatArr[i].Equals(null) ? "NULL" : floatArr[i].ToString());
                columnData.Add(doubleArr[i].Equals(null) ? "NULL" : doubleArr[i].ToString());
                columnData.Add(uTinyIntArr[i].Equals(null) ? "NULL" : uTinyIntArr[i].ToString());
                columnData.Add(uShortArr[i].Equals(null) ? "NULL" : uShortArr[i].ToString());
                columnData.Add(uIntArr[i].Equals(null) ? "NULL" : uIntArr[i].ToString());
                columnData.Add(uLongArr[i].Equals(null) ? "NULL" : uLongArr[i].ToString());
                columnData.Add(String.IsNullOrEmpty(binaryArrCn[i]) ? "NULL" : binaryArrCn[i]);
                columnData.Add(String.IsNullOrEmpty(NcharArrCn[i]) ? "NULL" : NcharArrCn[i]);
                columnData.AddRange(tagData);
                // Console.WriteLine("binaryArrCn[{0}]:{1},NcharArrCn[{0}]:{2}",i,String.IsNullOrEmpty(binaryArrCn[i]) ? "NULL" : binaryArrCn[i],String.IsNullOrEmpty(NcharArrCn[i]) ? "NULL" : NcharArrCn[i]);
                // Console.WriteLine("binaryArrCn[{0}]:{1},NcharArrCn[{0}]:{2}",i,String.IsNullOrEmpty(binaryArrCn[i]) ? 0 :binaryArrCn[i].Length, String.IsNullOrEmpty(NcharArrCn[i]) ? 0 : NcharArrCn[i].Length);
                // Console.WriteLine("========");

            }
            return columnData;
        }

        public static TAOS_BIND[] GetQueryCondition()
        {
            TAOS_BIND[] queryCondition = new TAOS_BIND[2];
            queryCondition[0] = TaosBind.BindTinyInt(0);
            queryCondition[1] = TaosBind.BindInt(1000);
            return queryCondition;

        }
        public static void FreeTaosBind(TAOS_BIND[] binds)
        {
            TaosBind.FreeTaosBind(binds);
        }

        public static void FreeTaosMBind(TAOS_MULTI_BIND[] mbinds)
        {
            TaosMultiBind.FreeTaosBind(mbinds);
        }
        //Get the TDengineMeta list from the ddl either normal table or stable
        public static List<TDengineMeta> GetMetaFromDLL(string dllStr)
        {
            var expectResMeta = new List<TDengineMeta>();
            //"CREATE TABLE meters(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(location BINARY(30), groupId INT);";
            int bracetInd = dllStr.IndexOf("(");
            //(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(location BINARY(30), groupId INT);
            string subDllStr = dllStr.Substring(bracetInd);

            String[] stableSeparators = new String[] { "tags", "TAGS" };
            //(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT)
            //(location BINARY(30), groupId INT)
            String[] dllStrElements = subDllStr.Split(stableSeparators, StringSplitOptions.RemoveEmptyEntries);
            //(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT)
            dllStrElements[0] = dllStrElements[0].Substring(1, dllStrElements[0].Length - 2);
            String[] finalStr1 = dllStrElements[0].Split(',', StringSplitOptions.RemoveEmptyEntries);
            foreach (string item in finalStr1)
            {
                //ts TIMESTAMP
                string[] itemArr = item.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                // Console.WriteLine("GetMetaFromDLL():{0},{1}",itemArr[0],itemArr[1]);
                expectResMeta.Add(UtilsTools.ConstructTDengineMeta(itemArr[0], itemArr[1]));
            }
            if (dllStr.Contains("TAGS") || dllStr.Contains("tags"))
            {
                //location BINARY(30), groupId INT
                dllStrElements[1] = dllStrElements[1].Substring(1, dllStrElements[1].Length - 2);
                //location BINARY(30)  groupId INT
                String[] finalStr2 = dllStrElements[1].Split(',', StringSplitOptions.RemoveEmptyEntries);
                Console.WriteLine("========");
                foreach (string item in finalStr2)
                {
                    //location BINARY(30)
                    string[] itemArr = item.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
                    // Console.WriteLine("GetMetaFromDLL():{0},{1}",itemArr[0],itemArr[1]);
                    expectResMeta.Add(UtilsTools.ConstructTDengineMeta(itemArr[0], itemArr[1]));
                }

            }
            return expectResMeta;
        }

    }
}