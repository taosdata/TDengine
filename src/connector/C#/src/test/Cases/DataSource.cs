using System;
using Test.UtilsTools;
using TDengineDriver;

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


        public static TAOS_BIND[] getTags()
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
        public static TAOS_BIND[] getNtableRow()
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


    }
}