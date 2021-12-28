using System;
using Xunit;
using TDengineDriver;
using System.Runtime.InteropServices;

namespace TDengineDriver.Test
{
    public class TestTaosBind
    {
        [Fact]
        public void TestBindBoolTrue()
        {
            int bufferType = 1;
            bool buffer = true;
            int bufferLength = sizeof(bool);
            int length = sizeof(bool);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBool(true);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            bool bindBuffer = Convert.ToBoolean(Marshal.ReadByte(bind.buffer));
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);

        }

        [Fact]
        public void TestBindBoolFalse()
        {
            int bufferType = 1;
            bool buffer = false;
            int bufferLength = sizeof(bool);
            int length = sizeof(bool);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBool(false);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            bool bindBuffer = Convert.ToBoolean(Marshal.ReadByte(bind.buffer));
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);

        }

        [Fact]
        public void TestBindTinyIntZero()
        {

            int bufferType = 2;
            sbyte buffer = 0;
            int bufferLength = sizeof(sbyte);
            int length = sizeof(sbyte);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTinyInt(0);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            sbyte bindBuffer = Convert.ToSByte(Marshal.ReadByte(bind.buffer));
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindTinyIntPositive()
        {

            int bufferType = 2;
            sbyte buffer = sbyte.MaxValue;
            int bufferLength = sizeof(sbyte);
            int length = sizeof(sbyte);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTinyInt(sbyte.MaxValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            sbyte bindBuffer = Convert.ToSByte(Marshal.ReadByte(bind.buffer));
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindTinyIntNegative()
        {

            int bufferType = 2;
            short buffer = sbyte.MinValue;
            int bufferLength = sizeof(sbyte);
            int length = sizeof(sbyte);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTinyInt(sbyte.MinValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            short bindBuffer = Marshal.ReadInt16(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindSmallIntNegative()
        {

            int bufferType = 3;
            short buffer = short.MinValue;
            int bufferLength = sizeof(short);
            int length = sizeof(short);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindSmallInt(short.MinValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            short bindBuffer = Marshal.ReadInt16(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindSmallIntZero()
        {

            int bufferType = 3;
            short buffer = 0;
            int bufferLength = sizeof(short);
            int length = sizeof(short);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindSmallInt(0);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            short bindBuffer = Marshal.ReadInt16(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindSmallIntPositive()
        {

            int bufferType = 3;
            short buffer = short.MaxValue;
            int bufferLength = sizeof(short);
            int length = sizeof(short);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindSmallInt(short.MaxValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            short bindBuffer = Marshal.ReadInt16(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindIntNegative()
        {

            int bufferType = 4;
            int buffer = int.MinValue;
            int bufferLength = sizeof(int);
            int length = sizeof(int);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindInt(int.MinValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            int bindBuffer = Marshal.ReadInt32(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindIntZero()
        {

            int bufferType = 4;
            int buffer = 0;
            int bufferLength = sizeof(int);
            int length = sizeof(int);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindInt(0);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            int bindBuffer = Marshal.ReadInt32(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindIntPositive()
        {

            int bufferType = 4;
            int buffer = int.MaxValue;
            int bufferLength = sizeof(int);
            int length = sizeof(int);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindInt(int.MaxValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            int bindBuffer = Marshal.ReadInt32(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindBigIntNegative()
        {

            int bufferType = 5;
            long buffer = long.MinValue;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBigInt(long.MinValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }
        [Fact]
        public void TestBindBigIntZero()
        {

            int bufferType = 5;
            long buffer = 0;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBigInt(0);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindBigIntPositive()
        {

            int bufferType = 5;
            long buffer = long.MaxValue;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBigInt(long.MaxValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindUTinyZero()
        {


            int bufferType = 11;
            byte buffer = 0;
            int bufferLength = sizeof(sbyte);
            int length = sizeof(sbyte);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUTinyInt(0);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            byte bindBuffer = Marshal.ReadByte(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindUTinyPositive()
        {


            int bufferType = 11;
            byte buffer = byte.MaxValue;
            int bufferLength = sizeof(sbyte);
            int length = sizeof(sbyte);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUTinyInt(byte.MaxValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            byte bindBuffer = Marshal.ReadByte(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindUSmallIntZero()
        {

            int bufferType = 12;
            ushort buffer = ushort.MinValue;
            int bufferLength = sizeof(ushort);
            int length = sizeof(ushort);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUSmallInt(ushort.MinValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            ushort bindBuffer = (ushort)Marshal.ReadInt16(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }
        [Fact]
        public void TestBindUSmallIntPositive()
        {

            int bufferType = 12;
            ushort buffer = ushort.MaxValue;
            int bufferLength = sizeof(ushort);
            int length = sizeof(ushort);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUSmallInt(ushort.MaxValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            ushort bindBuffer = (ushort)Marshal.ReadInt16(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindUIntZero()
        {
            int bufferType = 13;
            uint buffer = uint.MinValue;
            int bufferLength = sizeof(uint);
            int length = sizeof(uint);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUInt(uint.MinValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            uint bindBuffer = (uint)Marshal.ReadInt32(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindUIntPositive()
        {
            int bufferType = 13;
            uint buffer = uint.MaxValue;
            int bufferLength = sizeof(uint);
            int length = sizeof(uint);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUInt(uint.MaxValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            uint bindBuffer = (uint)Marshal.ReadInt32(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindUBigIntZero()
        {
            int bufferType = 14;
            ulong buffer = ulong.MinValue;
            int bufferLength = sizeof(ulong);
            int length = sizeof(ulong);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUBigInt(ulong.MinValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            ulong bindBuffer = (ulong)Marshal.ReadInt64(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindUBigIntPositive()
        {
            int bufferType = 14;
            ulong buffer = ulong.MaxValue;
            int bufferLength = sizeof(ulong);
            int length = sizeof(ulong);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUBigInt(ulong.MaxValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            ulong bindBuffer = (ulong)Marshal.ReadInt64(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindFloatNegative()
        {
            int bufferType = 6;
            float buffer = float.MinValue;
            int bufferLength = sizeof(float);
            int length = sizeof(float);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindFloat(float.MinValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            float[] bindBufferArr = new float[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindFloatZero()
        {
            int bufferType = 6;
            float buffer = 0;
            int bufferLength = sizeof(float);
            int length = sizeof(float);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindFloat(0F);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            float[] bindBufferArr = new float[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindFloatPositive()
        {
            int bufferType = 6;
            float buffer = float.MaxValue;
            int bufferLength = sizeof(float);
            int length = sizeof(float);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindFloat(float.MaxValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            float[] bindBufferArr = new float[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindDoubleZero()
        {
            int bufferType = 7;
            double buffer = 0;
            int bufferLength = sizeof(double);
            int length = sizeof(double);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindDouble(0D);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            double[] bindBufferArr = new double[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindDoublePositive()
        {
            int bufferType = 7;
            double buffer = double.MaxValue;
            int bufferLength = sizeof(double);
            int length = sizeof(double);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindDouble(double.MaxValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            double[] bindBufferArr = new double[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindDoubleNegative()
        {
            int bufferType = 7;
            double buffer = double.MinValue;
            int bufferLength = sizeof(double);
            int length = sizeof(double);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindDouble(double.MinValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            double[] bindBufferArr = new double[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindBinaryEn()
        {
            int bufferType = 8;
            String buffer = "qwertyuiopasdghjklzxcvbnm<>?:\"{}+_)(*&^%$#@!~QWERTYUIOP[]\\ASDFGHJKL;'ZXCVBNM,./`1234567890-=";
            int bufferLength = System.Text.Encoding.Default.GetBytes(buffer).Length;
            int length = System.Text.Encoding.Default.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBinary("qwertyuiopasdghjklzxcvbnm<>?:\"{}+_)(*&^%$#@!~QWERTYUIOP[]\\ASDFGHJKL;'ZXCVBNM,./`1234567890-=");
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringAnsi(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindBinaryCn()
        {
            int bufferType = 8;
            String buffer = "一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./";
            int bufferLength = System.Text.Encoding.Default.GetBytes(buffer).Length;
            int length = System.Text.Encoding.Default.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBinary("一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./");
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringAnsi(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindBinaryCnAndEn()
        {
            int bufferType = 8;
            String buffer = "一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
            int bufferLength = System.Text.Encoding.Default.GetBytes(buffer).Length;
            int length = System.Text.Encoding.Default.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBinary("一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM");
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringAnsi(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindNcharEn()
        {
            int bufferType = 10;
            String buffer = "qwertyuiopasdghjklzxcvbnm<>?:\"{}+_)(*&^%$#@!~QWERTYUIOP[]\\ASDFGHJKL;'ZXCVBNM,./`1234567890-=";
            int bufferLength = System.Text.Encoding.Default.GetBytes(buffer).Length;
            int length = System.Text.Encoding.Default.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindNchar("qwertyuiopasdghjklzxcvbnm<>?:\"{}+_)(*&^%$#@!~QWERTYUIOP[]\\ASDFGHJKL;'ZXCVBNM,./`1234567890-=");
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringAnsi(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }
        [Fact]
        public void TestBindNcharCn()
        {
            int bufferType = 10;
            String buffer = "一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./";
            int bufferLength = System.Text.Encoding.Default.GetBytes(buffer).Length;
            int length = System.Text.Encoding.Default.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindNchar("一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./");
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringAnsi(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }
        [Fact]
        public void TestBindNcharCnAndEn()
        {
            int bufferType = 10;
            String buffer = "一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
            int bufferLength = System.Text.Encoding.Default.GetBytes(buffer).Length;
            int length = System.Text.Encoding.Default.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindNchar("一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM");
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringAnsi(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindNil()
        {
            int bufferType = 0;
            int isNull = 1;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindNil();

            int bindIsNull = Marshal.ReadInt32(bind.is_null);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindIsNull, isNull);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        [Fact]
        public void TestBindTimestampNegative()
        {
            int bufferType = 9;
            long buffer = long.MinValue;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTimestamp(long.MinValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }
        [Fact]
        public void TestBindTimestampZero()
        {
            int bufferType = 9;
            long buffer = 0;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTimestamp(0);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }
        [Fact]
        public void TestBindTimestampPositive()
        {
            int bufferType = 9;
            long buffer = long.MaxValue;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTimestamp(long.MaxValue);
            int BindLengPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);
            Console.WriteLine("bind.buffer_type:{0},bufferType:{1}", bind.buffer_type, bufferType);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(BindLengPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

    }
}