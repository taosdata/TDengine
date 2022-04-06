using System;
using Xunit;
using TDengineDriver;
using System.Runtime.InteropServices;

namespace TDengineDriver.Test
{
    public class TestTaosBind
    {
        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindBoolTrue</Name>
        /// <describe>Unit test for binding boolean true value using TAOS_BIND struct through stmt</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindBoolTrue()
        {
            int bufferType = 1;
            bool buffer = true;
            int bufferLength = sizeof(bool);
            int length = sizeof(bool);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBool(true);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            bool bindBuffer = Convert.ToBoolean(Marshal.ReadByte(bind.buffer));


            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);

        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindBoolFalse</Name>
        /// <describe>Unit test for binding boolean false value using TAOS_BIND struct through stmt</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindBoolFalse()
        {
            int bufferType = 1;
            bool buffer = false;
            int bufferLength = sizeof(bool);
            int length = sizeof(bool);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBool(false);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            bool bindBuffer = Convert.ToBoolean(Marshal.ReadByte(bind.buffer));


            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);

        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindTinyIntZero</Name>
        /// <describe>Unit test for binding tinny int zero value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindTinyIntZero()
        {
            int bufferType = 2;
            sbyte buffer = 0;
            int bufferLength = sizeof(sbyte);
            int length = sizeof(sbyte);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTinyInt(0);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            sbyte bindBuffer = Convert.ToSByte(Marshal.ReadByte(bind.buffer));


            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }
        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindTinyIntPositive</Name>
        /// <describe>Unit test for binding tinny int positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindTinyIntPositive()
        {
            int bufferType = 2;
            sbyte buffer = sbyte.MaxValue;
            int bufferLength = sizeof(sbyte);
            int length = sizeof(sbyte);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTinyInt(sbyte.MaxValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            sbyte bindBuffer = Convert.ToSByte(Marshal.ReadByte(bind.buffer));


            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindTinyIntNegative</Name>
        /// <describe>Unit test for binding tinny int negative value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindTinyIntNegative()
        {
            int bufferType = 2;
            short buffer = sbyte.MinValue;
            int bufferLength = sizeof(sbyte);
            int length = sizeof(sbyte);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTinyInt(sbyte.MinValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            short bindBuffer = Marshal.ReadInt16(bind.buffer);


            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindSmallIntNegative</Name>
        /// <describe>Unit test for binding small int negative value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindSmallIntNegative()
        {
            int bufferType = 3;
            short buffer = short.MinValue;
            int bufferLength = sizeof(short);
            int length = sizeof(short);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindSmallInt(short.MinValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            short bindBuffer = Marshal.ReadInt16(bind.buffer);


            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindSmallIntZero</Name>
        /// <describe>Unit test for binding small int zero value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindSmallIntZero()
        {
            int bufferType = 3;
            short buffer = 0;
            int bufferLength = sizeof(short);
            int length = sizeof(short);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindSmallInt(0);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            short bindBuffer = Marshal.ReadInt16(bind.buffer);


            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindSmallIntPositive</Name>
        /// <describe>Unit test for binding small int positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindSmallIntPositive()
        {
            int bufferType = 3;
            short buffer = short.MaxValue;
            int bufferLength = sizeof(short);
            int length = sizeof(short);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindSmallInt(short.MaxValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            short bindBuffer = Marshal.ReadInt16(bind.buffer);


            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindIntNegative</Name>
        /// <describe>Unit test for binding small int positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindIntNegative()
        {
            int bufferType = 4;
            int buffer = int.MinValue;
            int bufferLength = sizeof(int);
            int length = sizeof(int);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindInt(int.MinValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            int bindBuffer = Marshal.ReadInt32(bind.buffer);


            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindIntZero</Name>
        /// <describe>Unit test for binding int zero value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindIntZero()
        {
            int bufferType = 4;
            int buffer = 0;
            int bufferLength = sizeof(int);
            int length = sizeof(int);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindInt(0);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            int bindBuffer = Marshal.ReadInt32(bind.buffer);


            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindIntPositive</Name>
        /// <describe>Unit test for binding int positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindIntPositive()
        {
            int bufferType = 4;
            int buffer = int.MaxValue;
            int bufferLength = sizeof(int);
            int length = sizeof(int);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindInt(int.MaxValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            int bindBuffer = Marshal.ReadInt32(bind.buffer);


            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindBigIntNegative</Name>
        /// <describe>Unit test for binding int negative value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindBigIntNegative()
        {
            int bufferType = 5;
            long buffer = long.MinValue;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBigInt(long.MinValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindBigIntZero</Name>
        /// <describe>Unit test for binding big int zero value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindBigIntZero()
        {
            int bufferType = 5;
            long buffer = 0;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBigInt(0);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindBigIntPositive</Name>
        /// <describe>Unit test for binding big int positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindBigIntPositive()
        {
            int bufferType = 5;
            long buffer = long.MaxValue;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBigInt(long.MaxValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindBigIntPositive</Name>
        /// <describe>Unit test for binding big int positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindUTinyZero()
        {
            int bufferType = 11;
            byte buffer = 0;
            int bufferLength = sizeof(sbyte);
            int length = sizeof(sbyte);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUTinyInt(0);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            byte bindBuffer = Marshal.ReadByte(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindUTinyPositive</Name>
        /// <describe>Unit test for binding unsigned tinny int positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindUTinyPositive()
        {
            int bufferType = 11;
            byte buffer = byte.MaxValue;
            int bufferLength = sizeof(sbyte);
            int length = sizeof(sbyte);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUTinyInt(byte.MaxValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            byte bindBuffer = Marshal.ReadByte(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindUSmallIntZero</Name>
        /// <describe>Unit test for binding unsigned small int zero value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result> 
        [Fact]
        public void TestBindUSmallIntZero()
        {
            int bufferType = 12;
            ushort buffer = ushort.MinValue;
            int bufferLength = sizeof(ushort);
            int length = sizeof(ushort);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUSmallInt(ushort.MinValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            ushort bindBuffer = (ushort)Marshal.ReadInt16(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindUSmallIntPositive</Name>
        /// <describe>Unit test for binding unsigned small int positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindUSmallIntPositive()
        {
            int bufferType = 12;
            ushort buffer = ushort.MaxValue;
            int bufferLength = sizeof(ushort);
            int length = sizeof(ushort);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUSmallInt(ushort.MaxValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            ushort bindBuffer = (ushort)Marshal.ReadInt16(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>
        /// <Name>TestTaosBind.TestBindUIntZero</Name>
        /// <describe>Unit test for binding unsigned int zero value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindUIntZero()
        {
            int bufferType = 13;
            uint buffer = uint.MinValue;
            int bufferLength = sizeof(uint);
            int length = sizeof(uint);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUInt(uint.MinValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            uint bindBuffer = (uint)Marshal.ReadInt32(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindUIntPositive</Name>
        /// <describe>Unit test for binding unsigned int positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindUIntPositive()
        {
            int bufferType = 13;
            uint buffer = uint.MaxValue;
            int bufferLength = sizeof(uint);
            int length = sizeof(uint);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUInt(uint.MaxValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            uint bindBuffer = (uint)Marshal.ReadInt32(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindUBigIntZero</Name>
        /// <describe>Unit test for binding unsigned big int zero value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindUBigIntZero()
        {
            int bufferType = 14;
            ulong buffer = ulong.MinValue;
            int bufferLength = sizeof(ulong);
            int length = sizeof(ulong);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUBigInt(ulong.MinValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            ulong bindBuffer = (ulong)Marshal.ReadInt64(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindUBigIntPositive</Name>
        /// <describe>Unit test for binding unsigned big int positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindUBigIntPositive()
        {
            int bufferType = 14;
            ulong buffer = ulong.MaxValue;
            int bufferLength = sizeof(ulong);
            int length = sizeof(ulong);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindUBigInt(ulong.MaxValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            ulong bindBuffer = (ulong)Marshal.ReadInt64(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindFloatNegative</Name>
        /// <describe>Unit test for binding float negative value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindFloatNegative()
        {
            int bufferType = 6;
            float buffer = float.MinValue;
            int bufferLength = sizeof(float);
            int length = sizeof(float);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindFloat(float.MinValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            float[] bindBufferArr = new float[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindFloatNegative</Name>
        /// <describe>Unit test for binding float zero value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindFloatZero()
        {
            int bufferType = 6;
            float buffer = 0;
            int bufferLength = sizeof(float);
            int length = sizeof(float);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindFloat(0F);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            float[] bindBufferArr = new float[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindFloatPositive</Name>
        /// <describe>Unit test for binding float positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindFloatPositive()
        {
            int bufferType = 6;
            float buffer = float.MaxValue;
            int bufferLength = sizeof(float);
            int length = sizeof(float);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindFloat(float.MaxValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            float[] bindBufferArr = new float[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindDoubleZero</Name>
        /// <describe>Unit test for binding double zero value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindDoubleZero()
        {
            int bufferType = 7;
            double buffer = 0;
            int bufferLength = sizeof(double);
            int length = sizeof(double);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindDouble(0D);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            double[] bindBufferArr = new double[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindDoublePositive</Name>
        /// <describe>Unit test for binding double positive value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindDoublePositive()
        {
            int bufferType = 7;
            double buffer = double.MaxValue;
            int bufferLength = sizeof(double);
            int length = sizeof(double);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindDouble(double.MaxValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            double[] bindBufferArr = new double[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindDoubleNegative</Name>
        /// <describe>Unit test for binding double negative value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindDoubleNegative()
        {
            int bufferType = 7;
            double buffer = double.MinValue;
            int bufferLength = sizeof(double);
            int length = sizeof(double);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindDouble(double.MinValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            double[] bindBufferArr = new double[1];
            Marshal.Copy(bind.buffer, bindBufferArr, 0, bindBufferArr.Length);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBufferArr[0], buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindBinaryEn</Name>
        /// <describe>Unit test for binding binary character without CN character using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindBinaryEn()
        {
            int bufferType = 8;
            String buffer = "qwertyuiopasdghjklzxcvbnm<>?:\"{}+_)(*&^%$#@!~QWERTYUIOP[]\\ASDFGHJKL;'ZXCVBNM,./`1234567890-=";
            int bufferLength = System.Text.Encoding.UTF8.GetBytes(buffer).Length;
            int length = System.Text.Encoding.UTF8.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBinary("qwertyuiopasdghjklzxcvbnm<>?:\"{}+_)(*&^%$#@!~QWERTYUIOP[]\\ASDFGHJKL;'ZXCVBNM,./`1234567890-=");
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringUTF8(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindBinaryCn</Name>
        /// <describe>Unit test for binding binary character with CN character using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindBinaryCn()
        {
            int bufferType = 8;
            String buffer = "一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./";
            int bufferLength = System.Text.Encoding.UTF8.GetBytes(buffer).Length;
            int length = System.Text.Encoding.UTF8.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBinary("一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./");
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringUTF8(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindBinaryCnAndEn</Name>
        /// <describe>Unit test for binding binary characters with CN and  other characters using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindBinaryCnAndEn()
        {
            int bufferType = 8;
            String buffer = "一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
            int bufferLength = System.Text.Encoding.UTF8.GetBytes(buffer).Length;
            int length = System.Text.Encoding.UTF8.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindBinary("一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM");
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringUTF8(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindNcharEn</Name>
        /// <describe>Unit test for binding nchar characters without cn using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindNcharEn()
        {
            int bufferType = 10;
            String buffer = "qwertyuiopasdghjklzxcvbnm<>?:\"{}+_)(*&^%$#@!~QWERTYUIOP[]\\ASDFGHJKL;'ZXCVBNM,./`1234567890-=";
            int bufferLength = System.Text.Encoding.UTF8.GetBytes(buffer).Length;
            int length = System.Text.Encoding.UTF8.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindNchar("qwertyuiopasdghjklzxcvbnm<>?:\"{}+_)(*&^%$#@!~QWERTYUIOP[]\\ASDFGHJKL;'ZXCVBNM,./`1234567890-=");
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringUTF8(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindNcharCn</Name>
        /// <describe>Unit test for binding nchar characters with cn using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindNcharCn()
        {
            int bufferType = 10;
            String buffer = "一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./";
            int bufferLength = System.Text.Encoding.UTF8.GetBytes(buffer).Length;
            int length = System.Text.Encoding.UTF8.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindNchar("一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./");
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringUTF8(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindNcharCnAndEn</Name>
        /// <describe>Unit test for binding nchar  with cn characters and other characters using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindNcharCnAndEn()
        {
            int bufferType = 10;
            String buffer = "一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
            int bufferLength = System.Text.Encoding.UTF8.GetBytes(buffer).Length;
            int length = System.Text.Encoding.UTF8.GetBytes(buffer).Length;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindNchar("一二两三四五六七八九十廿毛另壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分零整1234567890`~!@#$%^&*()_+[]{};':<>?,./qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM");
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            string bindBuffer = Marshal.PtrToStringUTF8(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindNil</Name>
        /// <describe>Unit test for binding null value using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindNil()
        {
            int bufferType = 0;
            int isNull = 1;

            TDengineDriver.TAOS_BIND bind = TaosBind.BindNil();

            int bindIsNull = Marshal.ReadInt32(bind.is_null);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindIsNull, isNull);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindTimestampNegative</Name>
        /// <describe>Unit test for binding negative timestamp using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindTimestampNegative()
        {
            int bufferType = 9;
            long buffer = long.MinValue;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTimestamp(long.MinValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindTimestampZero</Name>
        /// <describe>Unit test for binding zero timestamp using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindTimestampZero()
        {
            int bufferType = 9;
            long buffer = 0;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTimestamp(0);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }


        /// <author>xiaolei</author>    
        /// <Name>TestTaosBind.TestBindTimestampPositive</Name>
        /// <describe>Unit test for binding positive timestamp using TAOS_BIND struct through stmt.</describe>
        /// <filename>TestTaosBind.cs</filename>
        /// <result>pass or failed </result>
        [Fact]
        public void TestBindTimestampPositive()
        {
            int bufferType = 9;
            long buffer = long.MaxValue;
            int bufferLength = sizeof(long);
            int length = sizeof(long);

            TDengineDriver.TAOS_BIND bind = TaosBind.BindTimestamp(long.MaxValue);
            int bindLengthPtr = Marshal.ReadInt32(bind.length);
            long bindBuffer = Marshal.ReadInt64(bind.buffer);

            Assert.Equal(bind.buffer_type, bufferType);
            Assert.Equal(bindBuffer, buffer);
            Assert.Equal(bind.buffer_length, bufferLength);
            Assert.Equal(bindLengthPtr, length);

            Marshal.FreeHGlobal(bind.buffer);
            Marshal.FreeHGlobal(bind.length);
        }

    }
}