using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using TDengine.Driver.Impl.NativeMethods;

namespace TDengine.Driver.Client.Native
{
    public class NativeStmt : IStmt
    {
        private IntPtr _stmt;
        private readonly TimeZoneInfo _tz;

        public NativeStmt(IntPtr stmt, TimeZoneInfo tz)
        {
            _stmt = stmt;
            _tz = tz;
        }

        public void Prepare(string query)
        {
            var code = NativeMethods.StmtPrepare(_stmt, query);
            StmtCheckError(code);
        }

        private void StmtCheckError(int code)
        {
            if (code != 0)
            {
                var errorStr = NativeMethods.StmtErrorStr(_stmt);
                throw new TDengineError(code, errorStr);
            }
        }

        public bool IsInsert()
        {
            bool isInsert;
            IntPtr ptr = Marshal.AllocHGlobal(sizeof(int));
            try
            {
                var code = NativeMethods.StmtIsInsert(_stmt, ptr);
                StmtCheckError(code);
                isInsert = Marshal.ReadInt32(ptr) == 1;
            }
            finally
            {
                Marshal.FreeHGlobal(ptr);
            }

            return isInsert;
        }

        public void SetTableName(string tableName)
        {
            var code = NativeMethods.StmtSetTbname(_stmt, tableName);
            StmtCheckError(code);
        }

        public void SetTags(object[] tags)
        {
            if (tags.Length == 0)
            {
                return;
            }

            var fields = GetTagFields();
            if (tags.Length != fields.Length)
            {
                throw new ArgumentException(
                    $"The number of tags ({tags.Length}) does not match the number of tag fields ({fields.Length}).");
            }

            var param = GenerateBindList(tags, fields, out var needFreePtr, true);
            try
            {
                var code = NativeMethods.StmtSetTags(_stmt, param);
                StmtCheckError(code);
            }
            finally
            {
                foreach (var p in needFreePtr)
                {
                    if (p != IntPtr.Zero)
                    {
                        Marshal.FreeHGlobal(p);
                    }
                }
            }
        }

        private TAOS_MULTI_BIND[] GenerateBindList(object[] data, TaosFieldE[] fields, out IntPtr[] needFree,
            bool isInsert)
        {
            needFree = new IntPtr[]{};
            TAOS_MULTI_BIND[] binds = new TAOS_MULTI_BIND[data.Length];
            var needFreePointer = new List<IntPtr>();
            try
            {
                for (int i = 0; i < data.Length; i++)
                {
                    TAOS_MULTI_BIND bind = new TAOS_MULTI_BIND
                    {
                        num = 1
                    };
                    if (data[i] == null || Convert.IsDBNull(data[i]))
                    {
                        bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BOOL;
                        IntPtr p = Marshal.AllocHGlobal(TDengineConstant.ByteSize);
                        needFreePointer.Add(p);
                        Marshal.WriteByte(p, 1);
                        bind.is_null = p;
                    }
                    else
                    {
                        IntPtr p;
                        byte[] bs;
                        IntPtr lPtr;
                        switch (data[i])
                        {
                            case bool val:
                                if (isInsert && fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_BOOL)
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type bool to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BOOL;
                                p = Marshal.AllocHGlobal(TDengineConstant.BoolSize);
                                needFreePointer.Add(p);
                                bs = BitConverter.GetBytes(val);
                                Marshal.Copy(bs, 0, p, bs.Length);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)TDengineConstant.BoolSize;
                                break;
                            case sbyte val:
                                if (isInsert && fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT)
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type sbyte to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT;
                                p = Marshal.AllocHGlobal(TDengineConstant.Int8Size);
                                needFreePointer.Add(p);
                                Marshal.WriteByte(p, (byte)val);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)TDengineConstant.Int8Size;
                                break;
                            case short val:
                                if (isInsert && fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_SMALLINT)
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type short to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_SMALLINT;
                                p = Marshal.AllocHGlobal(TDengineConstant.Int16Size);
                                needFreePointer.Add(p);
                                bs = BitConverter.GetBytes(val);
                                Marshal.Copy(bs, 0, p, bs.Length);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)TDengineConstant.Int16Size;
                                break;
                            case int val:
                                if (isInsert && fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_INT)
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type short to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_INT;
                                p = Marshal.AllocHGlobal(TDengineConstant.Int32Size);
                                needFreePointer.Add(p);
                                bs = BitConverter.GetBytes(val);
                                Marshal.Copy(bs, 0, p, bs.Length);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)TDengineConstant.Int32Size;
                                break;
                            case long val:
                                if (isInsert)
                                {
                                    if ((TDengineDataType)fields[i].type == TDengineDataType.TSDB_DATA_TYPE_BIGINT ||
                                        (TDengineDataType)fields[i].type == TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                                        bind.buffer_type = fields[i].type;
                                    else
                                        throw new ArgumentException(
                                            $"BindIndex: {i}, field name: {fields[i].name}, bind param type long to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }
                                else
                                {
                                    bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT;
                                }

                                p = Marshal.AllocHGlobal(TDengineConstant.Int64Size);
                                needFreePointer.Add(p);
                                bs = BitConverter.GetBytes(val);
                                Marshal.Copy(bs, 0, p, bs.Length);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)TDengineConstant.Int64Size;
                                break;
                            case byte val:
                                if (isInsert && fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT)
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type byte to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT;
                                p = Marshal.AllocHGlobal(TDengineConstant.UInt8Size);
                                needFreePointer.Add(p);
                                Marshal.WriteByte(p, (byte)val);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)TDengineConstant.UInt8Size;
                                break;
                            case ushort val:
                                if (isInsert && fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT)
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type ushort to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT;
                                p = Marshal.AllocHGlobal(TDengineConstant.UInt16Size);
                                needFreePointer.Add(p);
                                bs = BitConverter.GetBytes(val);
                                Marshal.Copy(bs, 0, p, bs.Length);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)TDengineConstant.UInt16Size;
                                break;
                            case uint val:
                                if (isInsert && fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_UINT)
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type uint to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UINT;
                                p = Marshal.AllocHGlobal(TDengineConstant.UInt32Size);
                                needFreePointer.Add(p);
                                bs = BitConverter.GetBytes(val);
                                Marshal.Copy(bs, 0, p, bs.Length);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)TDengineConstant.UInt32Size;
                                break;
                            case ulong val:
                                if (isInsert && fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT)
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type ulong to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT;
                                p = Marshal.AllocHGlobal(TDengineConstant.UInt64Size);
                                needFreePointer.Add(p);
                                bs = BitConverter.GetBytes(val);
                                Marshal.Copy(bs, 0, p, bs.Length);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)TDengineConstant.UInt64Size;
                                break;
                            case float val:
                                if (isInsert && fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT)
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type float to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT;
                                p = Marshal.AllocHGlobal(TDengineConstant.Float32Size);
                                needFreePointer.Add(p);
                                bs = BitConverter.GetBytes(val);
                                Marshal.Copy(bs, 0, p, bs.Length);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)TDengineConstant.Float32Size;
                                break;
                            case double val:
                                if (isInsert && fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE)
                                {
                                    throw new ArgumentException(
                                        $"BindIndex: {i}, field name: {fields[i].name}, bind param type double to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                }

                                bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE;
                                p = Marshal.AllocHGlobal(TDengineConstant.Float64Size);
                                needFreePointer.Add(p);
                                bs = BitConverter.GetBytes(val);
                                Marshal.Copy(bs, 0, p, bs.Length);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)TDengineConstant.Float64Size;
                                break;
                            case DateTime val:
                                if (isInsert)
                                {
                                    if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                                    {
                                        throw new ArgumentException(
                                            $"BindIndex: {i}, field name: {fields[i].name}, bind param type DateTime to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                    }

                                    bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP;
                                    p = Marshal.AllocHGlobal(TDengineConstant.Int64Size);
                                    needFreePointer.Add(p);
                                    byte precision = fields[i].precision;
                                    var value = TDengineConstant.ConvertDateTimeToTimestamp(val,
                                        (TDenginePrecision)precision);
                                    bs = BitConverter.GetBytes(value);
                                    Marshal.Copy(bs, 0, p, bs.Length);
                                    bind.buffer = p;
                                    bind.buffer_length = (UIntPtr)TDengineConstant.Int64Size;
                                }
                                else
                                {
                                    bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BINARY;
                                    var time = val.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK");
                                    bs = Encoding.UTF8.GetBytes(time);
                                    p = Marshal.AllocHGlobal(bs.Length);
                                    needFreePointer.Add(p);
                                    Marshal.Copy(bs, 0, p, bs.Length);
                                    bind.buffer = p;
                                    bind.buffer_length = (UIntPtr)bs.Length;
                                    lPtr = Marshal.AllocHGlobal(sizeof(Int32));
                                    needFreePointer.Add(lPtr);
                                    Marshal.WriteInt32(lPtr, bs.Length);
                                    bind.length = lPtr;
                                }

                                break;
                            case DateTimeOffset val:
                                if (isInsert)
                                {
                                    if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP)
                                    {
                                        throw new ArgumentException(
                                            $"BindIndex: {i}, field name: {fields[i].name}, bind param type DateTimeOffset to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                    }

                                    bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP;
                                    p = Marshal.AllocHGlobal(TDengineConstant.Int64Size);
                                    needFreePointer.Add(p);
                                    byte precision = fields[i].precision;
                                    var value = TDengineConstant.ConvertDateTimeOffsetToTimestamp(val,
                                        (TDenginePrecision)precision);
                                    bs = BitConverter.GetBytes(value);
                                    Marshal.Copy(bs, 0, p, bs.Length);
                                    bind.buffer = p;
                                    bind.buffer_length = (UIntPtr)TDengineConstant.Int64Size;
                                }
                                else
                                {
                                    bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BINARY;
                                    var time = val.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffffK");
                                    bs = Encoding.UTF8.GetBytes(time);
                                    p = Marshal.AllocHGlobal(bs.Length);
                                    needFreePointer.Add(p);
                                    Marshal.Copy(bs, 0, p, bs.Length);
                                    bind.buffer = p;
                                    bind.buffer_length = (UIntPtr)bs.Length;
                                    lPtr = Marshal.AllocHGlobal(sizeof(Int32));
                                    needFreePointer.Add(lPtr);
                                    Marshal.WriteInt32(lPtr, bs.Length);
                                    bind.length = lPtr;
                                }

                                break;
                            case byte[] val:
                                if (isInsert)
                                {
                                    if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_BINARY &&
                                        fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_JSONTAG &&
                                        fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY &&
                                        fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_GEOMETRY
                                       )
                                    {
                                        throw new ArgumentException(
                                            $"BindIndex: {i}, field name: {fields[i].name}, bind param type byte[] to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                    }

                                    bind.buffer_type = fields[i].type;
                                }
                                else
                                {
                                    bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BINARY;
                                }

                                p = Marshal.AllocHGlobal(val.Length);
                                needFreePointer.Add(p);
                                Marshal.Copy(val, 0, p, val.Length);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)val.Length;
                                lPtr = Marshal.AllocHGlobal(sizeof(Int32));
                                needFreePointer.Add(lPtr);
                                Marshal.WriteInt32(lPtr, val.Length);
                                bind.length = lPtr;
                                break;
                            case string val:
                                if (isInsert)
                                {
                                    if (fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_BINARY &&
                                        fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_JSONTAG &&
                                        fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_VARBINARY &&
                                        fields[i].type != (int)TDengineDataType.TSDB_DATA_TYPE_NCHAR
                                       )
                                    {
                                        throw new ArgumentException(
                                            $"BindIndex: {i}, field name: {fields[i].name}, bind param type string to {TDengineConstant.GetFieldTypeName(fields[i].type)} not supported");
                                    }

                                    bind.buffer_type = fields[i].type;
                                }
                                else
                                {
                                    bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BINARY;
                                }

                                bs = Encoding.UTF8.GetBytes(val);
                                p = Marshal.AllocHGlobal(bs.Length);
                                needFreePointer.Add(p);
                                Marshal.Copy(bs, 0, p, bs.Length);
                                bind.buffer = p;
                                bind.buffer_length = (UIntPtr)bs.Length;
                                lPtr = Marshal.AllocHGlobal(sizeof(Int32));
                                needFreePointer.Add(lPtr);
                                Marshal.WriteInt32(lPtr, bs.Length);
                                bind.length = lPtr;
                                break;
                            default:
                                var fieldsPart = string.Empty;
                                if (isInsert)
                                {
                                    fieldsPart = $" field name: {fields[i].name},";
                                }

                                throw new ArgumentException(
                                    $"BindIndex: {i},{fieldsPart} stmt bind param type not supported: {data[i].GetType()}");
                        }
                    }

                    binds[i] = bind;
                }

                needFree = needFreePointer.ToArray();
                return binds;
            }
            catch
            {
                // if there is an error, free all allocated pointers
                foreach (var p in needFreePointer)
                {
                    if (p != IntPtr.Zero)
                    {
                        Marshal.FreeHGlobal(p);
                    }
                }

                throw;
            }
        }

        public TaosFieldE[] GetTagFields()
        {
            var code = NativeMethods.StmtGetTagFields(_stmt, out var fieldNum, out var fieldsPtr);
            if (code != 0)
            {
                throw new TDengineError(code, NativeMethods.StmtErrorStr(_stmt));
            }

            TaosFieldE[] fields = new TaosFieldE[fieldNum];
            for (int i = 0; i < fieldNum; i++)
            {
                IntPtr fieldPtr = IntPtr.Add(fieldsPtr, i * Marshal.SizeOf(typeof(TaosFieldE)));
                fields[i] = (TaosFieldE)Marshal.PtrToStructure(fieldPtr, typeof(TaosFieldE));
            }

            NativeMethods.StmtReclaimFields(_stmt, fieldsPtr);
            return fields;
        }

        public TaosFieldE[] GetColFields()
        {
            var code = NativeMethods.StmtGetColFields(_stmt, out var fieldNum, out var fieldsPtr);
            if (code != 0)
            {
                throw new TDengineError(code, NativeMethods.StmtErrorStr(_stmt));
            }

            TaosFieldE[] fields = new TaosFieldE[fieldNum];
            for (int i = 0; i < fieldNum; i++)
            {
                IntPtr fieldPtr = IntPtr.Add(fieldsPtr, i * Marshal.SizeOf(typeof(TaosFieldE)));
                fields[i] = (TaosFieldE)Marshal.PtrToStructure(fieldPtr, typeof(TaosFieldE));
            }

            NativeMethods.StmtReclaimFields(_stmt, fieldsPtr);
            return fields;
        }

        public void BindRow(object[] row)
        {
            if (row.Length == 0)
            {
                return;
            }

            var isInsert = IsInsert();
            TAOS_MULTI_BIND[] param;
            IntPtr[] needFreePtr;
            var fields = new TaosFieldE[] { };
            if (isInsert)
            {
                fields = GetColFields();
                if (row.Length != fields.Length)
                {
                    throw new ArgumentException(
                        $"The number of col ({row.Length}) does not match the number of col fields ({fields.Length})");
                }
            }

            param = GenerateBindList(row, fields, out needFreePtr, isInsert);

            try
            {
                var code = NativeMethods.StmtBindParam(_stmt, param);
                StmtCheckError(code);
            }
            finally
            {
                foreach (var p in needFreePtr)
                {
                    if (p != IntPtr.Zero)
                    {
                        Marshal.FreeHGlobal(p);
                    }
                }
            }
        }

        public void BindColumn(TaosFieldE[] field, params Array[] arrays)
        {
            var multiBind = new TAOS_MULTI_BIND[arrays.Length];
            try
            {
                for (int i = 0; i < arrays.Length; i++)
                {
                    multiBind[i] = GenerateBindColumn(arrays[i], field[i], i);
                }

                NativeMethods.StmtBindParamBatch(_stmt, multiBind);
            }
            finally
            {
                // if GenerateBindColumn throws an exception or StmtBindParamBatch finishes,
                // free all allocated memory for multiBind
                foreach (var bind in multiBind)
                {
                    MultiBind.FreeTaosBind(bind);
                }
            }
        }

        private static TAOS_MULTI_BIND GenerateBindColumn(Array array, TaosFieldE field, int bindIndex)
        {
            var elementType = array.GetType().GetElementType();
            if (elementType == null)
            {
                throw new ArgumentException(
                    $"BindIndex: {bindIndex}, field name: {field.name}, Expected an array type, but received {array.GetType().Name}");
            }

            switch ((TDengineDataType)field.type)
            {
                case TDengineDataType.TSDB_DATA_TYPE_BOOL:
                    if (elementType == typeof(bool?))
                    {
                        return MultiBind.MultiBindBool((bool?[])array);
                    }

                    if (elementType == typeof(bool))
                    {
                        return MultiBind.MultiBindBool((bool[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, BOOL database type requires bool[] or bool?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_TINYINT:
                    if (elementType == typeof(sbyte?))
                    {
                        return MultiBind.MultiBindTinyInt((sbyte?[])array);
                    }

                    if (elementType == typeof(sbyte))
                    {
                        return MultiBind.MultiBindTinyInt((sbyte[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, TINYINT database type requires sbyte[] or sbyte?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_SMALLINT:
                    if (elementType == typeof(short?))
                    {
                        return MultiBind.MultiBindSmallInt((short?[])array);
                    }

                    if (elementType == typeof(short))
                    {
                        return MultiBind.MultiBindSmallInt((short[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, SMALLINT database type requires short[] or short?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_INT:
                    if (elementType == typeof(int?))
                    {
                        return MultiBind.MultiBindInt((int?[])array);
                    }

                    if (elementType == typeof(int))
                    {
                        return MultiBind.MultiBindInt((int[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, INT database type requires int[] or int?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                    if (elementType == typeof(long?))
                    {
                        return MultiBind.MultiBindBigInt((long?[])array);
                    }

                    if (elementType == typeof(long))
                    {
                        return MultiBind.MultiBindBigInt((long[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, BIGINT database type requires long[] or long?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_FLOAT:
                    if (elementType == typeof(float?))
                    {
                        return MultiBind.MultiBindFloat((float?[])array);
                    }

                    if (elementType == typeof(float))
                    {
                        return MultiBind.MultiBindFloat((float[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, FLOAT database type requires float[] or float?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_DOUBLE:
                    if (elementType == typeof(double?))
                    {
                        return MultiBind.MultiBindDouble((double?[])array);
                    }

                    if (elementType == typeof(double))
                    {
                        return MultiBind.MultiBindDouble((double[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, DOUBLE database type requires double[] or double?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_BINARY:
                    if (elementType == typeof(byte[]))
                    {
                        return MultiBind.MultiBindBytesArray((byte[][])array, TDengineDataType.TSDB_DATA_TYPE_BINARY);
                    }

                    if (elementType == typeof(string))
                    {
                        return MultiBind.MultiBindStringArray((string[])array, TDengineDataType.TSDB_DATA_TYPE_BINARY);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, BINARY/VARCHAR database type requires byte[][] or string[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                    if (elementType == typeof(DateTime?))
                    {
                        return MultiBind.MultiBindTimestamp((DateTime?[])array, (TDenginePrecision)field.precision);
                    }

                    if (elementType == typeof(DateTime))
                    {
                        return MultiBind.MultiBindTimestamp((DateTime[])array, (TDenginePrecision)field.precision);
                    }

                    if (elementType == typeof(long?))
                    {
                        return MultiBind.MultiBindTimestamp((long?[])array);
                    }

                    if (elementType == typeof(long))
                    {
                        return MultiBind.MultiBindTimestamp((long[])array);
                    }

                    if (elementType == typeof(DateTimeOffset?))
                    {
                        return MultiBind.MultiBindTimestamp((DateTimeOffset?[])array,
                            (TDenginePrecision)field.precision);
                    }

                    if (elementType == typeof(DateTimeOffset))
                    {
                        return MultiBind.MultiBindTimestamp((DateTimeOffset[])array,
                            (TDenginePrecision)field.precision);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, TIMESTAMP database type requires one of the following array types: DateTime[], DateTime?[], long[], long?[], DateTimeOffset[], DateTimeOffset?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_NCHAR:
                    if (elementType == typeof(string))
                    {
                        return MultiBind.MultiBindStringArray((string[])array, TDengineDataType.TSDB_DATA_TYPE_NCHAR);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, NCHAR database type requires string[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_UTINYINT:
                    if (elementType == typeof(byte?))
                    {
                        return MultiBind.MultiBindUTinyInt((byte?[])array);
                    }

                    if (elementType == typeof(byte))
                    {
                        return MultiBind.MultiBindUTinyInt((byte[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, TINYINT UNSIGNED database type requires byte[] or byte?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_USMALLINT:
                    if (elementType == typeof(ushort?))
                    {
                        return MultiBind.MultiBindUSmallInt((ushort?[])array);
                    }

                    if (elementType == typeof(ushort))
                    {
                        return MultiBind.MultiBindUSmallInt((ushort[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, SMALLINT UNSIGNED database type requires ushort[] or ushort?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_UINT:
                    if (elementType == typeof(uint?))
                    {
                        return MultiBind.MultiBindUInt((uint?[])array);
                    }

                    if (elementType == typeof(uint))
                    {
                        return MultiBind.MultiBindUInt((uint[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, INT UNSIGNED database type requires uint[] or uint?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_UBIGINT:
                    if (elementType == typeof(ulong?))
                    {
                        return MultiBind.MultiBindUBigInt((ulong?[])array);
                    }

                    if (elementType == typeof(ulong))
                    {
                        return MultiBind.MultiBindUBigInt((ulong[])array);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, BIGINT UNSIGNED database type requires ulong[] or ulong?[], but got an array of {elementType.Name}");
                case TDengineDataType.TSDB_DATA_TYPE_JSONTAG:
                    if (elementType == typeof(byte[]))
                    {
                        return MultiBind.MultiBindBytesArray((byte[][])array, TDengineDataType.TSDB_DATA_TYPE_JSONTAG);
                    }

                    if (elementType == typeof(string))
                    {
                        return MultiBind.MultiBindStringArray((string[])array, TDengineDataType.TSDB_DATA_TYPE_JSONTAG);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, JSON database type requires byte[][] or string[], but got an array of {elementType.Name}");

                case TDengineDataType.TSDB_DATA_TYPE_VARBINARY:
                    if (elementType == typeof(byte[]))
                    {
                        return MultiBind.MultiBindBytesArray((byte[][])array,
                            TDengineDataType.TSDB_DATA_TYPE_VARBINARY);
                    }

                    if (elementType == typeof(string))
                    {
                        return MultiBind.MultiBindStringArray((string[])array,
                            TDengineDataType.TSDB_DATA_TYPE_VARBINARY);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, VARBINARY database type requires byte[][] or string[], but got an array of {elementType.Name}");

                case TDengineDataType.TSDB_DATA_TYPE_GEOMETRY:
                    if (elementType == typeof(byte[]))
                    {
                        return MultiBind.MultiBindBytesArray((byte[][])array,
                            TDengineDataType.TSDB_DATA_TYPE_GEOMETRY);
                    }

                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, GEOMETRY database type requires byte[][], but got an array of {elementType.Name}");

                default:
                    throw new ArgumentException(
                        $"BindIndex: {bindIndex}, field name: {field.name}, {TDengineConstant.GetFieldTypeName(field.type)} database type not supported");
            }
        }

        public void AddBatch()
        {
            var code = NativeMethods.StmtAddBatch(_stmt);
            StmtCheckError(code);
        }

        public void Exec()
        {
            var code = NativeMethods.StmtExecute(_stmt);
            StmtCheckError(code);
        }

        public long Affected()
        {
            return NativeMethods.StmtAffetcedRowsOnce(_stmt);
        }

        public IRows Result()
        {
            if (IsInsert())
            {
                return new NativeRows((int)Affected());
            }

            var result = NativeMethods.StmtUseResult(_stmt);
            if (result == IntPtr.Zero)
            {
                throw new Exception("stmt is not query");
            }

            return new NativeRows(result, _tz, true);
        }

        public void Dispose()
        {
            if (_stmt != IntPtr.Zero)
            {
                NativeMethods.StmtClose(_stmt);
                _stmt = IntPtr.Zero;
            }
        }
    }
}