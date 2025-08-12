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
                    Marshal.FreeHGlobal(p);
                }
            }
        }

        private TAOS_MULTI_BIND[] GenerateBindList(object[] data, TaosFieldE[] fields, out IntPtr[] needFree,
            bool isInsert)
        {
            TAOS_MULTI_BIND[] binds = new TAOS_MULTI_BIND[data.Length];
            var needFreePointer = new List<IntPtr>();
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
                    Marshal.WriteByte(p, 1);
                    needFreePointer.Add(p);
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
                            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BOOL;
                            p = Marshal.AllocHGlobal(TDengineConstant.BoolSize);
                            bs = BitConverter.GetBytes(val);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            needFreePointer.Add(p);
                            bind.buffer = p;
                            bind.buffer_length = (UIntPtr)TDengineConstant.BoolSize;
                            break;
                        case sbyte val:
                            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_TINYINT;
                            p = Marshal.AllocHGlobal(TDengineConstant.Int8Size);
                            Marshal.WriteByte(p, (byte)val);
                            needFreePointer.Add(p);
                            bind.buffer = p;
                            bind.buffer_length = (UIntPtr)TDengineConstant.Int8Size;
                            break;
                        case short val:
                            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_SMALLINT;
                            p = Marshal.AllocHGlobal(TDengineConstant.Int16Size);
                            bs = BitConverter.GetBytes(val);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            needFreePointer.Add(p);
                            bind.buffer = p;
                            bind.buffer_length = (UIntPtr)TDengineConstant.Int16Size;
                            break;
                        case int val:
                            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_INT;
                            p = Marshal.AllocHGlobal(TDengineConstant.Int32Size);
                            bs = BitConverter.GetBytes(val);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            needFreePointer.Add(p);
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
                                    throw new NotSupportedException(
                                        $"bind param type long to {fields[i].type} not supported");
                            }
                            else
                            {
                                bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_BIGINT;
                            }

                            p = Marshal.AllocHGlobal(TDengineConstant.Int64Size);
                            bs = BitConverter.GetBytes(val);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            needFreePointer.Add(p);
                            bind.buffer = p;
                            bind.buffer_length = (UIntPtr)TDengineConstant.Int64Size;
                            break;
                        case byte val:
                            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UTINYINT;
                            p = Marshal.AllocHGlobal(TDengineConstant.UInt8Size);
                            Marshal.WriteByte(p, (byte)val);
                            needFreePointer.Add(p);
                            bind.buffer = p;
                            bind.buffer_length = (UIntPtr)TDengineConstant.UInt8Size;
                            break;
                        case ushort val:
                            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_USMALLINT;
                            p = Marshal.AllocHGlobal(TDengineConstant.UInt16Size);
                            bs = BitConverter.GetBytes(val);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            needFreePointer.Add(p);
                            bind.buffer = p;
                            bind.buffer_length = (UIntPtr)TDengineConstant.UInt16Size;
                            break;
                        case uint val:
                            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UINT;
                            p = Marshal.AllocHGlobal(TDengineConstant.UInt32Size);
                            bs = BitConverter.GetBytes(val);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            needFreePointer.Add(p);
                            bind.buffer = p;
                            bind.buffer_length = (UIntPtr)TDengineConstant.UInt32Size;
                            break;
                        case ulong val:
                            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_UBIGINT;
                            p = Marshal.AllocHGlobal(TDengineConstant.UInt64Size);
                            bs = BitConverter.GetBytes(val);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            needFreePointer.Add(p);
                            bind.buffer = p;
                            bind.buffer_length = (UIntPtr)TDengineConstant.UInt64Size;
                            break;
                        case float val:
                            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_FLOAT;
                            p = Marshal.AllocHGlobal(TDengineConstant.Float32Size);
                            bs = BitConverter.GetBytes(val);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            needFreePointer.Add(p);
                            bind.buffer = p;
                            bind.buffer_length = (UIntPtr)TDengineConstant.Float32Size;
                            break;
                        case double val:
                            bind.buffer_type = (int)TDengineDataType.TSDB_DATA_TYPE_DOUBLE;
                            p = Marshal.AllocHGlobal(TDengineConstant.Float64Size);
                            bs = BitConverter.GetBytes(val);
                            Marshal.Copy(bs, 0, p, bs.Length);
                            needFreePointer.Add(p);
                            bind.buffer = p;
                            bind.buffer_length = (UIntPtr)TDengineConstant.Float64Size;
                            break;
                        case DateTime val:
                            if (isInsert)
                            {
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
                            throw new NotSupportedException($"stmt param not support type: {data[i].GetType()}");
                    }
                }

                binds[i] = bind;
            }

            needFree = needFreePointer.ToArray();
            return binds;
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
                    Marshal.FreeHGlobal(p);
                }
            }
        }

        public void BindColumn(TaosFieldE[] field, params Array[] arrays)
        {
            var multiBind = new TAOS_MULTI_BIND[arrays.Length];
            for (int i = 0; i < arrays.Length; i++)
            {
                multiBind[i] = GenerateBindColumn(arrays[i], field[i]);
            }

            try
            {
                NativeMethods.StmtBindParamBatch(_stmt, multiBind);
            }
            finally
            {
                foreach (var bind in multiBind)
                {
                    MultiBind.FreeTaosBind(bind);
                }
            }
        }

        private TAOS_MULTI_BIND GenerateBindColumn(Array array, TaosFieldE field)
        {
            switch (array.GetType().GetElementType())
            {
                case Type byteType when byteType == typeof(bool?):
                    return MultiBind.MultiBindBool((bool?[])array);
                case Type byteType when byteType == typeof(bool):
                    return MultiBind.MultiBindBool((bool[])array);

                case Type byteType when byteType == typeof(sbyte?):
                    return MultiBind.MultiBindTinyInt((sbyte?[])array);
                case Type byteType when byteType == typeof(sbyte):
                    return MultiBind.MultiBindTinyInt((sbyte[])array);

                case Type byteType when byteType == typeof(short?):
                    return MultiBind.MultiBindSmallInt((short?[])array);
                case Type byteType when byteType == typeof(short):
                    return MultiBind.MultiBindSmallInt((short[])array);

                case Type byteType when byteType == typeof(int?):
                    return MultiBind.MultiBindInt((int?[])array);
                case Type byteType when byteType == typeof(int):
                    return MultiBind.MultiBindInt((int[])array);

                case Type byteType when byteType == typeof(long?):
                    switch ((TDengineDataType)field.type)
                    {
                        case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                            return MultiBind.MultiBindBigInt((long?[])array);
                        case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                            return MultiBind.MultiBindTimestamp((long?[])array);
                        default:
                            throw new NotSupportedException($"bind param type long to {field.type} not supported");
                    }
                case Type byteType when byteType == typeof(long):
                    switch ((TDengineDataType)field.type)
                    {
                        case TDengineDataType.TSDB_DATA_TYPE_BIGINT:
                            return MultiBind.MultiBindBigInt((long[])array);
                        case TDengineDataType.TSDB_DATA_TYPE_TIMESTAMP:
                            return MultiBind.MultiBindTimestamp((long[])array);
                        default:
                            throw new NotSupportedException($"bind param type long to {field.type} not supported");
                    }

                case Type byteType when byteType == typeof(byte?):
                    return MultiBind.MultiBindUTinyInt((byte?[])array);
                case Type byteType when byteType == typeof(byte):
                    return MultiBind.MultiBindUTinyInt((byte[])array);

                case Type byteType when byteType == typeof(ushort?):
                    return MultiBind.MultiBindUSmallInt((ushort?[])array);
                case Type byteType when byteType == typeof(ushort):
                    return MultiBind.MultiBindUSmallInt((ushort[])array);

                case Type byteType when byteType == typeof(uint?):
                    return MultiBind.MultiBindUInt((uint?[])array);
                case Type byteType when byteType == typeof(uint):
                    return MultiBind.MultiBindUInt((uint[])array);

                case Type byteType when byteType == typeof(ulong?):
                    return MultiBind.MultiBindUBigInt((ulong?[])array);
                case Type byteType when byteType == typeof(ulong):
                    return MultiBind.MultiBindUBigInt((ulong[])array);

                case Type byteType when byteType == typeof(float?):
                    return MultiBind.MultiBindFloat((float?[])array);
                case Type byteType when byteType == typeof(float):
                    return MultiBind.MultiBindFloat((float[])array);

                case Type byteType when byteType == typeof(double?):
                    return MultiBind.MultiBindDouble((double?[])array);
                case Type byteType when byteType == typeof(double):
                    return MultiBind.MultiBindDouble((double[])array);

                case Type byteType when byteType == typeof(DateTime?):
                    return MultiBind.MultiBindTimestamp((DateTime?[])array, (TDenginePrecision)field.precision);
                case Type byteType when byteType == typeof(DateTime):
                    return MultiBind.MultiBindTimestamp((DateTime[])array, (TDenginePrecision)field.precision);

                case Type byteType when byteType == typeof(DateTimeOffset?):
                    return MultiBind.MultiBindTimestamp((DateTimeOffset?[])array, (TDenginePrecision)field.precision);
                case Type byteType when byteType == typeof(DateTimeOffset):
                    return MultiBind.MultiBindTimestamp((DateTimeOffset[])array, (TDenginePrecision)field.precision);

                case Type byteType when byteType == typeof(byte[]):
                    return MultiBind.MultiBindBytesArray((byte[][])array, (TDengineDataType)field.type);

                case Type byteType when byteType == typeof(string):
                    return MultiBind.MultiBindStringArray((string[])array, (TDengineDataType)field.type);
                default:
                    throw new NotSupportedException(
                        $"bind param type {array.GetType().GetElementType()} not supported");
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