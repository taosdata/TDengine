using System;
using System.Collections.Generic;

namespace TDengine.Driver.Client
{
    public abstract class AbstractRows : IRows
    {
        private readonly bool _isUpdate;
        private readonly List<TDengineMeta> _metas;
        protected int CurrentRow;
        protected int BlockSize;
        protected bool Completed;
        protected readonly BlockReader BlockReader;

        // update
        protected AbstractRows(int affectedRows)
        {
            _isUpdate = true;
            AffectRows = affectedRows;
        }

        // ws query
        protected AbstractRows(int blockOffset, int fieldCount, List<TDengineMeta> metas, byte[] types, byte[] scales,
            TimeZoneInfo tz,
            int precision)
        {
            _isUpdate = false;
            AffectRows = -1;
            FieldCount = fieldCount;
            _metas = metas;
            BlockReader = new BlockReader(blockOffset, FieldCount, precision, types, scales, tz);
        }

        // native query
        protected AbstractRows(int fieldCount, List<TDengineMeta> metas, TimeZoneInfo tz, int precision)
            : this(0, fieldCount, metas, ExtractTypes(metas), ExtractScales(metas), tz, precision)
        {
        }

        private static byte[] ExtractTypes(List<TDengineMeta> metas)
        {
            var types = new byte[metas.Count];
            for (int i = 0; i < metas.Count; i++)
            {
                types[i] = metas[i].type;
            }

            return types;
        }

        private static byte[] ExtractScales(List<TDengineMeta> metas)
        {
            var scales = new byte[metas.Count];
            for (int i = 0; i < metas.Count; i++)
            {
                scales[i] = metas[i].scale;
            }

            return scales;
        }

        public abstract void Dispose();

        public bool HasRows => _isUpdate == false;
        public int AffectRows { get; }
        public int FieldCount { get; }

        public long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return BlockReader.GetBytes(CurrentRow, ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public char GetChar(int ordinal)
        {
            return BlockReader.GetChar(CurrentRow, ordinal);
        }

        public long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return BlockReader.GetChars(CurrentRow, ordinal, dataOffset, buffer, bufferOffset, length);
        }

        public string GetDataTypeName(int ordinal) => _metas[ordinal].TypeName();

        public object GetValue(int ordinal)
        {
            return BlockReader.Read(CurrentRow, ordinal);
        }

        public Type GetFieldType(int ordinal) => _metas[ordinal].ScanType();

        public int GetFieldSize(int ordinal) => _metas[ordinal].size;

        public string GetName(int ordinal) => _metas[ordinal].name;

        public int GetFieldPrecision(int ordinal)
        {
            return _metas[ordinal].precision;
        }

        public int GetFieldScale(int ordinal)
        {
            return _metas[ordinal].scale;
        }

        public int GetOrdinal(string name) => _metas.FindIndex(m => m.name == name);

        public bool Read()
        {
            if (Completed) return false;
            if (!HasBlockData())
            {
                FetchBlock();
                return !Completed;
            }

            CurrentRow += 1;
            if (CurrentRow != BlockSize) return true;
            FetchBlock();
            return !Completed;
        }

        public bool IsDBNull(int ordinal)
        {
            return BlockReader.IsDBNull(CurrentRow, ordinal);
        }

        public byte GetByte(int ordinal)
        {
            return BlockReader.GetByte(CurrentRow, ordinal);
        }

        public short GetInt16(int ordinal)
        {
            return BlockReader.GetInt16(CurrentRow, ordinal);
        }

        public int GetInt32(int ordinal)
        {
            return BlockReader.GetInt32(CurrentRow, ordinal);
        }

        public long GetInt64(int ordinal)
        {
            return BlockReader.GetInt64(CurrentRow, ordinal);
        }

        public bool GetBoolean(int ordinal)
        {
            return BlockReader.GetBoolean(CurrentRow, ordinal);
        }

        public DateTime GetDateTime(int ordinal)
        {
            return BlockReader.GetDateTime(CurrentRow, ordinal);
        }

        public decimal GetDecimal(int ordinal)
        {
            return BlockReader.GetDecimal(CurrentRow, ordinal);
        }

        public double GetDouble(int ordinal)
        {
            return BlockReader.GetDouble(CurrentRow, ordinal);
        }

        public float GetFloat(int ordinal)
        {
            return BlockReader.GetFloat(CurrentRow, ordinal);
        }

        public string GetString(int ordinal)
        {
            return BlockReader.GetString(CurrentRow, ordinal);
        }

        public int GetValues(object[] values)
        {
            return BlockReader.GetValues(CurrentRow, values);
        }
        
        public DateTimeOffset GetDateTimeOffset(int ordinal)
        {
            return BlockReader.GetDateTimeOffset(CurrentRow, ordinal);
        }
        
        protected abstract bool HasBlockData();
        protected abstract void FetchBlock();
    }
}