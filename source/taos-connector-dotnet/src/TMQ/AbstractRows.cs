using System;
using System.Collections.Generic;
using TDengine.Driver;

namespace TDengine.TMQ
{
    public abstract class AbstractRows: ITMQRows
    {
        protected int CurrentRow;
        protected List<TDengineMeta> Metas;
        protected int BlockRows;
        protected byte[] Block;
        protected bool Completed;
        protected int BlockIndex;
        protected TMQBlockReader.TMQBlockInfo[] BlockInfo;
        protected readonly BlockReader BlockReader;
        protected readonly TMQBlockReader TmqBlockReader;
        
        protected abstract void FetchBlock();

        protected AbstractRows(int blockReaderOffset ,int tmqBlockReaderOffset, TimeZoneInfo tz)
        {
            BlockReader = new BlockReader(blockReaderOffset, tz);
            TmqBlockReader = new TMQBlockReader(tmqBlockReaderOffset);
        }
        
        public object GetValue(int ordinal)
        {
            return BlockReader.Read(CurrentRow, ordinal);
        }

        public bool Read()
        {
            if (Completed) return false;
            if (Block == null)
            {
                FetchBlock();
                return !Completed;
            }

            CurrentRow += 1;
            if (CurrentRow != BlockRows) return true;
            FetchBlock();
            return !Completed;
        }
        
        public int FieldCount { get; protected set; }
        public string TableName { get; protected set; }
        
        public string GetName(int ordinal) => Metas[ordinal].name;

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

        public DateTimeOffset GetDateTimeOffset(int ordinal)
        {
            return BlockReader.GetDateTimeOffset(CurrentRow, ordinal);
        }

        public bool IsDBNull(int ordinal)
        {
            return BlockReader.IsDBNull(CurrentRow, ordinal);
        }
        
    }
}