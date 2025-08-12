using System;

namespace TDengine.TMQ
{
    public interface ITMQRows
    {
        object GetValue(int ordinal);
        bool Read();
        bool IsDBNull(int ordinal);
        
        int FieldCount { get; }
        string TableName { get; }
        string GetName(int ordinal);
        
        byte GetByte(int ordinal);
        short GetInt16(int ordinal);
        int GetInt32(int ordinal);
        long GetInt64(int ordinal);
        bool GetBoolean(int ordinal);

        DateTime GetDateTime(int ordinal);

        decimal GetDecimal(int ordinal);

        double GetDouble(int ordinal);

        float GetFloat(int ordinal);

        string GetString(int ordinal);
        
        DateTimeOffset GetDateTimeOffset(int ordinal);
        
    }
}