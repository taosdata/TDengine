using System;

namespace TDengine.Driver
{
    public interface IRows : IDisposable
    {
        bool HasRows { get; }
        int AffectRows { get; }
        int FieldCount { get; }
        long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length);
        char GetChar(int ordinal);
        long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length);
        string GetDataTypeName(int ordinal);
        object GetValue(int ordinal);
        Type GetFieldType(int ordinal);
        int GetFieldSize(int ordinal);
        string GetName(int ordinal);

        int GetFieldPrecision(int ordinal);

        int GetFieldScale(int ordinal);

        int GetOrdinal(string name);
        bool Read();

        bool IsDBNull(int ordinal);

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

        int GetValues(object[] values);
        
        DateTimeOffset GetDateTimeOffset(int ordinal);
    }
}