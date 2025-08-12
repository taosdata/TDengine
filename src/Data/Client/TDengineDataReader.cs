using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using TDengine.Driver;

namespace TDengine.Data.Client
{
    public class TDengineDataReader : DbDataReader
    {
        private IRows _rows;
        private int _fieldCount;

        public TDengineDataReader(IRows rows)
        {
            _rows = rows;
            _fieldCount = rows.FieldCount;
        }

        public override DataTable GetSchemaTable()
        {
            var schemaTable = new DataTable();
            if (_fieldCount > 0)
            {
                var columns = schemaTable.Columns;

                var columnName = new DataColumn(SchemaTableColumn.ColumnName, typeof(string));
                var columnOrdinal = new DataColumn(SchemaTableColumn.ColumnOrdinal, typeof(int));
                var columnSize = new DataColumn(SchemaTableColumn.ColumnSize, typeof(int));
                var dataType = new DataColumn(SchemaTableColumn.DataType, typeof(Type));
                var dataTypeName = new DataColumn("DataTypeName", typeof(string));
                var numericPrecision = new DataColumn(SchemaTableColumn.NumericPrecision, typeof(int));
                var numericScale = new DataColumn(SchemaTableColumn.NumericScale, typeof(int));


                columns.Add(columnName);
                columns.Add(columnOrdinal);
                columns.Add(columnSize);
                columns.Add(dataType);
                columns.Add(dataTypeName);
                columns.Add(numericPrecision);
                columns.Add(numericScale);

                for (int i = 0; i < _fieldCount; i++)
                {
                    var schemaRow = schemaTable.NewRow();

                    schemaRow[columnName] = GetName(i);
                    schemaRow[columnOrdinal] = i;
                    schemaRow[columnSize] = GetFieldSize(i);
                    schemaRow[dataType] = GetFieldType(i);
                    schemaRow[dataTypeName] = GetDataTypeName(i);
                    schemaRow[numericPrecision] = _rows.GetFieldPrecision(i);
                    schemaRow[numericScale] = _rows.GetFieldScale(i);
                    schemaTable.Rows.Add(schemaRow);
                }
            }

            return schemaTable;
        }

        public override bool GetBoolean(int ordinal) => _rows.GetBoolean(ordinal);

        public override byte GetByte(int ordinal) => _rows.GetByte(ordinal);

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) =>
            _rows.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);

        public override char GetChar(int ordinal) => _rows.GetChar(ordinal);

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) =>
            _rows.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);

        public override string GetDataTypeName(int ordinal) => _rows.GetDataTypeName(ordinal);

        public override DateTime GetDateTime(int ordinal) => _rows.GetDateTime(ordinal);

        public override decimal GetDecimal(int ordinal) => _rows.GetDecimal(ordinal);

        public override double GetDouble(int ordinal) => _rows.GetDouble(ordinal);

        public override Type GetFieldType(int ordinal) => _rows.GetFieldType(ordinal);

        public override float GetFloat(int ordinal) => _rows.GetFloat(ordinal);

        public override Guid GetGuid(int ordinal) => GetFieldValue<Guid>(ordinal);

        public override short GetInt16(int ordinal) => _rows.GetInt16(ordinal);

        public override int GetInt32(int ordinal) => _rows.GetInt32(ordinal);

        public override long GetInt64(int ordinal) => _rows.GetInt64(ordinal);

        public override string GetName(int ordinal) => _rows.GetName(ordinal);

        public int GetFieldSize(int ordinal) => _rows.GetFieldSize(ordinal);

        public override int GetOrdinal(string name) => _rows.GetOrdinal(name);

        public override string GetString(int ordinal) => _rows.GetString(ordinal);

        public override object GetValue(int ordinal) => _rows.GetValue(ordinal);

        public override int GetValues(object[] values) => _rows.GetValues(values);
        
        public DateTimeOffset GetDateTimeOffset(int ordinal) => _rows.GetDateTimeOffset(ordinal);

        public override bool IsDBNull(int ordinal) => _rows.IsDBNull(ordinal);

        public override int FieldCount => _fieldCount;

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override int RecordsAffected => _rows.AffectRows;

        public override bool HasRows => _rows.HasRows;

        public override bool IsClosed => false;

        public override bool NextResult() => false;

        public override bool Read() => _rows.Read();

        public override int Depth => 0;

        public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

        public override void Close() => Dispose(true);

        protected override void Dispose(bool disposing)
        {
            if (_rows == null) return;
            _rows.Dispose();
            _rows = null;
        }
    }
}