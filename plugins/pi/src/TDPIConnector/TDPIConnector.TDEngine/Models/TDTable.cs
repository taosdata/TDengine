using System.Collections.Generic;

namespace TDPIConnector.TDEngine.Models
{
    public class TDTable
    {
        public TDTable(string name,string id, string sTableName)
        {
            Name = name; 
            Id = id;
            STableName = sTableName;
        }
        public TDTable(string name, int pointId, string sTableName, string columnType, List<TDColumn> columns)
        {
            Name = name;
            PointId = pointId;
            STableName = sTableName;
            ColumnType = columnType;
            Columns = columns;
        }

        public TDTable(string name, int pointId, string sTableName, string columnType = null)
        {
            Name = name;
            PointId = pointId;
            STableName = sTableName;
            ColumnType = columnType;
        }

        public string STableName { get; set; }
        public string ColumnType { get; }
        public string Name { get; set; }
        public string Id { get; set; }
        public string Location { get; set; }
        public string ElementPath { get; set; }
        public int PointId { get; set; }
        public IEnumerable<TDColumn> Columns { get; set; }

        public bool IsSingleValue { get { return string.IsNullOrEmpty(ColumnType); } }
    }

    public class TDSTable
    {
        public TDSTable(string name)
        {
            Name = name;
        }

        public string Name { get; set; }
        public IEnumerable<TDColumn> Columns { get; set; }

        public bool HasValidColumn() {
            foreach (var column in Columns)
            {
                if (!column.IsTDengineTag())
                {
                    return true;
                }
            }
            return false;
        }
    }
}
