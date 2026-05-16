using System;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
        public void AddBatch()
        {
            // check if the statement is prepared
            CheckPrepared();
            // check if the table name is set if required
            if (_needTableName && !IsTableNameSet)
            {
                throw new InvalidOperationException("Table name must be set before adding a batch.");
            }

            // check if tags are set if required
            if (NeedTags && !IsTagsSet)
            {
                throw new InvalidOperationException("Tags must be set before adding a batch.");
            }

            // check if columns are set
            if (!IsColSet)
            {
                throw new InvalidOperationException("Columns must be set before adding a batch.");
            }

            // check row count
            
            var rowCount = _currentTableInfo.Cols[0].Count;
            for (var i = 0; i < _currentTableInfo.Cols.Length; i++)
            {
                if (_currentTableInfo.Cols[i].Count == 0)
                {
                    throw new InvalidOperationException($"Column at index {i} has no rows to add.");
                }

                if (_currentTableInfo.Cols[i].Count != rowCount)
                {
                    throw new InvalidOperationException(
                        $"Column at index {i} has a different row count than the first column. Expected {rowCount}, but got {_currentTableInfo.Cols[i].Count}.");
                }
            }

            // cache to dictionary
            _tableInfos[_currentTableInfo.TableName] = _currentTableInfo;
            // reset the current table info

            _addBatched = true;
            CleanBatch();
        }
    }
}