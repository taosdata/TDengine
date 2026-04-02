using System;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
        public void SetTableName(string tableName)
        {
            CheckPrepared();
            if (_needTableName)
            {
                if (IsTableNameSet)
                {
                    throw new InvalidOperationException(
                        "Table name has already been set for current batch");
                }

                if (string.IsNullOrEmpty(tableName))
                {
                    throw new ArgumentException("Table name cannot be null or empty");
                }

                if (_tableInfos.TryGetValue(tableName, out var info))
                {
                    _currentTableInfo = info;
                }
                else
                {
                    _currentTableInfo.TableName = tableName;
                }

                IsTableNameSet = true;
            }
            else
            {
                throw new InvalidOperationException(
                    "Table name is not required for this statement or not supported in this context.");
            }
        }

        public void SetTags(object[] tags)
        {
            CheckPrepared();
            CheckTableNameSet();
            if (tags == null || tags.Length == 0)
            {
                throw new ArgumentException("Tags cannot be null or empty");
            }

            if (_tagFields == null || _tagFields.Length == 0 || !_isInsert)
            {
                throw new InvalidOperationException("This statement does not need tags.");
            }

            if (IsTagsSet)
            {
                return; 
            }

            if (tags.Length != _tagFields.Length)
            {
                throw new ArgumentException(
                    $"Expected {_tagFields.Length} tags, but got {tags.Length}");
            }

            CheckRowValue(tags, _tagFields);
            if (_currentTableInfo.Tags == null)
            {
                _currentTableInfo.Tags = tags;
            }

            IsTagsSet = true;
        }
    }
}