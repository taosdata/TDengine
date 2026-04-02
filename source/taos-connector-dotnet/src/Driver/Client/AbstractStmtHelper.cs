using System;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
        protected abstract bool IsConnectionAvailable(Exception exception);

        protected abstract void ReconnectInternal();
        
        protected abstract bool AutoReconnectInternal();

        private bool NeedTags => _tagFields != null && _tagFields.Length > 0;

        private bool IsTableNameSet
        {
            get => _isTableNameSet;
            set
            {
                _isTableNameSet = value;
                if (!value) return;
                _addBatched = false;
                _executed = false;
            }
        }

        private bool IsTagsSet
        {
            get => _isTagsSet;
            set
            {
                _isTagsSet = value;
                if (!value) return;
                _addBatched = false;
                _executed = false;
            }
        }

        private bool IsColSet
        {
            get => _isColSet;
            set
            {
                _isColSet = value;
                if (!value) return;
                _addBatched = false;
                _executed = false;
            }
        }

        private static bool CheckFieldsAllEqual(TaosFieldAll[] fields1, TaosFieldAll[] fields2)
        {
            if (fields1 == null && fields2 == null) return true;
            if (fields1 == null || fields2 == null) return false;
            if (fields1.Length != fields2.Length) return false;
            for (var i = 0; i < fields1.Length; i++)
            {
                if (fields1[i].name != fields2[i].name ||
                    fields1[i].type != fields2[i].type ||
                    fields1[i].precision != fields2[i].precision ||
                    fields1[i].scale != fields2[i].scale ||
                    fields1[i].bytes != fields2[i].bytes ||
                    fields1[i].field_type != fields2[i].field_type)
                {
                    return false;
                }
            }

            return true;
        }

        private void RePrepare()
        {
            // prepare again
            PrepareInternal(_sql, out var insert, out var count, out var fields);
            if (insert != _isInsert || count != _fieldsCount || !CheckFieldsAllEqual(fields, _fields))
            {
                _schemaChanged = true;
                // statement type or fields do not match
                throw new InvalidOperationException(
                    "Failed to re-prepare the statement. The statement type or fields do not match, you should call Prepare() again.");
            }
        }

        private void CheckPrepared()
        {
            if (string.IsNullOrEmpty(_sql))
            {
                throw new InvalidOperationException("This statement has not been prepared.");
            }

            if (_schemaChanged)
            {
                throw new InvalidOperationException("The schema has changed, you should call Prepare() again.");
            }
        }

        private void CheckTableNameSet()
        {
            if (_needTableName && !IsTableNameSet)
            {
                throw new InvalidOperationException("Table name is not set, you should call SetTableName() first.");
            }
        }

        public bool IsInsert()
        {
            CheckPrepared();
            return _isInsert;
        }

        public TaosFieldE[] GetTagFields()
        {
            CheckPrepared();
            return _tagFields;
        }

        public TaosFieldE[] GetColFields()
        {
            CheckPrepared();
            return _colFields;
        }
    }
}