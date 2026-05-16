using System;

namespace TDengine.Driver.Client
{
    public abstract partial class AbstractStmt
    {
        public long Affected()
        {
            return _affectedRows;
        }

        public IRows Result()
        {
            if (!_executed)
            {
                throw new InvalidOperationException("Statement has not been executed yet.");
            }

            if (_isInsert)
            {
                return InsertResultInternal(_affectedRows);
            }

            return QueryResultInternal();
        }

        protected abstract IRows QueryResultInternal();
        protected abstract IRows InsertResultInternal(int affectedRows);
    }
}