using System;
using TDengine.Driver;
using TDengine.Driver.Client;

namespace Driver.Test.Client.Query
{
    public class MockStmt:AbstractStmt
    {
        public delegate void PrepareAction(string query, out bool isInsert, out int count, out TaosFieldAll[] fields);
        private readonly PrepareAction _prepareAction;
        public delegate void BindBinaryAction(byte[] data, out int affectedRows);
        private readonly BindBinaryAction _bindBinaryAction;
        public MockStmt(PrepareAction prepare, BindBinaryAction bind):base(0)
        {
            _prepareAction = prepare;
            _bindBinaryAction = bind;
        }

        protected override void BindBinaryInternal(byte[] data, out int affectedRows)
        {
            _bindBinaryAction(data, out affectedRows);
        }

        protected override void PrepareInternal(string query, out bool isInsert, out int count, out TaosFieldAll[] fields)
        {
            _prepareAction(query, out isInsert, out count, out fields);
        }

        public override void Dispose()
        {
        }

        protected override bool IsConnectionAvailable(Exception exception)
        {
            return true;
        }

        protected override void ReconnectInternal()
        {
        }

        protected override bool AutoReconnectInternal()
        {
            return false;
        }

        protected override IRows QueryResultInternal()
        {
            return null;
        }

        protected override IRows InsertResultInternal(int affectedRows)
        {
            return null;
        }
    }
}