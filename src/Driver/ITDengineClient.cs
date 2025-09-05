using System;

namespace TDengine.Driver
{
    public interface ITDengineClient : IDisposable
    {
        IStmt StmtInit();
        IStmt StmtInit(long reqId);
        IRows Query(string query);
        IRows Query(string query, long reqId);
        long Exec(string query);
        long Exec(string query, long reqId);

        void SchemalessInsert(string[] lines, TDengineSchemalessProtocol protocol,
            TDengineSchemalessPrecision precision, int ttl, long reqId);

        bool ConnectionAvailable();
    }

    public interface IStmt : IDisposable
    {
        void Prepare(string query);
        bool IsInsert();
        void SetTableName(string tableName);
        void SetTags(object[] tags);
        TaosFieldE[] GetTagFields();
        TaosFieldE[] GetColFields();
        void BindRow(object[] row);
        void BindColumn( TaosFieldE[] _,params Array[] arrays);
        void AddBatch();
        void Exec();
        long Affected();
        IRows Result();
    }
}