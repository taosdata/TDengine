using System.Collections.Generic;

namespace TDengine.Driver.Client
{
    class Stmt2TableData
    {
        public string TableName;
        public List<object>[] Cols;
        public object[] Tags;
        
        public Stmt2TableData(List<object>[] cols)
        {
            TableName = string.Empty;
            Cols = cols;
        }

        public bool IsColSet => Cols[0].Count > 0;
        public int Rows => Cols[0].Count;
    }
    
    public abstract partial class AbstractStmt : IStmt
    {
        private readonly int _binaryHeaderLength;
        private string _sql = string.Empty;
        private bool _isInsert;
        private int _fieldsCount;
        private TaosFieldAll[] _fields;
        private TaosFieldE[] _tagFields;
        private TaosFieldE[] _colFields;

        // private IFieldBuilder[] _colBuilders;
        // private IFieldBuilder[] _tagBuilders;
        private bool _needTableName;
        
        private readonly Dictionary<string, Stmt2TableData> _tableInfos = new Dictionary<string, Stmt2TableData>();
        private Stmt2TableData _currentTableInfo;
        private bool _isTableNameSet;
        private bool _isTagsSet;
        private bool _isColSet;
        private bool _addBatched;
        private bool _executed;
        private int _affectedRows;
        private bool _schemaChanged;
        private TaosFieldE[] _queryFields;
        
        private readonly Queue<List<object>> _objectListQueue = new Queue<List<object>>();
        private readonly Queue<Stmt2TableData> _tableInfoQueue = new Queue<Stmt2TableData>();
        
        // after prepare or add batch, get a new table info
        private Stmt2TableData GetStmt2TableData()
        {
            var colLength = _isInsert ? _colFields.Length : _fieldsCount;

            var info =
                // get table info from cache
                _tableInfoQueue.Count > 0 ? _tableInfoQueue.Dequeue() :
                // create new table info
                new Stmt2TableData(new List<object>[colLength]);

            // ensure the array length is correct, if not enough, recreate it
            if (info.Cols.Length != colLength)
            {
                info.Cols = new List<object>[colLength];
            }
    
            // fill the lists
            for (var i = 0; i < info.Cols.Length; i++)
            {
                // get from cache or create new
                info.Cols[i] = _objectListQueue.Count > 0 ? _objectListQueue.Dequeue() : new List<object>();
            }
            return info;
        }
        
        // after execute, put table info to cache
        private void PutTableInfo(Stmt2TableData info)
        {
            if (info == null) return;
            // clear all column lists and return to cache
            for (var i = 0; i < info.Cols.Length; i++)
            {
                var list = info.Cols[i];
                list.Clear();
                _objectListQueue.Enqueue(list);
                info.Cols[i] = null;
            }
            info.Tags = null;
            info.TableName = string.Empty;
            // return to cache
            _tableInfoQueue.Enqueue(info);
        }
        
        protected AbstractStmt(int binaryHeaderLength = 0)
        {
            _binaryHeaderLength = binaryHeaderLength;
        }
        
        // before prepare or prepare failed, clean all cache
        private void CleanCache()
        {
            _sql = string.Empty;
            _isInsert = false;
            _fieldsCount = 0;
            _fields = null;
            _tagFields = null;
            _colFields = null;
            _needTableName = false;
            _tableInfos.Clear();
            _isTableNameSet = false;
            _isTagsSet = false;
            _addBatched = false;
            _executed = false;
            _schemaChanged = false;
            _currentTableInfo = null;
            // clean cached object lists and table info queue
            _tableInfoQueue.Clear();
            _objectListQueue.Clear();
        }

        // after add batch, clean current batch info
        private void CleanBatch()
        {
            _isTableNameSet = false;
            _isTagsSet = false;
            _currentTableInfo = GetStmt2TableData();
        }
        
        // after execute, put all table info to cache
        private void CleanExec()
        {
            if (!_isInsert)
            {
                _queryFields = null;
            }

            _addBatched = false;
            _executed = true;
            foreach (var tableInfo in _tableInfos.Values)
            {
                // return table info to cache
                PutTableInfo(tableInfo);
            }
            _tableInfos.Clear();
        }

        public abstract void Dispose();
    }
}