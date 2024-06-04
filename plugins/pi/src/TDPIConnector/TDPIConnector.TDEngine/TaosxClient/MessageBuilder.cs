#define CLOUD_LICENSE_ONLY_DISABLED
using log4net;
using System;
using Apache.Arrow;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow.Types;
using System.Collections;
using IpcDataType = System.String;
using TDPIConnector.TDEngine.Models;

namespace TDPIConnector.TDEngine.TaosxClient
{
    public class RawRecord {

        public string key;
        public DateTime ts;
        public Dictionary<string, string> valDic;
        public Dictionary<string, int> statusDic;

        public RawRecord(string key, DateTime ts, Dictionary<string, string> valDic, Dictionary<string, int> statusDic)
        {
            this.key = key;
            this.ts = ts;
            this.valDic = valDic;
            this.statusDic = statusDic;
        }
    }

    /// <summary>
    ///  暂存各种类型的数据，用于构建ArrowArray
    /// </summary>
    public class ColumnValueBuilder {
        public TDValueType ValueType { get; set; }
        public object ArrowArray;
        public ColumnValueBuilder(TDValueType valueType) {
            ValueType = valueType;
            switch (valueType)
            {
                case TDValueType.String:
                    ArrowArray = new StringArray.Builder();
                    break;
                case TDValueType.Int:
                    ArrowArray = new Int32Array.Builder();
                    break;
                case TDValueType.BigInt:
                    ArrowArray = new Int64Array.Builder();
                    break;
                case TDValueType.Float:
                    ArrowArray = new FloatArray.Builder();
                    break;
                case TDValueType.Double:
                    ArrowArray = new DoubleArray.Builder();
                    break;
                case TDValueType.Timestamp:
                    ArrowArray = new TimestampArray.Builder();
                    break;
                case TDValueType.Boolean:
                    ArrowArray = new BooleanArray.Builder();
                    break;
                default:
                    throw new Exception("Unsupported TDType");
            }
        }

        public void Append(TDValueType valueType, object value) { 
            if (valueType != ValueType)
            {
                throw new Exception("ValueType not match");
            }
            Append(value);
        }

        public void Append(object value) {
            switch (ValueType) {
                case TDValueType.Int:
                    if (value != null)
                    {
                        ((Int32Array.Builder)ArrowArray).Append((int)value);
                    }
                    else {
                        ((Int32Array.Builder)ArrowArray).Append(null);
                    }
                    break;
                case TDValueType.BigInt:
                    if (value != null)
                    {
                        ((Int64Array.Builder)ArrowArray).Append((long)value);
                    }
                    else {
                        ((Int64Array.Builder)ArrowArray).Append(null);
                    }
                    break;
                case TDValueType.Float:
                    if (value != null)
                    {
                        ((FloatArray.Builder)ArrowArray).Append((float)value);
                    }
                    else {
                        ((FloatArray.Builder)ArrowArray).Append(null);
                    }
                    break;
                case TDValueType.Double:
                    if (value != null)
                    {
                        ((DoubleArray.Builder)ArrowArray).Append((double)value);
                    }
                    else {
                        ((DoubleArray.Builder)ArrowArray).Append(null);
                    }
                    break;
                case TDValueType.Timestamp:
                    if (value != null)
                    {
                        ((TimestampArray.Builder)ArrowArray).Append((DateTime)value);
                    }
                    else {
                        ((TimestampArray.Builder)ArrowArray).Append(null);
                    }
                    break;
                case TDValueType.Boolean:
                    if (value != null)
                    {
                        ((BooleanArray.Builder)ArrowArray).Append((bool)value);
                    }
                    else {
                        ((BooleanArray.Builder)ArrowArray).Append(null);
                    }
                    break;
            }
        }

        public void Clear() {
            switch (ValueType) {
                case TDValueType.String:
                    ((StringArray.Builder)ArrowArray).Clear();
                    break;
                case TDValueType.Int:
                    ((Int32Array.Builder)ArrowArray).Clear();
                    break;
                case TDValueType.BigInt:
                    ((Int64Array.Builder)ArrowArray).Clear();
                    break;
                case TDValueType.Float:
                    ((FloatArray.Builder)ArrowArray).Clear();
                    break;
                case TDValueType.Double:
                    ((DoubleArray.Builder)ArrowArray).Clear();
                    break;
                case TDValueType.Timestamp:
                    ((TimestampArray.Builder)ArrowArray).Clear();
                    break;
                case TDValueType.Boolean:
                    ((BooleanArray.Builder)ArrowArray).Clear();
                    break;
                default:
                    throw new Exception("Unsupported TDType");
            }
        }

        public IArrowArray Build()
        {
            switch (ValueType)
            {
                case TDValueType.String:
                    return ((StringArray.Builder)ArrowArray).Build();
                case TDValueType.Int:
                    return ((Int32Array.Builder)ArrowArray).Build();
                case TDValueType.BigInt:
                    return ((Int64Array.Builder)ArrowArray).Build();
                case TDValueType.Float:
                    return ((FloatArray.Builder)ArrowArray).Build();
                case TDValueType.Double:
                    return ((DoubleArray.Builder)ArrowArray).Build();
                case TDValueType.Timestamp:
                    return ((TimestampArray.Builder)ArrowArray).Build();
                case TDValueType.Boolean:
                    return ((BooleanArray.Builder)ArrowArray).Build();
                default:
                    throw new Exception("Unsupported TDType");
            }
        }

    }

    public class MessageBuilder
    {
        public static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public List<KeyValuePair<string, TDValueType>> columnNameTypes
            = new List<KeyValuePair<string, TDValueType>>();
        public List<KeyValuePair<string, string>> tagNames;
        public StructType recordType { get; set; }
        public StructType tableType { get; set; }
        public StructType tagStruct { get; set; }

        public ArrayList subTables = new ArrayList();
        public List<StructType> records = new List<StructType>();

        public StringArray.Builder tableUniqKeyArrowArray;
        public TimestampArray.Builder tsArrowArray;

        public Dictionary<string, Int32Array.Builder> statusArrowArrayList
            = new Dictionary<string, Int32Array.Builder>();

        public Dictionary<string, ColumnValueBuilder> valArrowArrayList
            = new Dictionary<string, ColumnValueBuilder>();

        // pointId is table tag for PI mode 
        // public Dictionary<string, int> pointIds
        //    = new Dictionary<string, int>();
        // AFElement mode tags
        public Dictionary<string, List<KeyValuePair<string, string>>> tagVals
            = new Dictionary<string, List<KeyValuePair<string, string>>>();

        public IpcMetadata Metadata { get; set; }
        public List<IpcField> Columns { get; set; }
        public List<IpcField> Tags { get; set; }
        public Schema Schema { get; set; }
        public PIDataMode mode { get; set; }

        public readonly string stableName;

        public MessageBuilder(PIDataMode mode, string stableName, StreamType stream, AckType ackType)
        {
            this.mode = mode;
            this.stableName = stableName;
            Metadata = new IpcMetadata(stream, ackType);
            Columns = new List<IpcField>();
            Tags = new List<IpcField>();
            Schema = null;
        }

        public void GenerateMetadata(string name, List<IpcField> columns, List<IpcField> tags)
        {
            var init = new LushMessageInit
            {
                Name = name,
                Columns = columns.Select(f => new LushField
                {
                    Name = f.Name,
                    Type = f.IpcDataType ?? FromArrowDataType(f.ArrowDataType),
                }).ToList(),
                Tags = tags.Select(f => new LushField
                {
                    Name = f.Name,
                    Type = f.IpcDataType ?? FromArrowDataType(f.ArrowDataType),
                }).ToList(),
            };

            Columns = columns;
            Tags = tags;
            Metadata.Init = init;
        }

        private static IpcDataType FromArrowDataType(IArrowType arrowDataType)
        {
            switch (arrowDataType.TypeId)
            {
                case ArrowTypeId.Boolean:
                    return IpcDataTypes.BoolType;
                case ArrowTypeId.Int8:
                    return IpcDataTypes.Int8Type;
                case ArrowTypeId.Int16:
                    return IpcDataTypes.Int16Type;
                case ArrowTypeId.Int32:
                    return IpcDataTypes.Int32Type;
                case ArrowTypeId.Int64:
                    return IpcDataTypes.Int64Type;
                case ArrowTypeId.UInt8:
                    return IpcDataTypes.UInt8Type;
                case ArrowTypeId.UInt16:
                    return IpcDataTypes.UInt16Type;
                case ArrowTypeId.UInt32:
                    return IpcDataTypes.UInt32Type;
                case ArrowTypeId.UInt64:
                    return IpcDataTypes.UInt64Type;
                case ArrowTypeId.Float:
                case ArrowTypeId.HalfFloat:
                    return IpcDataTypes.Float32Type;
                case ArrowTypeId.Timestamp:
                    return IpcDataTypes.TimestampType;
                case ArrowTypeId.Binary:
                case ArrowTypeId.FixedSizedBinary:
                    return IpcDataTypes.VarCharType;
                default:
                    throw new Exception($"Arrow data type {arrowDataType} is not supported");
            }
        }

        public RecordBatch BuildInsertMessage()
        {
            var recordCounts = tableUniqKeyArrowArray.Length;
            IEnumerable<IArrowArray> arrays = CreateArrays(this, MessageType.Insert, recordCounts);
            var batch = new RecordBatch(
                Schema,
                arrays,
                1
                );
            return batch;
        }

        public RecordBatch BuildTablesMessage()
        {
            int length = 0;
            if (mode == PIDataMode.PointMode)
            {
                length = tagVals.Count;
            }
            else
            {
                length = tagVals.Count;
            }


            IEnumerable<IArrowArray> arrays = CreateArrays(this, MessageType.Children, length);
            var batch = new RecordBatch(
                Schema,
                arrays,
                1
                );
            return batch;
        }
        private IEnumerable<IArrowArray> CreateArrays(MessageBuilder builder, MessageType msgType, int recordCounts)
        {
            var schema = builder.Schema;
            const int fieldCount = 4;
            List<IArrowArray> arrays = new List<IArrowArray>(fieldCount);
            Field typeField = schema.GetFieldByName(TaosxConstants.TYPE);
            arrays.Add(CreateTypeArray(typeField, msgType));
            Field tablesField = schema.GetFieldByName(TaosxConstants.TABLES);
            arrays.Add(CreateTablesArray(builder, tablesField, msgType, recordCounts));
            //Field attrsField = schema.GetFieldByName(TaosxConstants.ATTRS);
            //arrays.Add(CreateAttrsArray(builder, attrsField, msgType));
            Field recordsField = schema.GetFieldByName(TaosxConstants.RECORDS);
            arrays.Add(CreateRecordsArray(builder, recordsField, msgType, recordCounts));

            return arrays;
        }

        // type just needs one piece of data
        private IArrowArray CreateTypeArray(Field typeField, MessageType msgType)
        {
            var creator = new TypeArrayCreator(1, msgType);
            typeField.DataType.Accept(creator);
            return creator.Array;
        }

        private IArrowArray CreateRecordsArray(MessageBuilder builder, Field recordsField, MessageType msgType, int recordCounts)
        {
            var creator = new ListRecordsArrayCreator(builder, recordCounts, msgType);
            recordsField.DataType.Accept(creator);
            return creator.Array;
        }

        private IArrowArray CreateTablesArray(MessageBuilder builder, Field tablesField, MessageType msgType, int recordCounts)
        {
            var creator = new ListTablesArrayCreator(builder, recordCounts, msgType);
            tablesField.DataType.Accept(creator);
            return creator.Array;
        }

        // attrs just needs one piece of data
        private IArrowArray CreateAttrsArray(MessageBuilder builder, Field attrsField, MessageType msgType)
        {
            var creator = new AttrsArrayCreator(builder, 1, msgType);
            attrsField.DataType.Accept(creator);
            return creator.Array;
        }

        public void initSchema()
        {
            log.Info($"Stable:{stableName},Init schema");
            // 用于生成 Metadata.Init 中的数据列
            var colIpcField = new List<IpcField>();
            // lush 消息 __records__ 列的各字段
            var colField = new List<Field>();
            foreach (var column in columnNameTypes)
            {
                colIpcField.Add(new IpcField(column.Key, true, TDTypeV1Converter.ToArrowType(column.Value), TDTypeV1Converter.ToIpcType(column.Value)));
                if (column.Key == "ts")
                {
                    colField.Add(new Field(column.Key, TimestampType.Default, true));
                }
                else {
                    colField.Add(new Field(column.Key, TDTypeV1Converter.ToArrowType(column.Value), true));
                }
            }
            // 用于生成 Metadata.Init 中的标签列
            var ipcTagField = new List<IpcField>();
            // lush 消息 __tables__ 列的各字段
            var tagField = new List<Field>();
         
            foreach (var tag in tagNames)
            {
                ipcTagField.Add(new IpcField(tag.Key.ToLower(), true, StringType.Default, tag.Value));
                tagField.Add(new Field(tag.Key.ToLower(), StringType.Default, true));
            }
            GenerateMetadata(stableName, colIpcField, ipcTagField);

            tagStruct = new StructType(tagField);
            Field tagStructField = new Field("item", tagStruct, true);
            recordType = new StructType(colField);
            Field recordtructField = new Field("item", recordType, true);

            //var arrType = new StructType(new List<Field>{
            //    new Field("table", BinaryType.Default, true),
            //    new Field("using", BinaryType.Default, true),
            //    new Field("tags", tagStruct, true)
            //    }
            //);

            Schema = new Schema.Builder()
                .Field(f => f.Name(TaosxConstants.TYPE)
                    .DataType(UInt8Type.Default)
                    .Nullable(false))
                .Field(f => f.Name(TaosxConstants.TABLES)
                    .DataType(new ListType(tagStructField))
                    .Nullable(true))
                //.Field(f => f.Name(TaosxConstants.ATTRS)
                //    .DataType(arrType)
                //    .Nullable(true))
                .Field(f => f.Name(TaosxConstants.RECORDS)
                    .DataType(new ListType(recordtructField))
                    .Nullable(false))
                .Metadata(Metadata.ToDictionary())
                .Build();
        }

        public StructType subTableType()
        {
            var table_fields = new List<Field>();   // self.table_fields();
            table_fields.Add(new Field("__name__", BinaryType.Default, true));
            table_fields.AddRange(tagFileds());
            return new StructType(table_fields);
        }

        public List<Field> tagFileds()
        {
            var tagFields = new List<Field>();
            foreach (var tag in tagNames)
            {
                tagFields.Add(new Field(tag.Key, BinaryType.Default, true));
            }
            return tagFields;
        }

    }
}
