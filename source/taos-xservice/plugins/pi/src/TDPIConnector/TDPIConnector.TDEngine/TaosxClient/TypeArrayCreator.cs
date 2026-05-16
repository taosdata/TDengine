using Apache.Arrow;
using Apache.Arrow.Types;
using System;
using Apache.Arrow.Memory;
using System.Collections.Generic;
using System.Linq;

namespace TDPIConnector.TDEngine.TaosxClient
{
    public class TypeArrayCreator :
        IArrowTypeVisitor<UInt8Type>
    {
        private MessageType msgType;
        public TypeArrayCreator(int recordCounts, MessageType msgType)
        {
            this.msgType = msgType;
        }

        public IArrowArray Array { get; private set; }

        public void Visit(UInt8Type type) {
            byte xx = (byte)msgType;
            Array = new UInt8Array.Builder().Append(xx).Build();
        }

        public void Visit(IArrowType type)
        {
            throw new NotImplementedException();
        }
    }

    public class StringArrayCreator :
        IArrowTypeVisitor<StringType>
    {
        private string[] values;
        public StringArrayCreator(string[] values)
        {
            this.values = values;
        }

        public IArrowArray Array { get; private set; }

        public void Visit(StringType type)
        {
            Array = new StringArray.Builder().AppendRange(values).Build();
        }

        public void Visit(IArrowType type)
        {
            throw new NotImplementedException();
        }
    }

    public class ListStringArrayCreator :
        IArrowTypeVisitor<ListType>
    {
        private string[] values;
        public ListStringArrayCreator(string[] values)
        {
            this.values = values;
        }

        public IArrowArray Array { get; private set; }

        public void Visit(ListType type)
        {
            var creator = new StringArrayCreator(values);
            type.ValueDataType.Accept(creator);

            ArrowBuffer offsetsBuffer = new ArrowBuffer.Builder<int>()
                           .Append(0).Append(values.Length).Build();

            Array = new ListArray(type, 1, offsetsBuffer, creator.Array, ArrowBuffer.Empty);
        }

        public void Visit(IArrowType type)
        {
            throw new NotImplementedException();
        }
    }
    public class RecordsArrayCreator :
    IArrowTypeVisitor<StructType>
    {
        public MessageBuilder messageBuilder { get; set; }
        private MessageType msgType;
        private int length { get; }

        public RecordsArrayCreator(MessageBuilder builder, int recordCounts, MessageType msgType)
        {
            messageBuilder = builder;
            this.length = recordCounts;
            this.msgType = msgType;
        }

        public IArrowArray Array { get; private set; }

        public void Visit(StructType type)
        {
            if (msgType == MessageType.Insert)
            {
                ArrowBuffer.BitmapBuilder nullBitmap = new ArrowBuffer.BitmapBuilder();
            
                Dictionary<string, IArrowArray> fieldArrays = new Dictionary<string, IArrowArray>();
                fieldArrays.Add("ts", messageBuilder.tsArrowArray.Build());
                if (messageBuilder.mode == PIDataMode.PointMode)
                {
                    fieldArrays.Add(TaosxConstants.POINTNAME, messageBuilder.tableUniqKeyArrowArray.Build());
                }
                else {
                    fieldArrays.Add(TaosxConstants.ELEMENTID, messageBuilder.tableUniqKeyArrowArray.Build());
                }

                foreach (var valarray in messageBuilder.valArrowArrayList)
                {
                    fieldArrays.Add(valarray.Key, valarray.Value.Build());
                }
                foreach (var statusarray in messageBuilder.statusArrowArrayList)
                {
                    fieldArrays.Add(statusarray.Key, statusarray.Value.Build());
                }
                List<IArrowArray> arrays = new List<IArrowArray>(fieldArrays.Count);
                foreach (var field in messageBuilder.recordType.Fields)
                {
                    arrays.Add(fieldArrays[field.Name]);
                }

                for (int i = 0; i < length; i++)
                {
                    nullBitmap.Append(true);
                }
                Array = new StructArray(type, length, arrays, nullBitmap.Build());
            }
            else
            {
                var creator = new BlankArrayCreator(length);
                type.Accept(creator);
                Array = creator.Array;
            }
        }

        public void Visit(IArrowType type)
        {
            throw new NotImplementedException();
        }
    }
    public class ListRecordsArrayCreator :
    IArrowTypeVisitor<ListType>
    {
        public MessageBuilder messageBuilder { get; set; }
        private MessageType msgType;
        private int length { get; }

        public ListRecordsArrayCreator(MessageBuilder builder, int recordCounts, MessageType msgType)
        {
            messageBuilder = builder;
            this.length = recordCounts;
            this.msgType = msgType;
        }
        public IArrowArray Array { get; private set; }

        public void Visit(ListType type)
        {
            var creator = new RecordsArrayCreator(messageBuilder, length, msgType);
            type.ValueDataType.Accept(creator);
            ArrowBuffer offsetsBuffer = new ArrowBuffer.Builder<int>()
                           .Append(0).Append(length).Build();

            Array = new ListArray(type, 1, offsetsBuffer, creator.Array, ArrowBuffer.Empty);
        }

        public void Visit(IArrowType type)
        {
            throw new NotImplementedException();
        }
    }

    public class ListTablesArrayCreator :
        IArrowTypeVisitor<ListType>
    {
        public MessageBuilder messageBuilder { get; set; }
        private MessageType msgType;
        private int length { get; }

        public ListTablesArrayCreator(MessageBuilder builder, int tableCounts, MessageType msgType)
        {
            messageBuilder = builder;
            length = tableCounts;
            this.msgType = msgType;
        }
        public IArrowArray Array { get; private set; }

        public void Visit(ListType type)
        {
            var creator = new TableArrayCreator(messageBuilder, length, msgType);
            type.ValueDataType.Accept(creator);

            ArrowBuffer offsetsBuffer = new ArrowBuffer.Builder<int>()
                           .Append(0).Append(length).Build();

            Array = new ListArray(type, 1, offsetsBuffer, creator.Array, ArrowBuffer.Empty);
            return;
        }

        public void Visit(IArrowType type)
        {
            throw new NotImplementedException();
        }
    }

    public class TableArrayCreator :
    IArrowTypeVisitor<StructType>
    {
        public MessageBuilder messageBuilder { get; set; }
        private MessageType msgType;
        private int length { get; }

        public TableArrayCreator(MessageBuilder builder, int recordCounts, MessageType msgType)
        {
            messageBuilder = builder;
            this.length = recordCounts;
            this.msgType = msgType;
        }

        public IArrowArray Array { get; private set; }

        public void Visit(StructType type)
        {
            if (msgType == MessageType.Children && messageBuilder.tagVals.Count > 0)
            {
                ArrowBuffer.BitmapBuilder nullBitmap = new ArrowBuffer.BitmapBuilder();

                int length = messageBuilder.tagVals.Count;
                    int tagNum = messageBuilder.tagStruct.Fields.Count;

                    var arrays = new StringArray.Builder[tagNum];
                    for (int i = 0; i < tagNum; i ++) {
                        arrays[i] = new StringArray.Builder();
                    }
                    if (tagNum > 0) {
                        foreach (var tb in messageBuilder.tagVals)
                        {
                        Dictionary<string, string> unsortTag = new Dictionary<string, string>();
                        foreach (var tag in tb.Value)
                        {
                            if (!unsortTag.ContainsKey(tag.Key))
                            {
                                unsortTag.Add(tag.Key, tag.Value);
                            }
                        }

                        int i = 0;
                            foreach (var tagFiled in messageBuilder.tagStruct.Fields) {
                                if (unsortTag.ContainsKey(tagFiled.Name))
                                {
                                    arrays[i].Append(unsortTag[tagFiled.Name]);
                                }
                                else {
                                    arrays[i].Append(null);
                                }
                                i++;
                            }
                        }

                        for (int i = 0; i < length; i++)
                        {
                            nullBitmap.Append(true);
                        }
                        Array = new StructArray(type, length, arrays.Select(array => array.Build()), nullBitmap.Build());
                        return;
                }
            }
            var creator = new BlankArrayCreator(length);
            type.Accept(creator);
            Array = creator.Array;
        }
        public void Visit(IArrowType type)
        {
            throw new NotImplementedException();
        }
    }
    public class AttrsArrayCreator :
    IArrowTypeVisitor<StructType>
    {
        public MessageBuilder messageBuilder { get; set; }
        private MessageType msgType;
        private int length { get; }

        public AttrsArrayCreator(MessageBuilder builder, int recordCounts, MessageType msgType)
        {
            messageBuilder = builder;
            length = recordCounts;
            this.msgType = msgType;
        }

        public IArrowArray Array { get; private set; }

        public void Visit(StructType type)
        {
            if (true) // msgType == MessageType.Insert
            {
                ArrowBuffer.BitmapBuilder nullBitmap = new ArrowBuffer.BitmapBuilder();
                nullBitmap.Append(true);
                List<IArrowArray> arrays = new List<IArrowArray>(3);

                arrays.Add(new BinaryArray.Builder().Append(new byte[0].AsEnumerable()).Build());
                arrays.Add(new BinaryArray.Builder().Append(System.Text.Encoding.UTF8.GetBytes(messageBuilder.stableName).AsEnumerable()).Build());
                var creator = new BlankArrayCreator(length);
                messageBuilder.tagStruct.Accept(creator);
                arrays.Add(creator.Array);
                Array = new StructArray(type, length, arrays, nullBitmap.Build());
            }
            else
            {
                ArrowBuffer.BitmapBuilder nullBitmap = new ArrowBuffer.BitmapBuilder();
                var memoryAllocator = new NativeMemoryAllocator(alignment: 64);
                var recordBuild = new RecordBatch.Builder(memoryAllocator);
                for (int i = 0; i < length; i++)
                {
                    recordBuild.Append(null);
                    nullBitmap.Append(false);
                }

                Array = new StructArray(type, length, recordBuild.Build().Arrays, nullBitmap.Build());
            }
        }
        public void Visit(IArrowType type)
        {
            throw new NotImplementedException();
        }
    }
    public class TagsArrayCreator :
    IArrowTypeVisitor<StructType>
    {
        public MessageBuilder messageBuilder { get; set; }

        public TagsArrayCreator(MessageBuilder builder, int recordCounts, MessageType msgType)
        {
            messageBuilder = builder;
        }

        public IArrowArray Array { get; private set; }

        public void Visit(StructType type)
        {
        }
        public void Visit(IArrowType type)
        {
            throw new NotImplementedException();
        }
    }
    public class BlankArrayCreator :
           IArrowTypeVisitor<BooleanType>,
           IArrowTypeVisitor<Date32Type>,
           IArrowTypeVisitor<Date64Type>,
           IArrowTypeVisitor<Time32Type>,
           IArrowTypeVisitor<Time64Type>,
           IArrowTypeVisitor<Int8Type>,
           IArrowTypeVisitor<Int16Type>,
           IArrowTypeVisitor<Int32Type>,
           IArrowTypeVisitor<Int64Type>,
           IArrowTypeVisitor<UInt8Type>,
           IArrowTypeVisitor<UInt16Type>,
           IArrowTypeVisitor<UInt32Type>,
           IArrowTypeVisitor<UInt64Type>,
           IArrowTypeVisitor<FloatType>,
           IArrowTypeVisitor<DoubleType>,
           IArrowTypeVisitor<TimestampType>,
           IArrowTypeVisitor<StringType>,
           IArrowTypeVisitor<ListType>,
           IArrowTypeVisitor<StructType>,
           IArrowTypeVisitor<Decimal128Type>,
           IArrowTypeVisitor<Decimal256Type>,
           IArrowTypeVisitor<DictionaryType>,
           IArrowTypeVisitor<FixedSizeBinaryType>
    {
        private int Length { get; }
        public IArrowArray Array { get; private set; }

        public BlankArrayCreator(int length)
        {
            Length = length;
        }

        public void Visit(BooleanType type) => GenerateArray(new BooleanArray.Builder(), x => false);
        public void Visit(Int8Type type) => GenerateArray(new Int8Array.Builder(), x => (sbyte)x);
        public void Visit(Int16Type type) => GenerateArray(new Int16Array.Builder(), x => (short)x);
        public void Visit(Int32Type type) => GenerateArray(new Int32Array.Builder(), x => x);
        public void Visit(Int64Type type) => GenerateArray(new Int64Array.Builder(), x => x);
        public void Visit(UInt8Type type) => GenerateArray(new UInt8Array.Builder(), x => (byte)x);
        public void Visit(UInt16Type type) => GenerateArray(new UInt16Array.Builder(), x => (ushort)x);
        public void Visit(UInt32Type type) => GenerateArray(new UInt32Array.Builder(), x => (uint)x);
        public void Visit(UInt64Type type) => GenerateArray(new UInt64Array.Builder(), x => (ulong)x);
        public void Visit(FloatType type) => GenerateArray(new FloatArray.Builder(), x => ((float)x / Length));
        public void Visit(DoubleType type) => GenerateArray(new DoubleArray.Builder(), x => ((double)x / Length));
        public void Visit(Decimal128Type type)
        {
            var builder = new Decimal128Array.Builder(type).Reserve(Length);

            for (var i = 0; i < Length; i++)
            {
                builder.AppendNull();
            }

            Array = builder.Build();
        }

        public void Visit(Decimal256Type type)
        {
            var builder = new Decimal256Array.Builder(type).Reserve(Length);

            for (var i = 0; i < Length; i++)
            {
                builder.AppendNull();
            }

            Array = builder.Build();
        }

        public void Visit(Date32Type type)
        {
            var builder = new Date32Array.Builder().Reserve(Length);

            for (var i = 0; i < Length; i++)
            {
                builder.AppendNull();
            }

            Array = builder.Build();
        }

        public void Visit(Date64Type type)
        {
            var builder = new Date64Array.Builder().Reserve(Length);

            for (var i = 0; i < Length; i++)
            {
                builder.AppendNull();
            }

            Array = builder.Build();
        }

        public void Visit(Time32Type type)
        {
            var builder = new Time32Array.Builder(type).Reserve(Length);

            for (var i = 0; i < Length; i++)
            {
                builder.AppendNull();
            }

            Array = builder.Build();
        }

        public void Visit(Time64Type type)
        {
            var builder = new Time64Array.Builder(type).Reserve(Length);

            for (var i = 0; i < Length; i++)
            {
                builder.AppendNull();
            }

            Array = builder.Build();
        }

        public void Visit(TimestampType type)
        {
            var builder = new TimestampArray.Builder().Reserve(Length);
            for (var i = 0; i < Length; i++)
            {
                builder.AppendNull();
            }

            Array = builder.Build();
        }

        public void Visit(StringType type)
        {
            var builder = new StringArray.Builder();

            for (var i = 0; i < Length; i++)
            {
                builder.AppendNull();
            }

            Array = builder.Build();
        }

        public void Visit(ListType type)
        {
            var builder = new ListArray.Builder(type.ValueField).Reserve(Length);

            var valueBuilder = (Int64Array.Builder)builder.ValueBuilder.Reserve(Length + 1);

            for (var i = 0; i < Length; i++)
            {
                builder.AppendNull();
                valueBuilder.AppendNull();
            }
            //Add a value to check if Values.Length can exceed ListArray.Length
            valueBuilder.Append(0);

            Array = builder.Build();
        }

        public void Visit(StructType type)
        {
            IArrowArray[] childArrays = new IArrowArray[type.Fields.Count];
            for (int i = 0; i < childArrays.Length; i++)
            {
                var creator = new BlankArrayCreator(Length);
                type.Fields[i].DataType.Accept(creator);
                childArrays[i] = creator.Array;
            }

            ArrowBuffer.BitmapBuilder nullBitmap = new ArrowBuffer.BitmapBuilder();
            for (int i = 0; i < Length; i++)
            {
                nullBitmap.Append(false);
            }

            Array = new StructArray(type, Length, childArrays, nullBitmap.Build());
        }

        public void Visit(DictionaryType type)
        {
            Int32Array.Builder indicesBuilder = new Int32Array.Builder().Reserve(Length);
            StringArray.Builder valueBuilder = new StringArray.Builder().Reserve(Length);

            for (int i = 0; i < Length; i++)
            {
                indicesBuilder.Append(i);
                valueBuilder.Append($"{i}");
            }

            Array = new DictionaryArray(type, indicesBuilder.Build(), valueBuilder.Build());
        }

        public void Visit(FixedSizeBinaryType type)
        {
            throw new NotImplementedException();
        }

        private void GenerateArray<T, TArray, TArrayBuilder>(IArrowArrayBuilder<T, TArray, TArrayBuilder> builder, Func<int, T> generator)
            where TArrayBuilder : IArrowArrayBuilder<T, TArray, TArrayBuilder>
            where TArray : IArrowArray
            where T : struct
        {
            for (var i = 0; i < Length; i++)
            {
                builder.AppendNull();
            }
            Array = builder.Build(default);
        }

        public void Visit(IArrowType type)
        {
            if (type == BinaryType.Default)
            {
                BinaryArray.Builder builder = new BinaryArray.Builder().Reserve(Length);
                for (var i = 0; i < Length; i++)
                {
                    builder.AppendNull();
                }
                Array = builder.Build(default);
                return;
            }
        }
    }
}
