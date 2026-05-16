using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using ZstdNet;

Schema.Builder builder = new Schema.Builder();
Field structField = new Field("item",
    new StructType(
    new[]{
          new Field("__table_name__", StringType.Default, nullable: false),
          new Field("ts", TimestampType.Default, nullable: true),
          new Field("c1", Int64Type.Default, nullable: true),
          }),
    nullable: true);

Field listField = new Field("__records__", new ListType(structField), nullable: false);
Field typeField = new Field("__type__", new UInt8Type(), false);
builder.Field(typeField);
builder.Field(listField);
builder.Metadata("ack", "none").Metadata("version", "1.0").Metadata("stream", "lush");
Schema schema = builder.Build();

StringArray stringArray = new StringArray.Builder().Append("d1001").Append("d1001").Append("d1001").Append("d1002").Append("d1002").Append("d1003").Build();

DateTimeOffset now = DateTimeOffset.Now;
TimestampArray tsArray = new TimestampArray.Builder().Append(now).Append(now.AddMilliseconds(1)).Append(now.AddMilliseconds(2)).Append(now.AddMilliseconds(3)).Append(now.AddMilliseconds(4)).Append(now.AddMilliseconds(5)).Build();
Int64Array intArray = new Int64Array.Builder().Append(1).Append(2).AppendNull().Append(4).Append(10).Append(55).Build();

ArrowBuffer nullBitmapBuffer = new ArrowBuffer.BitmapBuilder().Append(true).Append(true).Append(false).Append(true).Append(true).Append(true).Build();

StructArray structs = new StructArray(structField.DataType, 6, new IArrowArray[] { stringArray, tsArray, intArray }, nullBitmapBuffer, 1);

ArrowBuffer offsetsBuffer = new ArrowBuffer.Builder<int>().Append(0).Append(2).Append(6).Build();

ListArray listArray = new ListArray(listField.DataType, 2, offsetsBuffer, structs, ArrowBuffer.Empty);
UInt8Array typeArray = new UInt8Array.Builder().Append(3).Append(3).Build(); // Insert type id is 3.

RecordBatch batch = new RecordBatch(schema, new IArrowArray[] { typeArray, listArray }, 2);

FileStream file = File.Create("dotnet.arrow.zstd");

CompressionStream stream = new CompressionStream(file);

ArrowStreamWriter writer = new ArrowStreamWriter(stream, schema);
writer.WriteStart();
writer.WriteRecordBatch(batch);
writer.WriteEnd();
stream.Close();
file.Close();
