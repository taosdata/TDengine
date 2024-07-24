using System.Collections.Generic;
using Apache.Arrow.Types;
using Newtonsoft.Json;
using IpcDataType = System.String;

namespace TDPIConnector.TDEngine.TaosxClient
{
    public class IpcMetadata
    {
        private const string CURRENT_MESSAGE_SCHEMA_VERSION = "1.0";
        // Current version is 1.0.
        public string Version { get; set; }
        // Stream type enumeration, include `line`, `flat`, `lush`.
        public StreamType Stream { get; set; }
        // ACK type enumeration, include `none`, `code`, `lush`.
        public AckType Ack { get; set; }
        public LushMessageInit Init { get; set; }
        public string Preset { get; set; }

        public IpcMetadata(StreamType stream, AckType ackType)
        {
            Version = CURRENT_MESSAGE_SCHEMA_VERSION.ToString();
            Stream = stream;
            Ack = ackType;
            Preset = null;
            Init = null;
        }

        public Dictionary<string, string> ToDictionary()
        {
            var dict = new Dictionary<string, string>
            {
                { "version", Version },
                { "stream", Stream.ToString().ToLower() },
                { "ack", Ack.ToString().ToLower() }
            };

            if (Init != null)
            {
                dict.Add("init", JsonConvert.SerializeObject(Init).ToLower());
            }

            if (Preset != null)
            {
                dict.Add("preset", Preset);
            }

            return dict;
        }
    }
    public class IpcDataTypes
    {
        public const IpcDataType BoolType = "Bool";
        public const IpcDataType UInt8Type = "u8";
        public const IpcDataType UInt16Type = "u16";
        public const IpcDataType UInt32Type = "u32";
        public const IpcDataType UInt64Type = "u64";
        public const IpcDataType Int8Type = "i8";
        public const IpcDataType Int16Type = "i16";
        public const IpcDataType Int32Type = "i32";
        public const IpcDataType Int64Type = "i64";
        public const IpcDataType Float32Type = "f32";
        public const IpcDataType Float64Type = "f64";
        public const IpcDataType TimestampType = "timestamp";
        public const IpcDataType VarCharType = "varchar";
        public const IpcDataType NCharType = "nchar";
        public const IpcDataType JsonType = "json";
    }

    public enum StreamType
    {
        Line,
        Flat,
        Lush
    }
    public enum AckType
    {
        None,
        Code,
        Lush
    }
    public enum MessageType
    {
        Table = 1,
        Children,
        Insert,
        Control,
    }

    public class LushMessageInit
    {
        public string Name { get; set; }
        public List<LushField> Columns { get; set; }
        public List<LushField> Tags { get; set; }
    }

    public class LushField
    {
        public string Name { get; set; }
        public IpcDataType Type { get; set; }
    }

    public class IpcField
    {
        public string Name { get; set; }
        public bool Nullable { get; set; }
        public IArrowType ArrowDataType { get; set; }
        public IpcDataType IpcDataType { get; set; }
        public IpcField(string name, bool nullable, IArrowType arrowType, IpcDataType ipcDataType)
        {
            Name = name;
            Nullable = nullable;
            ArrowDataType = arrowType;
            IpcDataType = ipcDataType;
        }
    }

}
