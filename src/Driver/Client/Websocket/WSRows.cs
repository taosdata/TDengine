using System;
using System.Collections.Generic;
using System.Text;
using TDengine.Driver.Impl.WebSocketMethods;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.Driver.Client.Websocket
{
    public class WSRows : AbstractRows, IRows
    {
        private readonly Connection _connection;
        private readonly ulong _resultId;
        private bool _freed;
        private byte[] _block;
        private readonly Encoding _encoding;

        public WSRows(int affectedRows) : base(affectedRows)
        {
        }

        public WSRows(ulong resultId, IWSMetaResp result, Connection connection, TimeZoneInfo tz)
            : base(55, result.FieldsCount, ParseMetas(result), result.FieldsTypes, result.FieldsScales, tz,
                result.Precision)
        {
            _connection = connection;
            _resultId = resultId;
            _encoding = Encoding.UTF8;
        }

        private static List<TDengineMeta> ParseMetas(IWSMetaResp result)
        {
            var metaList = new List<TDengineMeta>(result.FieldsCount);

            for (int i = 0; i < result.FieldsCount; i++)
            {
                metaList.Add(new TDengineMeta
                {
                    name = result.FieldsNames[i],
                    type = result.FieldsTypes[i],
                    size = (int)result.FieldsLengths[i],
                    precision = (result.FieldsPrecisions != null && i < result.FieldsPrecisions.Length)
                        ? result.FieldsPrecisions[i]
                        : (byte)0,
                    scale = (result.FieldsScales != null && i < result.FieldsScales.Length)
                        ? result.FieldsScales[i]
                        : (byte)0
                });
            }

            return metaList;
        }

        public override void Dispose()
        {
            if (_freed) return;
            _freed = true;
            _block = null;

            if (_connection == null || !_connection.IsAvailable()) return;
            try
            {
                _connection.FreeResult(_resultId);
            }
            catch (Exception)
            {
                // ignored
            }
        }

        protected override bool HasBlockData() => _block != null;

        protected override void FetchBlock()
        {
            try
            {
                var fetchRawBlockResult = _connection.FetchRawBlockBinary(_resultId);
                //Flag           uint64 //8               0
                //Action         uint64 //8               8
                //Version        uint16 //2               16
                //Time           uint64 //8               18
                //ReqID          uint64 //8               26
                //Code           uint32 //4               34
                //MessageLen     uint32 //4               38
                //Message        string //MessageLen      42
                //ResultID       uint64 //8               42 + MessageLen
                //Finished       bool   //1               50 + MessageLen
                //RawBlockLength uint32 //4               51 + MessageLen
                //RawBlock       []byte //RawBlockLength  55 + MessageLen + RawBlockLength
                var version = BitConverter.ToUInt16(fetchRawBlockResult, 16);
                if (version != 1)
                    throw new Exception("Unsupported fetch raw block version " + version);

                var code = BitConverter.ToUInt32(fetchRawBlockResult, 34);
                var messageLen = BitConverter.ToUInt32(fetchRawBlockResult, 38);
                var message = _encoding.GetString(fetchRawBlockResult, 42, (int)messageLen);

                if (code != 0)
                    throw new TDengineError((int)code, message);

                Completed = BitConverter.ToBoolean(fetchRawBlockResult, 50 + (int)messageLen);
                if (Completed)
                    return;

                var rawBlockLength = BitConverter.ToUInt32(fetchRawBlockResult, 51 + (int)messageLen);
                if (fetchRawBlockResult.Length != 55 + (int)messageLen + rawBlockLength)
                    throw new Exception("Invalid fetch raw block result length");

                _block = fetchRawBlockResult;
                BlockReader.SetBlock(_block);
                BlockSize = BlockReader.GetRows();
                CurrentRow = 0;
            }
            catch
            {
                _block = null;
                Completed = true;
                throw;
            }
        }
    }
}