using System;
using System.Collections.Generic;
using TDengine.Driver;
using TDengine.Driver.Impl.WebSocketMethods;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.TMQ.WebSocket
{
    public class TMQWSRows : AbstractRows
    {
        private readonly TMQConnection _connection;
        private readonly ulong _resultId;

        public TMQWSRows(WSTMQPollResp result, TMQConnection connection, TimeZoneInfo tz):base(24,38,tz)
        {
            _connection = connection;
            _resultId = result.MessageId;
        }

        protected override void FetchBlock()
        {
            if (Block == null)
            {
                var fetchRawResp = _connection.FetchRawBlock(_resultId);
                Block = fetchRawResp;
                BlockInfo = TmqBlockReader.Parse(Block);
                BlockIndex = 0;
            }
            else
            {
                BlockIndex += 1;
            }

            if (BlockIndex == BlockInfo.Length)
            {
                Completed = true;
                return;
            }

            BlockReader.SetTMQBlock(Block, BlockInfo[BlockIndex].precision, BlockInfo[BlockIndex].rawBlockOffset);
            BlockRows = BlockReader.GetRows();
            CurrentRow = 0;

            FieldCount = BlockInfo[BlockIndex].schemas.Length;
            TableName = BlockInfo[BlockIndex].tableName;
            Metas = new List<TDengineMeta>();
            for (int i = 0; i < FieldCount; i++)
            {
                Metas.Add(new TDengineMeta
                {
                    name = BlockInfo[BlockIndex].schemas[i].name,
                    type = BlockInfo[BlockIndex].schemas[i].colType,
                    size = BlockInfo[BlockIndex].schemas[i].bytes,
                });
            }
        }
    }
}