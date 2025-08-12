using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using TDengine.Driver;
using TDengine.Driver.Impl.NativeMethods;

namespace TDengine.TMQ.Native
{
    public class TMQNativeRows : AbstractRows, ITMQRows
    {
        private readonly IntPtr _result;

        public TMQNativeRows(IntPtr result, TimeZoneInfo tz) : base(0,0,tz)
        {
            _result = result;
        }

        protected override void FetchBlock()
        {
            if (Block == null)
            {
                int structSize = Marshal.SizeOf(typeof(TMQRawData));
                IntPtr raw = Marshal.AllocHGlobal(structSize);
                try
                {
                    var code = NativeMethods.TmqGetRaw(_result, raw);
                    if (code != 0)
                    {
                        throw new TDengineError(code, NativeMethods.Error(_result));
                    }

                    TMQRawData rawData = (TMQRawData)Marshal.PtrToStructure(raw, typeof(TMQRawData));
                    Block = new byte[rawData.rawLen];
                    Marshal.Copy(rawData.raw, Block, 0, (int)rawData.rawLen);
                    BlockInfo = TmqBlockReader.Parse(Block);
                    BlockIndex = 0;
                }
                finally
                {
                    Marshal.FreeHGlobal(raw);
                }
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