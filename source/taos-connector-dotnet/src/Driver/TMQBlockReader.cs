using System;

namespace TDengine.Driver
{
    public class TMQBlockReader
    {
        public struct TMQRawDataSchema
        {
            public byte colType;
            public sbyte flag;
            public int bytes;
            public int colId;
            public string name;
        }

        public struct TMQBlockInfo
        {
            public string tableName;
            public int precision;
            public int rawBlockOffset;
            public TMQRawDataSchema[] schemas;
        }

        private readonly int _offset;

        private byte[] _block;
        private int _currentPos;

        public TMQBlockReader(int offset)
        {
            _offset = offset;
        }

        public TMQBlockInfo[] Parse(byte[] block)
        {
            _block = block;
            _currentPos = _offset;
            SkipHeader();
            return ParseBlockInfo();
        }

        private void SkipHeader()
        {
            var v = ParseInt8();
            if (v >= 100)
            {
                var skipCount = ParseInt32();
                Skip(skipCount);
            }
            else
            {
                var skipCount = GetTypeSkip(v);
                Skip(skipCount);
                v = ParseInt8();
                skipCount = GetTypeSkip(v);
                Skip(skipCount);
            }
        }

        private static int GetTypeSkip(sbyte t)
        {
            int skip;
            switch (t)
            {
                case 1:
                    skip = 8;
                    break;
                case 2:
                case 3:
                    skip = 16;
                    break;
                default:
                    throw new Exception("unknown type");
            }

            return skip;
        }

        private void Skip(int count)
        {
            _currentPos += count;
        }

        private byte ParseUint8()
        {
            var v = _block[_currentPos];
            Skip(1);
            return v;
        }

        private sbyte ParseInt8()
        {
            var v = _block[_currentPos];
            Skip(1);
            return (sbyte)v;
        }

        private int ParseInt32()
        {
            var v = BitConverter.ToInt32(_block, _currentPos);
            Skip(4);
            return v;
        }

        private bool ParseBool()
        {
            var v = BitConverter.ToBoolean(_block, _currentPos);
            Skip(1);
            return v;
        }

        private TMQBlockInfo[] ParseBlockInfo()
        {
            var blockNum = ParseInt32();
            var multiBlockInfo = new TMQBlockInfo[blockNum];
            var withTableName = ParseBool();
            var withSchema = ParseBool();
            for (var i = 0; i < blockNum; i++)
            {
                var blockInfo = new TMQBlockInfo();
                var blockTotalLen = ParseVariableByteInteger();
                Skip(17);
                blockInfo.precision = ParseUint8();
                blockInfo.rawBlockOffset = _currentPos;
                Skip(blockTotalLen - 18);
                if (withSchema)
                {
                    var cols = ParseZigzagVariableByteInteger();
                    _ = ParseZigzagVariableByteInteger();
                    blockInfo.schemas = new TMQRawDataSchema[cols];
                    for (var j = 0; j < cols; j++)
                    {
                        var schema = new TMQRawDataSchema();
                        schema.colType = ParseUint8();
                        schema.flag = ParseInt8();
                        schema.bytes = ParseZigzagVariableByteInteger();
                        schema.colId = ParseZigzagVariableByteInteger();
                        schema.name = ParseName();
                        blockInfo.schemas[j] = schema;
                    }
                }

                if (withTableName)
                {
                    blockInfo.tableName = ParseName();
                }

                multiBlockInfo[i] = blockInfo;
            }

            return multiBlockInfo;
        }

        private string ParseName()
        {
            var nameLen = ParseVariableByteInteger();
            var name = System.Text.Encoding.UTF8.GetString(_block, _currentPos, nameLen - 1);
            Skip(nameLen);
            return name;
        }

        private int ParseZigzagVariableByteInteger()
        {
            return ZigzagDecode(ParseVariableByteInteger());
        }

        private int ZigzagDecode(int n)
        {
            return (n >> 1) ^ (-(n & 1));
        }

        private int ParseVariableByteInteger()
        {
            var multiplier = 1;
            var value = 0;
            while (true)
            {
                var encodedByte = ParseUint8();
                value += (encodedByte & 127) * multiplier;
                if ((encodedByte & 128) == 0)
                {
                    break;
                }

                multiplier *= 128;
            }

            return value;
        }
    }
}