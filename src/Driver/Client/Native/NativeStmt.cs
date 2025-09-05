using System;
using System.Runtime.InteropServices;
using TDengine.Driver.Impl.NativeMethods;

namespace TDengine.Driver.Client.Native
{
    public class NativeStmt : AbstractStmt
    {
        private IntPtr _stmt;
        private readonly TimeZoneInfo _tz;

        public NativeStmt(IntPtr stmt, TimeZoneInfo tz): base(0)
        {
            _stmt = stmt;
            _tz = tz;
        }

        protected override void PrepareInternal(string query, out bool isInsert, out int count, out TaosFieldAll[] fields)
        {
            var code = NativeMethods.TaosStmt2Prepare(_stmt, query);
            StmtCheckError(code);
            code = NativeMethods.TaosStmt2IsInsert(_stmt, out isInsert);
            StmtCheckError(code);
            code = NativeMethods.TaosStmt2GetFields(_stmt, out count, out fields);
            StmtCheckError(code);
        }

        private void StmtCheckError(int code)
        {
            if (code == 0) return;
            var errorStr = NativeMethods.TaosStmt2Error(_stmt);
            throw new TDengineError(code, errorStr);
        }

        protected override void BindBinaryInternal(byte[] data, out int affectedRows)
        {
            Stmt2BindBinary(data);
            NativeMethods.TaosStmt2Exec(_stmt,out affectedRows);
        }

        private const int Stmt2BindBufferTypeOffset = 0;
        private const int Stmt2BindBufferOffset = 8;
        private const int Stmt2BindLengthOffset = 16;
        private const int Stmt2BindIsNullOffset = 24;
        private const int Stmt2BindNumOffset = 32;
        private static void GenerateStmt2Binds(IntPtr data, uint tableCount, uint fieldCount, uint fieldOffset,
            IntPtr bindStruct, IntPtr bindPtrArray)
        {
            IntPtr baseLength = IntPtr.Add(data, (int)fieldOffset);

            IntPtr dataPtr = IntPtr.Add(baseLength, (int)(tableCount * TDengineConstant.UInt32Size));

            for (int tableIndex = 0; tableIndex < tableCount; tableIndex++)
            {
                // first struct of each table
                var currentTableStartBindStructPtr = IntPtr.Add(bindStruct,
                    tableIndex * (int)fieldCount * TDengineConstant.TaosStmt2BindSize);
                // write the struct pointer to the bindPtrArray
                Marshal.WriteIntPtr(IntPtr.Add(bindPtrArray, tableIndex * IntPtr.Size), currentTableStartBindStructPtr);
                
                for (uint fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++)
                {
                    IntPtr bindDataPtr = dataPtr;
                    IntPtr bindPtr = IntPtr.Add(currentTableStartBindStructPtr, (int)fieldIndex * TDengineConstant.TaosStmt2BindSize);

                    // total length
                    var bindDataTotalLength = (uint)Marshal.ReadInt32(bindDataPtr);
                    bindDataPtr = IntPtr.Add(bindDataPtr, 4);

                    // buffer_type
                    var bufferType = Marshal.ReadInt32(bindDataPtr);
                    bindDataPtr = IntPtr.Add(bindDataPtr, 4);

                    // num
                    var num = Marshal.ReadInt32(bindDataPtr);
                    bindDataPtr = IntPtr.Add(bindDataPtr, 4);

                    // is_null
                    var isNull = bindDataPtr;
                    bindDataPtr = IntPtr.Add(bindDataPtr, num);

                    // have_length
                    var haveLength = Marshal.ReadByte(bindDataPtr);
                    bindDataPtr = IntPtr.Add(bindDataPtr, 1);

                    IntPtr length;
                    if (haveLength == 0)
                    {
                        length = IntPtr.Zero;
                    }
                    else
                    {
                        length = bindDataPtr;
                        bindDataPtr = IntPtr.Add(bindDataPtr, num * 4);
                    }

                    // buffer_length
                    var bufferLength = Marshal.ReadInt32(bindDataPtr);
                    bindDataPtr = IntPtr.Add(bindDataPtr, 4);

                    // buffer
                    IntPtr buffer;
                    if (bufferLength > 0)
                    {
                        buffer = bindDataPtr;
                        bindDataPtr = IntPtr.Add(bindDataPtr, bufferLength);
                    }
                    else
                    {
                        buffer = IntPtr.Zero;
                    }

                    // check bind data length
                    if (bindDataPtr.ToInt64() - dataPtr.ToInt64() != bindDataTotalLength)
                    {
                        throw new InvalidOperationException(
                            $"Bind data length error, tableIndex: {tableIndex}, fieldIndex: {fieldIndex}");
                    }
                    Marshal.WriteInt32(bindPtr,Stmt2BindBufferTypeOffset, bufferType);
                    Marshal.WriteIntPtr(bindPtr,Stmt2BindBufferOffset, buffer);
                    Marshal.WriteIntPtr(bindPtr,Stmt2BindLengthOffset, length);
                    Marshal.WriteIntPtr(bindPtr,Stmt2BindIsNullOffset, isNull);
                    Marshal.WriteInt32(bindPtr,Stmt2BindNumOffset, num);
                    dataPtr = bindDataPtr;
                }
            }
        }
        
        private void Stmt2BindBinary(byte[] data)
        {
            GCHandle dataHandle = GCHandle.Alloc(data, GCHandleType.Pinned);
            try
            {
                IntPtr dataPtr = dataHandle.AddrOfPinnedObject();

                uint totalLength = (uint)Marshal.ReadInt32(dataPtr);
                uint count = (uint)Marshal.ReadInt32(dataPtr, 4);
                uint tagCount = (uint)Marshal.ReadInt32(dataPtr, 8);
                uint colCount = (uint)Marshal.ReadInt32(dataPtr, 12);
                uint tableNamesOffset = (uint)Marshal.ReadInt32(dataPtr, 16);
                uint tagsOffset = (uint)Marshal.ReadInt32(dataPtr, 20);
                uint colsOffset = (uint)Marshal.ReadInt32(dataPtr, 24);

                // check table names
                if (tableNamesOffset > 0)
                {
                    uint tableNameEnd = tableNamesOffset + count * 2;
                    if (tableNameEnd > totalLength)
                    {
                        throw new ArgumentOutOfRangeException(
                            $"Table name lengths out of range, total length: {totalLength}, tableNamesLengthEnd: {tableNameEnd}");
                    }

                    IntPtr tableNameLengthPtr = IntPtr.Add(dataPtr, (int)tableNamesOffset);
                    // IntPtr tableNameDataPtr = IntPtr.Add(tableNameLengthPtr, (int)(count * 2));

                    for (int i = 0; i < count; ++i)
                    {
                        ushort length = (ushort)Marshal.ReadInt16(IntPtr.Add(tableNameLengthPtr, i * 2));
                        if (length == 0)
                        {
                            throw new ArgumentException($"Table name length is 0, tableIndex: {i}");
                        }

                        tableNameEnd += length;
                    }

                    if (tableNameEnd > totalLength)
                    {
                        throw new ArgumentOutOfRangeException(
                            $"Table names out of range, total length: {totalLength}, tableNameTotalLength: {tableNameEnd}");
                    }
                }

                // check tags
                if (tagsOffset > 0)
                {
                    if (tagCount == 0)
                    {
                        throw new ArgumentException("Tag count is 0, but tags offset is not 0");
                    }

                    uint tagEnd = tagsOffset + count * 4;
                    if (tagEnd > totalLength)
                    {
                        throw new ArgumentOutOfRangeException(
                            $"Tags out of range, total length: {totalLength}, tagEnd: {tagEnd}");
                    }

                    IntPtr tabLengthPtr = IntPtr.Add(dataPtr, (int)tagsOffset);
                    for (int i = 0; i < count; ++i)
                    {
                        uint length = (uint)Marshal.ReadInt32(IntPtr.Add(tabLengthPtr, i * 4));
                        if (length == 0)
                        {
                            throw new ArgumentException($"Tag length is 0, tableIndex: {i}");
                        }

                        tagEnd += length;
                    }

                    if (tagEnd > totalLength)
                    {
                        throw new ArgumentOutOfRangeException(
                            $"Tags out of range, total length: {totalLength}, tagsTotalLength: {tagEnd}");
                    }
                }

                // check cols
                if (colsOffset > 0)
                {
                    if (colCount == 0)
                    {
                        throw new ArgumentException("Col count is 0, but cols offset is not 0");
                    }

                    uint colEnd = colsOffset + count * 4;
                    if (colEnd > totalLength)
                    {
                        throw new ArgumentOutOfRangeException(
                            $"Cols out of range, total length: {totalLength}, colEnd: {colEnd}");
                    }

                    IntPtr colLengthPtr = IntPtr.Add(dataPtr, (int)colsOffset);
                    for (int i = 0; i < count; ++i)
                    {
                        uint length = (uint)Marshal.ReadInt32(IntPtr.Add(colLengthPtr, i * 4));
                        if (length == 0)
                        {
                            throw new ArgumentException($"Col length is 0, tableIndex: {i}");
                        }

                        colEnd += length;
                    }

                    if (colEnd > totalLength)
                    {
                        throw new ArgumentOutOfRangeException(
                            $"Cols out of range, total length: {totalLength}, colsTotalLength: {colEnd}");
                    }
                }

                // generate bindv struct
                IntPtr tbnamesPtr = IntPtr.Zero;
                IntPtr bindStruct = IntPtr.Zero;
                IntPtr bindPtr = IntPtr.Zero;
                try
                {
                    TAOS_STMT2_BINDV bindV = new TAOS_STMT2_BINDV
                    {
                        count = (int)count
                    };

                    if (tableNamesOffset > 0)
                    {
                        tbnamesPtr = Marshal.AllocHGlobal((int)count * IntPtr.Size);
                        IntPtr tableNameLengthPtr = IntPtr.Add(dataPtr, (int)tableNamesOffset);
                        IntPtr tableNameDataPtr = IntPtr.Add(tableNameLengthPtr, (int)(count * 2));
                        
                        for (int i = 0; i < count; i++)
                        {
                            IntPtr currentPos = IntPtr.Add(tbnamesPtr, i * IntPtr.Size);
                            Marshal.WriteIntPtr(currentPos, tableNameDataPtr);
                            ushort length = (ushort)Marshal.ReadInt16(IntPtr.Add(tableNameLengthPtr, i * 2));
                            tableNameDataPtr = IntPtr.Add(tableNameDataPtr, length);
                        }
                        
                        bindV.tbnames = tbnamesPtr;
                    }
                    else
                    {
                        bindV.tbnames = IntPtr.Zero;
                    }

                    uint bindStructCount = 0;
                    uint bindPtrCount = 0;

                    if (tagsOffset == 0)
                    {
                        bindV.tags = IntPtr.Zero;
                    }
                    else
                    {
                        bindStructCount += count * tagCount;
                        bindPtrCount += count;
                    }

                    if (colsOffset == 0)
                    {
                        bindV.bind_cols = IntPtr.Zero;
                    }
                    else
                    {
                        bindStructCount += count * colCount;
                        bindPtrCount += count;
                    }


                    if (bindStructCount == 0)
                    {
                        bindV.tags = IntPtr.Zero;
                        bindV.bind_cols = IntPtr.Zero;
                    }
                    else
                    {
                        // Allocate bind struct array
                        bindStruct = Marshal.AllocHGlobal((int)bindStructCount * TDengineConstant.TaosStmt2BindSize);

                        // Allocate bind pointer array
                        bindPtr = Marshal.AllocHGlobal((int)bindPtrCount * IntPtr.Size);

                        uint structIndex = 0;
                        uint ptrIndex = 0;

                        if (tagsOffset > 0)
                        {
                            GenerateStmt2Binds(dataPtr, count, tagCount, tagsOffset,
                                bindStruct, bindPtr);
                            bindV.tags = bindPtr;
                            structIndex += count * tagCount;
                            ptrIndex += count;
                        }

                        if (colsOffset > 0)
                        {
                            IntPtr colBindStruct = IntPtr.Add(bindStruct,
                                (int)structIndex * TDengineConstant.TaosStmt2BindSize);
                            IntPtr colBindPtr = IntPtr.Add(bindPtr, (int)ptrIndex * IntPtr.Size);
                            GenerateStmt2Binds(dataPtr, count, colCount, colsOffset,
                                colBindStruct, colBindPtr);
                            bindV.bind_cols = colBindPtr;
                        }
                    }

                    var code = NativeMethods.TaosStmt2BindParam(_stmt, ref bindV);
                    if (code == 0) return;
                    var msg = NativeMethods.TaosStmt2Error(_stmt);
                    throw new TDengineError(code,msg);
                }
                finally
                {
                    if (bindStruct != IntPtr.Zero)
                    {
                        Marshal.FreeHGlobal(bindStruct);
                    }

                    if (bindPtr != IntPtr.Zero)
                    {
                        Marshal.FreeHGlobal(bindPtr);
                    }

                    if (tbnamesPtr != IntPtr.Zero)
                    {
                        Marshal.FreeHGlobal(tbnamesPtr);
                    }
                }
            }
            finally
            {
                if (dataHandle.IsAllocated)
                {
                    dataHandle.Free();
                }
            }
        }

        protected override IRows QueryResultInternal()
        {
            var result = NativeMethods.TaosStmt2Result(_stmt);
            if (result == IntPtr.Zero)
            {
                throw new InvalidOperationException("stmt is not query");
            }

            return new NativeRows(result, _tz, true);
        }

        protected override IRows InsertResultInternal(int affectedRows)
        {
            return new NativeRows(affectedRows);
        }

        protected override bool IsConnectionAvailable(Exception exception)
        {
            return true;
        }

        protected override void ReconnectInternal()
        {
        }

        protected override bool AutoReconnectInternal()
        {
            return false;
        }

        public override void Dispose()
        {
            if (_stmt == IntPtr.Zero) return;
            NativeMethods.TaosStmt2Close(_stmt);
            _stmt = IntPtr.Zero;
        }
    }
}