using System;
using System.Runtime.InteropServices;
using TDengine.Driver.Impl.NativeMethods;

namespace TDengine.Driver.Client.Native
{
    public class NativeRows : AbstractRows, IRows
    {
        private IntPtr _result;
        private IntPtr _block = IntPtr.Zero;
        private readonly bool _disableFreeResult;

        public NativeRows(int affectedRows) : base(affectedRows)
        {
        }

        public NativeRows(IntPtr result, TimeZoneInfo tz, bool disableFreeResult)
            : base(NativeMethods.FieldCount(result), NativeMethods.FetchFields(result), tz,
                NativeMethods.ResultPrecision(result))
        {
            _disableFreeResult = disableFreeResult;
            _result = result;
        }

        public override void Dispose()
        {
            if (_disableFreeResult)
            {
                return;
            }

            if (_result != IntPtr.Zero)
            {
                NativeMethods.FreeResult(_result);
                _result = IntPtr.Zero;
            }
        }

        protected override bool HasBlockData() => _block != IntPtr.Zero;

        protected override void FetchBlock()
        {
            IntPtr numOfRowsPrt = Marshal.AllocHGlobal(sizeof(Int32));
            IntPtr pDataPtr = Marshal.AllocHGlobal(IntPtr.Size);
            try
            {
                int code = NativeMethods.FetchRawBlock(_result, numOfRowsPrt, pDataPtr);
                if (code != 0)
                {
                    throw new TDengineError(code, NativeMethods.Error(_result));
                }

                int numOfRows = Marshal.ReadInt32(numOfRowsPrt);
                if (numOfRows == 0)
                {
                    Completed = true;
                }
                else if (numOfRows < 0)
                {
                    throw new TDengineError(NativeMethods.ErrorNo(_result), NativeMethods.Error(_result));
                }
                else
                {
                    BlockSize = numOfRows;
                    CurrentRow = 0;
                    var dataPtr = Marshal.ReadIntPtr(pDataPtr);
                    _block = dataPtr;
                    BlockReader.SetBlockPtr(dataPtr, BlockSize);
                }
            }
            catch
            {
                _block = IntPtr.Zero;
                Completed = true;
                throw;
            }
            finally
            {
                Marshal.FreeHGlobal(numOfRowsPrt);
                Marshal.FreeHGlobal(pDataPtr);
            }
        }
    }
}