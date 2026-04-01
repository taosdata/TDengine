using System;

namespace TDengine.Driver
{
    public class TDengineError : Exception
    {
        public int Code { get; }
        public string Error { get; }

        public byte[] ExtendedErrorBytes { get; }
        public string ExtendedErrorString { get; }

        public enum InternalErrorCode
        {
            WS_RECONNECT_FAILED = 0xf001,
            WS_UNEXPECTED_MESSAGE = 0xf002,
            WS_CONNECTION_CLOSED = 0xf003,
            WS_WRITE_TIMEOUT = 0xf004,
            [Obsolete("Typo. Use WS_CONNECT_FAILED instead.")]
            WS_CONNEC_FAILED = 0xf005, // typo, for compatibility, do not change the name
            WS_CONNECT_FAILED = 0xf005,
            WS_RECEIVE_CLOSE_FRAME = 0xf006,
        }

        public TDengineError(int code, string error) : base($"code:[0x{(code & 0xffff):x}],error:{error}")
        {
            Code = code & 0xffff;
            Error = error;
        }

        public TDengineError(int code, string error, byte[] extendedErrorBytes, string extendedErrorString) : base(
            $"code:[0x{(code & 0xffff):x}],error:{error},extendedBytes:{Format(extendedErrorBytes)},extendedString:{extendedErrorString}")
        {
            Code = code & 0xffff;
            Error = error;
            ExtendedErrorBytes = extendedErrorBytes;
            ExtendedErrorString = extendedErrorString;
        }

        public TDengineError(int code, string error, string extendedErrorString) : base(
            $"code:[0x{(code & 0xffff):x}],error:{error},extendedString:{extendedErrorString}")
        {
            Code = code & 0xffff;
            Error = error;
            ExtendedErrorString = extendedErrorString;
        }

        private static string Format(byte[] extendedError)
        {
            string hexString = "";
            for (int i = 0; i < extendedError.Length; i++)
            {
                hexString += $"0x{extendedError[i]:X2},";
            }

            return hexString;
        }
    }
}