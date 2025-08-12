using System;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading;

namespace TDengine.Driver
{
    public static class ReqId
    {
        private static readonly long UuidHashId;
        private static long _serialNo;
        private static readonly long Pid;
        static ReqId()
        {
            using (var md5 = MD5.Create())
            {
                var tUuidBytes = Guid.NewGuid().ToByteArray();
                var hashBytes = md5.ComputeHash(tUuidBytes);
                UuidHashId = ((long)BitConverter.ToUInt32(hashBytes, 0) & 0x07ff) << 52;
            }

            Pid = ((long)(Process.GetCurrentProcess().Id & 0x0f)) << 48;
        }

        public static long GetReqId()
        {
            long timeSpan = ((DateTime.UtcNow.Ticks -TDengineConstant.TimeZero.Ticks) / 10000)>> 8;
            long val = Interlocked.Increment(ref _serialNo);
            return UuidHashId | Pid | ((timeSpan & 0x3ffffff) << 20) | (val & 0xfffff);
        }

        private const uint C1 = 0xcc9e2d51;
        private const uint C2 = 0x1b873593;

        public static uint MurmurHash32(byte[] data, uint seed)
        {
            uint h1 = seed;

            int nBlocks = data.Length / 4;
            int p = 0;
            uint k1;
            for (int i = 0; i < nBlocks; i++)
            {
                k1 = BitConverter.ToUInt32(data, p);

                k1 *= C1;
                k1 = (k1 << 15) | (k1 >> 17);
                k1 *= C2;

                h1 ^= k1;
                h1 = (h1 << 13) | (h1 >> 19);
                h1 = h1 * 5 + 0xe6546b64;

                p += 4;
            }

            byte[] tail = new byte[data.Length - nBlocks * 4];
            Buffer.BlockCopy(data, nBlocks * 4, tail, 0, tail.Length);

            k1 = 0;
            switch (tail.Length & 3)
            {
                case 3:
                    k1 ^= (uint)tail[2] << 16;
                    goto case 2;
                case 2:
                    k1 ^= (uint)tail[1] << 8;
                    goto case 1;
                case 1:
                    k1 ^= (uint)tail[0];
                    k1 *= C1;
                    k1 = (k1 << 15) | (k1 >> 17);
                    k1 *= C2;
                    h1 ^= k1;
                    break;
            }

            h1 ^= (uint)data.Length;

            h1 ^= h1 >> 16;
            h1 *= 0x85ebca6b;
            h1 ^= h1 >> 13;
            h1 *= 0xc2b2ae35;
            h1 ^= h1 >> 16;

            return h1;
        }
        
    }
}