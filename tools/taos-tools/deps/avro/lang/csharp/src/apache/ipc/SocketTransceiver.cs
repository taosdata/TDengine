/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace Avro.ipc
{
    public class SocketTransceiver : Transceiver
    {
        private readonly byte[] header = new byte[4];
        private readonly string host;
        private readonly int port;
        private readonly Stopwatch timeoutStopWatch;
        private Socket channel;

        private int serialNumber;

        public SocketTransceiver(string host, int port)
            : this(CreateSocket())
        {
            this.host = host;
            this.port = port;

            Connect();
        }

        public SocketTransceiver(Socket channel)
        {
            this.channel = channel;
            this.channel.NoDelay = true;

            timeoutStopWatch = new Stopwatch();
        }

        public override bool IsConnected
        {
            get
            {
                LockChannel();

                try
                {
                    return Remote != null;
                }
                finally
                {
                    UnlockChannel();
                }
            }
        }

        public override Protocol Remote { get; set; }

        public bool SocketConnected
        {
            get { return channel.Connected; }
        }

        public override string RemoteName
        {
            get
            {
                var ipEndPoint = ((IPEndPoint) channel.RemoteEndPoint);
                return ipEndPoint.Address.ToString();
            }
        }

        private static Socket CreateSocket()
        {
            return new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        }

        public void Connect()
        {
            channel.Connect(host, port);
        }

        public void Reconnect()
        {
            if (host == null)
                throw new InvalidOperationException("Cannot reconnect to a null host");

            channel = CreateSocket();
            Connect();

            Remote = null;
        }

        public void Disconnect()
        {
            if (channel != null && channel.Connected)
            {
                var myOpts = new LingerOption(true, 0);
                channel.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.DontLinger, true);
                channel.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger, myOpts);
                channel.Close();
            }
        }

        public override IList<MemoryStream> ReadBuffers()
        {
            // Ignore first part for now.
            ReadBuffer(header, 4);

            int numberOfLists = ReadInt();

            // With 8 byte references, this supports creation of a generous 2GB list
            const int MaxNumLists = 250000000;

            if (numberOfLists > MaxNumLists)
            {
                Disconnect();
                throw new AvroRuntimeException(
                    string.Format("Excessively large list allocation request detected: {0} items! Connection closed.",
                                  numberOfLists));
            }

            var buffers = new List<MemoryStream>(numberOfLists);

            for (int i = 0; i < numberOfLists; i++)
            {
                // size of the list.
                int length = ReadInt();

                var buffer = new byte[length];
                ReadBuffer(buffer, length);

                buffers.Add(new MemoryStream(buffer));
            }

            return buffers;
        }

        public override void WriteBuffers(IList<MemoryStream> buffers)
        {
            if (buffers == null) return;

            Interlocked.Increment(ref serialNumber);

            byte[] serial = ConvertIntToBytes(serialNumber);
            channel.Send(serial);

            byte[] numBuffers = ConvertIntToBytes(buffers.Count);
            channel.Send(numBuffers);

            foreach (MemoryStream buffer in buffers)
            {
                var length = (int) buffer.Length;

                byte[] bufferLength = ConvertIntToBytes(length);
                channel.Send(bufferLength);

                byte[] bytes = buffer.GetBuffer();
                channel.Send(bytes, (int) buffer.Length, SocketFlags.None);
            }
        }

        private int ReadInt()
        {
            ReadBuffer(header, 4);

            int num = BitConverter.ToInt32(header, 0);
            num = IPAddress.NetworkToHostOrder(num);
            return num;
        }

        private static byte[] ConvertIntToBytes(int length)
        {
            int hostToNetworkOrder = IPAddress.HostToNetworkOrder(length);
            byte[] bufferLength = BitConverter.GetBytes(hostToNetworkOrder);
            return bufferLength;
        }

        private void ReadBuffer(byte[] buffer, int length)
        {
            if (length == 0)
                return;

            int numReceived = 0;
            do
            {
                numReceived += channel.Receive(buffer, numReceived, length - numReceived, SocketFlags.None);

                Timeout(numReceived);
            } while (numReceived < length);
        }

        private void Timeout(int numReceived)
        {
            if (numReceived == 0)
            {
                if (!timeoutStopWatch.IsRunning)
                {
                    timeoutStopWatch.Start();
                }
                else if (timeoutStopWatch.ElapsedMilliseconds > 10000)
                {
                    throw new TimeoutException(string.Format("Failed to receive any data after [{0}] milliseconds.",
                                                             timeoutStopWatch.ElapsedMilliseconds));
                }
            }
        }

        public override void VerifyConnection()
        {
            if (!SocketConnected)
            {
                Reconnect();
            }
        }

        public void Close()
        {
        }
    }
}