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
using System.Linq;
using System.Reflection;
using System.IO;
using System.Net;

namespace Avro.ipc
{
    public class HttpTransceiver : Transceiver
    {
        private byte[] _intBuffer = new byte[4]; //this buffer is used by read/write behind the latch controlled by base class so we are sure there is no race condition
        private HttpWebRequest _httpRequest;
        private HttpWebRequest _modelRequest;

        public override string RemoteName
        {
            get
            {
                return _modelRequest.RequestUri.AbsoluteUri;
            }
        }

        public HttpTransceiver(HttpWebRequest modelRequest)
        {
            _modelRequest = modelRequest;
        }

        public HttpTransceiver(Uri serviceUri, int timeoutMs)
        {
            _modelRequest = (HttpWebRequest)WebRequest.Create(serviceUri);
            _modelRequest.Method = "POST";
            _modelRequest.ContentType = "avro/binary";
            _modelRequest.Timeout = timeoutMs;
        }

        private static int ReadInt(Stream stream, byte[] buffer)
        {
            stream.Read(buffer, 0, 4);
            return IPAddress.NetworkToHostOrder(BitConverter.ToInt32(buffer, 0));
        }

        public static byte[] ConvertIntToBytes(int value)
        {
            return BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value));
        }

        public static int CalculateLength(IList<MemoryStream> buffers)
        {
            int num = 0;
            foreach (MemoryStream memoryStream in (IEnumerable<MemoryStream>)buffers)
            {
                num += 4;
                num += (int)memoryStream.Length;
            }
            return num + 4;
        }

        public static IList<MemoryStream> ReadBuffers(Stream inStream, byte[] intBuffer)
        {
            List<MemoryStream> list = new List<MemoryStream>();
            while (true)
            {
                int length = ReadInt(inStream, intBuffer);

                if (length == 0) //end of transmission
                    break;

                byte[] buffer = new byte[length];
                int offset = 0;
                int count = length;
                while (offset < length)
                {
                    int num = inStream.Read(buffer, offset, count);
                    if (num == 0)
                        throw new Exception(string.Format("Unexpected end of response binary stream - expected {0} more bytes in current chunk", (object)count));
                    offset += num;
                    count -= num;
                }

                list.Add(new MemoryStream(buffer));
            }
            return (IList<MemoryStream>)list;
        }

        public override IList<MemoryStream> ReadBuffers()
        {
            using (Stream responseStream = this._httpRequest.GetResponse().GetResponseStream())
            {
                return ReadBuffers(responseStream, _intBuffer);
            }
        }

        protected HttpWebRequest CreateAvroHttpRequest(long contentLength)
        {
            HttpWebRequest wr = (HttpWebRequest)WebRequest.Create(_modelRequest.RequestUri);

            //TODO: what else to copy from model request?
            wr.AllowAutoRedirect = _modelRequest.AllowAutoRedirect;
            wr.AllowWriteStreamBuffering = _modelRequest.AllowWriteStreamBuffering;
            wr.AuthenticationLevel = _modelRequest.AuthenticationLevel;
            wr.AutomaticDecompression = _modelRequest.AutomaticDecompression;
            wr.CachePolicy = _modelRequest.CachePolicy;
            wr.ClientCertificates.AddRange(_modelRequest.ClientCertificates);
            wr.ConnectionGroupName = _modelRequest.ConnectionGroupName;
            wr.ContinueDelegate = _modelRequest.ContinueDelegate;
            wr.CookieContainer = _modelRequest.CookieContainer;
            wr.Credentials = _modelRequest.Credentials;
            wr.UnsafeAuthenticatedConnectionSharing = _modelRequest.UnsafeAuthenticatedConnectionSharing;
            wr.UseDefaultCredentials = _modelRequest.UseDefaultCredentials;

            wr.KeepAlive = _modelRequest.KeepAlive;
            wr.Expect = _modelRequest.Expect;
            //wr.Date = _modelRequest.Date;
            //wr.Host = _modelRequest.Host;
            wr.UserAgent = _modelRequest.UserAgent;
            //wr.Headers = _modelRequest.Headers;
            wr.Referer = _modelRequest.Referer;

            wr.Pipelined = _modelRequest.Pipelined;
            wr.PreAuthenticate = _modelRequest.PreAuthenticate;
            wr.ProtocolVersion = _modelRequest.ProtocolVersion;
            wr.Proxy = _modelRequest.Proxy;
            wr.ReadWriteTimeout = _modelRequest.ReadWriteTimeout;
            wr.Timeout = _modelRequest.Timeout;

            //the properties which are defined by Avro specification
            wr.Method = "POST";
            wr.ContentType = "avro/binary";
            wr.ContentLength = contentLength;

            return wr;
        }

        public static void WriteBuffers(IList<MemoryStream> buffers, Stream outStream)
        {
            foreach (MemoryStream memoryStream in buffers)
            {
                int num = (int)memoryStream.Length;
                outStream.Write(ConvertIntToBytes(num), 0, 4);
                memoryStream.WriteTo(outStream);
            }
            outStream.Write(ConvertIntToBytes(0), 0, 4);
            outStream.Flush();
        }

        public override void WriteBuffers(IList<MemoryStream> buffers)
        {
            _httpRequest = CreateAvroHttpRequest(CalculateLength(buffers));
            using (Stream requestStream = _httpRequest.GetRequestStream())
            {
                WriteBuffers(buffers, requestStream);
            }
        }
    }
}
