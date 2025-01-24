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
using System.IO;
using System.Threading;
using Avro.IO;

namespace Avro.ipc
{
    public abstract class Transceiver
    {
        private readonly object channelLock = new object();
        private Thread threadWhenLocked;

        public virtual bool IsConnected
        {
            get { return false; }
        }

        public abstract String RemoteName { get; }

        public virtual Protocol Remote
        {
            get { throw new InvalidOperationException("Not connected."); }
            set { }
        }

        public virtual IList<MemoryStream> Transceive(IList<MemoryStream> request)
        {
            if (request == null) throw new ArgumentNullException("request");

            LockChannel();
            try
            {
                WriteBuffers(request);
                return ReadBuffers();
            }
            finally
            {
                UnlockChannel();
            }
        }

        public virtual void VerifyConnection()
        {
        }

        public void Transceive(IList<MemoryStream> request, ICallback<IList<MemoryStream>> callback)
        {
            if (request == null) throw new ArgumentNullException("request");

            try
            {
                IList<MemoryStream> response = Transceive(request);
                callback.HandleResult(response);
            }
            catch (IOException e)
            {
                callback.HandleException(e);
            }
        }

        public void LockChannel()
        {
            Monitor.Enter(channelLock);

            threadWhenLocked = Thread.CurrentThread;
        }

        public void UnlockChannel()
        {
            if (Thread.CurrentThread == threadWhenLocked)
            {
                Monitor.Exit(channelLock);
            }
        }

        public abstract IList<MemoryStream> ReadBuffers();
        public abstract void WriteBuffers(IList<MemoryStream> getBytes);
    }
}