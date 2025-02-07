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
using System.Threading;

namespace Avro.ipc
{
    public class CountdownLatch : IDisposable
    {
        private ManualResetEvent evt;
        private int currentCount;

        public int CurrentCount
        {
            get { return currentCount; }
        }

        public CountdownLatch(int count)
        {
            currentCount = count;
            evt = new ManualResetEvent(false);
        }

        public void Signal()
        {
            if (Interlocked.Decrement(ref currentCount) == 0)
                evt.Set();
        }

        public void Wait()
        {
            evt.WaitOne();
        }

        public bool Wait(int milliseconds)
        {
            return evt.WaitOne(milliseconds);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~CountdownLatch()
        {
            // Finalizer calls Dispose(false)
            Dispose(false);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // free managed resources
                if (evt != null)
                {
                    evt.Close();
                    evt = null;
                }
            }
        }
    }
}