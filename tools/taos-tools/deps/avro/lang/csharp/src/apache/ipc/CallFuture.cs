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
using Avro.IO;

namespace Avro.ipc
{
    public class CallFuture<T> : ICallback<T>, IDisposable
    {
        private readonly ICallback<T> chainedCallback;
        private CountdownLatch latch = new CountdownLatch(1);

        public CallFuture(ICallback<T> chainedCallback = null)
        {
            this.chainedCallback = chainedCallback;
        }

        public T Result { get; private set; }
        public Exception Error { get; private set; }

        public bool IsDone
        {
            get { return latch.CurrentCount == 0; }
        }

        public virtual void HandleResult(T result)
        {
            Result = result;
            latch.Signal();
            if (chainedCallback != null)
            {
                chainedCallback.HandleResult(result);
            }
        }

        public virtual void HandleException(Exception exception)
        {
            Error = exception;
            latch.Signal();
            if (chainedCallback != null)
            {
                chainedCallback.HandleException(exception);
            }
        }

        public T WaitForResult()
        {
            latch.Wait();
            if (Error != null)
            {
                throw Error;
            }
            return Result;
        }

        public T WaitForResult(int millisecondsTimeout)
        {
            if (latch.Wait(millisecondsTimeout))
            {
                if (Error != null)
                {
                    throw Error;
                }
                return Result;
            }

            throw new TimeoutException();
        }

        public void Wait()
        {
            latch.Wait();
        }

        public void Wait(int millisecondsTimeout)
        {
            if (!latch.Wait(millisecondsTimeout))
            {
                throw new TimeoutException();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~CallFuture()
        {
            // Finalizer calls Dispose(false)
            Dispose(false);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                // free managed resources
                if (latch != null)
                {
                    latch.Dispose();
                    latch = null;
                }
            }
        }
    }
}