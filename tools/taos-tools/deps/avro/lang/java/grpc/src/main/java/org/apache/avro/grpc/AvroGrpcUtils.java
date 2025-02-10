/*
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

package org.apache.avro.grpc;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;

import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.KnownLength;

/** Utility methods for using Avro IDL and serialization with gRPC. */
public final class AvroGrpcUtils {
  private static final Logger LOG = Logger.getLogger(AvroGrpcUtils.class.getName());

  private AvroGrpcUtils() {
  }

  /**
   * Provides a a unique gRPC service name for Avro RPC interface or its subclass
   * Callback Interface.
   *
   * @param iface Avro RPC interface.
   * @return unique service name for gRPC.
   */
  public static String getServiceName(Class iface) {
    Protocol protocol = getProtocol(iface);
    return protocol.getNamespace() + "." + protocol.getName();
  }

  /**
   * Gets the {@link Protocol} from the Avro Interface.
   */
  public static Protocol getProtocol(Class iface) {
    try {
      Protocol p = (Protocol) (iface.getDeclaredField("PROTOCOL").get(null));
      return p;
    } catch (NoSuchFieldException e) {
      throw new AvroRuntimeException("Not a Specific protocol: " + iface);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /**
   * Skips any unread bytes from InputStream and closes it.
   */
  static void skipAndCloseQuietly(InputStream stream) {
    try {
      if (stream instanceof KnownLength && stream.available() > 0) {
        stream.skip(stream.available());
      } else {
        // don't expect this for an inputStream provided by gRPC but just to be on safe
        // side.
        byte[] skipBuffer = new byte[4096];
        while (true) {
          int read = stream.read(skipBuffer);
          if (read < skipBuffer.length) {
            break;
          }
        }
      }
      stream.close();
    } catch (Exception e) {
      LOG.log(Level.WARNING, "failed to skip/close the input stream, may cause memory leak", e);
    }
  }
}
