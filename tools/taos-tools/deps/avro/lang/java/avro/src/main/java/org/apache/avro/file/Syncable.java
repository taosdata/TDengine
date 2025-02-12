/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.avro.file;

import java.io.IOException;

public interface Syncable {

  /**
   * Sync the file to disk. On supported platforms, this method behaves like POSIX
   * <code>fsync</code> and syncs all underlying OS buffers for this file
   * descriptor to disk. On these platforms, if this method returns, the data
   * written to this instance is guaranteed to be persisted on disk.
   *
   * @throws IOException - if an error occurred while attempting to sync the data
   *                     to disk.
   */
  void sync() throws IOException;
}
