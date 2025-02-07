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

package org.apache.avro.ipc.jetty;

import java.net.URL;

import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.util.resource.Resource;

/**
 * Very simple servlet class capable of serving static files.
 */
public class StaticServlet extends DefaultServlet {
  private static final long serialVersionUID = 1L;

  @Override
  public Resource getResource(String pathInContext) {
    // Take only last slice of the URL as a filename, so we can adjust path.
    // This also prevents mischief like '../../foo.css'
    String[] parts = pathInContext.split("/");
    String filename = parts[parts.length - 1];

    URL resource = getClass().getClassLoader().getResource("org/apache/avro/ipc/stats/static/" + filename);
    if (resource == null) {
      return null;
    }
    return Resource.newResource(resource);
  }
}
