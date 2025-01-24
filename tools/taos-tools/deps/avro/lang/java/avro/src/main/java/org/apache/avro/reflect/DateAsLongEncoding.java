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
package org.apache.avro.reflect;

import java.io.IOException;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

/**
 * This encoder/decoder writes a java.util.Date object as a long to avro and
 * reads a Date object from long. The long stores the number of milliseconds
 * since January 1, 1970, 00:00:00 GMT represented by the Date object.
 */
public class DateAsLongEncoding extends CustomEncoding<Date> {
  {
    schema = Schema.create(Schema.Type.LONG);
    schema.addProp("CustomEncoding", "DateAsLongEncoding");
  }

  @Override
  protected final void write(Object datum, Encoder out) throws IOException {
    out.writeLong(((Date) datum).getTime());
  }

  @Override
  protected final Date read(Object reuse, Decoder in) throws IOException {
    if (reuse instanceof Date) {
      ((Date) reuse).setTime(in.readLong());
      return (Date) reuse;
    } else
      return new Date(in.readLong());
  }

}
