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
package org.apache.avro.specific;

import java.io.IOException;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

/**
 * {@link org.apache.avro.io.DatumWriter DatumWriter} for generated Java
 * classes.
 */
public class SpecificDatumWriter<T> extends GenericDatumWriter<T> {
  public SpecificDatumWriter() {
    super(SpecificData.get());
  }

  public SpecificDatumWriter(Class<T> c) {
    super(SpecificData.get().getSchema(c), SpecificData.getForClass(c));
  }

  public SpecificDatumWriter(Schema schema) {
    super(schema, SpecificData.getForSchema(schema));
  }

  public SpecificDatumWriter(Schema root, SpecificData specificData) {
    super(root, specificData);
  }

  protected SpecificDatumWriter(SpecificData specificData) {
    super(specificData);
  }

  /** Returns the {@link SpecificData} implementation used by this writer. */
  public SpecificData getSpecificData() {
    return (SpecificData) getData();
  }

  @Override
  protected void writeEnum(Schema schema, Object datum, Encoder out) throws IOException {
    if (!(datum instanceof Enum))
      super.writeEnum(schema, datum, out); // punt to generic
    else
      out.writeEnum(((Enum) datum).ordinal());
  }

  @Override
  protected void writeString(Schema schema, Object datum, Encoder out) throws IOException {
    if (!(datum instanceof CharSequence) && getSpecificData().isStringable(datum.getClass())) {
      datum = datum.toString(); // convert to string
    }
    writeString(datum, out);
  }

  @Override
  protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
    if (datum instanceof SpecificRecordBase && this.getSpecificData().useCustomCoders()) {
      SpecificRecordBase d = (SpecificRecordBase) datum;
      if (d.hasCustomCoders()) {
        d.customEncode(out);
        return;
      }
    }
    super.writeRecord(schema, datum, out);
  }

  @Override
  protected void writeField(Object datum, Schema.Field f, Encoder out, Object state) throws IOException {
    if (datum instanceof SpecificRecordBase) {
      Conversion<?> conversion = ((SpecificRecordBase) datum).getConversion(f.pos());
      Schema fieldSchema = f.schema();
      LogicalType logicalType = fieldSchema.getLogicalType();

      Object value = getData().getField(datum, f.name(), f.pos());
      if (conversion != null && logicalType != null) {
        value = convert(fieldSchema, logicalType, conversion, value);
      }

      try {
        writeWithoutConversion(fieldSchema, value, out);
      } catch (NullPointerException e) {
        throw npe(e, " in field '" + f.name() + "'");
      } catch (ClassCastException cce) {
        throw addClassCastMsg(cce, " in field '" + f.name() + "'");
      } catch (AvroTypeException ate) {
        throw addAvroTypeMsg(ate, " in field '" + f.name() + "'");
      }

    } else {
      super.writeField(datum, f, out, state);
    }
  }
}
