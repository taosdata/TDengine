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
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Avro.Generic
{
    /// <summary>
    /// The default type used by GenericReader and GenericWriter for RecordSchema.
    /// </summary>
    public class GenericRecord : IEquatable<GenericRecord>
    {
        /// <summary>
        /// Schema for this record.
        /// </summary>
        public RecordSchema Schema { get; private set; }

        private readonly object[] contents;

        /// <summary>
        /// Initializes a new instance of the <see cref="GenericRecord"/> class.
        /// </summary>
        /// <param name="schema">Schema for this record.</param>
        public GenericRecord(RecordSchema schema)
        {
            this.Schema = schema;
            contents = new object[schema.Fields.Count];
        }

        /// <summary>
        /// Returns the value of the field with the given name.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <returns>Value of the field with the given name.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="fieldName"/> is null.
        /// </exception>
        /// <exception cref="KeyNotFoundException">
        /// <paramref name="fieldName"/> does not exist in this record.
        /// </exception>
        public object this[string fieldName]
        {
            get
            {
                if (Schema.TryGetField(fieldName, out Field field))
                {
                    return contents[field.Pos];
                }
                throw new KeyNotFoundException("Key name: " + fieldName);
            }
        }

        /// <summary>
        /// Sets the value for a field. You may call this method multiple times with the same
        /// field name to change its value. The given field name must exist in the schema.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="fieldValue">Value of the field.</param>
        /// <exception cref="AvroException">
        /// <paramref name="fieldName"/> does not exist in this record.
        /// </exception>
        public void Add(string fieldName, object fieldValue)
        {
            if (Schema.TryGetField(fieldName, out Field field))
            {
                // TODO: Use a matcher to verify that object has the right type for the field.
                //contents.Add(fieldName, fieldValue);
                contents[field.Pos] = fieldValue;
                return;
            }

            throw new AvroException("No such field: " + fieldName);
        }

        /// <summary>
        /// Gets the value the specified field name.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="result">
        /// When this method returns true, contains the value of the specified field;
        /// otherwise, null.
        /// </param>
        /// <returns>
        /// True if the field was found in the record. This method will only return true if
        /// <see cref="Add(string, object)"/> has been called for the given field name.
        /// </returns>
        public bool TryGetValue(string fieldName, out object result)
        {
            if (!Schema.TryGetField(fieldName, out Field field))
            {
                result = null;
                return false;
            }

            result = contents[field.Pos];
            return true;
        }

        /// <summary>
        /// Returns the value of the field with the given position.
        /// </summary>
        /// <param name="fieldPos">The position of the field.</param>
        /// <returns>Value of the field with the given position.</returns>
        /// <exception cref="IndexOutOfRangeException">
        /// <paramref name="fieldPos"/>
        /// </exception>
        public object GetValue(int fieldPos)
        {
            return contents[fieldPos];
        }


        /// <summary>
        /// Adds the value in the specified field position.
        /// </summary>
        /// <param name="fieldPos">Position of the field.</param>
        /// <param name="fieldValue">The value to add.</param>
        /// <exception cref="IndexOutOfRangeException">
        /// <paramref name="fieldPos"/>.
        /// </exception>
        public void Add(int fieldPos, object fieldValue) => contents[fieldPos] = fieldValue;

        /// <summary>
        /// Gets the value in the specified field position.
        /// </summary>
        /// <param name="fieldPos">Position of the field.</param>
        /// <param name="result">
        /// When this method returns true, contains the value of the specified field;
        /// otherwise, null.
        /// </param>
        /// <returns>
        /// True if the field position is valid.
        /// </returns>
        public bool TryGetValue(int fieldPos, out object result)
        {
            if (fieldPos < contents.Length)
            {
                result = contents[fieldPos];
                return true;
            }

            result = null;
            return false;
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            if (this == obj) return true;
            return obj is GenericRecord
                && Equals((GenericRecord)obj);
        }

        /// <inheritdoc/>
        public bool Equals(GenericRecord other)
        {
            return Schema.Equals(other.Schema)
                && arraysEqual(contents, other.contents);
        }

        private static bool mapsEqual(IDictionary d1, IDictionary d2)
        {
            if (d1.Count != d2.Count) return false;

            foreach (DictionaryEntry kv in d1)
            {
                if (!d2.Contains(kv.Key))
                    return false;
                if (!objectsEqual(d2[kv.Key], kv.Value))
                    return false;
            }
            return true;
        }

        private static bool objectsEqual(object o1, object o2)
        {
            if (o1 == null) return o2 == null;
            if (o2 == null) return false;
            if (o1 is Array)
            {
                if (!(o2 is Array)) return false;
                return arraysEqual((Array)o1 , (Array)o2);
            }

            if (o1 is IDictionary)
            {
                if (!(o2 is IDictionary)) return false;
                return mapsEqual((IDictionary)o1, (IDictionary)o2);
            }

            return o1.Equals(o2);
        }

        private static bool arraysEqual(Array a1, Array a2)
        {
            if (a1.Length != a2.Length) return false;
            for (int i = 0; i < a1.Length; i++)
            {
                if (!objectsEqual(a1.GetValue(i), a2.GetValue(i))) return false;
            }
            return true;
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return 31 * contents.GetHashCode()/* + 29 * Schema.GetHashCode()*/;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("Schema: ");
            sb.Append(Schema);
            sb.Append(", contents: ");
            sb.Append("{ ");
            foreach (var field in Schema.Fields)
            {
                sb.Append(field.Name);
                sb.Append(": ");
                sb.Append(contents[field.Pos]);
                sb.Append(", ");
            }
            sb.Append('}');
            return sb.ToString();
        }
    }
}
