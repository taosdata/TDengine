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

namespace Avro.Reflect
{
    /// <summary>
    /// Constructor
    /// </summary>
    /// <typeparam name="TAvro">Avro type</typeparam>
    /// <typeparam name="TProperty">Property type</typeparam>
    public abstract class TypedFieldConverter<TAvro, TProperty> : IAvroFieldConverter
    {
        /// <summary>
        /// Convert from Avro type to property type
        /// </summary>
        /// <param name="o">Avro value</param>
        /// <param name="s">Schema</param>
        /// <returns>Property value</returns>
        public abstract TProperty From(TAvro o, Schema s);

        /// <summary>
        /// Convert from property type to Avro type
        /// </summary>
        /// <param name="o"></param>
        /// <param name="s"></param>
        /// <returns></returns>
        public abstract TAvro To(TProperty o, Schema s);

        /// <summary>
        /// Implement untyped interface
        /// </summary>
        /// <param name="o"></param>
        /// <param name="s"></param>
        /// <returns></returns>
        public object FromAvroType(object o, Schema s)
        {
            if (!typeof(TAvro).IsAssignableFrom(o.GetType()))
            {
                throw new AvroException($"Converter from {typeof(TAvro).Name} to {typeof(TProperty).Name} cannot convert object of type {o.GetType().Name} to {typeof(TAvro).Name}, object {o.ToString()}");
            }

            return From((TAvro)o, s);
        }

        /// <summary>
        /// Implement untyped interface
        /// </summary>
        /// <returns></returns>
        public Type GetAvroType()
        {
            return typeof(TAvro);
        }

        /// <summary>
        /// Implement untyped interface
        /// </summary>
        /// <returns></returns>
        public Type GetPropertyType()
        {
            return typeof(TProperty);
        }

        /// <summary>
        /// Implement untyped interface
        /// </summary>
        /// <param name="o"></param>
        /// <param name="s"></param>
        /// <returns></returns>
        public object ToAvroType(object o, Schema s)
        {
            if (!typeof(TProperty).IsAssignableFrom(o.GetType()))
            {
                throw new AvroException($"Converter from {typeof(TAvro).Name} to {typeof(TProperty).Name} cannot convert object of type {o.GetType().Name} to {typeof(TProperty).Name}, object {o.ToString()}");
            }

            return To((TProperty)o, s);
        }
    }
}
