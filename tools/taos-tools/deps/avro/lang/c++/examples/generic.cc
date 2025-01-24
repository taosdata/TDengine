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

#include <fstream>
#include <complex>

#include "cpx.hh"

#include "avro/Compiler.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/Specific.hh"
#include "avro/Generic.hh"

int
main()
{
    std::ifstream ifs("cpx.json");

    avro::ValidSchema cpxSchema;
    avro::compileJsonSchema(ifs, cpxSchema);

    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);
    c::cpx c1;
    c1.re = 100.23;
    c1.im = 105.77;
    avro::encode(*e, c1);

    std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(*out);
    avro::DecoderPtr d = avro::binaryDecoder();
    d->init(*in);

    avro::GenericDatum datum(cpxSchema);
    avro::decode(*d, datum);
    std::cout << "Type: " << datum.type() << std::endl;
    if (datum.type() == avro::AVRO_RECORD) {
        const avro::GenericRecord& r = datum.value<avro::GenericRecord>();
        std::cout << "Field-count: " << r.fieldCount() << std::endl;
        if (r.fieldCount() == 2) {
            const avro::GenericDatum& f0 = r.fieldAt(0);
            if (f0.type() == avro::AVRO_DOUBLE) {
                std::cout << "Real: " << f0.value<double>() << std::endl;
            }
            const avro::GenericDatum& f1 = r.fieldAt(1);
            if (f1.type() == avro::AVRO_DOUBLE) {
                std::cout << "Imaginary: " << f1.value<double>() << std::endl;
            }
        }
    }
    return 0;
}
