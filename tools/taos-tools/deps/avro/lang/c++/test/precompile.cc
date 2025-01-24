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
#include <iostream>

#include "Compiler.hh"
#include "ValidSchema.hh"

int main(int argc, char **argv) {
    int ret = 0;
    try {
        avro::ValidSchema schema;
        if (argc > 1) {
            std::ifstream in(argv[1]);
            avro::compileJsonSchema(in, schema);
        } else {
            avro::compileJsonSchema(std::cin, schema);
        }

        if (argc > 2) {
            std::ofstream out(argv[2]);
            schema.toFlatList(out);
        } else {
            schema.toFlatList(std::cout);
        }
    } catch (std::exception &e) {
        std::cerr << "Failed to parse or compile schema: " << e.what() << std::endl;
        ret = 1;
    }

    return ret;
}
