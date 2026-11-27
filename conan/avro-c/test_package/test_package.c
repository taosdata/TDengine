/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <avro.h>
#include <stdio.h>

int main() {
    printf("Avro C version: Testing avro-c library\n");
    
    // Simple test to ensure the library is linkable
    avro_schema_t schema = avro_schema_string();
    if (schema) {
        printf("Successfully created Avro string schema\n");
        avro_schema_decref(schema);
        return 0;
    }
    
    printf("Failed to create Avro schema\n");
    return 1;
}
