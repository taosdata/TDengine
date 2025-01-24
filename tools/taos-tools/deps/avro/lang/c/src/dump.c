/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License. 
 */

#include <ctype.h>
#include <string.h>
#include "avro_private.h"
#include "dump.h"

static void dump_line(FILE * out, const char *addr, const long len)
{
	int i;
	fprintf(out, "|");
	for (i = 0; i < 16; i++) {
		if (i < len) {
			fprintf(out, " %02X", ((uint8_t *) addr)[i]);
		} else {
			fprintf(out, " ..");
		}
		if (!((i + 1) % 8)) {
			fprintf(out, " |");
		}
	}
	fprintf(out, "\t");
	for (i = 0; i < 16; i++) {
		char c = 0x7f & ((uint8_t *) addr)[i];
		if (i < len && isprint(c)) {
			fprintf(out, "%c", c);
		} else {
			fprintf(out, ".");
		}
	}
}

void dump(FILE * out, const char *addr, const long len)
{
	int i;
	for (i = 0; i < len; i += 16) {
		dump_line(out, addr + i, (len - i) < 16 ? (len - i) : 16);
		fprintf(out, "\n");
	}
	fflush(out);
}
