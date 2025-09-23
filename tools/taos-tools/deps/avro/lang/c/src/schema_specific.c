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

#include "avro_private.h"
#include <stdio.h>
#include <errno.h>
#include <string.h>

#include "schema.h"

enum specific_state {
	START_STATE,
};
typedef enum specific_state specific_state;

struct specific_ctx {
	FILE *header;
	FILE *source;
	int depth;
	specific_state state;
};
typedef struct specific_ctx specific_ctx;

static void indent(specific_ctx * ctx, FILE * fp)
{
	int i;
	for (i = 0; i < ctx->depth; i++) {
		fprintf(fp, "   ");
	}
}

static int avro_schema_to_source(avro_schema_t schema, specific_ctx * ctx)
{
	switch (schema->type) {
	default:
		return 0;
	}
	return EINVAL;
}

static int avro_schema_to_header(avro_schema_t schema, specific_ctx * ctx)
{
	size_t i;
	FILE *fp = ctx->header;

	indent(ctx, fp);
	ctx->depth++;

	if (is_avro_primitive(schema) && !ctx->name) {
		return 0;
	}

	switch (schema->type) {
	case AVRO_STRING:
		fprintf(fp, "char *%s;\n", ctx->name);
		break;

	case AVRO_BYTES:
		fprintf(fp, "struct %s { size_t %s_len; char *%s_val } %s;\n",
			ctx->name, ctx->name, ctx->name, ctx->name);
		break;

	case AVRO_INT:
		fprintf(fp, "int %s;\n", ctx->name);
		break;

	case AVRO_LONG:
		fprintf(fp, "long %s;\n", ctx->name);
		break;

	case AVRO_FLOAT:
		fprintf(fp, "float %s;\n", ctx->name);
		break;

	case AVRO_DOUBLE:
		fprintf(fp, "double %s;\n", ctx->name);
		break;

	case AVRO_BOOLEAN:
		fprintf(fp, "int %s; /* boolean */\n", ctx->name);
		break;

	case AVRO_NULL:
		break;

	case AVRO_RECORD:
		{
			struct schema_record_t *record_schema =
			    avro_schema_to_record(schema);
			fprintf(fp, "struct %s {\n", record_schema->name);
			for (i = 0; i < record_schema->num_fields; i++) {
				struct record_field_t *field =
				    record_schema->fields[i];
				ctx->name = field->name;
				avro_schema_to_header(field->type, ctx);
				ctx->name = NULL;
			}
			fprintf(fp, "};\n");
			fprintf(fp, "typedef struct %s %s;\n\n",
				record_schema->name, record_schema->name);
		}
		break;

	case AVRO_ENUM:
		{
			struct schema_enum_t *enum_schema =
			    avro_schema_to_enum(schema);
			fprintf(fp, "enum %s {\n", enum_schema->name);
			ctx->depth++;
			for (i = 0; i < enum_schema->num_symbols; i++) {
				indent(ctx, fp);
				fprintf(fp, "%s = %ld,\n",
					enum_schema->symbols[i], i);
			}
			ctx->depth--;
			fprintf(fp, "};\n");
			fprintf(fp, "typedef enum %s %s;\n\n",
				enum_schema->name, enum_schema->name);
		}
		break;

	case AVRO_FIXED:
		{
			struct schema_fixed_t *fixed_schema =
			    avro_schema_to_fixed(schema);
			fprintf(fp, "char %s[%ld];\n", fixed_schema->name,
				fixed_schema->size);
		}
		break;

	case AVRO_MAP:
		{

		}
		break;

	case AVRO_ARRAY:
		{
			struct schema_array_t *array_schema =
			    avro_schema_to_array(schema);
			if (!ctx->name) {
				break;
			}
			fprintf(fp, "struct { size_t %s_len; ", ctx->name);
			if (is_avro_named_type(array_schema->items)) {
				fprintf(fp, "%s",
					avro_schema_name(array_schema->items));
			} else if (is_avro_link(array_schema->items)) {
				struct schema_link_t *link_schema =
				    avro_schema_to_link(array_schema->items);
				fprintf(fp, "struct %s",
					avro_schema_name(link_schema->to));
			} else {
				avro_schema_to_header(array_schema->items, ctx);
			}
			fprintf(fp, " *%s_val;} %s;\n", ctx->name, ctx->name);
		}
		break;
	case AVRO_UNION:
		{
			struct schema_union_t *union_schema =
			    avro_schema_to_array(schema);
			if (!ctx->name) {
				break;
			}
			fprintf(fp, "union {\n");
			for (i = 0; i < union_schema->num_schemas; i++) {
				avro_schema_to_header(union_schema->schemas[i],
						      ctx);
			}
			fprintf(fp, "%s_u;\n");
		}
		break;
	case AVRO_LINK:
		break;
	default:
		return EINVAL;
	}

	ctx->depth--;
	return 0;
}

int avro_schema_to_specific(avro_schema_t schema, const char *prefix)
{
	specific_ctx ctx;
	char buf[1024];
	int rval;

	if (!schema) {
		return EINVAL;
	}

	memset(&ctx, 0, sizeof(ctx));
	snprintf(buf, sizeof(buf), "%s_avro.h", prefix);
	ctx.header = fopen(buf, "w");
	if (!ctx.header) {
		return errno;
	}
	snprintf(buf, sizeof(buf), "%s_avro.c", prefix);
	ctx.source = fopen(buf, "w");
	if (!ctx.source) {
		fclose(ctx.header);
		return errno;
	}

	rval = avro_schema_to_header(schema, &ctx);
	if (rval) {
		goto out;
	}

	rval = avro_schema_to_source(schema, &ctx);

      out:
	fclose(ctx.header);
	fclose(ctx.source);
	return rval;
}
