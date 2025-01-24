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

#include "avro.h"
#include "avro_private.h"
#include <dirent.h>
#include <stddef.h>
#include <stdio.h>

int should_test(char *d_name)
{
    // check filename extension
    char *ext_pos = strrchr(d_name, '.');
    if (ext_pos == NULL)
    {
        return 0;
    }

    ptrdiff_t diff = d_name + strlen(d_name) - ext_pos;
    char *substr = malloc(sizeof(char) * diff);
    strncpy(substr, ext_pos + 1, diff - 1);
    *(substr + diff - 1) = '\0';
    if (strcmp(substr, "avro") != 0)
    {
        free(substr);
        return 0;
    }
    free(substr);

    // check compression codec
    char *codec_pos = strrchr(d_name, '_');
    if (codec_pos == NULL)
    {
        return 1;
    }
    if (ext_pos < codec_pos)
    {
        return 0;
    }

    diff = ext_pos - codec_pos;
    substr = malloc(sizeof(char) * diff);
    strncpy(substr, codec_pos + 1, diff - 1);
    *(substr + diff - 1) = '\0';
    if (strcmp(substr, "deflate") == 0 || strcmp(substr, "snappy") == 0)
    {
        free(substr);
        return 1;
    }
    free(substr);

    return 0;
}

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        fprintf(stderr, "%s accepts just one input file\n", argv[0]);
        return EXIT_FAILURE;
    }

    DIR *dir;
    if ((dir = opendir(argv[1])) == NULL)
    {
        fprintf(stderr, "The specified path is not a directory: %s\n", argv[1]);
        return EXIT_FAILURE;
    }

    struct dirent *ent;
    while ((ent = readdir(dir)) != NULL)
    {
        avro_file_reader_t reader;
        avro_value_t value;

        if (ent->d_type != DT_REG) continue;

        char *d_name = ent->d_name;
        size_t d_name_len = strlen(d_name);
        size_t size = strlen(argv[1]) + d_name_len + 2;
        char* path = malloc(sizeof(char) * size);
        sprintf(path, "%s/%s", argv[1], d_name);

        if (!should_test(d_name))
        {
            printf("Skipping file: %s\n", path);
            free(path);
            continue;
        }
        printf("Checking file: %s\n", path);

        if (avro_file_reader(path, &reader))
        {
            fprintf(stderr, "Failed to read from a file: %s\n", path);
            free(path);
            return EXIT_FAILURE;
        }
        avro_schema_t schema = avro_file_reader_get_writer_schema(reader);
        avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
        avro_generic_value_new(iface, &value);

        int i, rval;
        for (i = 0; (rval = avro_file_reader_read_value(reader, &value)) == 0; i++, avro_value_reset(&value));
        if (rval != EOF)
        {
            fprintf(stderr, "Error(%d) occurred while reading file: %s\n", rval, path);
            free(path);
            return EXIT_FAILURE;
        }
        if (i == 0)
        {
            fprintf(stderr, "Input file %s is supposed to be non-empty\n", path);
            free(path);
            return EXIT_FAILURE;
        }

        free(path);
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        avro_file_reader_close(reader);
    }

    closedir(dir);

    return EXIT_SUCCESS;
}
