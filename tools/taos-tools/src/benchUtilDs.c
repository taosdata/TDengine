/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#include "bench.h"

static dstr * ds_header(char *s)
{
    if (NULL == s) return NULL;
    dstr *ds = (dstr *)(s - DS_HEADER_SIZE);

    if (ds->magic != MAGIC_NUMBER)
        return NULL;
    else
        return ds;
}

char * new_ds(size_t cap)
{
    dstr *ds;

    ds = malloc(DS_HEADER_SIZE + cap + 1);
    if (ds == NULL)
    {
        errorPrint("%s()->malloc()\n", __func__);
        exit(1);
    }

    ds->magic   = MAGIC_NUMBER;
    ds->custom  = 0;
    ds->cap     = cap;
    ds->len     = 0;
    ds->data[0] = '\0';

    return ds->data;
}

void free_ds(char **ps)
{
    dstr * ds = ds_header(*ps);

    if (ds == NULL)
    {
        debugPrint("%s(): not a dynamic string\n", __func__);
        exit(1);
    }

    free(ds);
    *ps = NULL;
}

uint64_t ds_len(const char *s)
{
    return ((uint64_t *)s)[OFF_LEN];
}

static inline void ds_set_len(char *s, uint64_t len)
{
    ((uint64_t *)s)[OFF_LEN] = len;
    s[len] = '\0';
}

uint64_t ds_cap(const char *s)
{
    return ((uint64_t *)s)[OFF_CAP];
}


static inline void ds_set_cap(char *s, uint64_t cap)
{
    ((uint64_t *)s)[OFF_CAP] = cap;
    s[cap] = '\0';
}

char * ds_end(char *s)
{
    return s + ds_len(s);
}

float fast_sqrt(float x)
{
    float xhalf = 0.5f * x;
    int i;
    memcpy(&i, &x, sizeof(int));
    i = 0x5f375a86 - (i >> 1);
    memcpy(&x, &i, sizeof(float));
    x = x * (1.5f - xhalf * x * x);
    return 1/x;
}

char * ds_grow(char **ps, size_t needsize)
{
    char *s = *ps;

    uint64_t len = ds_len(s) + needsize;
    uint64_t cap = ds_cap(s);

    if (len <= cap)
        return s;

    if (len > 128*1024*1024)
        cap = len + 32*1024*1024;
    else if (len > 4*1024*1024)
        cap = len + fast_sqrt(len * 8) * 1024;
    else if (len < 16)
        cap = 16;
    else
        cap = len * 2;

    return ds_resize(ps, cap);
}

char * ds_resize(char **ps, size_t cap)
{
    char *s = *ps;

    s = realloc(s - DS_HEADER_SIZE, DS_HEADER_SIZE + cap + 1);
    if (s == NULL)
    {
        errorPrint("%s()->realloc()\n", __func__);
        exit(1);
    }
    s += DS_HEADER_SIZE;

    if (ds_len(s) > cap)
        ds_set_len(s, cap);

    ds_set_cap(s, cap);

    *ps = s;

    return s;
}


char * ds_add_str(char **ps, const char* sub)
{
    size_t len = strlen(sub);
    ds_grow(ps, len);

    char *s = *ps;

    memcpy(ds_end(s), sub, len);
    // 修正长度并补上 \0
    ds_set_len(s, ds_len(s) + len);

    return s;
}

char * ds_add_strs(char **ps, int count, ...)
{
    va_list valist;

    va_start(valist, count);

    for (int i=0; i<count; i++) {
        const char *sub = va_arg(valist, char *);
        ds_add_str(ps, sub);
    }

    va_end(valist);

    return *ps;
}

