/*
 * Copyright (c) 2009-2016 Petri Lehtinen <petri@digip.org>
 *
 * Jansson is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "jansson_private.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "jansson.h"
#include "strbuffer.h"
#include "utf.h"

#define MAX_INTEGER_STR_LENGTH 100
#define MAX_REAL_STR_LENGTH    100

#define FLAGS_TO_INDENT(f)    ((f)&0x1F)
#define FLAGS_TO_PRECISION(f) (((f) >> 11) & 0x1F)

struct buffer {
    const size_t size;
    size_t used;
    char *data;
};

static int dump_to_strbuffer(const char *buffer, size_t size, void *data) {
    return strbuffer_append_bytes((strbuffer_t *)data, buffer, size);
}

static int dump_to_buffer(const char *buffer, size_t size, void *data) {
    struct buffer *buf = (struct buffer *)data;

    if (buf->used + size <= buf->size)
        memcpy(&buf->data[buf->used], buffer, size);

    buf->used += size;
    return 0;
}

static int dump_to_file(const char *buffer, size_t size, void *data) {
    FILE *dest = (FILE *)data;
    if (fwrite(buffer, size, 1, dest) != 1)
        return -1;
    return 0;
}

static int dump_to_fd(const char *buffer, size_t size, void *data) {
#ifdef HAVE_UNISTD_H
    int *dest = (int *)data;
    if (write(*dest, buffer, size) == (ssize_t)size)
        return 0;
#endif
    return -1;
}

/* 32 spaces (the maximum indentation size) */
static const char whitespace[] = "                                ";

static int dump_indent(size_t flags, int depth, int space, json_dump_callback_t dump,
                       void *data) {
    if (FLAGS_TO_INDENT(flags) > 0) {
        unsigned int ws_count = FLAGS_TO_INDENT(flags), n_spaces = depth * ws_count;

        if (dump("\n", 1, data))
            return -1;

        while (n_spaces > 0) {
            int cur_n =
                n_spaces < sizeof whitespace - 1 ? n_spaces : sizeof whitespace - 1;

            if (dump(whitespace, cur_n, data))
                return -1;

            n_spaces -= cur_n;
        }
    } else if (space && !(flags & JSON_COMPACT)) {
        return dump(" ", 1, data);
    }
    return 0;
}

static int dump_string(const char *str, size_t len, json_dump_callback_t dump, void *data,
                       size_t flags) {
    const char *pos, *end, *lim;
    int32_t codepoint = 0;

    if (dump("\"", 1, data))
        return -1;

    end = pos = str;
    lim = str + len;
    while (1) {
        const char *text;
        char seq[13];
        int length;

        while (end < lim) {
            end = utf8_iterate(pos, lim - pos, &codepoint);
            if (!end)
                return -1;

            /* mandatory escape or control char */
            if (codepoint == '\\' || codepoint == '"' || codepoint < 0x20)
                break;

            /* slash */
            if ((flags & JSON_ESCAPE_SLASH) && codepoint == '/')
                break;

            /* non-ASCII */
            if ((flags & JSON_ENSURE_ASCII) && codepoint > 0x7F)
                break;

            pos = end;
        }

        if (pos != str) {
            if (dump(str, pos - str, data))
                return -1;
        }

        if (end == pos)
            break;

        /* handle \, /, ", and control codes */
        length = 2;
        switch (codepoint) {
            case '\\':
                text = "\\\\";
                break;
            case '\"':
                text = "\\\"";
                break;
            case '\b':
                text = "\\b";
                break;
            case '\f':
                text = "\\f";
                break;
            case '\n':
                text = "\\n";
                break;
            case '\r':
                text = "\\r";
                break;
            case '\t':
                text = "\\t";
                break;
            case '/':
                text = "\\/";
                break;
            default: {
                /* codepoint is in BMP */
                if (codepoint < 0x10000) {
                    snprintf(seq, sizeof(seq), "\\u%04X", (unsigned int)codepoint);
                    length = 6;
                }

                /* not in BMP -> construct a UTF-16 surrogate pair */
                else {
                    int32_t first, last;

                    codepoint -= 0x10000;
                    first = 0xD800 | ((codepoint & 0xffc00) >> 10);
                    last = 0xDC00 | (codepoint & 0x003ff);

                    snprintf(seq, sizeof(seq), "\\u%04X\\u%04X", (unsigned int)first,
                             (unsigned int)last);
                    length = 12;
                }

                text = seq;
                break;
            }
        }

        if (dump(text, length, data))
            return -1;

        str = pos = end;
    }

    return dump("\"", 1, data);
}

struct key_len {
    const char *key;
    int len;
};

static int compare_keys(const void *key1, const void *key2) {
    const struct key_len *k1 = key1;
    const struct key_len *k2 = key2;
    const size_t min_size = k1->len < k2->len ? k1->len : k2->len;
    int res = memcmp(k1->key, k2->key, min_size);

    if (res)
        return res;

    return k1->len - k2->len;
}

static int do_dump(const json_t *json, size_t flags, int depth, hashtable_t *parents,
                   json_dump_callback_t dump, void *data) {
    int embed = flags & JSON_EMBED;

    flags &= ~JSON_EMBED;

    if (!json)
        return -1;

    switch (json_typeof(json)) {
        case JSON_NULL:
            return dump("null", 4, data);

        case JSON_TRUE:
            return dump("true", 4, data);

        case JSON_FALSE:
            return dump("false", 5, data);

        case JSON_INTEGER: {
            char buffer[MAX_INTEGER_STR_LENGTH];
            int size;

            size = snprintf(buffer, MAX_INTEGER_STR_LENGTH, "%" JSON_INTEGER_FORMAT,
                            json_integer_value(json));
            if (size < 0 || size >= MAX_INTEGER_STR_LENGTH)
                return -1;

            return dump(buffer, size, data);
        }

        case JSON_REAL: {
            char buffer[MAX_REAL_STR_LENGTH];
            int size;
            double value = json_real_value(json);

            size = jsonp_dtostr(buffer, MAX_REAL_STR_LENGTH, value,
                                FLAGS_TO_PRECISION(flags));
            if (size < 0)
                return -1;

            return dump(buffer, size, data);
        }

        case JSON_STRING:
            return dump_string(json_string_value(json), json_string_length(json), dump,
                               data, flags);

        case JSON_ARRAY: {
            size_t n;
            size_t i;
            /* Space for "0x", double the sizeof a pointer for the hex and a
             * terminator. */
            char key[2 + (sizeof(json) * 2) + 1];
            size_t key_len;

            /* detect circular references */
            if (jsonp_loop_check(parents, json, key, sizeof(key), &key_len))
                return -1;

            n = json_array_size(json);

            if (!embed && dump("[", 1, data))
                return -1;
            if (n == 0) {
                hashtable_del(parents, key, key_len);
                return embed ? 0 : dump("]", 1, data);
            }
            if (dump_indent(flags, depth + 1, 0, dump, data))
                return -1;

            for (i = 0; i < n; ++i) {
                if (do_dump(json_array_get(json, i), flags, depth + 1, parents, dump,
                            data))
                    return -1;

                if (i < n - 1) {
                    if (dump(",", 1, data) ||
                        dump_indent(flags, depth + 1, 1, dump, data))
                        return -1;
                } else {
                    if (dump_indent(flags, depth, 0, dump, data))
                        return -1;
                }
            }

            hashtable_del(parents, key, key_len);
            return embed ? 0 : dump("]", 1, data);
        }

        case JSON_OBJECT: {
            void *iter;
            const char *separator;
            int separator_length;
            char loop_key[LOOP_KEY_LEN];
            size_t loop_key_len;

            if (flags & JSON_COMPACT) {
                separator = ":";
                separator_length = 1;
            } else {
                separator = ": ";
                separator_length = 2;
            }

            /* detect circular references */
            if (jsonp_loop_check(parents, json, loop_key, sizeof(loop_key),
                                 &loop_key_len))
                return -1;

            iter = json_object_iter((json_t *)json);

            if (!embed && dump("{", 1, data))
                return -1;
            if (!iter) {
                hashtable_del(parents, loop_key, loop_key_len);
                return embed ? 0 : dump("}", 1, data);
            }
            if (dump_indent(flags, depth + 1, 0, dump, data))
                return -1;

            if (flags & JSON_SORT_KEYS) {
                struct key_len *keys;
                size_t size, i;

                size = json_object_size(json);
                keys = jsonp_malloc(size * sizeof(struct key_len));
                if (!keys)
                    return -1;

                i = 0;
                while (iter) {
                    struct key_len *keylen = &keys[i];

                    keylen->key = json_object_iter_key(iter);
                    keylen->len = json_object_iter_key_len(iter);

                    iter = json_object_iter_next((json_t *)json, iter);
                    i++;
                }
                assert(i == size);

                qsort(keys, size, sizeof(struct key_len), compare_keys);

                for (i = 0; i < size; i++) {
                    const struct key_len *key;
                    json_t *value;

                    key = &keys[i];
                    value = json_object_getn(json, key->key, key->len);
                    assert(value);

                    dump_string(key->key, key->len, dump, data, flags);
                    if (dump(separator, separator_length, data) ||
                        do_dump(value, flags, depth + 1, parents, dump, data)) {
                        jsonp_free(keys);
                        return -1;
                    }

                    if (i < size - 1) {
                        if (dump(",", 1, data) ||
                            dump_indent(flags, depth + 1, 1, dump, data)) {
                            jsonp_free(keys);
                            return -1;
                        }
                    } else {
                        if (dump_indent(flags, depth, 0, dump, data)) {
                            jsonp_free(keys);
                            return -1;
                        }
                    }
                }

                jsonp_free(keys);
            } else {
                /* Don't sort keys */

                while (iter) {
                    void *next = json_object_iter_next((json_t *)json, iter);
                    const char *key = json_object_iter_key(iter);
                    const size_t key_len = json_object_iter_key_len(iter);

                    dump_string(key, key_len, dump, data, flags);
                    if (dump(separator, separator_length, data) ||
                        do_dump(json_object_iter_value(iter), flags, depth + 1, parents,
                                dump, data))
                        return -1;

                    if (next) {
                        if (dump(",", 1, data) ||
                            dump_indent(flags, depth + 1, 1, dump, data))
                            return -1;
                    } else {
                        if (dump_indent(flags, depth, 0, dump, data))
                            return -1;
                    }

                    iter = next;
                }
            }

            hashtable_del(parents, loop_key, loop_key_len);
            return embed ? 0 : dump("}", 1, data);
        }

        default:
            /* not reached */
            return -1;
    }
}

char *json_dumps(const json_t *json, size_t flags) {
    strbuffer_t strbuff;
    char *result;

    if (strbuffer_init(&strbuff))
        return NULL;

    if (json_dump_callback(json, dump_to_strbuffer, (void *)&strbuff, flags))
        result = NULL;
    else
        result = jsonp_strdup(strbuffer_value(&strbuff));

    strbuffer_close(&strbuff);
    return result;
}

size_t json_dumpb(const json_t *json, char *buffer, size_t size, size_t flags) {
    struct buffer buf = {size, 0, buffer};

    if (json_dump_callback(json, dump_to_buffer, (void *)&buf, flags))
        return 0;

    return buf.used;
}

int json_dumpf(const json_t *json, FILE *output, size_t flags) {
    return json_dump_callback(json, dump_to_file, (void *)output, flags);
}

int json_dumpfd(const json_t *json, int output, size_t flags) {
    return json_dump_callback(json, dump_to_fd, (void *)&output, flags);
}

int json_dump_file(const json_t *json, const char *path, size_t flags) {
    int result;

    FILE *output = fopen(path, "w");
    if (!output)
        return -1;

    result = json_dumpf(json, output, flags);

    if (fclose(output) != 0)
        return -1;

    return result;
}

int json_dump_callback(const json_t *json, json_dump_callback_t callback, void *data,
                       size_t flags) {
    int res;
    hashtable_t parents_set;

    if (!(flags & JSON_ENCODE_ANY)) {
        if (!json_is_array(json) && !json_is_object(json))
            return -1;
    }

    if (hashtable_init(&parents_set))
        return -1;
    res = do_dump(json, flags, 0, &parents_set, callback, data);
    hashtable_close(&parents_set);

    return res;
}
