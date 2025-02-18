/*
 * Copyright (c) 2009-2016 Petri Lehtinen <petri@digip.org>
 *
 * Jansson is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#include "jansson_private_config.h"

#include <jansson.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include "util.h"
#ifdef __MINGW32__
#include <fcntl.h>
#define pipe(fds) _pipe(fds, 1024, _O_BINARY)
#endif

static int encode_null_callback(const char *buffer, size_t size, void *data) {
    (void)buffer;
    (void)size;
    (void)data;
    return 0;
}

static void encode_null() {
    if (json_dumps(NULL, JSON_ENCODE_ANY) != NULL)
        fail("json_dumps didn't fail for NULL");

    if (json_dumpb(NULL, NULL, 0, JSON_ENCODE_ANY) != 0)
        fail("json_dumpb didn't fail for NULL");

    if (json_dumpf(NULL, stderr, JSON_ENCODE_ANY) != -1)
        fail("json_dumpf didn't fail for NULL");

#ifdef HAVE_UNISTD_H
    if (json_dumpfd(NULL, STDERR_FILENO, JSON_ENCODE_ANY) != -1)
        fail("json_dumpfd didn't fail for NULL");
#endif

    /* Don't test json_dump_file to avoid creating a file */

    if (json_dump_callback(NULL, encode_null_callback, NULL, JSON_ENCODE_ANY) != -1)
        fail("json_dump_callback didn't fail for NULL");
}

static void encode_twice() {
    /* Encode an empty object/array, add an item, encode again */

    json_t *json;
    char *result;

    json = json_object();
    result = json_dumps(json, 0);
    if (!result || strcmp(result, "{}"))
        fail("json_dumps failed");
    free(result);

    json_object_set_new(json, "foo", json_integer(5));
    result = json_dumps(json, 0);
    if (!result || strcmp(result, "{\"foo\": 5}"))
        fail("json_dumps failed");
    free(result);

    json_decref(json);

    json = json_array();
    result = json_dumps(json, 0);
    if (!result || strcmp(result, "[]"))
        fail("json_dumps failed");
    free(result);

    json_array_append_new(json, json_integer(5));
    result = json_dumps(json, 0);
    if (!result || strcmp(result, "[5]"))
        fail("json_dumps failed");
    free(result);

    json_decref(json);
}

static void circular_references() {
    /* Construct a JSON object/array with a circular reference:

       object: {"a": {"b": {"c": <circular reference to $.a>}}}
       array: [[[<circular reference to the $[0] array>]]]

       Encode it, remove the circular reference and encode again.
    */

    json_t *json;
    char *result;

    json = json_object();
    json_object_set_new(json, "a", json_object());
    json_object_set_new(json_object_get(json, "a"), "b", json_object());
    json_object_set(json_object_get(json_object_get(json, "a"), "b"), "c",
                    json_object_get(json, "a"));

    if (json_dumps(json, 0))
        fail("json_dumps encoded a circular reference!");

    json_object_del(json_object_get(json_object_get(json, "a"), "b"), "c");

    result = json_dumps(json, 0);
    if (!result || strcmp(result, "{\"a\": {\"b\": {}}}"))
        fail("json_dumps failed!");
    free(result);

    json_decref(json);

    json = json_array();
    json_array_append_new(json, json_array());
    json_array_append_new(json_array_get(json, 0), json_array());
    json_array_append(json_array_get(json_array_get(json, 0), 0),
                      json_array_get(json, 0));

    if (json_dumps(json, 0))
        fail("json_dumps encoded a circular reference!");

    json_array_remove(json_array_get(json_array_get(json, 0), 0), 0);

    result = json_dumps(json, 0);
    if (!result || strcmp(result, "[[[]]]"))
        fail("json_dumps failed!");
    free(result);

    json_decref(json);
}

static void encode_other_than_array_or_object() {
    /* Encoding anything other than array or object should only
     * succeed if the JSON_ENCODE_ANY flag is used */

    json_t *json;
    char *result;

    json = json_string("foo");
    if (json_dumps(json, 0) != NULL)
        fail("json_dumps encoded a string!");
    if (json_dumpf(json, NULL, 0) == 0)
        fail("json_dumpf encoded a string!");
    if (json_dumpfd(json, -1, 0) == 0)
        fail("json_dumpfd encoded a string!");

    result = json_dumps(json, JSON_ENCODE_ANY);
    if (!result || strcmp(result, "\"foo\"") != 0)
        fail("json_dumps failed to encode a string with JSON_ENCODE_ANY");

    free(result);
    json_decref(json);

    json = json_integer(42);
    if (json_dumps(json, 0) != NULL)
        fail("json_dumps encoded an integer!");
    if (json_dumpf(json, NULL, 0) == 0)
        fail("json_dumpf encoded an integer!");
    if (json_dumpfd(json, -1, 0) == 0)
        fail("json_dumpfd encoded an integer!");

    result = json_dumps(json, JSON_ENCODE_ANY);
    if (!result || strcmp(result, "42") != 0)
        fail("json_dumps failed to encode an integer with JSON_ENCODE_ANY");

    free(result);
    json_decref(json);
}

static void escape_slashes() {
    /* Test dump escaping slashes */

    json_t *json;
    char *result;

    json = json_object();
    json_object_set_new(json, "url", json_string("https://github.com/akheron/jansson"));

    result = json_dumps(json, 0);
    if (!result || strcmp(result, "{\"url\": \"https://github.com/akheron/jansson\"}"))
        fail("json_dumps failed to not escape slashes");

    free(result);

    result = json_dumps(json, JSON_ESCAPE_SLASH);
    if (!result ||
        strcmp(result, "{\"url\": \"https:\\/\\/github.com\\/akheron\\/jansson\"}"))
        fail("json_dumps failed to escape slashes");

    free(result);
    json_decref(json);
}

static void encode_nul_byte() {
    json_t *json;
    char *result;

    json = json_stringn("nul byte \0 in string", 20);
    result = json_dumps(json, JSON_ENCODE_ANY);
    if (!result || memcmp(result, "\"nul byte \\u0000 in string\"", 27))
        fail("json_dumps failed to dump an embedded NUL byte");

    free(result);
    json_decref(json);
}

static void dump_file() {
    json_t *json;
    int result;

    result = json_dump_file(NULL, "", 0);
    if (result != -1)
        fail("json_dump_file succeeded with invalid args");

    json = json_object();
    result = json_dump_file(json, "json_dump_file.json", 0);
    if (result != 0)
        fail("json_dump_file failed");

    json_decref(json);
    remove("json_dump_file.json");
}

static void dumpb() {
    char buf[2];
    json_t *obj;
    size_t size;

    obj = json_object();

    size = json_dumpb(obj, buf, sizeof(buf), 0);
    if (size != 2 || strncmp(buf, "{}", 2))
        fail("json_dumpb failed");

    json_decref(obj);
    obj = json_pack("{s:s}", "foo", "bar");

    size = json_dumpb(obj, buf, sizeof(buf), JSON_COMPACT);
    if (size != 13)
        fail("json_dumpb size check failed");

    json_decref(obj);
}

static void dumpfd() {
#ifdef HAVE_UNISTD_H
    int fds[2] = {-1, -1};
    json_t *a, *b;

    if (pipe(fds))
        fail("pipe() failed");

    a = json_pack("{s:s}", "foo", "bar");

    if (json_dumpfd(a, fds[1], 0))
        fail("json_dumpfd() failed");
    close(fds[1]);

    b = json_loadfd(fds[0], 0, NULL);
    if (!b)
        fail("json_loadfd() failed");
    close(fds[0]);

    if (!json_equal(a, b))
        fail("json_equal() failed for fd test");

    json_decref(a);
    json_decref(b);
#endif
}

static void embed() {
    static const char *plains[] = {"{\"bar\":[],\"foo\":{}}", "[[],{}]", "{}", "[]",
                                   NULL};

    size_t i;

    for (i = 0; plains[i]; i++) {
        const char *plain = plains[i];
        json_t *parse = NULL;
        char *embed = NULL;
        size_t psize = 0;
        size_t esize = 0;

        psize = strlen(plain) - 2;
        embed = calloc(1, psize);
        parse = json_loads(plain, 0, NULL);
        esize =
            json_dumpb(parse, embed, psize, JSON_COMPACT | JSON_SORT_KEYS | JSON_EMBED);
        json_decref(parse);
        if (esize != psize)
            fail("json_dumpb(JSON_EMBED) returned an invalid size");
        if (strncmp(plain + 1, embed, esize) != 0)
            fail("json_dumps(JSON_EMBED) returned an invalid value");
        free(embed);
    }
}

static void run_tests() {
    encode_null();
    encode_twice();
    circular_references();
    encode_other_than_array_or_object();
    escape_slashes();
    encode_nul_byte();
    dump_file();
    dumpb();
    dumpfd();
    embed();
}
