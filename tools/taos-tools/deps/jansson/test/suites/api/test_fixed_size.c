/*
 * Copyright (c) 2020 Petri Lehtinen <petri@digip.org>
 *
 * Jansson is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#include "util.h"
#include <jansson.h>
#include <string.h>

static void test_keylen_iterator(json_t *object) {
    const char key1[] = {'t', 'e', 's', 't', '1'};
    const char key2[] = {'t', 'e', 's', 't'};
    const char key3[] = {'t', 'e', 's', '\0', 't'};
    const char key4[] = {'t', 'e', 's', 't', '\0'};
    const char *reference_keys[] = {key1, key2, key3, key4};
    const size_t reference_keys_len[] = {sizeof(key1), sizeof(key2), sizeof(key3),
                                         sizeof(key4)};
    size_t index = 0;
    json_t *value;
    const char *key;
    size_t keylen;

    json_object_keylen_foreach(object, key, keylen, value) {
        if (keylen != reference_keys_len[index])
            fail("invalid key len in iterator");
        if (memcmp(key, reference_keys[index], reference_keys_len[index]) != 0)
            fail("invalid key in iterator");

        index++;
    }
}

static void test_keylen(void) {
    json_t *obj = json_object();
    const char key[] = {'t', 'e', 's', 't', '1'};
    const char key2[] = {'t', 'e', 's', 't'};
    const char key3[] = {'t', 'e', 's', '\0', 't'};
    const char key4[] = {'t', 'e', 's', 't', '\0'};

    if (json_object_size(obj) != 0)
        fail("incorrect json");

    json_object_set_new_nocheck(obj, "test1", json_true());

    if (json_object_size(obj) != 1)
        fail("incorrect json");

    if (json_object_getn(obj, key, sizeof(key)) != json_true())
        fail("json_object_getn failed");

    if (json_object_getn(obj, key2, sizeof(key2)) != NULL)
        fail("false positive json_object_getn by key2");

    if (json_object_setn_nocheck(obj, key2, sizeof(key2), json_false()))
        fail("json_object_setn_nocheck for key2 failed");

    if (json_object_size(obj) != 2)
        fail("incorrect json");

    if (json_object_get(obj, "test") != json_false())
        fail("json_object_setn_nocheck for key2 failed");

    if (json_object_getn(obj, key2, sizeof(key2)) != json_false())
        fail("json_object_getn by key 2 failed");

    if (json_object_getn(obj, key3, sizeof(key3)) != NULL)
        fail("false positive json_object_getn by key3");

    if (json_object_setn_nocheck(obj, key3, sizeof(key3), json_false()))
        fail("json_object_setn_nocheck for key3 failed");

    if (json_object_size(obj) != 3)
        fail("incorrect json");

    if (json_object_getn(obj, key3, sizeof(key3)) != json_false())
        fail("json_object_getn by key 3 failed");

    if (json_object_getn(obj, key4, sizeof(key4)) != NULL)
        fail("false positive json_object_getn by key3");

    if (json_object_setn_nocheck(obj, key4, sizeof(key4), json_false()))
        fail("json_object_setn_nocheck for key3 failed");

    if (json_object_size(obj) != 4)
        fail("incorrect json");

    test_keylen_iterator(obj);

    if (json_object_getn(obj, key4, sizeof(key4)) != json_false())
        fail("json_object_getn by key 3 failed");

    if (json_object_size(obj) != 4)
        fail("incorrect json");

    if (json_object_deln(obj, key4, sizeof(key4)))
        fail("json_object_deln failed");
    if (json_object_getn(obj, key4, sizeof(key4)) != NULL)
        fail("json_object_deln failed");
    if (json_object_size(obj) != 3)
        fail("incorrect json");

    if (json_object_deln(obj, key3, sizeof(key3)))
        fail("json_object_deln failed");
    if (json_object_getn(obj, key3, sizeof(key3)) != NULL)
        fail("json_object_deln failed");
    if (json_object_size(obj) != 2)
        fail("incorrect json");

    if (json_object_deln(obj, key2, sizeof(key2)))
        fail("json_object_deln failed");
    if (json_object_getn(obj, key2, sizeof(key2)) != NULL)
        fail("json_object_deln failed");
    if (json_object_size(obj) != 1)
        fail("incorrect json");

    if (json_object_deln(obj, key, sizeof(key)))
        fail("json_object_deln failed");
    if (json_object_getn(obj, key, sizeof(key)) != NULL)
        fail("json_object_deln failed");
    if (json_object_size(obj) != 0)
        fail("incorrect json");

    json_decref(obj);
}

static void test_invalid_keylen(void) {
    json_t *obj = json_object();
    const char key[] = {'t', 'e', 's', 't', '1'};

    json_object_set_new_nocheck(obj, "test1", json_true());

    if (json_object_getn(NULL, key, sizeof(key)) != NULL)
        fail("json_object_getn on NULL failed");

    if (json_object_getn(obj, NULL, sizeof(key)) != NULL)
        fail("json_object_getn on NULL failed");

    if (json_object_getn(obj, key, 0) != NULL)
        fail("json_object_getn on NULL failed");

    if (!json_object_setn_new(obj, NULL, sizeof(key), json_true()))
        fail("json_object_setn_new with NULL key failed");

    if (!json_object_setn_new_nocheck(obj, NULL, sizeof(key), json_true()))
        fail("json_object_setn_new_nocheck with NULL key failed");

    if (!json_object_del(obj, NULL))
        fail("json_object_del with NULL failed");

    json_decref(obj);
}

static void test_binary_keys(void) {
    json_t *obj = json_object();
    int key1 = 0;
    int key2 = 1;

    json_object_setn_nocheck(obj, (const char *)&key1, sizeof(key1), json_true());
    json_object_setn_nocheck(obj, (const char *)&key2, sizeof(key2), json_true());

    if (!json_is_true(json_object_getn(obj, (const char *)&key1, sizeof(key1))))
        fail("cannot get integer key1");

    if (!json_is_true(json_object_getn(obj, (const char *)&key1, sizeof(key2))))
        fail("cannot get integer key2");

    json_decref(obj);
}

static void test_dump_order(void) {
    json_t *obj = json_object();
    char key1[] = {'k', '\0', '-', '2'};
    char key2[] = {'k', '\0', '-', '1'};
    const char expected_sorted_str[] =
        "{\"k\\u0000-1\": \"first\", \"k\\u0000-2\": \"second\"}";
    const char expected_nonsorted_str[] =
        "{\"k\\u0000-2\": \"second\", \"k\\u0000-1\": \"first\"}";
    char *out;

    json_object_setn_new_nocheck(obj, key1, sizeof(key1), json_string("second"));
    json_object_setn_new_nocheck(obj, key2, sizeof(key2), json_string("first"));

    out = malloc(512);

    json_dumpb(obj, out, 512, 0);

    if (memcmp(expected_nonsorted_str, out, sizeof(expected_nonsorted_str) - 1) != 0)
        fail("preserve order failed");

    json_dumpb(obj, out, 512, JSON_SORT_KEYS);
    if (memcmp(expected_sorted_str, out, sizeof(expected_sorted_str) - 1) != 0)
        fail("utf-8 sort failed");

    free(out);
    json_decref(obj);
}

static void run_tests() {
    test_keylen();
    test_invalid_keylen();
    test_binary_keys();
    test_dump_order();
}
