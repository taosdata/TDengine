#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "util.h"
#include <jansson.h>
#include <stdio.h>
#include <string.h>

static int chaos_pos = 0;
static int chaos_fail = 0;
#define CHAOS_MAX_FAILURE 100

void *chaos_malloc(size_t size) {
    if (chaos_pos == chaos_fail)
        return NULL;

    chaos_pos++;

    return malloc(size);
}

void chaos_free(void *obj) { free(obj); }

/* Test all potential allocation failures. */
#define chaos_loop(condition, code, cleanup)                                             \
    {                                                                                    \
        chaos_pos = chaos_fail = 0;                                                      \
        while (condition) {                                                              \
            if (chaos_fail > CHAOS_MAX_FAILURE)                                          \
                fail("too many chaos failures");                                         \
            code chaos_pos = 0;                                                          \
            chaos_fail++;                                                                \
        }                                                                                \
        cleanup                                                                          \
    }

#define chaos_loop_new_value(json, initcall)                                             \
    chaos_loop(!json, json = initcall;, json_decref(json); json = NULL;)

int test_unpack() {
    int ret = -1;
    int v1;
    int v2;
    json_error_t error;
    json_t *root = json_pack("{s:i, s:i, s:i, s:i}", "n1", 1, "n2", 2, "n3", 3, "n4", 4);

    if (!root)
        return -1;

    if (!json_unpack_ex(root, &error, JSON_STRICT, "{s:i, s:i}", "n1", &v1, "n2", &v2))
        fail("Unexpected success");

    if (json_error_code(&error) != json_error_end_of_input_expected) {
        if (json_error_code(&error) != json_error_out_of_memory)
            fail("Unexpected error code");

        goto out;
    }

    if (strcmp(error.text, "2 object item(s) left unpacked: n3, n4"))
        goto out;

    ret = 0;

out:
    json_decref(root);
    return ret;
}

int dump_chaos_callback(const char *buffer, size_t size, void *data) {
    json_t *obj = json_object();

    (void)buffer;
    (void)size;
    (void)data;

    if (!obj)
        return -1;

    json_decref(obj);

    return 0;
}

static void test_chaos() {
    json_malloc_t orig_malloc;
    json_free_t orig_free;
    json_t *json = NULL;
    json_t *obj = json_object();
    json_t *arr1 = json_array();
    json_t *arr2 = json_array();
    json_t *txt = json_string("test");
    json_t *intnum = json_integer(1);
    json_t *dblnum = json_real(0.5);
    char *dumptxt = NULL;
    json_t *dumpobj = json_pack("{s:[iiis], s:s}", "key1", 1, 2, 3, "txt", "key2", "v2");
    int keyno;

    if (!obj || !arr1 || !arr2 || !txt || !intnum || !dblnum || !dumpobj)
        fail("failed to allocate basic objects");

    json_get_alloc_funcs(&orig_malloc, &orig_free);
    json_set_alloc_funcs(chaos_malloc, chaos_free);

    chaos_loop_new_value(json, json_pack("{s:s}", "key", "value"));
    chaos_loop_new_value(json, json_pack("{s:[]}", "key"));
    chaos_loop_new_value(json, json_pack("[biIf]", 1, 1, (json_int_t)1, 1.0));
    chaos_loop_new_value(json, json_pack("[s*,s*]", "v1", "v2"));
    chaos_loop_new_value(json, json_pack("o", json_incref(txt)));
    chaos_loop_new_value(json, json_pack("O", txt));
    chaos_loop_new_value(json, json_pack("s++", "a", "long string to force realloc",
                                         "another long string to force yet another "
                                         "reallocation of the string because "
                                         "that's what we are testing."));

    chaos_loop(test_unpack(), , );

    chaos_loop(json_dump_callback(dumpobj, dump_chaos_callback, NULL, JSON_INDENT(1)),
               , );
    chaos_loop(json_dump_callback(dumpobj, dump_chaos_callback, NULL,
                                  JSON_INDENT(1) | JSON_SORT_KEYS),
               , );
    chaos_loop(!dumptxt, dumptxt = json_dumps(dumpobj, JSON_COMPACT);, free(dumptxt);
               dumptxt = NULL;);

    chaos_loop_new_value(json, json_copy(obj));
    chaos_loop_new_value(json, json_deep_copy(obj));

    chaos_loop_new_value(json, json_copy(arr1));
    chaos_loop_new_value(json, json_deep_copy(arr1));

    chaos_loop_new_value(json, json_copy(txt));
    chaos_loop_new_value(json, json_copy(intnum));
    chaos_loop_new_value(json, json_copy(dblnum));

#define JSON_LOAD_TXT "{\"n\":[1,2,3,4,5,6,7,8,9,10]}"
    chaos_loop_new_value(json, json_loads(JSON_LOAD_TXT, 0, NULL));
    chaos_loop_new_value(json, json_loadb(JSON_LOAD_TXT, strlen(JSON_LOAD_TXT), 0, NULL));

    chaos_loop_new_value(json, json_sprintf("%s", "string"));

    for (keyno = 0; keyno < 100; ++keyno) {
#if !defined(_MSC_VER) || _MSC_VER >= 1900
        /* Skip this test on old Windows compilers. */
        char testkey[10];

        snprintf(testkey, sizeof(testkey), "test%d", keyno);
        chaos_loop(json_object_set_new_nocheck(obj, testkey, json_object()), , );
#endif
        chaos_loop(json_array_append_new(arr1, json_null()), , );
        chaos_loop(json_array_insert_new(arr2, 0, json_null()), , );
    }

    chaos_loop(json_array_extend(arr1, arr2), , );
    chaos_loop(json_string_set_nocheck(txt, "test"), , );

    json_set_alloc_funcs(orig_malloc, orig_free);
    json_decref(obj);
    json_decref(arr1);
    json_decref(arr2);
    json_decref(txt);
    json_decref(intnum);
    json_decref(dblnum);
    json_decref(dumpobj);
}

static void run_tests() { test_chaos(); }
