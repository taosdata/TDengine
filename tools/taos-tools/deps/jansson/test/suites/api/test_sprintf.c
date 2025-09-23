#include "util.h"
#include <jansson.h>
#include <string.h>

static void test_sprintf() {
    json_t *s = json_sprintf("foo bar %d", 42);
    if (!s)
        fail("json_sprintf returned NULL");
    if (!json_is_string(s))
        fail("json_sprintf didn't return a JSON string");
    if (strcmp(json_string_value(s), "foo bar 42"))
        fail("json_sprintf generated an unexpected string");

    json_decref(s);

    s = json_sprintf("%s", "");
    if (!s)
        fail("json_sprintf returned NULL");
    if (!json_is_string(s))
        fail("json_sprintf didn't return a JSON string");
    if (json_string_length(s) != 0)
        fail("string is not empty");
    json_decref(s);

    if (json_sprintf("%s", "\xff\xff"))
        fail("json_sprintf unexpected success with invalid UTF");
}

static void run_tests() { test_sprintf(); }
