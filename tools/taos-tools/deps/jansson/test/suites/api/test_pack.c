/*
 * Copyright (c) 2009-2016 Petri Lehtinen <petri@digip.org>
 * Copyright (c) 2010-2012 Graeme Smecher <graeme.smecher@mail.mcgill.ca>
 *
 * Jansson is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#ifdef HAVE_CONFIG_H
#include <jansson_private_config.h>
#endif

#include <jansson_config.h>

#include "util.h"
#include <jansson.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#ifdef INFINITY
// This test triggers "warning C4756: overflow in constant arithmetic"
// in Visual Studio. This warning is triggered here by design, so disable it.
// (This can only be done on function level so we keep these tests separate)
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4756)
#endif
static void test_inifity() {
    json_error_t error;

    if (json_pack_ex(&error, 0, "f", INFINITY))
        fail("json_pack infinity incorrectly succeeded");
    check_error(json_error_numeric_overflow, "Invalid floating point value", "<args>", 1,
                1, 1);

    if (json_pack_ex(&error, 0, "[f]", INFINITY))
        fail("json_pack infinity array element incorrectly succeeded");
    check_error(json_error_numeric_overflow, "Invalid floating point value", "<args>", 1,
                2, 2);

    if (json_pack_ex(&error, 0, "{s:f}", "key", INFINITY))
        fail("json_pack infinity object value incorrectly succeeded");
    check_error(json_error_numeric_overflow, "Invalid floating point value", "<args>", 1,
                4, 4);

#ifdef _MSC_VER
#pragma warning(pop)
#endif
}
#endif // INFINITY

static void run_tests() {
    json_t *value;
    int i;
    char buffer[4] = {'t', 'e', 's', 't'};
    json_error_t error;

    /*
     * Simple, valid json_pack cases
     */
    /* true */
    value = json_pack("b", 1);
    if (!json_is_true(value))
        fail("json_pack boolean failed");
    if (value->refcount != (size_t)-1)
        fail("json_pack boolean refcount failed");
    json_decref(value);

    /* false */
    value = json_pack("b", 0);
    if (!json_is_false(value))
        fail("json_pack boolean failed");
    if (value->refcount != (size_t)-1)
        fail("json_pack boolean refcount failed");
    json_decref(value);

    /* null */
    value = json_pack("n");
    if (!json_is_null(value))
        fail("json_pack null failed");
    if (value->refcount != (size_t)-1)
        fail("json_pack null refcount failed");
    json_decref(value);

    /* integer */
    value = json_pack("i", 1);
    if (!json_is_integer(value) || json_integer_value(value) != 1)
        fail("json_pack integer failed");
    if (value->refcount != (size_t)1)
        fail("json_pack integer refcount failed");
    json_decref(value);

    /* integer from json_int_t */
    value = json_pack("I", (json_int_t)555555);
    if (!json_is_integer(value) || json_integer_value(value) != 555555)
        fail("json_pack json_int_t failed");
    if (value->refcount != (size_t)1)
        fail("json_pack integer refcount failed");
    json_decref(value);

    /* real */
    value = json_pack("f", 1.0);
    if (!json_is_real(value) || json_real_value(value) != 1.0)
        fail("json_pack real failed");
    if (value->refcount != (size_t)1)
        fail("json_pack real refcount failed");
    json_decref(value);

    /* string */
    value = json_pack("s", "test");
    if (!json_is_string(value) || strcmp("test", json_string_value(value)))
        fail("json_pack string failed");
    if (value->refcount != (size_t)1)
        fail("json_pack string refcount failed");
    json_decref(value);

    /* nullable string (defined case) */
    value = json_pack("s?", "test");
    if (!json_is_string(value) || strcmp("test", json_string_value(value)))
        fail("json_pack nullable string (defined case) failed");
    if (value->refcount != (size_t)1)
        fail("json_pack nullable string (defined case) refcount failed");
    json_decref(value);

    /* nullable string (NULL case) */
    value = json_pack("s?", NULL);
    if (!json_is_null(value))
        fail("json_pack nullable string (NULL case) failed");
    if (value->refcount != (size_t)-1)
        fail("json_pack nullable string (NULL case) refcount failed");
    json_decref(value);

    /* nullable string concatenation */
    if (json_pack_ex(&error, 0, "s?+", "test", "ing"))
        fail("json_pack failed to catch invalid format 's?+'");
    check_error(json_error_invalid_format, "Cannot use '+' on optional strings",
                "<format>", 1, 2, 2);

    /* nullable string with integer length */
    if (json_pack_ex(&error, 0, "s?#", "test", 4))
        fail("json_pack failed to catch invalid format 's?#'");
    check_error(json_error_invalid_format, "Cannot use '#' on optional strings",
                "<format>", 1, 2, 2);

    /* string and length (int) */
    value = json_pack("s#", "test asdf", 4);
    if (!json_is_string(value) || strcmp("test", json_string_value(value)))
        fail("json_pack string and length failed");
    if (value->refcount != (size_t)1)
        fail("json_pack string and length refcount failed");
    json_decref(value);

    /* string and length (size_t) */
    value = json_pack("s%", "test asdf", (size_t)4);
    if (!json_is_string(value) || strcmp("test", json_string_value(value)))
        fail("json_pack string and length failed");
    if (value->refcount != (size_t)1)
        fail("json_pack string and length refcount failed");
    json_decref(value);

    /* string and length (int), non-NUL terminated string */
    value = json_pack("s#", buffer, 4);
    if (!json_is_string(value) || strcmp("test", json_string_value(value)))
        fail("json_pack string and length (int) failed");
    if (value->refcount != (size_t)1)
        fail("json_pack string and length (int) refcount failed");
    json_decref(value);

    /* string and length (size_t), non-NUL terminated string */
    value = json_pack("s%", buffer, (size_t)4);
    if (!json_is_string(value) || strcmp("test", json_string_value(value)))
        fail("json_pack string and length (size_t) failed");
    if (value->refcount != (size_t)1)
        fail("json_pack string and length (size_t) refcount failed");
    json_decref(value);

    /* string concatenation */
    if (json_pack("s+", "test", NULL))
        fail("json_pack string concatenation succeeded with NULL string");

    value = json_pack("s++", "te", "st", "ing");
    if (!json_is_string(value) || strcmp("testing", json_string_value(value)))
        fail("json_pack string concatenation failed");
    if (value->refcount != (size_t)1)
        fail("json_pack string concatenation refcount failed");
    json_decref(value);

    /* string concatenation and length (int) */
    value = json_pack("s#+#+", "test", 1, "test", 2, "test");
    if (!json_is_string(value) || strcmp("ttetest", json_string_value(value)))
        fail("json_pack string concatenation and length (int) failed");
    if (value->refcount != (size_t)1)
        fail("json_pack string concatenation and length (int) refcount failed");
    json_decref(value);

    /* string concatenation and length (size_t) */
    value = json_pack("s%+%+", "test", (size_t)1, "test", (size_t)2, "test");
    if (!json_is_string(value) || strcmp("ttetest", json_string_value(value)))
        fail("json_pack string concatenation and length (size_t) failed");
    if (value->refcount != (size_t)1)
        fail("json_pack string concatenation and length (size_t) refcount "
             "failed");
    json_decref(value);

    /* empty object */
    value = json_pack("{}", 1.0);
    if (!json_is_object(value) || json_object_size(value) != 0)
        fail("json_pack empty object failed");
    if (value->refcount != (size_t)1)
        fail("json_pack empty object refcount failed");
    json_decref(value);

    /* empty list */
    value = json_pack("[]", 1.0);
    if (!json_is_array(value) || json_array_size(value) != 0)
        fail("json_pack empty list failed");
    if (value->refcount != (size_t)1)
        fail("json_pack empty list failed");
    json_decref(value);

    /* non-incref'd object */
    value = json_pack("o", json_integer(1));
    if (!json_is_integer(value) || json_integer_value(value) != 1)
        fail("json_pack object failed");
    if (value->refcount != (size_t)1)
        fail("json_pack integer refcount failed");
    json_decref(value);

    /* non-incref'd nullable object (defined case) */
    value = json_pack("o?", json_integer(1));
    if (!json_is_integer(value) || json_integer_value(value) != 1)
        fail("json_pack nullable object (defined case) failed");
    if (value->refcount != (size_t)1)
        fail("json_pack nullable object (defined case) refcount failed");
    json_decref(value);

    /* non-incref'd nullable object (NULL case) */
    value = json_pack("o?", NULL);
    if (!json_is_null(value))
        fail("json_pack nullable object (NULL case) failed");
    if (value->refcount != (size_t)-1)
        fail("json_pack nullable object (NULL case) refcount failed");
    json_decref(value);

    /* incref'd object */
    value = json_pack("O", json_integer(1));
    if (!json_is_integer(value) || json_integer_value(value) != 1)
        fail("json_pack object failed");
    if (value->refcount != (size_t)2)
        fail("json_pack integer refcount failed");
    json_decref(value);
    json_decref(value);

    /* incref'd nullable object (defined case) */
    value = json_pack("O?", json_integer(1));
    if (!json_is_integer(value) || json_integer_value(value) != 1)
        fail("json_pack incref'd nullable object (defined case) failed");
    if (value->refcount != (size_t)2)
        fail("json_pack incref'd nullable object (defined case) refcount "
             "failed");
    json_decref(value);
    json_decref(value);

    /* incref'd nullable object (NULL case) */
    value = json_pack("O?", NULL);
    if (!json_is_null(value))
        fail("json_pack incref'd nullable object (NULL case) failed");
    if (value->refcount != (size_t)-1)
        fail("json_pack incref'd nullable object (NULL case) refcount failed");

    /* simple object */
    value = json_pack("{s:[]}", "foo");
    if (!json_is_object(value) || json_object_size(value) != 1)
        fail("json_pack array failed");
    if (!json_is_array(json_object_get(value, "foo")))
        fail("json_pack array failed");
    if (json_object_get(value, "foo")->refcount != (size_t)1)
        fail("json_pack object refcount failed");
    json_decref(value);

    /* object with complex key */
    value = json_pack("{s+#+: []}", "foo", "barbar", 3, "baz");
    if (!json_is_object(value) || json_object_size(value) != 1)
        fail("json_pack array failed");
    if (!json_is_array(json_object_get(value, "foobarbaz")))
        fail("json_pack array failed");
    if (json_object_get(value, "foobarbaz")->refcount != (size_t)1)
        fail("json_pack object refcount failed");
    json_decref(value);

    /* object with optional members */
    value = json_pack("{s:s,s:o,s:O}", "a", NULL, "b", NULL, "c", NULL);
    if (value)
        fail("json_pack object optional incorrectly succeeded");

    value = json_pack("{s:**}", "a", NULL);
    if (value)
        fail("json_pack object optional invalid incorrectly succeeded");

    if (json_pack_ex(&error, 0, "{s:i*}", "a", 1))
        fail("json_pack object optional invalid incorrectly succeeded");
    check_error(json_error_invalid_format, "Expected format 's', got '*'", "<format>", 1,
                5, 5);

    value = json_pack("{s:s*,s:o*,s:O*}", "a", NULL, "b", NULL, "c", NULL);
    if (!json_is_object(value) || json_object_size(value) != 0)
        fail("json_pack object optional failed");
    json_decref(value);

    value = json_pack("{s:s*}", "key", "\xff\xff");
    if (value)
        fail("json_pack object optional with invalid UTF-8 incorrectly "
             "succeeded");

    if (json_pack_ex(&error, 0, "{s: s*#}", "key", "test", 1))
        fail("json_pack failed to catch invalid format 's*#'");
    check_error(json_error_invalid_format, "Cannot use '#' on optional strings",
                "<format>", 1, 6, 6);

    if (json_pack_ex(&error, 0, "{s: s*+}", "key", "test", "ing"))
        fail("json_pack failed to catch invalid format 's*+'");
    check_error(json_error_invalid_format, "Cannot use '+' on optional strings",
                "<format>", 1, 6, 6);

    /* simple array */
    value = json_pack("[i,i,i]", 0, 1, 2);
    if (!json_is_array(value) || json_array_size(value) != 3)
        fail("json_pack object failed");
    for (i = 0; i < 3; i++) {
        if (!json_is_integer(json_array_get(value, i)) ||
            json_integer_value(json_array_get(value, i)) != i)

            fail("json_pack integer array failed");
    }
    json_decref(value);

    /* simple array with optional members */
    value = json_pack("[s,o,O]", NULL, NULL, NULL);
    if (value)
        fail("json_pack array optional incorrectly succeeded");

    if (json_pack_ex(&error, 0, "[i*]", 1))
        fail("json_pack array optional invalid incorrectly succeeded");
    check_error(json_error_invalid_format, "Unexpected format character '*'", "<format>",
                1, 3, 3);

    value = json_pack("[**]", NULL);
    if (value)
        fail("json_pack array optional invalid incorrectly succeeded");
    value = json_pack("[s*,o*,O*]", NULL, NULL, NULL);
    if (!json_is_array(value) || json_array_size(value) != 0)
        fail("json_pack array optional failed");
    json_decref(value);

#ifdef NAN
    /* Invalid float values */
    if (json_pack_ex(&error, 0, "f", NAN))
        fail("json_pack NAN incorrectly succeeded");
    check_error(json_error_numeric_overflow, "Invalid floating point value", "<args>", 1,
                1, 1);

    if (json_pack_ex(&error, 0, "[f]", NAN))
        fail("json_pack NAN array element incorrectly succeeded");
    check_error(json_error_numeric_overflow, "Invalid floating point value", "<args>", 1,
                2, 2);

    if (json_pack_ex(&error, 0, "{s:f}", "key", NAN))
        fail("json_pack NAN object value incorrectly succeeded");
    check_error(json_error_numeric_overflow, "Invalid floating point value", "<args>", 1,
                4, 4);
#endif

#ifdef INFINITY
    test_inifity();
#endif

    /* Whitespace; regular string */
    value = json_pack(" s\t ", "test");
    if (!json_is_string(value) || strcmp("test", json_string_value(value)))
        fail("json_pack string (with whitespace) failed");
    json_decref(value);

    /* Whitespace; empty array */
    value = json_pack("[ ]");
    if (!json_is_array(value) || json_array_size(value) != 0)
        fail("json_pack empty array (with whitespace) failed");
    json_decref(value);

    /* Whitespace; array */
    value = json_pack("[ i , i,  i ] ", 1, 2, 3);
    if (!json_is_array(value) || json_array_size(value) != 3)
        fail("json_pack array (with whitespace) failed");
    json_decref(value);

    /*
     * Invalid cases
     */

    /* newline in format string */
    if (json_pack_ex(&error, 0, "{\n\n1"))
        fail("json_pack failed to catch invalid format '1'");
    check_error(json_error_invalid_format, "Expected format 's', got '1'", "<format>", 3,
                1, 4);

    /* mismatched open/close array/object */
    if (json_pack_ex(&error, 0, "[}"))
        fail("json_pack failed to catch mismatched '}'");
    check_error(json_error_invalid_format, "Unexpected format character '}'", "<format>",
                1, 2, 2);

    if (json_pack_ex(&error, 0, "{]"))
        fail("json_pack failed to catch mismatched ']'");
    check_error(json_error_invalid_format, "Expected format 's', got ']'", "<format>", 1,
                2, 2);

    /* missing close array */
    if (json_pack_ex(&error, 0, "["))
        fail("json_pack failed to catch missing ']'");
    check_error(json_error_invalid_format, "Unexpected end of format string", "<format>",
                1, 2, 2);

    /* missing close object */
    if (json_pack_ex(&error, 0, "{"))
        fail("json_pack failed to catch missing '}'");
    check_error(json_error_invalid_format, "Unexpected end of format string", "<format>",
                1, 2, 2);

    /* garbage after format string */
    if (json_pack_ex(&error, 0, "[i]a", 42))
        fail("json_pack failed to catch garbage after format string");
    check_error(json_error_invalid_format, "Garbage after format string", "<format>", 1,
                4, 4);

    if (json_pack_ex(&error, 0, "ia", 42))
        fail("json_pack failed to catch garbage after format string");
    check_error(json_error_invalid_format, "Garbage after format string", "<format>", 1,
                2, 2);

    /* NULL string */
    if (json_pack_ex(&error, 0, "s", NULL))
        fail("json_pack failed to catch null argument string");
    check_error(json_error_null_value, "NULL string", "<args>", 1, 1, 1);

    /* + on its own */
    if (json_pack_ex(&error, 0, "+", NULL))
        fail("json_pack failed to a lone +");
    check_error(json_error_invalid_format, "Unexpected format character '+'", "<format>",
                1, 1, 1);

    /* Empty format */
    if (json_pack_ex(&error, 0, ""))
        fail("json_pack failed to catch empty format string");
    check_error(json_error_invalid_argument, "NULL or empty format string", "<format>",
                -1, -1, 0);

    /* NULL format */
    if (json_pack_ex(&error, 0, NULL))
        fail("json_pack failed to catch NULL format string");
    check_error(json_error_invalid_argument, "NULL or empty format string", "<format>",
                -1, -1, 0);

    /* NULL key */
    if (json_pack_ex(&error, 0, "{s:i}", NULL, 1))
        fail("json_pack failed to catch NULL key");
    check_error(json_error_null_value, "NULL object key", "<args>", 1, 2, 2);

    /* NULL value followed by object still steals the object's ref */
    value = json_incref(json_object());
    if (json_pack_ex(&error, 0, "{s:s,s:o}", "badnull", NULL, "dontleak", value))
        fail("json_pack failed to catch NULL value");
    check_error(json_error_null_value, "NULL string", "<args>", 1, 4, 4);
    if (value->refcount != (size_t)1)
        fail("json_pack failed to steal reference after error.");
    json_decref(value);

    /* More complicated checks for row/columns */
    if (json_pack_ex(&error, 0, "{ {}: s }", "foo"))
        fail("json_pack failed to catch object as key");
    check_error(json_error_invalid_format, "Expected format 's', got '{'", "<format>", 1,
                3, 3);

    /* Complex object */
    if (json_pack_ex(&error, 0, "{ s: {},  s:[ii{} }", "foo", "bar", 12, 13))
        fail("json_pack failed to catch missing ]");
    check_error(json_error_invalid_format, "Unexpected format character '}'", "<format>",
                1, 19, 19);

    /* Complex array */
    if (json_pack_ex(&error, 0, "[[[[[   [[[[[  [[[[ }]]]] ]]]] ]]]]]"))
        fail("json_pack failed to catch extra }");
    check_error(json_error_invalid_format, "Unexpected format character '}'", "<format>",
                1, 21, 21);

    /* Invalid UTF-8 in object key */
    if (json_pack_ex(&error, 0, "{s:i}", "\xff\xff", 42))
        fail("json_pack failed to catch invalid UTF-8 in an object key");
    check_error(json_error_invalid_utf8, "Invalid UTF-8 object key", "<args>", 1, 2, 2);

    /* Invalid UTF-8 in a string */
    if (json_pack_ex(&error, 0, "{s:s}", "foo", "\xff\xff"))
        fail("json_pack failed to catch invalid UTF-8 in a string");
    check_error(json_error_invalid_utf8, "Invalid UTF-8 string", "<args>", 1, 4, 4);

    /* Invalid UTF-8 in an optional '?' string */
    if (json_pack_ex(&error, 0, "{s:s?}", "foo", "\xff\xff"))
        fail("json_pack failed to catch invalid UTF-8 in an optional '?' "
             "string");
    check_error(json_error_invalid_utf8, "Invalid UTF-8 string", "<args>", 1, 5, 5);

    /* Invalid UTF-8 in an optional '*' string */
    if (json_pack_ex(&error, 0, "{s:s*}", "foo", "\xff\xff"))
        fail("json_pack failed to catch invalid UTF-8 in an optional '*' "
             "string");
    check_error(json_error_invalid_utf8, "Invalid UTF-8 string", "<args>", 1, 5, 5);

    /* Invalid UTF-8 in a concatenated key */
    if (json_pack_ex(&error, 0, "{s+:i}", "\xff\xff", "concat", 42))
        fail("json_pack failed to catch invalid UTF-8 in an object key");
    check_error(json_error_invalid_utf8, "Invalid UTF-8 object key", "<args>", 1, 3, 3);

    if (json_pack_ex(&error, 0, "{s:o}", "foo", NULL))
        fail("json_pack failed to catch nullable object");
    check_error(json_error_null_value, "NULL object", "<args>", 1, 4, 4);

    if (json_pack_ex(&error, 0, "{s:O}", "foo", NULL))
        fail("json_pack failed to catch nullable incref object");
    check_error(json_error_null_value, "NULL object", "<args>", 1, 4, 4);

    if (json_pack_ex(&error, 0, "{s+:o}", "foo", "bar", NULL))
        fail("json_pack failed to catch non-nullable object value");
    check_error(json_error_null_value, "NULL object", "<args>", 1, 5, 5);

    if (json_pack_ex(&error, 0, "[1s", "Hi"))
        fail("json_pack failed to catch invalid format");
    check_error(json_error_invalid_format, "Unexpected format character '1'", "<format>",
                1, 2, 2);

    if (json_pack_ex(&error, 0, "[1s+", "Hi", "ya"))
        fail("json_pack failed to catch invalid format");
    check_error(json_error_invalid_format, "Unexpected format character '1'", "<format>",
                1, 2, 2);

    if (json_pack_ex(&error, 0, "[so]", NULL, json_object()))
        fail("json_pack failed to catch NULL value");
    check_error(json_error_null_value, "NULL string", "<args>", 1, 2, 2);
}
