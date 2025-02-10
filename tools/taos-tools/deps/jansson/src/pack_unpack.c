/*
 * Copyright (c) 2009-2016 Petri Lehtinen <petri@digip.org>
 * Copyright (c) 2011-2012 Graeme Smecher <graeme.smecher@mail.mcgill.ca>
 *
 * Jansson is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#include "jansson.h"
#include "jansson_private.h"
#include "utf.h"
#include <string.h>

typedef struct {
    int line;
    int column;
    size_t pos;
    char token;
} token_t;

typedef struct {
    const char *start;
    const char *fmt;
    token_t prev_token;
    token_t token;
    token_t next_token;
    json_error_t *error;
    size_t flags;
    int line;
    int column;
    size_t pos;
    int has_error;
} scanner_t;

#define token(scanner) ((scanner)->token.token)

static const char *const type_names[] = {"object", "array", "string", "integer",
                                         "real",   "true",  "false",  "null"};

#define type_name(x) type_names[json_typeof(x)]

static const char unpack_value_starters[] = "{[siIbfFOon";

static void scanner_init(scanner_t *s, json_error_t *error, size_t flags,
                         const char *fmt) {
    s->error = error;
    s->flags = flags;
    s->fmt = s->start = fmt;
    memset(&s->prev_token, 0, sizeof(token_t));
    memset(&s->token, 0, sizeof(token_t));
    memset(&s->next_token, 0, sizeof(token_t));
    s->line = 1;
    s->column = 0;
    s->pos = 0;
    s->has_error = 0;
}

static void next_token(scanner_t *s) {
    const char *t;
    s->prev_token = s->token;

    if (s->next_token.line) {
        s->token = s->next_token;
        s->next_token.line = 0;
        return;
    }

    if (!token(s) && !*s->fmt)
        return;

    t = s->fmt;
    s->column++;
    s->pos++;

    /* skip space and ignored chars */
    while (*t == ' ' || *t == '\t' || *t == '\n' || *t == ',' || *t == ':') {
        if (*t == '\n') {
            s->line++;
            s->column = 1;
        } else
            s->column++;

        s->pos++;
        t++;
    }

    s->token.token = *t;
    s->token.line = s->line;
    s->token.column = s->column;
    s->token.pos = s->pos;

    if (*t)
        t++;
    s->fmt = t;
}

static void prev_token(scanner_t *s) {
    s->next_token = s->token;
    s->token = s->prev_token;
}

static void set_error(scanner_t *s, const char *source, enum json_error_code code,
                      const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);

    jsonp_error_vset(s->error, s->token.line, s->token.column, s->token.pos, code, fmt,
                     ap);

    jsonp_error_set_source(s->error, source);

    va_end(ap);
}

static json_t *pack(scanner_t *s, va_list *ap);

/* ours will be set to 1 if jsonp_free() must be called for the result
   afterwards */
static char *read_string(scanner_t *s, va_list *ap, const char *purpose, size_t *out_len,
                         int *ours, int optional) {
    char t;
    strbuffer_t strbuff;
    const char *str;
    size_t length;

    next_token(s);
    t = token(s);
    prev_token(s);

    *ours = 0;
    if (t != '#' && t != '%' && t != '+') {
        /* Optimize the simple case */
        str = va_arg(*ap, const char *);

        if (!str) {
            if (!optional) {
                set_error(s, "<args>", json_error_null_value, "NULL %s", purpose);
                s->has_error = 1;
            }
            return NULL;
        }

        length = strlen(str);

        if (!utf8_check_string(str, length)) {
            set_error(s, "<args>", json_error_invalid_utf8, "Invalid UTF-8 %s", purpose);
            s->has_error = 1;
            return NULL;
        }

        *out_len = length;
        return (char *)str;
    } else if (optional) {
        set_error(s, "<format>", json_error_invalid_format,
                  "Cannot use '%c' on optional strings", t);
        s->has_error = 1;

        return NULL;
    }

    if (strbuffer_init(&strbuff)) {
        set_error(s, "<internal>", json_error_out_of_memory, "Out of memory");
        s->has_error = 1;
    }

    while (1) {
        str = va_arg(*ap, const char *);
        if (!str) {
            set_error(s, "<args>", json_error_null_value, "NULL %s", purpose);
            s->has_error = 1;
        }

        next_token(s);

        if (token(s) == '#') {
            length = va_arg(*ap, int);
        } else if (token(s) == '%') {
            length = va_arg(*ap, size_t);
        } else {
            prev_token(s);
            length = s->has_error ? 0 : strlen(str);
        }

        if (!s->has_error && strbuffer_append_bytes(&strbuff, str, length) == -1) {
            set_error(s, "<internal>", json_error_out_of_memory, "Out of memory");
            s->has_error = 1;
        }

        next_token(s);
        if (token(s) != '+') {
            prev_token(s);
            break;
        }
    }

    if (s->has_error) {
        strbuffer_close(&strbuff);
        return NULL;
    }

    if (!utf8_check_string(strbuff.value, strbuff.length)) {
        set_error(s, "<args>", json_error_invalid_utf8, "Invalid UTF-8 %s", purpose);
        strbuffer_close(&strbuff);
        s->has_error = 1;
        return NULL;
    }

    *out_len = strbuff.length;
    *ours = 1;
    return strbuffer_steal_value(&strbuff);
}

static json_t *pack_object(scanner_t *s, va_list *ap) {
    json_t *object = json_object();
    next_token(s);

    while (token(s) != '}') {
        char *key;
        size_t len;
        int ours;
        json_t *value;
        char valueOptional;

        if (!token(s)) {
            set_error(s, "<format>", json_error_invalid_format,
                      "Unexpected end of format string");
            goto error;
        }

        if (token(s) != 's') {
            set_error(s, "<format>", json_error_invalid_format,
                      "Expected format 's', got '%c'", token(s));
            goto error;
        }

        key = read_string(s, ap, "object key", &len, &ours, 0);

        next_token(s);

        next_token(s);
        valueOptional = token(s);
        prev_token(s);

        value = pack(s, ap);
        if (!value) {
            if (ours)
                jsonp_free(key);

            if (valueOptional != '*') {
                set_error(s, "<args>", json_error_null_value, "NULL object value");
                s->has_error = 1;
            }

            next_token(s);
            continue;
        }

        if (s->has_error)
            json_decref(value);

        if (!s->has_error && json_object_set_new_nocheck(object, key, value)) {
            set_error(s, "<internal>", json_error_out_of_memory,
                      "Unable to add key \"%s\"", key);
            s->has_error = 1;
        }

        if (ours)
            jsonp_free(key);

        next_token(s);
    }

    if (!s->has_error)
        return object;

error:
    json_decref(object);
    return NULL;
}

static json_t *pack_array(scanner_t *s, va_list *ap) {
    json_t *array = json_array();
    next_token(s);

    while (token(s) != ']') {
        json_t *value;
        char valueOptional;

        if (!token(s)) {
            set_error(s, "<format>", json_error_invalid_format,
                      "Unexpected end of format string");
            /* Format string errors are unrecoverable. */
            goto error;
        }

        next_token(s);
        valueOptional = token(s);
        prev_token(s);

        value = pack(s, ap);
        if (!value) {
            if (valueOptional != '*') {
                s->has_error = 1;
            }

            next_token(s);
            continue;
        }

        if (s->has_error)
            json_decref(value);

        if (!s->has_error && json_array_append_new(array, value)) {
            set_error(s, "<internal>", json_error_out_of_memory,
                      "Unable to append to array");
            s->has_error = 1;
        }

        next_token(s);
    }

    if (!s->has_error)
        return array;

error:
    json_decref(array);
    return NULL;
}

static json_t *pack_string(scanner_t *s, va_list *ap) {
    char *str;
    char t;
    size_t len;
    int ours;
    int optional;

    next_token(s);
    t = token(s);
    optional = t == '?' || t == '*';
    if (!optional)
        prev_token(s);

    str = read_string(s, ap, "string", &len, &ours, optional);

    if (!str)
        return t == '?' && !s->has_error ? json_null() : NULL;

    if (s->has_error) {
        /* It's impossible to reach this point if ours != 0, do not free str. */
        return NULL;
    }

    if (ours)
        return jsonp_stringn_nocheck_own(str, len);

    return json_stringn_nocheck(str, len);
}

static json_t *pack_object_inter(scanner_t *s, va_list *ap, int need_incref) {
    json_t *json;
    char ntoken;

    next_token(s);
    ntoken = token(s);

    if (ntoken != '?' && ntoken != '*')
        prev_token(s);

    json = va_arg(*ap, json_t *);

    if (json)
        return need_incref ? json_incref(json) : json;

    switch (ntoken) {
        case '?':
            return json_null();
        case '*':
            return NULL;
        default:
            break;
    }

    set_error(s, "<args>", json_error_null_value, "NULL object");
    s->has_error = 1;
    return NULL;
}

static json_t *pack_integer(scanner_t *s, json_int_t value) {
    json_t *json = json_integer(value);

    if (!json) {
        set_error(s, "<internal>", json_error_out_of_memory, "Out of memory");
        s->has_error = 1;
    }

    return json;
}

static json_t *pack_real(scanner_t *s, double value) {
    /* Allocate without setting value so we can identify OOM error. */
    json_t *json = json_real(0.0);

    if (!json) {
        set_error(s, "<internal>", json_error_out_of_memory, "Out of memory");
        s->has_error = 1;

        return NULL;
    }

    if (json_real_set(json, value)) {
        json_decref(json);

        set_error(s, "<args>", json_error_numeric_overflow,
                  "Invalid floating point value");
        s->has_error = 1;

        return NULL;
    }

    return json;
}

static json_t *pack(scanner_t *s, va_list *ap) {
    switch (token(s)) {
        case '{':
            return pack_object(s, ap);

        case '[':
            return pack_array(s, ap);

        case 's': /* string */
            return pack_string(s, ap);

        case 'n': /* null */
            return json_null();

        case 'b': /* boolean */
            return va_arg(*ap, int) ? json_true() : json_false();

        case 'i': /* integer from int */
            return pack_integer(s, va_arg(*ap, int));

        case 'I': /* integer from json_int_t */
            return pack_integer(s, va_arg(*ap, json_int_t));

        case 'f': /* real */
            return pack_real(s, va_arg(*ap, double));

        case 'O': /* a json_t object; increments refcount */
            return pack_object_inter(s, ap, 1);

        case 'o': /* a json_t object; doesn't increment refcount */
            return pack_object_inter(s, ap, 0);

        default:
            set_error(s, "<format>", json_error_invalid_format,
                      "Unexpected format character '%c'", token(s));
            s->has_error = 1;
            return NULL;
    }
}

static int unpack(scanner_t *s, json_t *root, va_list *ap);

static int unpack_object(scanner_t *s, json_t *root, va_list *ap) {
    int ret = -1;
    int strict = 0;
    int gotopt = 0;

    /* Use a set (emulated by a hashtable) to check that all object
       keys are accessed. Checking that the correct number of keys
       were accessed is not enough, as the same key can be unpacked
       multiple times.
    */
    hashtable_t key_set;

    if (hashtable_init(&key_set)) {
        set_error(s, "<internal>", json_error_out_of_memory, "Out of memory");
        return -1;
    }

    if (root && !json_is_object(root)) {
        set_error(s, "<validation>", json_error_wrong_type, "Expected object, got %s",
                  type_name(root));
        goto out;
    }
    next_token(s);

    while (token(s) != '}') {
        const char *key;
        json_t *value;
        int opt = 0;

        if (strict != 0) {
            set_error(s, "<format>", json_error_invalid_format,
                      "Expected '}' after '%c', got '%c'", (strict == 1 ? '!' : '*'),
                      token(s));
            goto out;
        }

        if (!token(s)) {
            set_error(s, "<format>", json_error_invalid_format,
                      "Unexpected end of format string");
            goto out;
        }

        if (token(s) == '!' || token(s) == '*') {
            strict = (token(s) == '!' ? 1 : -1);
            next_token(s);
            continue;
        }

        if (token(s) != 's') {
            set_error(s, "<format>", json_error_invalid_format,
                      "Expected format 's', got '%c'", token(s));
            goto out;
        }

        key = va_arg(*ap, const char *);
        if (!key) {
            set_error(s, "<args>", json_error_null_value, "NULL object key");
            goto out;
        }

        next_token(s);

        if (token(s) == '?') {
            opt = gotopt = 1;
            next_token(s);
        }

        if (!root) {
            /* skipping */
            value = NULL;
        } else {
            value = json_object_get(root, key);
            if (!value && !opt) {
                set_error(s, "<validation>", json_error_item_not_found,
                          "Object item not found: %s", key);
                goto out;
            }
        }

        if (unpack(s, value, ap))
            goto out;

        hashtable_set(&key_set, key, strlen(key), json_null());
        next_token(s);
    }

    if (strict == 0 && (s->flags & JSON_STRICT))
        strict = 1;

    if (root && strict == 1) {
        /* We need to check that all non optional items have been parsed */
        const char *key;
        size_t key_len;
        /* keys_res is 1 for uninitialized, 0 for success, -1 for error. */
        int keys_res = 1;
        strbuffer_t unrecognized_keys;
        json_t *value;
        long unpacked = 0;

        if (gotopt || json_object_size(root) != key_set.size) {
            json_object_foreach(root, key, value) {
                key_len = strlen(key);
                if (!hashtable_get(&key_set, key, key_len)) {
                    unpacked++;

                    /* Save unrecognized keys for the error message */
                    if (keys_res == 1) {
                        keys_res = strbuffer_init(&unrecognized_keys);
                    } else if (!keys_res) {
                        keys_res = strbuffer_append_bytes(&unrecognized_keys, ", ", 2);
                    }

                    if (!keys_res)
                        keys_res =
                            strbuffer_append_bytes(&unrecognized_keys, key, key_len);
                }
            }
        }
        if (unpacked) {
            set_error(s, "<validation>", json_error_end_of_input_expected,
                      "%li object item(s) left unpacked: %s", unpacked,
                      keys_res ? "<unknown>" : strbuffer_value(&unrecognized_keys));
            strbuffer_close(&unrecognized_keys);
            goto out;
        }
    }

    ret = 0;

out:
    hashtable_close(&key_set);
    return ret;
}

static int unpack_array(scanner_t *s, json_t *root, va_list *ap) {
    size_t i = 0;
    int strict = 0;

    if (root && !json_is_array(root)) {
        set_error(s, "<validation>", json_error_wrong_type, "Expected array, got %s",
                  type_name(root));
        return -1;
    }
    next_token(s);

    while (token(s) != ']') {
        json_t *value;

        if (strict != 0) {
            set_error(s, "<format>", json_error_invalid_format,
                      "Expected ']' after '%c', got '%c'", (strict == 1 ? '!' : '*'),
                      token(s));
            return -1;
        }

        if (!token(s)) {
            set_error(s, "<format>", json_error_invalid_format,
                      "Unexpected end of format string");
            return -1;
        }

        if (token(s) == '!' || token(s) == '*') {
            strict = (token(s) == '!' ? 1 : -1);
            next_token(s);
            continue;
        }

        if (!strchr(unpack_value_starters, token(s))) {
            set_error(s, "<format>", json_error_invalid_format,
                      "Unexpected format character '%c'", token(s));
            return -1;
        }

        if (!root) {
            /* skipping */
            value = NULL;
        } else {
            value = json_array_get(root, i);
            if (!value) {
                set_error(s, "<validation>", json_error_index_out_of_range,
                          "Array index %lu out of range", (unsigned long)i);
                return -1;
            }
        }

        if (unpack(s, value, ap))
            return -1;

        next_token(s);
        i++;
    }

    if (strict == 0 && (s->flags & JSON_STRICT))
        strict = 1;

    if (root && strict == 1 && i != json_array_size(root)) {
        long diff = (long)json_array_size(root) - (long)i;
        set_error(s, "<validation>", json_error_end_of_input_expected,
                  "%li array item(s) left unpacked", diff);
        return -1;
    }

    return 0;
}

static int unpack(scanner_t *s, json_t *root, va_list *ap) {
    switch (token(s)) {
        case '{':
            return unpack_object(s, root, ap);

        case '[':
            return unpack_array(s, root, ap);

        case 's':
            if (root && !json_is_string(root)) {
                set_error(s, "<validation>", json_error_wrong_type,
                          "Expected string, got %s", type_name(root));
                return -1;
            }

            if (!(s->flags & JSON_VALIDATE_ONLY)) {
                const char **str_target;
                size_t *len_target = NULL;

                str_target = va_arg(*ap, const char **);
                if (!str_target) {
                    set_error(s, "<args>", json_error_null_value, "NULL string argument");
                    return -1;
                }

                next_token(s);

                if (token(s) == '%') {
                    len_target = va_arg(*ap, size_t *);
                    if (!len_target) {
                        set_error(s, "<args>", json_error_null_value,
                                  "NULL string length argument");
                        return -1;
                    }
                } else
                    prev_token(s);

                if (root) {
                    *str_target = json_string_value(root);
                    if (len_target)
                        *len_target = json_string_length(root);
                }
            }
            return 0;

        case 'i':
            if (root && !json_is_integer(root)) {
                set_error(s, "<validation>", json_error_wrong_type,
                          "Expected integer, got %s", type_name(root));
                return -1;
            }

            if (!(s->flags & JSON_VALIDATE_ONLY)) {
                int *target = va_arg(*ap, int *);
                if (root)
                    *target = (int)json_integer_value(root);
            }

            return 0;

        case 'I':
            if (root && !json_is_integer(root)) {
                set_error(s, "<validation>", json_error_wrong_type,
                          "Expected integer, got %s", type_name(root));
                return -1;
            }

            if (!(s->flags & JSON_VALIDATE_ONLY)) {
                json_int_t *target = va_arg(*ap, json_int_t *);
                if (root)
                    *target = json_integer_value(root);
            }

            return 0;

        case 'b':
            if (root && !json_is_boolean(root)) {
                set_error(s, "<validation>", json_error_wrong_type,
                          "Expected true or false, got %s", type_name(root));
                return -1;
            }

            if (!(s->flags & JSON_VALIDATE_ONLY)) {
                int *target = va_arg(*ap, int *);
                if (root)
                    *target = json_is_true(root);
            }

            return 0;

        case 'f':
            if (root && !json_is_real(root)) {
                set_error(s, "<validation>", json_error_wrong_type,
                          "Expected real, got %s", type_name(root));
                return -1;
            }

            if (!(s->flags & JSON_VALIDATE_ONLY)) {
                double *target = va_arg(*ap, double *);
                if (root)
                    *target = json_real_value(root);
            }

            return 0;

        case 'F':
            if (root && !json_is_number(root)) {
                set_error(s, "<validation>", json_error_wrong_type,
                          "Expected real or integer, got %s", type_name(root));
                return -1;
            }

            if (!(s->flags & JSON_VALIDATE_ONLY)) {
                double *target = va_arg(*ap, double *);
                if (root)
                    *target = json_number_value(root);
            }

            return 0;

        case 'O':
            if (root && !(s->flags & JSON_VALIDATE_ONLY))
                json_incref(root);
            /* Fall through */

        case 'o':
            if (!(s->flags & JSON_VALIDATE_ONLY)) {
                json_t **target = va_arg(*ap, json_t **);
                if (root)
                    *target = root;
            }

            return 0;

        case 'n':
            /* Never assign, just validate */
            if (root && !json_is_null(root)) {
                set_error(s, "<validation>", json_error_wrong_type,
                          "Expected null, got %s", type_name(root));
                return -1;
            }
            return 0;

        default:
            set_error(s, "<format>", json_error_invalid_format,
                      "Unexpected format character '%c'", token(s));
            return -1;
    }
}

json_t *json_vpack_ex(json_error_t *error, size_t flags, const char *fmt, va_list ap) {
    scanner_t s;
    va_list ap_copy;
    json_t *value;

    if (!fmt || !*fmt) {
        jsonp_error_init(error, "<format>");
        jsonp_error_set(error, -1, -1, 0, json_error_invalid_argument,
                        "NULL or empty format string");
        return NULL;
    }
    jsonp_error_init(error, NULL);

    scanner_init(&s, error, flags, fmt);
    next_token(&s);

    va_copy(ap_copy, ap);
    value = pack(&s, &ap_copy);
    va_end(ap_copy);

    /* This will cover all situations where s.has_error is true */
    if (!value)
        return NULL;

    next_token(&s);
    if (token(&s)) {
        json_decref(value);
        set_error(&s, "<format>", json_error_invalid_format,
                  "Garbage after format string");
        return NULL;
    }

    return value;
}

json_t *json_pack_ex(json_error_t *error, size_t flags, const char *fmt, ...) {
    json_t *value;
    va_list ap;

    va_start(ap, fmt);
    value = json_vpack_ex(error, flags, fmt, ap);
    va_end(ap);

    return value;
}

json_t *json_pack(const char *fmt, ...) {
    json_t *value;
    va_list ap;

    va_start(ap, fmt);
    value = json_vpack_ex(NULL, 0, fmt, ap);
    va_end(ap);

    return value;
}

int json_vunpack_ex(json_t *root, json_error_t *error, size_t flags, const char *fmt,
                    va_list ap) {
    scanner_t s;
    va_list ap_copy;

    if (!root) {
        jsonp_error_init(error, "<root>");
        jsonp_error_set(error, -1, -1, 0, json_error_null_value, "NULL root value");
        return -1;
    }

    if (!fmt || !*fmt) {
        jsonp_error_init(error, "<format>");
        jsonp_error_set(error, -1, -1, 0, json_error_invalid_argument,
                        "NULL or empty format string");
        return -1;
    }
    jsonp_error_init(error, NULL);

    scanner_init(&s, error, flags, fmt);
    next_token(&s);

    va_copy(ap_copy, ap);
    if (unpack(&s, root, &ap_copy)) {
        va_end(ap_copy);
        return -1;
    }
    va_end(ap_copy);

    next_token(&s);
    if (token(&s)) {
        set_error(&s, "<format>", json_error_invalid_format,
                  "Garbage after format string");
        return -1;
    }

    return 0;
}

int json_unpack_ex(json_t *root, json_error_t *error, size_t flags, const char *fmt,
                   ...) {
    int ret;
    va_list ap;

    va_start(ap, fmt);
    ret = json_vunpack_ex(root, error, flags, fmt, ap);
    va_end(ap);

    return ret;
}

int json_unpack(json_t *root, const char *fmt, ...) {
    int ret;
    va_list ap;

    va_start(ap, fmt);
    ret = json_vunpack_ex(root, NULL, 0, fmt, ap);
    va_end(ap);

    return ret;
}
