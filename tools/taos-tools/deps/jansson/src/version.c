/*
 * Copyright (c) 2019 Sean Bright <sean.bright@gmail.com>
 *
 * Jansson is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "jansson.h"

const char *jansson_version_str(void) { return JANSSON_VERSION; }

int jansson_version_cmp(int major, int minor, int micro) {
    int diff;

    if ((diff = JANSSON_MAJOR_VERSION - major)) {
        return diff;
    }

    if ((diff = JANSSON_MINOR_VERSION - minor)) {
        return diff;
    }

    return JANSSON_MICRO_VERSION - micro;
}
