#include "jansson_private.h"
#include <string.h>

void jsonp_error_init(json_error_t *error, const char *source) {
    if (error) {
        error->text[0] = '\0';
        error->line = -1;
        error->column = -1;
        error->position = 0;
        if (source)
            jsonp_error_set_source(error, source);
        else
            error->source[0] = '\0';
    }
}

void jsonp_error_set_source(json_error_t *error, const char *source) {
    size_t length;

    if (!error || !source)
        return;

    length = strlen(source);
    if (length < JSON_ERROR_SOURCE_LENGTH)
        strncpy(error->source, source, length + 1);
    else {
        size_t extra = length - JSON_ERROR_SOURCE_LENGTH + 4;
        memcpy(error->source, "...", 3);
        strncpy(error->source + 3, source + extra, length - extra + 1);
    }
}

void jsonp_error_set(json_error_t *error, int line, int column, size_t position,
                     enum json_error_code code, const char *msg, ...) {
    va_list ap;

    va_start(ap, msg);
    jsonp_error_vset(error, line, column, position, code, msg, ap);
    va_end(ap);
}

void jsonp_error_vset(json_error_t *error, int line, int column, size_t position,
                      enum json_error_code code, const char *msg, va_list ap) {
    if (!error)
        return;

    if (error->text[0] != '\0') {
        /* error already set */
        return;
    }

    error->line = line;
    error->column = column;
    error->position = (int)position;

    vsnprintf(error->text, JSON_ERROR_TEXT_LENGTH - 1, msg, ap);
    error->text[JSON_ERROR_TEXT_LENGTH - 2] = '\0';
    error->text[JSON_ERROR_TEXT_LENGTH - 1] = code;
}
