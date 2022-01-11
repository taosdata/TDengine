#ifndef TRAFT_UTIL_H
#define TRAFT_UTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"

int dirOK(const char *path);
int splitString(const char *str, char *separator, char (*arr)[TOKEN_LEN], int n_arr);

#ifdef __cplusplus
}
#endif

#endif
