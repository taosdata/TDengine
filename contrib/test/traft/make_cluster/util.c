#include "util.h"
#include <dirent.h>
#include <stdlib.h>
#include <string.h>

int dirOK(const char *path) {
  DIR *dir = opendir(path);
  if (dir != NULL) {
    closedir(dir);
    return 1;
  } else {
    return 0;
  }
}

int splitString(const char *str, char *separator, char (*arr)[TOKEN_LEN], int n_arr) {
  if (n_arr <= 0) {
    return -1;
  }

  char *tmp = (char *)malloc(strlen(str) + 1);
  strcpy(tmp, str);
  char *context;
  int   n = 0;

  char *token = strtok_r(tmp, separator, &context);
  if (!token) {
    goto ret;
  }
  strncpy(arr[n], token, TOKEN_LEN);
  n++;

  while (1) {
    token = strtok_r(NULL, separator, &context);
    if (!token || n >= n_arr) {
      goto ret;
    }
    strncpy(arr[n], token, TOKEN_LEN);
    n++;
  }

ret:
  free(tmp);
  return n;
}
