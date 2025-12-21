#include "ttqMisc.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <grp.h>
#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>

#include "ttqLogging.h"

FILE *tmqtt__fopen(const char *path, const char *mode, bool restrict_read) {
  FILE       *fptr;
  struct stat statbuf;

  if (restrict_read) {
    mode_t old_mask;

    old_mask = umask(0077);
    fptr = fopen(path, mode);
    umask(old_mask);
  } else {
    fptr = fopen(path, mode);
  }
  if (!fptr) return NULL;

  if (fstat(fileno(fptr), &statbuf) < 0) {
    fclose(fptr);
    return NULL;
  }

  if (restrict_read) {
    if (statbuf.st_mode & S_IRWXO) {
      ttq_log(NULL, TTQ_LOG_WARNING,
              "Warning: File %s has world readable permissions. Future versions will refuse to load this file.\n"
              "To fix this, use `chmod 0700 %s`.",
              path, path);
    }
    if (statbuf.st_uid != getuid()) {
      char          buf[4096];
      struct passwd pw, *result;

      getpwuid_r(getuid(), &pw, buf, sizeof(buf), &result);
      if (result) {
        ttq_log(NULL, TTQ_LOG_WARNING,
                "Warning: File %s owner is not %s. Future versions will refuse to load this file."
                "To fix this, use `chown %s %s`.",
                path, result->pw_name, result->pw_name, path);
      }
    }
    if (statbuf.st_gid != getgid()) {
      char         buf[4096];
      struct group grp, *result;

      getgrgid_r(getgid(), &grp, buf, sizeof(buf), &result);
      if (result) {
        ttq_log(NULL, TTQ_LOG_WARNING,
                "Warning: File %s group is not %s. Future versions will refuse to load this file.", path,
                result->gr_name);
      }
    }
  }

  if (!S_ISREG(statbuf.st_mode) && !S_ISLNK(statbuf.st_mode)) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: %s is not a file.", path);
    fclose(fptr);
    return NULL;
  }
  return fptr;
}

char *misc__trimblanks(char *str) {
  char *endptr;

  if (str == NULL) return NULL;

  while (isspace(str[0])) {
    str++;
  }
  endptr = &str[strlen(str) - 1];
  while (endptr > str && isspace(endptr[0])) {
    endptr[0] = '\0';
    endptr--;
  }
  return str;
}
