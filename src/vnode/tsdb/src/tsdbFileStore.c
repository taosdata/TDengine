#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tsdbFile.h"

const char *tsdbFileSuffix[] = {
    ".head",  // TSDB_FILE_TYPE_HEAD
    ".data",  // TSDB_FILE_TYPE_DATA
    ".last",  // TSDB_FILE_TYPE_LAST
    ".meta"   // TSDB_FILE_TYPE_META
};

char *tsdbGetFileName(char *dirName, char *fname, TSDB_FILE_TYPE type) {
  if (!IS_VALID_TSDB_FILE_TYPE(type)) return NULL;

  char *fileName = (char *)malloc(strlen(dirName) + strlen(fname) + strlen(tsdbFileSuffix[type]) + 5);
  if (fileName == NULL) return NULL;

  sprintf(fileName, "%s/%s%s", dirName, fname, tsdbFileSuffix[type]);
  return fileName;
}