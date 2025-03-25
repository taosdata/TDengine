#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define ALLOW_FORBID_FUNC
#include "geosWrapper.h"

static int verbose = 0;
static int success = 0;
static int failure = 0;

static void process(const char *arg)
{
  unsigned char *geom = NULL;
  size_t         size = 0;
  int32_t r = doGeomFromText(arg, &geom, &size);
  if (r) {
    if (verbose) {
      fprintf(stderr, "%s:%s\n", arg, getGeosErrMsg(r));
    }
    failure += 1;
  } else {
    success += 1;
  }
  if (geom) {
    geosFreeBuffer(geom);
    geom = NULL;
  }
}

static void usage(const char *app)
{
  fprintf(stderr, "%s [options] <wkt...>\n", app);
  fprintf(stderr, " options:\n");
  fprintf(stderr, "  -h                show this help page\n");
  fprintf(stderr, "  -x                process with non-pcre2 method\n");
  fprintf(stderr, "  -n <count>        loops for <count> times\n");
}

int main(int argc, char *argv[])
{
  int opt;
  int count = 1;
  int with_pcre2 = 1;

  while ((opt = getopt(argc, argv, ":hxn:v")) != -1) {
    switch (opt) {
      case 'h':
        usage(argv[0]);
        return 0;
      case 'x':
        with_pcre2 = 0;
        break;
      case 'n':
        count = atoi(optarg);
        if (count <= 0) {
          fprintf(stderr, "option 'n' requires an positive integer\n");
          return 1;
        }
        break;
      case 'v':
        verbose = 1;
        break;
      case ':':
        fprintf(stderr, "option requires an argument -- %s\n", argv[optind-1]+1);
        return 1;
      case '?':
        fprintf(stderr, "invalid option -- %s\n", argv[optind-1]+1);
        return 1;
    }
  }

  set_with_pcre2(with_pcre2);
  int32_t r = initCtxGeomFromText();
  if (r) {
    fprintf(stderr, "initCtxGeomFromText failed\n");
    return 1;
  }

  for (int i=optind; i<argc; ++i) {
    printf("argument = %s\n", argv[i]);
    for (int j=0; j<count; ++j) {
      process(argv[i]);
    }
  }

  fprintf(stderr, "success(%d); failure(%d)\n", success, failure);

  return 0;
}

