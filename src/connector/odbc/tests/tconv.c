#include "../src/todbc_log.h"

#ifdef _MSC_VER
#include <winsock2.h>
#include <windows.h>
#endif

#include <iconv.h>


#include <stdio.h>
#include <string.h>

static void usage(const char *arg0);
static int  do_conv(iconv_t cnv, FILE *fin, FILE *fout);

int main(int argc, char *argv[]) {
  const char *from_enc = "UTF-8";
  const char *to_enc   = "UTF-8";
  const char *dst_file = NULL;
  const char *src      = NULL;
#ifdef _MSC_VER
  from_enc = "CP936";
  to_enc   = "CP936";
#endif
  for (int i = 1; i < argc; i++) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h") == 0) {
      usage(argv[0]);
      return 0;
    } else if (strcmp(arg, "-f") == 0 ) {
      i += 1;
      if (i>=argc) {
        fprintf(stderr, "expecing <from_enc>, but got nothing\n");
        return 1;
      }
      from_enc = argv[i];
      continue;
    } else if (strcmp(arg, "-t") == 0 ) {
      i += 1;
      if (i>=argc) {
        fprintf(stderr, "expecing <to_enc>, but got nothing\n");
        return 1;
      }
      to_enc = argv[i];
      continue;
    } else if (strcmp(arg, "-o") == 0 ) {
      i += 1;
      if (i>=argc) {
        fprintf(stderr, "expecing <dst_file>, but got nothing\n");
        return 1;
      }
      dst_file = argv[i];
      continue;
    } else if (arg[0]=='-') {
      fprintf(stderr, "unknown argument: [%s]\n", arg);
      return 1;
    } else {
      if (src) {
        fprintf(stderr, "does not allow multiple files\n");
        return 1;
      }
      src = arg;
      continue;
    }
  }
  int r = -1;
  FILE *fin = src ? fopen(src, "rb") : stdin;
  FILE *fout = dst_file ? fopen(dst_file, "wb") : stdout;
  iconv_t cnv = iconv_open(to_enc, from_enc);
  do {
    if (!fin) {
      fprintf(stderr, "failed to open file [%s]\n", src);
      break;
    }
    if (!fout) {
      fprintf(stderr, "failed to open file [%s]\n", dst_file);
      break;
    }
#ifdef _MSC_VER
    if (fout == stdout) {
      r = _setmode(_fileno(fout), _O_BINARY);
      if (r == -1) {
        fprintf(stderr, "Cannot set binary mode for output stream: %d[%s]\n", errno, strerror(errno));
      }
    }
#endif

    if (cnv == (iconv_t)-1) {
      fprintf(stderr, "failed to open conv from [%s] to [%s]: [%s]\n", from_enc, to_enc, strerror(errno));
      break;
    }
    r = do_conv(cnv, fin, fout);
    iconv_close(cnv);
    cnv = (iconv_t)-1;
  } while (0);
  if (fin && fin != stdin) fclose(fin);
  if (fout && fout != stdout) fclose(fout);
  return r ? 1 : 0;
}

static void usage(const char *arg0) {
  fprintf(stderr, "%s -h | [-f <from_enc>] [-t <to_enc>] [-o <dst file>] [file]\n", arg0);
  return;
}

#define IN_SIZE     (64*1024)
#define OUT_SIZE    (8*IN_SIZE)
static int do_conv(iconv_t cnv, FILE *fin, FILE *fout) {
  int r = 0;
  char src[IN_SIZE];
  size_t slen = sizeof(src);
  char dst[OUT_SIZE];
  size_t dlen = sizeof(dst);
  char *start = src;
  while (!feof(fin)) {
    slen = (size_t)(src + sizeof(src)  - start);
    size_t n = fread(start, 1, slen, fin);
    if (n>0) {
      char   *ss = src;
      size_t  sl = n;
      while (sl) {
        char   *dd = dst;
        size_t  dn = dlen;
        size_t v = iconv(cnv, &ss, &sl, &dd, &dn);
        if (v==(size_t)-1) {
          int err = errno;
          if (err == EILSEQ) {
            fprintf(stderr, "failed to convert: [%s]\n", strerror(err));
            r = -1;
            break;
          }
          if (err == EINVAL) {
            fprintf(stderr, "[%s]\n", strerror(errno));
            size_t ava = (size_t)(src + sizeof(src) - ss);
            memcpy(src, ss, ava);
            start = ss;
          } else {
            fprintf(stderr, "internal logic error: [%s]\n", strerror(errno));
            r = -1;
            break;
          }
        }
        n = fwrite(dst, 1, (size_t)(dd-dst), fout);
        if (n<dd-dst) {
          fprintf(stderr, "failed to write: [%s]\n", strerror(errno));
          r = -1;
          break;
        }
      }
      if (r) break;
    }
  }
  return r ? -1 : 0;
}

