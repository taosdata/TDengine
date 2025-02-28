#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <inttypes.h>

#include "jansson.h"

static int enable_diags;

#define FUZZ_DEBUG(FMT, ...)                                                  \
        if (enable_diags)                                                     \
        {                                                                     \
          fprintf(stderr, FMT, ##__VA_ARGS__);                                \
          fprintf(stderr, "\n");                                              \
        }


static int json_dump_counter(const char *buffer, size_t size, void *data)
{
  uint64_t *counter = reinterpret_cast<uint64_t *>(data);
  *counter += size;
  return 0;
}


#define NUM_COMMAND_BYTES  (sizeof(size_t) + sizeof(size_t) + 1)

#define FUZZ_DUMP_CALLBACK 0x00
#define FUZZ_DUMP_STRING   0x01

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
  json_error_t error;
  unsigned char dump_mode;

  // Enable or disable diagnostics based on the FUZZ_VERBOSE environment flag.
  enable_diags = (getenv("FUZZ_VERBOSE") != NULL);

  FUZZ_DEBUG("Input data length: %zd", size);

  if (size < NUM_COMMAND_BYTES)
  {
    return 0;
  }

  // Use the first sizeof(size_t) bytes as load flags.
  size_t load_flags = *(const size_t*)data;
  data += sizeof(size_t);

  FUZZ_DEBUG("load_flags: 0x%zx\n"
             "& JSON_REJECT_DUPLICATES =  0x%zx\n"
             "& JSON_DECODE_ANY =         0x%zx\n"
             "& JSON_DISABLE_EOF_CHECK =  0x%zx\n"
             "& JSON_DECODE_INT_AS_REAL = 0x%zx\n"
             "& JSON_ALLOW_NUL =          0x%zx\n",
             load_flags,
             load_flags & JSON_REJECT_DUPLICATES,
             load_flags & JSON_DECODE_ANY,
             load_flags & JSON_DISABLE_EOF_CHECK,
             load_flags & JSON_DECODE_INT_AS_REAL,
             load_flags & JSON_ALLOW_NUL);

  // Use the next sizeof(size_t) bytes as dump flags.
  size_t dump_flags = *(const size_t*)data;
  data += sizeof(size_t);

  FUZZ_DEBUG("dump_flags: 0x%zx\n"
             "& JSON_MAX_INDENT =     0x%zx\n"
             "& JSON_COMPACT =        0x%zx\n"
             "& JSON_ENSURE_ASCII =   0x%zx\n"
             "& JSON_SORT_KEYS =      0x%zx\n"
             "& JSON_PRESERVE_ORDER = 0x%zx\n"
             "& JSON_ENCODE_ANY =     0x%zx\n"
             "& JSON_ESCAPE_SLASH =   0x%zx\n"
             "& JSON_REAL_PRECISION = 0x%zx\n"
             "& JSON_EMBED =          0x%zx\n",
             dump_flags,
             dump_flags & JSON_MAX_INDENT,
             dump_flags & JSON_COMPACT,
             dump_flags & JSON_ENSURE_ASCII,
             dump_flags & JSON_SORT_KEYS,
             dump_flags & JSON_PRESERVE_ORDER,
             dump_flags & JSON_ENCODE_ANY,
             dump_flags & JSON_ESCAPE_SLASH,
             ((dump_flags >> 11) & 0x1F) << 11,
             dump_flags & JSON_EMBED);

  // Use the next byte as the dump mode.
  dump_mode = data[0];
  data++;

  FUZZ_DEBUG("dump_mode: 0x%x", (unsigned int)dump_mode);

  // Remove the command bytes from the size total.
  size -= NUM_COMMAND_BYTES;

  // Attempt to load the remainder of the data with the given load flags.
  const char* text = reinterpret_cast<const char *>(data);
  json_t* jobj = json_loadb(text, size, load_flags, &error);

  if (jobj == NULL)
  {
    return 0;
  }

  if (dump_mode & FUZZ_DUMP_STRING)
  {
    // Dump as a string. Remove indents so that we don't run out of memory.
    char *out = json_dumps(jobj, dump_flags & ~JSON_MAX_INDENT);
    if (out != NULL)
    {
      free(out);
    }
  }
  else
  {
    // Default is callback mode.
    //
    // Attempt to dump the loaded json object with the given dump flags.
    uint64_t counter = 0;

    json_dump_callback(jobj, json_dump_counter, &counter, dump_flags);
    FUZZ_DEBUG("Counter function counted %" PRIu64 " bytes.", counter);
  }

  if (jobj)
  {
    json_decref(jobj);
  }

  return 0;
}
