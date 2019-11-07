#include <stdint.h>
#include <stddef.h>
#ifdef _OPENMP
#include <omp.h>
#endif

#include "../include/libbase64.h"
#include "codecs.h"
#include "tables.h"

// These static function pointers are initialized once when the library is
// first used, and remain in use for the remaining lifetime of the program.
// The idea being that CPU features don't change at runtime.
static struct codec codec = { NULL, NULL };

const uint8_t
base64_table_enc[] =
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	"abcdefghijklmnopqrstuvwxyz"
	"0123456789+/";

// In the lookup table below, note that the value for '=' (character 61) is
// 254, not 255. This character is used for in-band signaling of the end of
// the datastream, and we will use that later. The characters A-Z, a-z, 0-9
// and + / are mapped to their "decoded" values. The other bytes all map to
// the value 255, which flags them as "invalid input".

const uint8_t
base64_table_dec[] =
{
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,		//   0..15
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,		//  16..31
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,  62, 255, 255, 255,  63,		//  32..47
	 52,  53,  54,  55,  56,  57,  58,  59,  60,  61, 255, 255, 255, 254, 255, 255,		//  48..63
	255,   0,   1,   2,   3,   4,   5,   6,   7,   8,   9,  10,  11,  12,  13,  14,		//  64..79
	 15,  16,  17,  18,  19,  20,  21,  22,  23,  24,  25, 255, 255, 255, 255, 255,		//  80..95
	255,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,  36,  37,  38,  39,  40,		//  96..111
	 41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51, 255, 255, 255, 255, 255,		// 112..127
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,		// 128..143
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
	255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
};

void
base64_stream_encode_init (struct base64_state *state, int flags)
{
	// If any of the codec flags are set, redo choice:
	if (codec.enc == NULL || flags & 0xFF) {
		codec_choose(&codec, flags);
	}
	state->eof = 0;
	state->bytes = 0;
	state->carry = 0;
	state->flags = flags;
}

void
base64_stream_encode
	( struct base64_state	*state
	, const char		*src
	, size_t		 srclen
	, char			*out
	, size_t		*outlen
	)
{
	codec.enc(state, src, srclen, out, outlen);
}

void
base64_stream_encode_final
	( struct base64_state	*state
	, char			*out
	, size_t		*outlen
	)
{
	uint8_t *o = (uint8_t *)out;

	if (state->bytes == 1) {
		*o++ = base64_table_enc[state->carry];
		*o++ = '=';
		*o++ = '=';
		*outlen = 3;
		return;
	}
	if (state->bytes == 2) {
		*o++ = base64_table_enc[state->carry];
		*o++ = '=';
		*outlen = 2;
		return;
	}
	*outlen = 0;
}

void
base64_stream_decode_init (struct base64_state *state, int flags)
{
	// If any of the codec flags are set, redo choice:
	if (codec.dec == NULL || flags & 0xFF) {
		codec_choose(&codec, flags);
	}
	state->eof = 0;
	state->bytes = 0;
	state->carry = 0;
	state->flags = flags;
}

int
base64_stream_decode
	( struct base64_state	*state
	, const char		*src
	, size_t		 srclen
	, char			*out
	, size_t		*outlen
	)
{
	return codec.dec(state, src, srclen, out, outlen);
}

#ifdef _OPENMP

	// Due to the overhead of initializing OpenMP and creating a team of
	// threads, we require the data length to be larger than a threshold:
	#define OMP_THRESHOLD 20000

	// Conditionally include OpenMP-accelerated codec implementations:
	#include "lib_openmp.c"
#endif

void
base64_encode
	( const char	*src
	, size_t	 srclen
	, char		*out
	, size_t	*outlen
	, int		 flags
	)
{
	size_t s;
	size_t t;
	struct base64_state state;

	#ifdef _OPENMP
	if (srclen >= OMP_THRESHOLD) {
		base64_encode_openmp(src, srclen, out, outlen, flags);
		return;
	}
	#endif

	// Init the stream reader:
	base64_stream_encode_init(&state, flags);

	// Feed the whole string to the stream reader:
	base64_stream_encode(&state, src, srclen, out, &s);

	// Finalize the stream by writing trailer if any:
	base64_stream_encode_final(&state, out + s, &t);

	// Final output length is stream length plus tail:
	*outlen = s + t;
}

int
base64_decode
	( const char	*src
	, size_t	 srclen
	, char		*out
	, size_t	*outlen
	, int		 flags
	)
{
	int ret;
	struct base64_state state;

	#ifdef _OPENMP
	if (srclen >= OMP_THRESHOLD) {
		return base64_decode_openmp(src, srclen, out, outlen, flags);
	}
	#endif

	// Init the stream reader:
	base64_stream_decode_init(&state, flags);

	// Feed the whole string to the stream reader:
	ret = base64_stream_decode(&state, src, srclen, out, outlen);

	// If when decoding a whole block, we're still waiting for input then fail:
	if (ret && (state.bytes == 0)) {
		return ret;
	}
	return 0;
}
