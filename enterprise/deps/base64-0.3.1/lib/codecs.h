#include "config.h"

// Function parameters for encoding functions:
#define BASE64_ENC_PARAMS			\
	( struct base64_state	*state		\
	, const char		*src		\
	, size_t		 srclen		\
	, char			*out		\
	, size_t		*outlen		\
	)

// Function parameters for decoding functions:
#define BASE64_DEC_PARAMS			\
	( struct base64_state	*state		\
	, const char		*src		\
	, size_t		 srclen		\
	, char			*out		\
	, size_t		*outlen		\
	)

// Function signature for encoding functions:
#define BASE64_ENC_FUNCTION(arch)		\
	void					\
	base64_stream_encode_ ## arch		\
	BASE64_ENC_PARAMS

// Function signature for decoding functions:
#define BASE64_DEC_FUNCTION(arch)		\
	int					\
	base64_stream_decode_ ## arch		\
	BASE64_DEC_PARAMS

// Cast away unused variable, silence compiler:
#define UNUSED(x)		((void)(x))

// Stub function when encoder arch unsupported:
#define BASE64_ENC_STUB				\
	UNUSED(state);				\
	UNUSED(src);				\
	UNUSED(srclen);				\
	UNUSED(out);				\
						\
	*outlen = 0;

// Stub function when decoder arch unsupported:
#define BASE64_DEC_STUB				\
	UNUSED(state);				\
	UNUSED(src);				\
	UNUSED(srclen);				\
	UNUSED(out);				\
	UNUSED(outlen);				\
						\
	return -1;

struct codec
{
	void (* enc) BASE64_ENC_PARAMS;
	int  (* dec) BASE64_DEC_PARAMS;
};

// Define machine endianness. This is for GCC:
#if (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
	#define BASE64_LITTLE_ENDIAN 1
#else
	#define BASE64_LITTLE_ENDIAN 0
#endif

// This is for Clang:
#ifdef __LITTLE_ENDIAN__
	#define BASE64_LITTLE_ENDIAN 1
#endif

#ifdef __BIG_ENDIAN__
	#define BASE64_LITTLE_ENDIAN 0
#endif

// Endian conversion functions:
#if BASE64_LITTLE_ENDIAN
	#if defined(_MSC_VER)
		// Microsoft Visual C++:
		#define cpu_to_be32(x)	_byteswap_ulong(x)
		#define cpu_to_be64(x)	_byteswap_uint64(x)
		#define be32_to_cpu(x)	_byteswap_ulong(x)
		#define be64_to_cpu(x)	_byteswap_uint64(x)
	#else
		// GCC and Clang:
		#define cpu_to_be32(x)	__builtin_bswap32(x)
		#define cpu_to_be64(x)	__builtin_bswap64(x)
		#define be32_to_cpu(x)	__builtin_bswap32(x)
		#define be64_to_cpu(x)	__builtin_bswap64(x)
	#endif
#else
	// No conversion needed:
	#define cpu_to_be32(x)	(x)
	#define cpu_to_be64(x)	(x)
	#define be32_to_cpu(x)	(x)
	#define be64_to_cpu(x)	(x)
#endif 

// detect word size
#ifdef _INTEGRAL_MAX_BITS
#define BASE64_WORDSIZE _INTEGRAL_MAX_BITS
#else
#define BASE64_WORDSIZE __WORDSIZE
#endif

// end-of-file definitions
// Almost end-of-file when waiting for the last '=' character:
#define BASE64_AEOF 1
// End-of-file when stream end has been reached or invalid input provided:
#define BASE64_EOF 2

// GCC 7 defaults to issuing a warning for fallthrough in switch statements,
// unless the fallthrough cases are marked with an attribute. As we use
// fallthrough deliberately, define an alias for the attribute:
#if __GNUC__ >= 7
  #define BASE64_FALLTHROUGH  __attribute__((fallthrough));
#else
  #define BASE64_FALLTHROUGH
#endif

extern void codec_choose (struct codec *, int flags);

// These tables are used by all codecs
// for fallback plain encoding/decoding:
extern const uint8_t base64_table_enc[];
extern const uint8_t base64_table_dec[];

extern const uint32_t base64_table_dec_d0[];
extern const uint32_t base64_table_dec_d1[];
extern const uint32_t base64_table_dec_d2[];
extern const uint32_t base64_table_dec_d3[];
