#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

#include "../../../include/libbase64.h"
#include "../../codecs.h"

#if HAVE_SSSE3
#include <tmmintrin.h>

#include "../sse2/compare_macros.h"

#include "dec_reshuffle.c"
#include "enc_reshuffle.c"
#include "enc_translate.c"

#endif	// HAVE_SSSE3

BASE64_ENC_FUNCTION(ssse3)
{
#if HAVE_SSSE3
	#include "../generic/enc_head.c"
	#include "enc_loop.c"
	#include "../generic/enc_tail.c"
#else
	BASE64_ENC_STUB
#endif
}

BASE64_DEC_FUNCTION(ssse3)
{
#if HAVE_SSSE3
	#include "../generic/dec_head.c"
	#include "dec_loop.c"
	#include "../generic/dec_tail.c"
#else
	BASE64_DEC_STUB
#endif
}
