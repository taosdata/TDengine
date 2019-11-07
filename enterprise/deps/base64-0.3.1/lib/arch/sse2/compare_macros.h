#define CMPGT(s,n)	_mm_cmpgt_epi8((s), _mm_set1_epi8(n))
#define CMPEQ(s,n)	_mm_cmpeq_epi8((s), _mm_set1_epi8(n))
#define REPLACE(s,n)	_mm_and_si128((s), _mm_set1_epi8(n))
#define RANGE(s,a,b)	_mm_andnot_si128(CMPGT((s), (b)), CMPGT((s), (a) - 1))
