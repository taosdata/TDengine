// Read source 4 bytes at a time
// Since we might be writing one byte more than needed,
// we need to make sure there will still be some room
// for one extra byte in o.
// This will be the case if srclen > 0 when the loop
// is exited
while (srclen > 4)
{
	union {
		uint32_t asint;
		uint8_t  aschar[4];
	} x;

	x.asint = base64_table_dec_d0[c[0]]
	        | base64_table_dec_d1[c[1]]
	        | base64_table_dec_d2[c[2]]
	        | base64_table_dec_d3[c[3]];

#if BASE64_LITTLE_ENDIAN
	// LUTs for little-endian set Most Significant Bit
	// in case of invalid character
	if (x.asint & 0x80000000U) break;
#else
	// LUTs for big-endian set Least Significant Bit
	// in case of invalid character
	if (x.asint & 1U) break;
#endif

#if HAVE_FAST_UNALIGNED_ACCESS
	// This might segfault or be too slow on
	// some architectures, do this only if specified
	// with HAVE_FAST_UNALIGNED_ACCESS macro
	// We write one byte more than needed
	*(uint32_t*)o = x.asint;
#else
	// Fallback, write bytes one by one
	o[0] = x.aschar[0];
	o[1] = x.aschar[1];
	o[2] = x.aschar[2];
#endif

	c += 4;
	o += 3;
	outl += 3;
	srclen -= 4;
}
