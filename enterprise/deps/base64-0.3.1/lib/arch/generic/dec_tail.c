		if (srclen-- == 0) {
			ret = 1;
			break;
		}
		if ((q = base64_table_dec[*c++]) >= 254) {
			st.eof = BASE64_EOF;
			// Treat character '=' as invalid for byte 0:
			break;
		}
		st.carry = q << 2;
		st.bytes++;

		// Deliberate fallthrough:
		BASE64_FALLTHROUGH

	case 1:	if (srclen-- == 0) {
			ret = 1;
			break;
		}
		if ((q = base64_table_dec[*c++]) >= 254) {
			st.eof = BASE64_EOF;
			// Treat character '=' as invalid for byte 1:
			break;
		}
		*o++ = st.carry | (q >> 4);
		st.carry = q << 4;
		st.bytes++;
		outl++;

		// Deliberate fallthrough:
		BASE64_FALLTHROUGH

	case 2:	if (srclen-- == 0) {
			ret = 1;
			break;
		}
		if ((q = base64_table_dec[*c++]) >= 254) {
			st.bytes++;
			// When q == 254, the input char is '='.
			// Check if next byte is also '=':
			if (q == 254) {
				if (srclen-- != 0) {
					st.bytes = 0;
					// EOF:
					st.eof = BASE64_EOF;
					q = base64_table_dec[*c++];
					ret = ((q == 254) && (srclen == 0)) ? 1 : 0;
					break;
				}
				else {
					// Almost EOF
					st.eof = BASE64_AEOF;
					ret = 1;
					break;
				}
			}
			// If we get here, there was an error:
			break;
		}
		*o++ = st.carry | (q >> 2);
		st.carry = q << 6;
		st.bytes++;
		outl++;

		// Deliberate fallthrough:
		BASE64_FALLTHROUGH

	case 3:	if (srclen-- == 0) {
			ret = 1;
			break;
		}
		if ((q = base64_table_dec[*c++]) >= 254) {
			st.bytes = 0;
			st.eof = BASE64_EOF;
			// When q == 254, the input char is '='. Return 1 and EOF.
			// When q == 255, the input char is invalid. Return 0 and EOF.
			ret = ((q == 254) && (srclen == 0)) ? 1 : 0;
			break;
		}
		*o++ = st.carry | q;
		st.carry = 0;
		st.bytes = 0;
		outl++;
	}
}
state->eof = st.eof;
state->bytes = st.bytes;
state->carry = st.carry;
*outlen = outl;
return ret;
