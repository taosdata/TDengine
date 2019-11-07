		if (srclen-- == 0) {
			break;
		}
		*o++ = base64_table_enc[*c >> 2];
		st.carry = (*c++ << 4) & 0x30;
		st.bytes++;
		outl += 1;

		// Deliberate fallthrough:
		BASE64_FALLTHROUGH

	case 1:	if (srclen-- == 0) {
			break;
		}
		*o++ = base64_table_enc[st.carry | (*c >> 4)];
		st.carry = (*c++ << 2) & 0x3C;
		st.bytes++;
		outl += 1;

		// Deliberate fallthrough:
		BASE64_FALLTHROUGH

	case 2:	if (srclen-- == 0) {
			break;
		}
		*o++ = base64_table_enc[st.carry | (*c >> 6)];
		*o++ = base64_table_enc[*c++ & 0x3F];
		st.bytes = 0;
		outl += 2;
	}
}
state->bytes = st.bytes;
state->carry = st.carry;
*outlen = outl;
