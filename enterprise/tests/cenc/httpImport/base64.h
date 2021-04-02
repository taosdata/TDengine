#ifndef _BASE64_H_INCLUDED_
#define _BASE64_H_INCLUDED_


char *base64_encode(const unsigned char *value, int vlen);
unsigned char *base64_decode(const char *value, int inlen, int *outlen);

#endif
