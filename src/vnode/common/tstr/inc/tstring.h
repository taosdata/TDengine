/* A dynamic string library
 */ 
#if !defined(_TD_TSTRING_H_)
#define _TD_TSTRING_H_

#include <stdint.h>

typedef char* tstring_t;

// The string header
typedef struct {
    int32_t strLen;  // Allocated data space
    int32_t avail;   // Available space
    char data[];
} STStrHdr;

// Get the data length of the string
#define TSTRLEN(pstr) ((STStrHdr *)pstr)->strLen
// Get the available space
#define TSTRAVAIL(pstr) ((STStrHdr *)pstr)->avail
// Get the real allocated string length
#define TSTRRLEN(pstr) ((STStrHdr *)pstr)->strLen + sizeof(STStrHdr)

#endif // _TD_TSTRING_H_
