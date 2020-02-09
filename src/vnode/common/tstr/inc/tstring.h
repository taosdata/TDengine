/* A dynamic string library
 */ 
#if !defined(_TD_TSTRING_H_)
#define _TD_TSTRING_H_

#include <stdint.h>
#include <string.h>

#define TD_TSTRING_INIT_SIZE 16

typedef char* tstring_t;

// The string header
typedef struct {
    int32_t space;  // Allocated data space
    char data[];
} STStrHdr;

// Get the data length of the string
#define TSTRLEN(pstr) strlen((char *)pstr)
// Get the real allocated string length
#define TSTRSPACE(pstr) (*(int32_t *)((char *)pstr - sizeof(STStrHdr)))
// Get the available space
#define TSTAVAIL(pstr) (TSTRSPACE(pstr) - TSTRLEN(pstr))

// Create an empty tstring with default size
tstring_t tdNewTString();
// Create an empty tstring with size
tstring_t tdNewTStringWithSize(uint32_t size);
// Create a tstring with a init value
tstring_t tdNewTStringWithValue(char *value);
// Create a tstring with a init value & size
tstring_t tdNewTStringWithValueSize(char *value, uint32_t size);

tstring_t tstrcat(tstring_t dest, tstring_t src);
int32_t   tstrcmp(tstring_t str1, tstring_t str2);
int32_t   tstrncmp(tstring_t str1, tstring_t str2, int32_t n);


#endif // _TD_TSTRING_H_
