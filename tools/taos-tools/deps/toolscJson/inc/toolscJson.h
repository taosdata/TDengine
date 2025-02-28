/*
  Copyright (c) 2009-2017 Dave Gamble and tools_cJSON contributors

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
*/

#ifndef tools_cJSON__h
#define tools_cJSON__h

#ifdef __cplusplus
extern "C"
{
#endif

/* project version */
#define CJSON_VERSION_MAJOR 1
#define CJSON_VERSION_MINOR 5
#define CJSON_VERSION_PATCH 9

#include <stddef.h>
#include <stdint.h>

/* tools_cJSON Types: */
#define tools_cJSON_Invalid (0)
#define tools_cJSON_False  (1 << 0)
#define tools_cJSON_True   (1 << 1)
#define tools_cJSON_NULL   (1 << 2)
#define tools_cJSON_Number (1 << 3)
#define tools_cJSON_String (1 << 4)
#define tools_cJSON_Array  (1 << 5)
#define tools_cJSON_Object (1 << 6)
#define tools_cJSON_Raw    (1 << 7) /* raw json */

#define tools_cJSON_IsReference 256
#define tools_cJSON_StringIsConst 512

/* The tools_cJSON structure: */
typedef struct tools_cJSON
{
    /* next/prev allow you to walk array/object chains. Alternatively, use GetArraySize/GetArrayItem/GetObjectItem */
    struct tools_cJSON *next;
    struct tools_cJSON *prev;
    /* An array or object item will have a child pointer pointing to a chain of the items in the array/object. */
    struct tools_cJSON *child;

    /* The type of the item, as above. */
    int type;

    /* The item's string, if type==tools_cJSON_String  and type == tools_cJSON_Raw */
    char *valuestring;
    /* writing to valueint is DEPRECATED, use tools_cJSON_SetNumberValue instead */
    int64_t valueint;
    /* The item's number, if type==tools_cJSON_Number */
    double valuedouble;

    /* The item's name string, if this item is the child of, or is in the list of subitems of an object. */
    char *string;

    //Keep the original string of number
    char numberstring[64];
} tools_cJSON;

typedef struct tools_cJSON_Hooks
{
      void *(*malloc_fn)(size_t sz);
      void (*free_fn)(void *ptr);
} tools_cJSON_Hooks;

typedef int tools_cJSON_bool;

#if !defined(__WINDOWS__) && (defined(WIN32) || defined(WIN64) || defined(_MSC_VER) || defined(_WIN32))
#define __WINDOWS__
#endif
#ifdef __WINDOWS__

/* When compiling for windows, we specify a specific calling convention to avoid issues where we are being called from a project with a different default calling convention.  For windows you have 2 define options:

CJSON_HIDE_SYMBOLS - Define this in the case where you don't want to ever dllexport symbols
CJSON_EXPORT_SYMBOLS - Define this on library build when you want to dllexport symbols (default)
CJSON_IMPORT_SYMBOLS - Define this if you want to dllimport symbol

For *nix builds that support visibility attribute, you can define similar behavior by

setting default visibility to hidden by adding
-fvisibility=hidden (for gcc)
or
-xldscope=hidden (for sun cc)
to CFLAGS

then using the CJSON_API_VISIBILITY flag to "export" the same symbols the way CJSON_EXPORT_SYMBOLS does

*/

/* export symbols by default, this is necessary for copy pasting the C and header file */
#if !defined(CJSON_HIDE_SYMBOLS) && !defined(CJSON_IMPORT_SYMBOLS) && !defined(CJSON_EXPORT_SYMBOLS)
#define CJSON_EXPORT_SYMBOLS
#endif

#if defined(CJSON_HIDE_SYMBOLS)
#define CJSON_PUBLIC(type)   type __stdcall
#elif defined(CJSON_EXPORT_SYMBOLS)
#define CJSON_PUBLIC(type)   __declspec(dllexport) type __stdcall
#elif defined(CJSON_IMPORT_SYMBOLS)
#define CJSON_PUBLIC(type)   __declspec(dllimport) type __stdcall
#endif
#else /* !WIN32 */
#if (defined(__GNUC__) || defined(__SUNPRO_CC) || defined (__SUNPRO_C)) && defined(CJSON_API_VISIBILITY)
#define CJSON_PUBLIC(type)   __attribute__((visibility("default"))) type
#else
#define CJSON_PUBLIC(type) type
#endif
#endif

/* Limits how deeply nested arrays/objects can be before tools_cJSON rejects to parse them.
 * This is to prevent stack overflows. */
#ifndef CJSON_NESTING_LIMIT
#define CJSON_NESTING_LIMIT 1000
#endif

/* returns the version of tools_cJSON as a string */
CJSON_PUBLIC(const char*) tools_cJSON_Version(void);

/* Supply malloc, realloc and free functions to tools_cJSON */
CJSON_PUBLIC(void) tools_cJSON_InitHooks(tools_cJSON_Hooks* hooks);

/* Memory Management: the caller is always responsible to free the results from all variants of tools_cJSON_Parse (with tools_cJSON_Delete) and tools_cJSON_Print (with stdlib free, tools_cJSON_Hooks.free_fn, or tools_cJSON_free as appropriate). The exception is tools_cJSON_PrintPreallocated, where the caller has full responsibility of the buffer. */
/* Supply a block of JSON, and this returns a tools_cJSON object you can interrogate. */
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_Parse(const char *value);
/* ParseWithOpts allows you to require (and check) that the JSON is null terminated, and to retrieve the pointer to the final byte parsed. */
/* If you supply a ptr in return_parse_end and parsing fails, then return_parse_end will contain a pointer to the error so will match tools_cJSON_GetErrorPtr(). */
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_ParseWithOpts(const char *value, const char **return_parse_end, tools_cJSON_bool require_null_terminated);

/* Render a tools_cJSON entity to text for transfer/storage. */
CJSON_PUBLIC(char *) tools_cJSON_Print(const tools_cJSON *item);
/* Render a tools_cJSON entity to text for transfer/storage without any formatting. */
CJSON_PUBLIC(char *) tools_cJSON_PrintUnformatted(const tools_cJSON *item);
/* Render a tools_cJSON entity to text using a buffered strategy. prebuffer is a guess at the final size. guessing well reduces reallocation. fmt=0 gives unformatted, =1 gives formatted */
CJSON_PUBLIC(char *) tools_cJSON_PrintBuffered(const tools_cJSON *item, int prebuffer, tools_cJSON_bool fmt);
/* Render a tools_cJSON entity to text using a buffer already allocated in memory with given length. Returns 1 on success and 0 on failure. */
/* NOTE: tools_cJSON is not always 100% accurate in estimating how much memory it will use, so to be safe allocate 5 bytes more than you actually need */
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_PrintPreallocated(tools_cJSON *item, char *buffer, const int length, const tools_cJSON_bool format);
/* Delete a tools_cJSON entity and all subentities. */
CJSON_PUBLIC(void) tools_cJSON_Delete(tools_cJSON *c);

/* Returns the number of items in an array (or object). */
CJSON_PUBLIC(int) tools_cJSON_GetArraySize(const tools_cJSON *array);
/* Retrieve item number "item" from array "array". Returns NULL if unsuccessful. */
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_GetArrayItem(const tools_cJSON *array, int index);
/* Get item "string" from object. Case insensitive. */
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_GetObjectItem(const tools_cJSON * const object, const char * const string);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_GetObjectItemCaseSensitive(const tools_cJSON * const object, const char * const string);
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_HasObjectItem(const tools_cJSON *object, const char *string);
/* For analysing failed parses. This returns a pointer to the parse error. You'll probably need to look a few chars back to make sense of it. Defined when tools_cJSON_Parse() returns 0. 0 when tools_cJSON_Parse() succeeds. */
CJSON_PUBLIC(const char *) tools_cJSON_GetErrorPtr(void);

/* These functions check the type of an item */
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_IsInvalid(const tools_cJSON * const item);
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_IsFalse(const tools_cJSON * const item);
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_IsTrue(const tools_cJSON * const item);
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_IsBool(const tools_cJSON * const item);
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_IsNull(const tools_cJSON * const item);
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_IsNumber(const tools_cJSON * const item);
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_IsString(const tools_cJSON * const item);
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_IsArray(const tools_cJSON * const item);
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_IsObject(const tools_cJSON * const item);
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_IsRaw(const tools_cJSON * const item);

/* These calls create a tools_cJSON item of the appropriate type. */
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateNull(void);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateTrue(void);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateFalse(void);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateBool(tools_cJSON_bool boolean);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateNumber(double num);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateString(const char *string);
/* raw json */
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateRaw(const char *raw);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateArray(void);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateObject(void);

/* These utilities create an Array of count items. */
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateIntArray(const int *numbers, int count);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateFloatArray(const float *numbers, int count);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateDoubleArray(const double *numbers, int count);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_CreateStringArray(const char **strings, int count);

/* Append item to the specified array/object. */
CJSON_PUBLIC(void) tools_cJSON_AddItemToArray(tools_cJSON *array, tools_cJSON *item);
CJSON_PUBLIC(void) tools_cJSON_AddItemToObject(tools_cJSON *object, const char *string, tools_cJSON *item);
/* Use this when string is definitely const (i.e. a literal, or as good as), and will definitely survive the tools_cJSON object.
 * WARNING: When this function was used, make sure to always check that (item->type & tools_cJSON_StringIsConst) is zero before
 * writing to `item->string` */
CJSON_PUBLIC(void) tools_cJSON_AddItemToObjectCS(tools_cJSON *object, const char *string, tools_cJSON *item);
/* Append reference to item to the specified array/object. Use this when you want to add an existing tools_cJSON to a new tools_cJSON, but don't want to corrupt your existing tools_cJSON. */
CJSON_PUBLIC(void) tools_cJSON_AddItemReferenceToArray(tools_cJSON *array, tools_cJSON *item);
CJSON_PUBLIC(void) tools_cJSON_AddItemReferenceToObject(tools_cJSON *object, const char *string, tools_cJSON *item);

/* Remove/Detatch items from Arrays/Objects. */
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_DetachItemViaPointer(tools_cJSON *parent, tools_cJSON * const item);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_DetachItemFromArray(tools_cJSON *array, int which);
CJSON_PUBLIC(void) tools_cJSON_DeleteItemFromArray(tools_cJSON *array, int which);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_DetachItemFromObject(tools_cJSON *object, const char *string);
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_DetachItemFromObjectCaseSensitive(tools_cJSON *object, const char *string);
CJSON_PUBLIC(void) tools_cJSON_DeleteItemFromObject(tools_cJSON *object, const char *string);
CJSON_PUBLIC(void) tools_cJSON_DeleteItemFromObjectCaseSensitive(tools_cJSON *object, const char *string);

/* Update array items. */
CJSON_PUBLIC(void) tools_cJSON_InsertItemInArray(tools_cJSON *array, int which, tools_cJSON *newitem); /* Shifts pre-existing items to the right. */
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_ReplaceItemViaPointer(tools_cJSON * const parent, tools_cJSON * const item, tools_cJSON * replacement);
CJSON_PUBLIC(void) tools_cJSON_ReplaceItemInArray(tools_cJSON *array, int which, tools_cJSON *newitem);
CJSON_PUBLIC(void) tools_cJSON_ReplaceItemInObject(tools_cJSON *object,const char *string,tools_cJSON *newitem);
CJSON_PUBLIC(void) tools_cJSON_ReplaceItemInObjectCaseSensitive(tools_cJSON *object,const char *string,tools_cJSON *newitem);

/* Duplicate a tools_cJSON item */
CJSON_PUBLIC(tools_cJSON *) tools_cJSON_Duplicate(const tools_cJSON *item, tools_cJSON_bool recurse);
/* Duplicate will create a new, identical tools_cJSON item to the one you pass, in new memory that will
need to be released. With recurse!=0, it will duplicate any children connected to the item.
The item->next and ->prev pointers are always zero on return from Duplicate. */
/* Recursively compare two tools_cJSON items for equality. If either a or b is NULL or invalid, they will be considered unequal.
 * case_sensitive determines if object keys are treated case sensitive (1) or case insensitive (0) */
CJSON_PUBLIC(tools_cJSON_bool) tools_cJSON_Compare(const tools_cJSON * const a, const tools_cJSON * const b, const tools_cJSON_bool case_sensitive);


CJSON_PUBLIC(void) tools_cJSON_Minify(char *json);

/* Macros for creating things quickly. */
#define tools_cJSON_AddNullToObject(object,name) tools_cJSON_AddItemToObject(object, name, tools_cJSON_CreateNull())
#define tools_cJSON_AddTrueToObject(object,name) tools_cJSON_AddItemToObject(object, name, tools_cJSON_CreateTrue())
#define tools_cJSON_AddFalseToObject(object,name) tools_cJSON_AddItemToObject(object, name, tools_cJSON_CreateFalse())
#define tools_cJSON_AddBoolToObject(object,name,b) tools_cJSON_AddItemToObject(object, name, tools_cJSON_CreateBool(b))
#define tools_cJSON_AddNumberToObject(object,name,n) tools_cJSON_AddItemToObject(object, name, tools_cJSON_CreateNumber(n))
#define tools_cJSON_AddStringToObject(object,name,s) tools_cJSON_AddItemToObject(object, name, tools_cJSON_CreateString(s))
#define tools_cJSON_AddRawToObject(object,name,s) tools_cJSON_AddItemToObject(object, name, tools_cJSON_CreateRaw(s))

/* When assigning an integer value, it needs to be propagated to valuedouble too. */
#define tools_cJSON_SetIntValue(object, number) ((object) ? (object)->valueint = (object)->valuedouble = (number) : (number))
/* helper for the tools_cJSON_SetNumberValue macro */
CJSON_PUBLIC(double) tools_cJSON_SetNumberHelper(tools_cJSON *object, double number);
#define tools_cJSON_SetNumberValue(object, number) ((object != NULL) ? tools_cJSON_SetNumberHelper(object, (double)number) : (number))

/* Macro for iterating over an array or object */
#define tools_cJSON_ArrayForEach(element, array) for(element = (array != NULL) ? (array)->child : NULL; element != NULL; element = element->next)

/* malloc/free objects using the malloc/free functions that have been set with tools_cJSON_InitHooks */
CJSON_PUBLIC(void *) tools_cJSON_malloc(size_t size);
CJSON_PUBLIC(void) tools_cJSON_free(void *object);

#ifdef __cplusplus
}
#endif

#endif
