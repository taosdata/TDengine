#ifndef __SIMPLE_HASH_H__
#define __SIMPLE_HASH_H__

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

uint32_t mySimpleHash(const char* data, size_t n, uint32_t seed);

typedef struct SimpleHashNode {
  uint32_t               hashCode;
  void*                  key;
  size_t                 keyLen;
  void*                  data;
  size_t                 dataLen;
  struct SimpleHashNode* next;
} SimpleHashNode;

typedef struct SimpleHash {
  // public:

  int (*insert)(struct SimpleHash* ths, char* key, size_t keyLen, char* data, size_t dataLen);
  int (*remove)(struct SimpleHash* ths, char* key, size_t keyLen);
  SimpleHashNode** (*find)(struct SimpleHash* ths, char* key, size_t keyLen);

  // wrapper
  int (*insert_cstr)(struct SimpleHash* ths, char* key, char* data);
  int (*remove_cstr)(struct SimpleHash* ths, char* key);
  SimpleHashNode** (*find_cstr)(struct SimpleHash* ths, char* key);

  void (*print_cstr)(struct SimpleHash* ths);
  void (*destroy)(struct SimpleHash* ths);

  uint32_t length;
  uint32_t elems;

  // private:
  void (*resize)(struct SimpleHash* ths);
  uint32_t (*hashFunc)(const char* key, size_t keyLen);

  SimpleHashNode** table;

} SimpleHash;

int              insertCStrSimpleHash(struct SimpleHash* ths, char* key, char* data);
int              removeCStrSimpleHash(struct SimpleHash* ths, char* key);
SimpleHashNode** findCStrSimpleHash(struct SimpleHash* ths, char* key);
void             printCStrSimpleHash(struct SimpleHash* ths);

int              insertSimpleHash(struct SimpleHash* ths, char* key, size_t keyLen, char* data, size_t dataLen);
int              removeSimpleHash(struct SimpleHash* ths, char* key, size_t keyLen);
SimpleHashNode** findSimpleHash(struct SimpleHash* ths, char* key, size_t keyLen);
void             destroySimpleHash(struct SimpleHash* ths);
void             resizeSimpleHash(struct SimpleHash* ths);
uint32_t         simpleHashFunc(const char* key, size_t keyLen);

struct SimpleHash* newSimpleHash(size_t length);

#endif
