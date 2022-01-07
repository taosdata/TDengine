#include "simpleHash.h"

uint32_t mySimpleHash(const char* data, size_t n, uint32_t seed) {
  // Similar to murmur hash
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char*    limit = data + n;
  uint32_t       h = seed ^ (n * m);

  // Pick up four bytes at a time
  while (data + 4 <= limit) {
    // uint32_t w = DecodeFixed32(data);
    uint32_t w;
    memcpy(&w, data, 4);

    data += 4;
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

  // Pick up remaining bytes
  switch (limit - data) {
    case 3:
      h += (unsigned char)(data[2]) << 16;
      do {
      } while (0);
    case 2:
      h += (unsigned char)(data[1]) << 8;
      do {
      } while (0);
    case 1:
      h += (unsigned char)(data[0]);
      h *= m;
      h ^= (h >> r);
      break;
  }
  return h;
}

int insertCStrSimpleHash(struct SimpleHash* ths, char* key, char* data) {
  return insertSimpleHash(ths, key, strlen(key) + 1, data, strlen(data) + 1);
}

int removeCStrSimpleHash(struct SimpleHash* ths, char* key) { return removeSimpleHash(ths, key, strlen(key) + 1); }

SimpleHashNode** findCStrSimpleHash(struct SimpleHash* ths, char* key) {
  return findSimpleHash(ths, key, strlen(key) + 1);
}

int insertSimpleHash(struct SimpleHash* ths, char* key, size_t keyLen, char* data, size_t dataLen) {
  SimpleHashNode** pp = ths->find(ths, key, keyLen);
  if (*pp != NULL) {
    fprintf(stderr, "insertSimpleHash, already has key \n");
    return -1;
  }

  SimpleHashNode* node = malloc(sizeof(*node));
  node->hashCode = ths->hashFunc(key, keyLen);
  node->key = malloc(keyLen);
  node->keyLen = keyLen;
  memcpy(node->key, key, keyLen);
  node->data = malloc(dataLen);
  node->dataLen = dataLen;
  memcpy(node->data, data, dataLen);
  node->next = NULL;

  // printf("insertSimpleHash: <%s, %ld, %s, %ld, %u> \n", node->key, node->keyLen, node->data, node->dataLen,
  //       node->hashCode);

  size_t index = node->hashCode & (ths->length - 1);

  SimpleHashNode* ptr = ths->table[index];
  if (ptr != NULL) {
    node->next = ptr;
    ths->table[index] = node;

  } else {
    ths->table[index] = node;
  }
  ths->elems++;
  if (ths->elems > 2 * ths->length) {
    ths->resize(ths);
  }

  return 0;
}

int removeSimpleHash(struct SimpleHash* ths, char* key, size_t keyLen) {
  SimpleHashNode** pp = ths->find(ths, key, keyLen);
  if (*pp == NULL) {
    fprintf(stderr, "removeSimpleHash, key not exist \n");
    return -1;
  }

  SimpleHashNode* del = *pp;
  *pp = del->next;
  free(del->key);
  free(del->data);
  free(del);
  ths->elems--;

  return 0;
}

SimpleHashNode** findSimpleHash(struct SimpleHash* ths, char* key, size_t keyLen) {
  uint32_t hashCode = ths->hashFunc(key, keyLen);
  // size_t   index = hashCode % ths->length;
  size_t index = hashCode & (ths->length - 1);

  // printf("findSimpleHash: %s %ld %u \n", key, keyLen, hashCode);

  SimpleHashNode** pp = &(ths->table[index]);
  while (*pp != NULL && ((*pp)->hashCode != hashCode || memcmp(key, (*pp)->key, keyLen) != 0)) {
    pp = &((*pp)->next);
  }

  return pp;
}

void printCStrSimpleHash(struct SimpleHash* ths) {
  printf("\n--- printCStrSimpleHash: elems:%d length:%d \n", ths->elems, ths->length);
  for (size_t i = 0; i < ths->length; ++i) {
    SimpleHashNode* ptr = ths->table[i];
    if (ptr != NULL) {
      printf("%zu: ", i);
      while (ptr != NULL) {
        printf("<%u, %s, %ld, %s, %ld> ", ptr->hashCode, (char*)ptr->key, ptr->keyLen, (char*)ptr->data, ptr->dataLen);
        ptr = ptr->next;
      }
      printf("\n");
    }
  }
  printf("---------------\n");
}

void destroySimpleHash(struct SimpleHash* ths) {
  for (size_t i = 0; i < ths->length; ++i) {
    SimpleHashNode* ptr = ths->table[i];
    while (ptr != NULL) {
      SimpleHashNode* tmp = ptr;
      ptr = ptr->next;
      free(tmp->key);
      free(tmp->data);
      free(tmp);
    }
  }

  ths->length = 0;
  ths->elems = 0;
  free(ths->table);
  free(ths);
}

void resizeSimpleHash(struct SimpleHash* ths) {
  uint32_t new_length = ths->length;
  while (new_length < ths->elems) {
    new_length *= 2;
  }

  printf("resizeSimpleHash: %p from %u to %u \n", ths, ths->length, new_length);

  SimpleHashNode** new_table = malloc(new_length * sizeof(SimpleHashNode*));
  memset(new_table, 0, new_length * sizeof(SimpleHashNode*));

  uint32_t count = 0;
  for (uint32_t i = 0; i < ths->length; i++) {
    if (ths->table[i] == NULL) {
      continue;
    }

    SimpleHashNode* it = ths->table[i];
    while (it != NULL) {
      SimpleHashNode* move_node = it;
      it = it->next;

      // move move_node
      move_node->next = NULL;
      size_t index = move_node->hashCode & (new_length - 1);

      SimpleHashNode* ptr = new_table[index];
      if (ptr != NULL) {
        move_node->next = ptr;
        new_table[index] = move_node;
      } else {
        new_table[index] = move_node;
      }
      count++;
    }
  }

  assert(ths->elems == count);
  free(ths->table);
  ths->table = new_table;
  ths->length = new_length;
}

uint32_t simpleHashFunc(const char* key, size_t keyLen) { return mySimpleHash(key, keyLen, 1); }

struct SimpleHash* newSimpleHash(size_t length) {
  struct SimpleHash* ths = malloc(sizeof(*ths));

  ths->length = length;
  ths->elems = 0;
  ths->table = malloc(length * sizeof(SimpleHashNode*));
  memset(ths->table, 0, length * sizeof(SimpleHashNode*));

  ths->insert = insertSimpleHash;
  ths->remove = removeSimpleHash;
  ths->find = findSimpleHash;
  ths->insert_cstr = insertCStrSimpleHash;
  ths->remove_cstr = removeCStrSimpleHash;
  ths->find_cstr = findCStrSimpleHash;
  ths->print_cstr = printCStrSimpleHash;
  ths->destroy = destroySimpleHash;
  ths->resize = resizeSimpleHash;
  ths->hashFunc = simpleHashFunc;
}
