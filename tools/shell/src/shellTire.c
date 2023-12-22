/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define __USE_XOPEN

#include "os.h"
#include "shellTire.h"

// ----------- interface -------------

// create prefix search tree
STire* createTire(char type) {
  STire* tire = taosMemoryMalloc(sizeof(STire));
  memset(tire, 0, sizeof(STire));
  tire->ref = 1;  // init is 1
  tire->type = type;
  tire->root.d = (STireNode**)taosMemoryCalloc(CHAR_CNT, sizeof(STireNode*));
  return tire;
}

// free tire node
void freeTireNode(STireNode* node) {
  if (node == NULL) return;

  // nest free sub node on array d
  if (node->d) {
    for (int i = 0; i < CHAR_CNT; i++) {
      freeTireNode(node->d[i]);
    }
    taosMemoryFree(node->d);
  }

  // free self
  taosMemoryFree(node);
}

// destroy prefix search tree
void freeTire(STire* tire) {
  // free nodes
  for (int i = 0; i < CHAR_CNT; i++) {
    freeTireNode(tire->root.d[i]);
  }
  taosMemoryFree(tire->root.d);

  // free from list
  StrName* item = tire->head;
  while (item) {
    StrName* next = item->next;
    // free string
    taosMemoryFree(item->name);
    // free node
    taosMemoryFree(item);

    // move next
    item = next;
  }
  tire->head = tire->tail = NULL;

  // free tire
  taosMemoryFree(tire);
}

// insert a new word to list
bool insertToList(STire* tire, char* word) {
  StrName* p = (StrName*)taosMemoryMalloc(sizeof(StrName));
  p->name = taosStrdup(word);
  p->next = NULL;

  if (tire->head == NULL) {
    tire->head = p;
    tire->tail = p;
  } else {
    tire->tail->next = p;
    tire->tail = p;
  }

  return true;
}

// insert a new word to tree
bool insertToTree(STire* tire, char* word, int len) {
  int         m = 0;
  STireNode** nodes = tire->root.d;
  for (int i = 0; i < len; i++) {
    m = word[i] - FIRST_ASCII;
    if (m < 0 || m >= CHAR_CNT) {
      return false;
    }

    if (nodes[m] == NULL) {
      // no pointer
      STireNode* p = (STireNode*)taosMemoryMalloc(sizeof(STireNode));
      memset(p, 0, sizeof(STireNode));
      nodes[m] = p;
      if (i == len - 1) {
        // is end
        p->end = true;
        break;
      }
    }

    if (nodes[m]->d == NULL) {
      // malloc d
      nodes[m]->d = (STireNode**)taosMemoryCalloc(CHAR_CNT, sizeof(STireNode*));
    }

    // move to next node
    nodes = nodes[m]->d;
  }

  // add count
  tire->count += 1;
  return true;
}

// insert a new word
bool insertWord(STire* tire, char* word) {
  int len = strlen(word);
  if (len >= MAX_WORD_LEN) {
    return false;
  }

  switch (tire->type) {
    case TIRE_TREE:
      return insertToTree(tire, word, len);
    case TIRE_LIST:
      return insertToList(tire, word);
    default:
      break;
  }
  return false;
}

// delete one word from list
bool deleteFromList(STire* tire, char* word) {
  StrName* item = tire->head;
  while (item) {
    if (strcmp(item->name, word) == 0) {
      // found, reset empty to delete
      item->name[0] = 0;
    }

    // move next
    item = item->next;
  }
  return true;
}

// delete one word from tree
bool deleteFromTree(STire* tire, char* word, int len) {
  int  m = 0;
  bool del = false;

  STireNode** nodes = tire->root.d;
  for (int i = 0; i < len; i++) {
    m = word[i] - FIRST_ASCII;
    if (m < 0 || m >= CHAR_CNT) {
      return false;
    }

    if (nodes[m] == NULL) {
      // no found
      return false;
    } else {
      // not null
      if (i == len - 1) {
        // this is last, only set end false , not free node
        nodes[m]->end = false;
        del = true;
        break;
      }
    }

    if (nodes[m]->d == NULL) break;
    // move to next node
    nodes = nodes[m]->d;
  }

  // reduce count
  if (del) {
    tire->count -= 1;
  }

  return del;
}

// insert a new word
bool deleteWord(STire* tire, char* word) {
  int len = strlen(word);
  if (len >= MAX_WORD_LEN) {
    return false;
  }

  switch (tire->type) {
    case TIRE_TREE:
      return deleteFromTree(tire, word, len);
    case TIRE_LIST:
      return deleteFromList(tire, word);
    default:
      break;
  }
  return false;
}

void addWordToMatch(SMatch* match, char* word) {
  // malloc new
  SMatchNode* node = (SMatchNode*)taosMemoryMalloc(sizeof(SMatchNode));
  memset(node, 0, sizeof(SMatchNode));
  node->word = taosStrdup(word);

  // append to match
  if (match->head == NULL) {
    match->head = match->tail = node;
  } else {
    match->tail->next = node;
    match->tail = node;
  }
  match->count += 1;
}

// enum all words from node
void enumAllWords(STireNode** nodes, char* prefix, SMatch* match) {
  STireNode* c;
  char       word[MAX_WORD_LEN];
  int        len = strlen(prefix);
  for (int i = 0; i < CHAR_CNT; i++) {
    c = nodes[i];

    if (c == NULL) {
      // chain end node
      continue;
    } else {
      // combine word string
      memset(word, 0, tListLen(word));
      strncpy(word, prefix, len);
      word[len] = FIRST_ASCII + i;  // append current char

      // chain middle node
      if (c->end) {
        // have end flag
        addWordToMatch(match, word);
      }
      // nested call next layer
      if (c->d) enumAllWords(c->d, word, match);
    }
  }
}

// match prefix from list
void matchPrefixFromList(STire* tire, char* prefix, SMatch* match) {
  StrName* item = tire->head;
  int      len = strlen(prefix);
  while (item) {
    if (strncmp(item->name, prefix, len) == 0) {
      // prefix matched
      addWordToMatch(match, item->name);
    }

    // move next
    item = item->next;
  }
}

// match prefix words, if match is not NULL , put all item to match and return match
void matchPrefixFromTree(STire* tire, char* prefix, SMatch* match) {
  int        m = 0;
  STireNode* c = 0;
  int        len = strlen(prefix);
  if (len >= MAX_WORD_LEN) {
    return;
  }

  STireNode** nodes = tire->root.d;
  for (int i = 0; i < len; i++) {
    m = prefix[i] - FIRST_ASCII;
    if (m < 0 || m > CHAR_CNT) {
      return;
    }

    // match
    c = nodes[m];
    if (c == NULL) {
      // arrive end
      break;
    }

    // previous items already matched
    if (i == len - 1) {
      // prefix is match to end char
      if (c->d) enumAllWords(c->d, prefix, match);
    } else {
      // move to next node continue match
      if (c->d == NULL) break;
      nodes = c->d;
    }
  }
}

void matchPrefix(STire* tire, char* prefix, SMatch* match) {
  if (match == NULL) {
    return;
  }

  switch (tire->type) {
    case TIRE_TREE:
      matchPrefixFromTree(tire, prefix, match);
      break;
    case TIRE_LIST:
      matchPrefixFromList(tire, prefix, match);
      break;
    default:
      break;
  }
}

// get all items from tires tree
void enumFromList(STire* tire, SMatch* match) {
  StrName* item = tire->head;
  while (item) {
    if (item->name[0] != 0) {
      // not delete
      addWordToMatch(match, item->name);
    }

    // move next
    item = item->next;
  }
}

// get all items from tires tree
void enumFromTree(STire* tire, SMatch* match) {
  char       pre[2] = {0, 0};
  STireNode* c;

  // enum first layer
  for (int i = 0; i < CHAR_CNT; i++) {
    pre[0] = FIRST_ASCII + i;

    // each node
    c = tire->root.d[i];
    if (c == NULL) {
      // this branch no data
      continue;
    }

    // this branch have data
    if (c->end) {
      addWordToMatch(match, pre);
    } else {
      matchPrefix(tire, pre, match);
    }
  }
}

// get all items from tires tree
SMatch* enumAll(STire* tire) {
  SMatch* match = (SMatch*)taosMemoryMalloc(sizeof(SMatch));
  memset(match, 0, sizeof(SMatch));

  switch (tire->type) {
    case TIRE_TREE:
      enumFromTree(tire, match);
      break;
    case TIRE_LIST:
      enumFromList(tire, match);
      break;
    default:
      break;
  }

  // return if need
  if (match->count == 0) {
    freeMatch(match);
    match = NULL;
  }

  return match;
}

// free match result
void freeMatchNode(SMatchNode* node) {
  // first free next
  if (node->next) freeMatchNode(node->next);

  // second free self
  if (node->word) taosMemoryFree(node->word);
  taosMemoryFree(node);
}

// free match result
void freeMatch(SMatch* match) {
  // first free next
  if (match->head) {
    freeMatchNode(match->head);
  }

  // second free self
  taosMemoryFree(match);
}