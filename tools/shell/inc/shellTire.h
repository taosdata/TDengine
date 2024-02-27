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

#ifndef __TRIE__
#define __TRIE__

//
// The prefix search tree is a efficient storage words and search words tree, it support 95 visible ascii code character
//
#define FIRST_ASCII 32   // first visible char is '0'
#define LAST_ASCII  126  // last visilbe char is 'z'

// capacity save char is 95
#define CHAR_CNT     (LAST_ASCII - FIRST_ASCII + 1)
#define MAX_WORD_LEN 256  // max insert word length

// define STire
#define TIRE_TREE 0
#define TIRE_LIST 1

typedef struct STireNode {
  struct STireNode** d;
  bool               end;  // record end flag
} STireNode;

typedef struct StrName {
  char*           name;
  struct StrName* next;
} StrName;

typedef struct STire {
  char      type;  // see define TIRE_
  STireNode root;

  StrName* head;
  StrName* tail;

  int count;  // all count
  int ref;
} STire;

typedef struct SMatchNode {
  char*              word;
  struct SMatchNode* next;
} SMatchNode;

typedef struct SMatch {
  SMatchNode* head;
  SMatchNode* tail;  // append node to tail
  int         count;
  char        pre[MAX_WORD_LEN];
} SMatch;

// ----------- interface -------------

// create prefix search tree, return value call freeTire to free
STire* createTire(char type);

// destroy prefix search tree
void freeTire(STire* tire);

// add a new word
bool insertWord(STire* tire, char* word);

// add a new word
bool deleteWord(STire* tire, char* word);

// match prefix words, if match is not NULL , put all item to match and return match
void matchPrefix(STire* tire, char* prefix, SMatch* match);

// get all items from tires tree
SMatch* enumAll(STire* tire);

// free match result
void freeMatch(SMatch* match);

#endif
