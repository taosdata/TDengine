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

#include "freeBlockMgr.h"
#include "osMemory.h"

int32_t compareFreeBlock(const SRBTreeNode *a, const SRBTreeNode *b) {
  const FreeBlock *fa = (const FreeBlock *)a;
  const FreeBlock *fb = (const FreeBlock *)b;
  return fa->length - fb->length;
}

FreeBlock *createFreeBlock(int start, int length) {
  FreeBlock *block = taosMemCalloc(1, sizeof(FreeBlock));
  if (!block) return NULL;

  block->start = start;
  block->length = length;
  block->nextInBucket = NULL;

  block->node.parent = NULL;
  block->node.left = NULL;
  block->node.right = NULL;
  block->node.color = BLACK;

  return block;
}

void destroyFreeBlock(FreeBlock *block) { taosMemFree(block); }

void insertFreeBlock(SRBTree *tree, FreeBlock *newBlock) {
  FreeBlock *existing = (FreeBlock *)tRBTreeGet(tree, &newBlock->node);

  if (existing != NULL) {
    // There is already a block of the same length, add it to the bucket linked list
    newBlock->nextInBucket = existing->nextInBucket;
    existing->nextInBucket = newBlock;
  } else {
    newBlock->nextInBucket = NULL;
    tRBTreePut(tree, &newBlock->node);
  }
}

FreeBlock *findBestFitBlock(SRBTree *tree, int requestLength) {
  // Create a temporary key node for comparison
  FreeBlock    tempKey = {.length = requestLength};
  SRBTreeNode *pNode = tree->root;
  SRBTreeNode *candidate = NULL;

  while (pNode != tree->NIL) {
    int cmp = compareFreeBlock((SRBTreeNode *)&tempKey, pNode);
    if (cmp < 0) {
      candidate = pNode;  // It may be the upper bound, continue to the left
      pNode = pNode->left;
    } else {
      pNode = pNode->right;
    }
  }

  return (candidate != NULL) ? (FreeBlock *)candidate : NULL;
}

void removeFreeBlock(SRBTree *tree, FreeBlock *target) {
  FreeBlock *bucketHead = (FreeBlock *)tRBTreeGet(tree, &target->node);

  if (bucketHead == NULL) {
    // This block does not exist (error or deleted)
    return;
  }

  if (bucketHead == target) {
    // The bucket head is the target block
    if (target->nextInBucket != NULL) {
      // There are subsequent elements, pick the next one as the new head
      FreeBlock *newHead = target->nextInBucket;
      newHead->node = target->node;  // Copy Red-Black Tree node fields (preserving tree structure)
      tRBTreeDrop(tree, &target->node);
      tRBTreePut(tree, &newHead->node);
    } else {
      tRBTreeDrop(tree, &target->node);
    }
  } else {
    // The target block is not the head of the bucket, find and remove it
    FreeBlock *prev = bucketHead;
    while (prev && prev->nextInBucket != NULL) {
      if (prev->nextInBucket == target) {
        prev->nextInBucket = target->nextInBucket;
        break;
      }
      prev = prev->nextInBucket;
    }
  }

  target->nextInBucket = NULL;
}

FreeBlock *popBestFitBlock(SRBTree *tree, int requestLength) {
  FreeBlock *bestFit = findBestFitBlock(tree, requestLength);
  if (bestFit == NULL) {
    return NULL;  // No suitable block found
  }

  // Remove the best fit block from the tree
  removeFreeBlock(tree, bestFit);

  return bestFit;  // Return the best fit block itself
}

void clearAllFreeBlocks(SRBTree *tree) {
  if (!tree || tree->root == tree->NIL) return;

  SRBTreeIter iter;
  iter = tRBTreeIterCreate(tree, 1);

  FreeBlock *current = NULL;

  while ((current = (FreeBlock *)tRBTreeIterNext(&iter)) != NULL) {
    FreeBlock *block = current;
    tRBTreeDrop(tree, &block->node);
    while (block != NULL) {
      FreeBlock *next = block->nextInBucket;
      destroyFreeBlock(block);
      block = next;
    }
  }

  tRBTreeClear(tree);
}
