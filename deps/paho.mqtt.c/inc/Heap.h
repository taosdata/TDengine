/*******************************************************************************
 * Copyright (c) 2009, 2020 IBM Corp. and others
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *    Ian Craggs - use tree data structure instead of list
 *******************************************************************************/


#if !defined(HEAP_H)
#define HEAP_H

#if defined(HIGH_PERFORMANCE)
#define NO_HEAP_TRACKING 1
#endif

#define PAHO_MEMORY_ERROR -99

#include "MQTTExportDeclarations.h"

#include <stdio.h>
#include <stdlib.h>

#if !defined(NO_HEAP_TRACKING)

#if !defined(TREE_C)
/**
 * redefines malloc to use "mymalloc" so that heap allocation can be tracked
 * @param x the size of the item to be allocated
 * @return the pointer to the item allocated, or NULL
 */
#define malloc(x) mymalloc(__FILE__, __LINE__, x)

/**
 * redefines realloc to use "myrealloc" so that heap allocation can be tracked
 * @param a the heap item to be reallocated
 * @param b the new size of the item
 * @return the new pointer to the heap item
 */
#define realloc(a, b) myrealloc(__FILE__, __LINE__, a, b)

/**
 * redefines free to use "myfree" so that heap allocation can be tracked
 * @param x the size of the item to be freed
 */
#define free(x) myfree(__FILE__, __LINE__, x)

#endif

/**
 * Information about the state of the heap.
 */
typedef struct
{
	size_t current_size;	/**< current size of the heap in bytes */
	size_t max_size;		/**< max size the heap has reached in bytes */
} heap_info;

#if defined(__cplusplus)
 extern "C" {
#endif

void* mymalloc(char*, int, size_t size);
void* myrealloc(char*, int, void* p, size_t size);
void myfree(char*, int, void* p);

void Heap_scan(FILE* file);
int Heap_initialize(void);
void Heap_terminate(void);
LIBMQTT_API heap_info* Heap_get_info(void);
int HeapDump(FILE* file);
int HeapDumpString(FILE* file, char* str);
void* Heap_findItem(void* p);
void Heap_unlink(char* file, int line, void* p);
#ifdef __cplusplus
     }
#endif

#endif

#endif
