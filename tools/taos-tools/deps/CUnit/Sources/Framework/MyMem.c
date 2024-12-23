/*
 *  CUnit - A Unit testing framework library for C.
 *  Copyright (C) 2001       Anil Kumar
 *  Copyright (C) 2004-2006  Anil Kumar, Jerry St.Clair
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public
 *  License as published by the Free Software Foundation; either
 *  version 2 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

/*
 *  Memory management functions used throughout CUnit.
 *
 *  13/Oct/2001   Moved some of the generic functions definitions from other
 *                files to this one so as to use the functions consitently.
 *                This file is not included in the distribution headers because
 *                it is used internally by CUnit. (AK)
 *
 *  18-Jul-2004   New interface, doxygen comments, made local functions &
 *                constants static, fixed reporting of memory tracking (valid
 *                vs invalid cycles), restructured memory tracking to detect
 *                reallocations & multiple deletions. (JDS)
 *
 *  24-Apr-2005   Changed type of allocated sizes to size_t to avoid
 *                signed-unsigned mismatch. (JDS)
 *
 *  02-May-2006   Added internationalization hooks.  (JDS)
 */

/** @file
 * Memory management & reporting functions (implementation).
 */
/** @addtogroup Framework
 @{
*/

#ifdef _WIN32
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <time.h>

#include "CUnit/CUnit.h"
#include "CUnit/MyMem.h"
#include "CUnit/CUnit_intl.h"

#ifdef MEMTRACE

#define MAX_NAME_LENGTH  256

/** Default name for memory dump file. */
static const char* f_szDefaultDumpFileName = "CUnit-Memory-Dump.xml";
/**< Default name for memory dump file. */

#ifdef CUNIT_BUILD_TESTS
/** For testing use (only) to simulate memory exhaustion -
 * if CU_FALSE, allocation requests will always fail and return NULL.
 */
static CU_BOOL f_bTestCunitMallocActive = CU_TRUE;
#endif

/** Structure holding the details of a memory allocation/deallocation event. */
typedef struct mem_event {
  size_t            Size;
  unsigned int      AllocLine;
  char              AllocFilename[MAX_NAME_LENGTH];
  char              AllocFunction[MAX_NAME_LENGTH];
  unsigned int      DeallocLine;
  char              DeallocFilename[MAX_NAME_LENGTH];
  char              DeallocFunction[MAX_NAME_LENGTH];
  struct mem_event* pNext;
} MEMORY_EVENT;
typedef MEMORY_EVENT* PMEMORY_EVENT;

#define NOT_ALLOCATED 0
#define NOT_DELETED 0

/** Structure holding the details of a memory node having allocation/deallocation events. */
typedef struct mem_node {
  void*             pLocation;
  unsigned int      EventCount;
  PMEMORY_EVENT     pFirstEvent;
  struct mem_node*  pNext;
} MEMORY_NODE;
typedef MEMORY_NODE* PMEMORY_NODE;

static PMEMORY_NODE f_pMemoryTrackerHead = NULL;  /**< Head of double-linked list of memory nodes. */
static unsigned int f_nMemoryNodes = 0;           /**< Counter for memory nodes created. */
/*------------------------------------------------------------------------*/
/** Locate the memory node for the specified memory location (returns NULL if none). */
static PMEMORY_NODE find_memory_node(void* pLocation)
{
  PMEMORY_NODE pMemoryNode = f_pMemoryTrackerHead;
  while (NULL != pMemoryNode) {
    if (pLocation == pMemoryNode->pLocation) {
      break;
    }
    pMemoryNode = pMemoryNode->pNext;
  }

  return pMemoryNode;
}
/*------------------------------------------------------------------------*/
/** Create a new memory node for the specified memory location. */
static PMEMORY_NODE create_memory_node(void* pLocation)
{
  PMEMORY_NODE pTempNode = NULL;
  PMEMORY_NODE pMemoryNode = find_memory_node(pLocation);

  /* a memory node for pLocation should not exist yet */
  if (NULL == pMemoryNode) {

    pMemoryNode = (PMEMORY_NODE)malloc(sizeof(MEMORY_NODE));
    assert(NULL != pMemoryNode);

    pMemoryNode->pLocation = pLocation;
    pMemoryNode->EventCount = 0;
    pMemoryNode->pFirstEvent = NULL;
    pMemoryNode->pNext = NULL;

    /* add new node to linked list */
    pTempNode = f_pMemoryTrackerHead;
    if (NULL == pTempNode) {
      f_pMemoryTrackerHead = pMemoryNode;
    }
    else {
      while (NULL != pTempNode->pNext) {
        pTempNode = pTempNode->pNext;
      }
      pTempNode->pNext = pMemoryNode;
    }

    ++f_nMemoryNodes;
  }
  return pMemoryNode;
}
/*------------------------------------------------------------------------*/
/** Add a new memory event having the specified parameters. */
static PMEMORY_EVENT add_memory_event(PMEMORY_NODE pMemoryNode,
                                      size_t size,
                                      unsigned int alloc_line,
                                      const char* alloc_filename,
                                      const char* alloc_function)
{
  PMEMORY_EVENT pMemoryEvent = NULL;
  PMEMORY_EVENT pTempEvent = NULL;

  assert (NULL != pMemoryNode);

  /* create and set up the new event */
  pMemoryEvent = (PMEMORY_EVENT)malloc(sizeof(MEMORY_EVENT));
  assert(NULL != pMemoryEvent);

  pMemoryEvent->Size = size;
  pMemoryEvent->AllocLine = alloc_line;
  strncpy(pMemoryEvent->AllocFilename, alloc_filename, (size_t) MAX_NAME_LENGTH-1);
  pMemoryEvent->AllocFilename[MAX_NAME_LENGTH-1] = (char)0;
  strncpy(pMemoryEvent->AllocFunction, alloc_function, (size_t) MAX_NAME_LENGTH-1);
  pMemoryEvent->AllocFunction[MAX_NAME_LENGTH-1] = (char)0;
  pMemoryEvent->DeallocLine = NOT_DELETED;
  pMemoryEvent->DeallocFilename[0] = (char)0;
  pMemoryEvent->DeallocFunction[0] = (char)0;
  pMemoryEvent->pNext = NULL;

  /* add the new event to the end of the linked list */
  pTempEvent = pMemoryNode->pFirstEvent;
  if (NULL == pTempEvent) {
    pMemoryNode->pFirstEvent = pMemoryEvent;
  }
  else {
    while (NULL != pTempEvent->pNext) {
      pTempEvent = pTempEvent->pNext;
    }
    pTempEvent->pNext = pMemoryEvent;
  }

  ++pMemoryNode->EventCount;

  return pMemoryEvent;
}
/*------------------------------------------------------------------------*/
/** Record memory allocation event. */
static PMEMORY_NODE allocate_memory(size_t nSize,
                                    void* pLocation,
                                    unsigned int uiAllocationLine,
                                    const char* szAllocationFile,
                                    const char* szAllocationFunction)
{
  PMEMORY_NODE pMemoryNode = NULL;

  /* attempt to locate an existing record for this pLocation */
  pMemoryNode = find_memory_node(pLocation);

  /* pLocation not found - create a new event record */
  if (NULL == pMemoryNode) {
    pMemoryNode = create_memory_node(pLocation);
  }

  /* add the new event record */
  add_memory_event(pMemoryNode, nSize, uiAllocationLine, szAllocationFile, szAllocationFunction);

  return pMemoryNode;
}

/*------------------------------------------------------------------------*/
/** Record memory deallocation event. */
static void deallocate_memory(void* pLocation,
                              unsigned int uiDeletionLine,
                              const char* szDeletionFileName,
                              const char* szDeletionFunction)
{
  PMEMORY_NODE  pMemoryNode = NULL;
  PMEMORY_EVENT pTempEvent = NULL;

  assert(0 != uiDeletionLine);
  assert(NULL != szDeletionFileName);

  /* attempt to locate an existing record for this pLocation */
  pMemoryNode = find_memory_node(pLocation);

  /* if no entry, then an unallocated pointer was freed */
  if (NULL == pMemoryNode) {
    pMemoryNode = create_memory_node(pLocation);
    pTempEvent = add_memory_event(pMemoryNode, 0, NOT_ALLOCATED, "", "");
  }
  else {
    /* there should always be at least 1 event for an existing memory node */
    assert(NULL != pMemoryNode->pFirstEvent);

    /* locate last memory event for this pLocation */
    pTempEvent = pMemoryNode->pFirstEvent;
    while (NULL != pTempEvent->pNext) {
      pTempEvent = pTempEvent->pNext;
    }

    /* if pointer has already been freed, create a new event for double deletion */
    if (NOT_DELETED != pTempEvent->DeallocLine) {
      pTempEvent = add_memory_event(pMemoryNode, pTempEvent->Size, NOT_ALLOCATED, "", "");
    }
  }

  pTempEvent->DeallocLine = uiDeletionLine;
  strncpy(pTempEvent->DeallocFilename, szDeletionFileName, MAX_NAME_LENGTH-1);
  pTempEvent->DeallocFilename[MAX_NAME_LENGTH-1] = (char)0;
  strncpy(pTempEvent->DeallocFunction, szDeletionFunction, MAX_NAME_LENGTH-1);
  pTempEvent->DeallocFunction[MAX_NAME_LENGTH-1] = (char)0;
}

/*------------------------------------------------------------------------*/
/** Custom calloc function with memory event recording. */
void* CU_calloc(size_t nmemb, size_t size, unsigned int uiLine, const char* szFileName, const char* szFunction)
{
  void* pVoid = NULL;

#ifdef CUNIT_BUILD_TESTS
  if (CU_FALSE == f_bTestCunitMallocActive) {
    return NULL;
  }
#endif

  pVoid = calloc(nmemb, size);
  if (NULL != pVoid) {
    allocate_memory(nmemb * size, pVoid, uiLine, szFileName, szFunction);
  }

  return pVoid;
}

/*------------------------------------------------------------------------*/
/** Custom malloc function with memory event recording. */
void* CU_malloc(size_t size, unsigned int uiLine, const char* szFileName, const char* szFunction)
{
  void* pVoid = NULL;

#ifdef CUNIT_BUILD_TESTS
  if (CU_FALSE == f_bTestCunitMallocActive) {
    return NULL;
  }
#endif

  pVoid = malloc(size);
  if (NULL != pVoid) {
    allocate_memory(size, pVoid, uiLine, szFileName, szFunction);
  }

  return pVoid;
}

/*------------------------------------------------------------------------*/
/** Custom free function with memory event recording. */
void CU_free(void *ptr, unsigned int uiLine, const char* szFileName, const char* szFunction)
{
  deallocate_memory(ptr, uiLine, szFileName, szFunction);
  free(ptr);
}

/*------------------------------------------------------------------------*/
/** Custom realloc function with memory event recording. */
void* CU_realloc(void *ptr, size_t size, unsigned int uiLine, const char* szFileName, const char* szFunction)
{
  void* pVoid = NULL;

  deallocate_memory(ptr, uiLine, szFileName, szFunction);

#ifdef CUNIT_BUILD_TESTS
  if (CU_FALSE == f_bTestCunitMallocActive) {
    free(ptr);
    return NULL;
  }
#endif

  pVoid = realloc(ptr, size);

  if (NULL != pVoid) {
    allocate_memory(size, pVoid, uiLine, szFileName, szFunction);
  }

  return pVoid;
}

/*------------------------------------------------------------------------*/
/** Print a report of memory events to file. */
void CU_dump_memory_usage(const char* szFilename)
{
  char* szDumpFileName = (char*)f_szDefaultDumpFileName;
  unsigned int nValid;
  unsigned int nInvalid;
  PMEMORY_NODE pTempNode = NULL;
  PMEMORY_EVENT pTempEvent = NULL;
  FILE* pFile = NULL;
  time_t tTime = 0;

  /* use the specified file name, if supplied) */
  if ((NULL != szFilename) && strlen(szFilename) > 0) {
    szDumpFileName = (char*)szFilename;
  }

  if (NULL == (pFile = fopen(szDumpFileName, "w"))) {
    fprintf(stderr, _("Failed to open file \"%s\" : %s"), szDumpFileName, strerror(errno));
    return;
  }

  setvbuf(pFile, NULL, _IONBF, 0);

  fprintf(pFile, "<\?xml version=\"1.0\" \?>");
  fprintf(pFile, "\n<\?xml-stylesheet type=\"text/xsl\" href=\"Memory-Dump.xsl\" \?>");
  fprintf(pFile, "\n<!DOCTYPE MEMORY_DUMP_REPORT SYSTEM \"Memory-Dump.dtd\">");
  fprintf(pFile, "\n<MEMORY_DUMP_REPORT>");
  fprintf(pFile, "\n  <MD_HEADER/>");
  fprintf(pFile, "\n  <MD_RUN_LISTING>");

  nValid = 0;
  nInvalid = 0;
  pTempNode = f_pMemoryTrackerHead;
  while (NULL != pTempNode) {
    fprintf(pFile, "\n    <MD_RUN_RECORD>");
    fprintf(pFile, "\n      <MD_POINTER> %p </MD_POINTER>", pTempNode->pLocation);
    fprintf(pFile, "\n      <MD_EVENT_COUNT> %u </MD_EVENT_COUNT>", pTempNode->EventCount);

    pTempEvent = pTempNode->pFirstEvent;
    while (NULL != pTempEvent) {
      fprintf(pFile, "\n      <MD_EVENT_RECORD>");
      fprintf(pFile, "\n        <MD_SIZE> %u </MD_SIZE>", (unsigned int)pTempEvent->Size);
      fprintf(pFile, "\n        <MD_ALLOC_FUNCTION> %s </MD_ALLOC_FUNCTION>", pTempEvent->AllocFunction);
      fprintf(pFile, "\n        <MD_ALLOC_FILE> %s </MD_ALLOC_FILE>", pTempEvent->AllocFilename);
      fprintf(pFile, "\n        <MD_ALLOC_LINE> %u </MD_ALLOC_LINE>", pTempEvent->AllocLine);
      fprintf(pFile, "\n        <MD_DEALLOC_FUNCTION> %s </MD_DEALLOC_FUNCTION>", pTempEvent->DeallocFunction);
      fprintf(pFile, "\n        <MD_DEALLOC_FILE> %s </MD_DEALLOC_FILE>", pTempEvent->DeallocFilename);
      fprintf(pFile, "\n        <MD_DEALLOC_LINE> %u </MD_DEALLOC_LINE>", pTempEvent->DeallocLine);
      fprintf(pFile, "\n      </MD_EVENT_RECORD>");

      if ((0 != pTempEvent->AllocLine) && (0 != pTempEvent->DeallocLine)) {
        ++nValid;
      }
      else {
        ++nInvalid;
      }

      pTempEvent = pTempEvent->pNext;
    }

    fprintf(pFile, "\n    </MD_RUN_RECORD>");
    pTempNode = pTempNode->pNext;
  }

  fprintf(pFile, "\n  </MD_RUN_LISTING>");

  fprintf(pFile, "\n  <MD_SUMMARY>");
  fprintf(pFile, "\n    <MD_SUMMARY_VALID_RECORDS> %u </MD_SUMMARY_VALID_RECORDS>", nValid);
  fprintf(pFile, "\n    <MD_SUMMARY_INVALID_RECORDS> %u </MD_SUMMARY_INVALID_RECORDS>", nInvalid);
  fprintf(pFile, "\n    <MD_SUMMARY_TOTAL_RECORDS> %u </MD_SUMMARY_TOTAL_RECORDS>", nValid + nInvalid);
  fprintf(pFile, "\n  </MD_SUMMARY>");

  time(&tTime);
  fprintf(pFile, "\n  <MD_FOOTER> Memory Trace for CUnit Run at %s </MD_FOOTER>", ctime(&tTime));
  fprintf(pFile, "</MEMORY_DUMP_REPORT>");

  fclose(pFile);
}

#endif  /* MEMTRACE */

/** @} */

#ifdef CUNIT_BUILD_TESTS
#include "test_cunit.h"

/** Deactivate CUnit memory allocation
 * After calling this function, all Cunit memory
 * allocation routines will fail and return NULL.
 */
void test_cunit_deactivate_malloc(void)
{
  f_bTestCunitMallocActive = CU_FALSE;
}

/** Activate CUnit memory allocation
 * After calling this function, all Cunit memory
 * allocation routines will behave normally (allocating
 * memory if it is available).
 */
void test_cunit_activate_malloc(void)
{
  f_bTestCunitMallocActive = CU_TRUE;
}

/** Retrieve the number of memory events recorded for a given pointer. */
unsigned int test_cunit_get_n_memevents(void* pLocation)
{
  PMEMORY_NODE pNode = find_memory_node(pLocation);
  return (pNode) ? pNode->EventCount : 0;
}

/** Retrieve the number of memory allocations recorded for a given pointer. */
unsigned int test_cunit_get_n_allocations(void* pLocation)
{
  PMEMORY_NODE pNode = find_memory_node(pLocation);
  PMEMORY_EVENT pEvent = NULL;
  int result = 0;

  if (NULL != pNode) {
    pEvent = pNode->pFirstEvent;
    while (NULL != pEvent) {
      if (pEvent->AllocLine != NOT_ALLOCATED)
        ++result;
      pEvent = pEvent->pNext;
    }
  }

  return result;
}

/** Retrieve the number of memory deallocations recorded for a given pointer. */
unsigned int test_cunit_get_n_deallocations(void* pLocation)
{
  PMEMORY_NODE pNode = find_memory_node(pLocation);
  PMEMORY_EVENT pEvent = NULL;
  int result = 0;

  if (NULL != pNode) {
    pEvent = pNode->pFirstEvent;
    while (NULL != pEvent) {
      if (pEvent->DeallocLine != NOT_DELETED)
        ++result;
      pEvent = pEvent->pNext;
    }
  }

  return result;
}

void test_CU_calloc(void)
{
  void* ptr1 = NULL;
  void* ptr2 = calloc(2, sizeof(int));
  unsigned int n2 = test_cunit_get_n_memevents(ptr2);

  /* test allocation failure */
  test_cunit_deactivate_malloc();
  ptr1 = CU_CALLOC(2, sizeof(int));
  TEST(NULL == ptr1);
  TEST(test_cunit_get_n_allocations(ptr1) == test_cunit_get_n_deallocations(ptr1));
  TEST(test_cunit_get_n_allocations(ptr2) == test_cunit_get_n_deallocations(ptr2));
  test_cunit_activate_malloc();

  /* normal allocation */
  ptr1 = CU_CALLOC(2, sizeof(int));
  TEST_FATAL(NULL != ptr1);
  TEST(test_cunit_get_n_allocations(ptr1) != test_cunit_get_n_deallocations(ptr1));
  TEST(test_cunit_get_n_allocations(ptr2) == test_cunit_get_n_deallocations(ptr2));

  CU_FREE(ptr1);
  TEST(test_cunit_get_n_allocations(ptr1) == test_cunit_get_n_deallocations(ptr1));
  TEST(test_cunit_get_n_allocations(ptr2) == test_cunit_get_n_deallocations(ptr2));
  TEST(n2 == test_cunit_get_n_memevents(ptr2));

  free(ptr2);
}

void test_CU_malloc(void)
{
  void* ptr1 = NULL;
  void* ptr2 = malloc(sizeof(int));
  unsigned int n2 = test_cunit_get_n_memevents(ptr2);

  /* test allocation failure */
  test_cunit_deactivate_malloc();
  ptr1 = CU_MALLOC(sizeof(int));
  TEST(NULL == ptr1);
  TEST(test_cunit_get_n_allocations(ptr1) == test_cunit_get_n_deallocations(ptr1));
  TEST(test_cunit_get_n_allocations(ptr2) == test_cunit_get_n_deallocations(ptr2));
  test_cunit_activate_malloc();

  /* normal allocation */
  ptr1 = CU_MALLOC(sizeof(int));
  TEST_FATAL(NULL != ptr1);
  TEST(test_cunit_get_n_allocations(ptr1) != test_cunit_get_n_deallocations(ptr1));
  TEST(test_cunit_get_n_allocations(ptr2) == test_cunit_get_n_deallocations(ptr2));

  CU_FREE(ptr1);
  TEST(test_cunit_get_n_allocations(ptr1) == test_cunit_get_n_deallocations(ptr1));
  TEST(test_cunit_get_n_allocations(ptr2) == test_cunit_get_n_deallocations(ptr2));
  TEST(n2 == test_cunit_get_n_memevents(ptr2));

  free(ptr2);
}

void test_CU_free(void)
{
  /* covered by other test functions */
}

void test_CU_realloc(void)
{
  void* ptr1 = CU_MALLOC(sizeof(int));
  void* ptr2 = malloc(sizeof(int));
  void* ptr3;
  void* ptr4;
  unsigned int n2 = test_cunit_get_n_memevents(ptr2);

  /* test allocation failure */
  test_cunit_deactivate_malloc();
  ptr1 = CU_REALLOC(ptr1, sizeof(long int));
  TEST(NULL == ptr1);
  TEST(test_cunit_get_n_allocations(ptr1) == test_cunit_get_n_deallocations(ptr1));
  TEST(test_cunit_get_n_allocations(ptr2) == test_cunit_get_n_deallocations(ptr2));
  test_cunit_activate_malloc();

  /* normal allocation */
  ptr3 = CU_MALLOC(sizeof(int));
  TEST_FATAL(NULL != ptr3);
  TEST(test_cunit_get_n_allocations(ptr1) == test_cunit_get_n_deallocations(ptr1));
  TEST(test_cunit_get_n_allocations(ptr2) == test_cunit_get_n_deallocations(ptr2));
  TEST(test_cunit_get_n_allocations(ptr3) != test_cunit_get_n_deallocations(ptr3));

  ptr4 = CU_REALLOC(ptr3, sizeof(long int));
  TEST_FATAL(NULL != ptr4);
  TEST(test_cunit_get_n_allocations(ptr1) == test_cunit_get_n_deallocations(ptr1));
  TEST(test_cunit_get_n_allocations(ptr2) == test_cunit_get_n_deallocations(ptr2));
  if (ptr3 != ptr4)
    TEST(test_cunit_get_n_allocations(ptr3) == test_cunit_get_n_deallocations(ptr3));
  TEST(test_cunit_get_n_allocations(ptr4) != test_cunit_get_n_deallocations(ptr4));

  CU_FREE(ptr4);
  TEST(test_cunit_get_n_allocations(ptr1) == test_cunit_get_n_deallocations(ptr1));
  TEST(test_cunit_get_n_allocations(ptr2) == test_cunit_get_n_deallocations(ptr2));
  TEST(test_cunit_get_n_allocations(ptr3) == test_cunit_get_n_deallocations(ptr3));
  TEST(test_cunit_get_n_allocations(ptr4) == test_cunit_get_n_deallocations(ptr4));
  TEST(n2 == test_cunit_get_n_memevents(ptr2));

  free(ptr2);
}

/** The main internal testing function for MyMem.c. */
void test_cunit_MyMem(void)
{
  test_cunit_start_tests("MyMem.c");

  test_CU_calloc();
  test_CU_malloc();
  test_CU_free();
  test_CU_realloc();

  test_cunit_end_tests();
}

#endif    /* CUNIT_BUILD_TESTS */
