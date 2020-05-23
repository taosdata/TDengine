/*******************************************************************************
 * Copyright (c) 2009, 2020 IBM Corp.
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
 *    Ian Craggs - updates for the async client
 *******************************************************************************/

/**
 * @file
 * \brief functions which apply to linked list structures.
 *
 * These linked lists can hold data of any sort, pointed to by the content pointer of the
 * ListElement structure.  ListElements hold the points to the next and previous items in the
 * list.
 * */

#include "LinkedList.h"

#include <stdlib.h>
#include <string.h>

#include "Heap.h"


static int ListUnlink(List* aList, void* content, int(*callback)(void*, void*), int freeContent);


/**
 * Sets a list structure to empty - all null values.  Does not remove any items from the list.
 * @param newl a pointer to the list structure to be initialized
 */
void ListZero(List* newl)
{
	memset(newl, '\0', sizeof(List));
}


/**
 * Allocates and initializes a new list structure.
 * @return a pointer to the new list structure
 */
List* ListInitialize(void)
{
	List* newl = malloc(sizeof(List));
	if (newl)
		ListZero(newl);
	return newl;
}


/**
 * Append an already allocated ListElement and content to a list.  Can be used to move
 * an item from one list to another.
 * @param aList the list to which the item is to be added
 * @param content the list item content itself
 * @param newel the ListElement to be used in adding the new item
 * @param size the size of the element
 */
void ListAppendNoMalloc(List* aList, void* content, ListElement* newel, size_t size)
{ /* for heap use */
	newel->content = content;
	newel->next = NULL;
	newel->prev = aList->last;
	if (aList->first == NULL)
		aList->first = newel;
	else
		aList->last->next = newel;
	aList->last = newel;
	++(aList->count);
	aList->size += size;
}


/**
 * Append an item to a list.
 * @param aList the list to which the item is to be added
 * @param content the list item content itself
 * @param size the size of the element
 */
ListElement* ListAppend(List* aList, void* content, size_t size)
{
	ListElement* newel = malloc(sizeof(ListElement));
	if (newel)
		ListAppendNoMalloc(aList, content, newel, size);
	return newel;
}


/**
 * Insert an item to a list at a specific position.
 * @param aList the list to which the item is to be added
 * @param content the list item content itself
 * @param size the size of the element
 * @param index the position in the list. If NULL, this function is equivalent
 * to ListAppend.
 */
ListElement* ListInsert(List* aList, void* content, size_t size, ListElement* index)
{
	ListElement* newel = malloc(sizeof(ListElement));

	if (newel == NULL)
		return newel;
	if ( index == NULL )
		ListAppendNoMalloc(aList, content, newel, size);
	else
	{
		newel->content = content;
		newel->next = index;
		newel->prev = index->prev;

		index->prev = newel;
		if ( newel->prev != NULL )
			newel->prev->next = newel;
		else
			aList->first = newel;

		++(aList->count);
		aList->size += size;
	}
	return newel;
}


/**
 * Finds an element in a list by comparing the content pointers, rather than the contents
 * @param aList the list in which the search is to be conducted
 * @param content pointer to the list item content itself
 * @return the list item found, or NULL
 */
ListElement* ListFind(List* aList, void* content)
{
	return ListFindItem(aList, content, NULL);
}


/**
 * Finds an element in a list by comparing the content or pointer to the content.  A callback
 * function is used to define the method of comparison for each element.
 * @param aList the list in which the search is to be conducted
 * @param content pointer to the content to look for
 * @param callback pointer to a function which compares each element (NULL means compare by content pointer)
 * @return the list element found, or NULL
 */
ListElement* ListFindItem(List* aList, void* content, int(*callback)(void*, void*))
{
	ListElement* rc = NULL;

	if (aList->current != NULL && ((callback == NULL && aList->current->content == content) ||
		   (callback != NULL && callback(aList->current->content, content))))
		rc = aList->current;
	else
	{
		ListElement* current = NULL;

		/* find the content */
		while (ListNextElement(aList, &current) != NULL)
		{
			if (callback == NULL)
			{
				if (current->content == content)
				{
					rc = current;
					break;
				}
			}
			else
			{
				if (callback(current->content, content))
				{
					rc = current;
					break;
				}
			}
		}
		if (rc != NULL)
			aList->current = rc;
	}
	return rc;
}


/**
 * Removes and optionally frees an element in a list by comparing the content.
 * A callback function is used to define the method of comparison for each element.
 * @param aList the list in which the search is to be conducted
 * @param content pointer to the content to look for
 * @param callback pointer to a function which compares each element
 * @param freeContent boolean value to indicate whether the item found is to be freed
 * @return 1=item removed, 0=item not removed
 */
static int ListUnlink(List* aList, void* content, int(*callback)(void*, void*), int freeContent)
{
	ListElement* next = NULL;
	ListElement* saved = aList->current;
	int saveddeleted = 0;

	if (!ListFindItem(aList, content, callback))
		return 0; /* false, did not remove item */

	if (aList->current->prev == NULL)
		/* so this is the first element, and we have to update the "first" pointer */
		aList->first = aList->current->next;
	else
		aList->current->prev->next = aList->current->next;

	if (aList->current->next == NULL)
		aList->last = aList->current->prev;
	else
		aList->current->next->prev = aList->current->prev;

	next = aList->current->next;
	if (freeContent)
    {
		free(aList->current->content);
                aList->current->content = NULL;
    }
	if (saved == aList->current)
		saveddeleted = 1;
	free(aList->current);
	if (saveddeleted)
		aList->current = next;
	else
		aList->current = saved;
	--(aList->count);
	return 1; /* successfully removed item */
}


/**
 * Removes but does not free an item in a list by comparing the pointer to the content.
 * @param aList the list in which the search is to be conducted
 * @param content pointer to the content to look for
 * @return 1=item removed, 0=item not removed
 */
int ListDetach(List* aList, void* content)
{
	return ListUnlink(aList, content, NULL, 0);
}


/**
 * Removes and frees an item in a list by comparing the pointer to the content.
 * @param aList the list from which the item is to be removed
 * @param content pointer to the content to look for
 * @return 1=item removed, 0=item not removed
 */
int ListRemove(List* aList, void* content)
{
	return ListUnlink(aList, content, NULL, 1);
}


/**
 * Removes and frees an the first item in a list.
 * @param aList the list from which the item is to be removed
 * @return 1=item removed, 0=item not removed
 */
void* ListDetachHead(List* aList)
{
	void *content = NULL;
	if (aList->count > 0)
	{
		ListElement* first = aList->first;
		if (aList->current == first)
			aList->current = first->next;
		if (aList->last == first) /* i.e. no of items in list == 1 */
			aList->last = NULL;
		content = first->content;
		aList->first = aList->first->next;
		if (aList->first)
			aList->first->prev = NULL;
		free(first);
		--(aList->count);
	}
	return content;
}


/**
 * Removes and frees an the first item in a list.
 * @param aList the list from which the item is to be removed
 * @return 1=item removed, 0=item not removed
 */
int ListRemoveHead(List* aList)
{
	free(ListDetachHead(aList));
	return 0;
}


/**
 * Removes but does not free the last item in a list.
 * @param aList the list from which the item is to be removed
 * @return the last item removed (or NULL if none was)
 */
void* ListPopTail(List* aList)
{
	void* content = NULL;
	if (aList->count > 0)
	{
		ListElement* last = aList->last;
		if (aList->current == last)
			aList->current = last->prev;
		if (aList->first == last) /* i.e. no of items in list == 1 */
			aList->first = NULL;
		content = last->content;
		aList->last = aList->last->prev;
		if (aList->last)
			aList->last->next = NULL;
		free(last);
		--(aList->count);
	}
	return content;
}


/**
 * Removes but does not free an element in a list by comparing the content.
 * A callback function is used to define the method of comparison for each element.
 * @param aList the list in which the search is to be conducted
 * @param content pointer to the content to look for
 * @param callback pointer to a function which compares each element
 * @return 1=item removed, 0=item not removed
 */
int ListDetachItem(List* aList, void* content, int(*callback)(void*, void*))
{ /* do not free the content */
	return ListUnlink(aList, content, callback, 0);
}


/**
 * Removes and frees an element in a list by comparing the content.
 * A callback function is used to define the method of comparison for each element
 * @param aList the list in which the search is to be conducted
 * @param content pointer to the content to look for
 * @param callback pointer to a function which compares each element
 * @return 1=item removed, 0=item not removed
 */
int ListRemoveItem(List* aList, void* content, int(*callback)(void*, void*))
{ /* remove from list and free the content */
	return ListUnlink(aList, content, callback, 1);
}


/**
 * Removes and frees all items in a list, leaving the list ready for new items.
 * @param aList the list to which the operation is to be applied
 */
void ListEmpty(List* aList)
{
	while (aList->first != NULL)
	{
		ListElement* first = aList->first;
		if (first->content != NULL)
                {
			free(first->content);
                        first->content = NULL;
                }
		aList->first = first->next;
		free(first);
	}
	aList->count = 0;
	aList->size = 0;
	aList->current = aList->first = aList->last = NULL;
}

/**
 * Removes and frees all items in a list, and frees the list itself
 * @param aList the list to which the operation is to be applied
 */
void ListFree(List* aList)
{
	ListEmpty(aList);
	free(aList);
}


/**
 * Removes and but does not free all items in a list, and frees the list itself
 * @param aList the list to which the operation is to be applied
 */
void ListFreeNoContent(List* aList)
{
	while (aList->first != NULL)
	{
		ListElement* first = aList->first;
		aList->first = first->next;
		free(first);
	}
	free(aList);
}


/**
 * Forward iteration through a list
 * @param aList the list to which the operation is to be applied
 * @param pos pointer to the current position in the list.  NULL means start from the beginning of the list
 * This is updated on return to the same value as that returned from this function
 * @return pointer to the current list element
 */
ListElement* ListNextElement(List* aList, ListElement** pos)
{
	return *pos = (*pos == NULL) ? aList->first : (*pos)->next;
}


/**
 * Backward iteration through a list
 * @param aList the list to which the operation is to be applied
 * @param pos pointer to the current position in the list.  NULL means start from the end of the list
 * This is updated on return to the same value as that returned from this function
 * @return pointer to the current list element
 */
ListElement* ListPrevElement(List* aList, ListElement** pos)
{
	return *pos = (*pos == NULL) ? aList->last : (*pos)->prev;
}


/**
 * List callback function for comparing integers
 * @param a first integer value
 * @param b second integer value
 * @return boolean indicating whether a and b are equal
 */
int intcompare(void* a, void* b)
{
	return *((int*)a) == *((int*)b);
}


/**
 * List callback function for comparing C strings
 * @param a first integer value
 * @param b second integer value
 * @return boolean indicating whether a and b are equal
 */
int stringcompare(void* a, void* b)
{
	return strcmp((char*)a, (char*)b) == 0;
}


#if defined(UNIT_TESTS)


int main(int argc, char *argv[])
{
	int i, *ip, *todelete;
	ListElement* current = NULL;
	List* l = ListInitialize();
	printf("List initialized\n");

	for (i = 0; i < 10; i++)
	{
		ip = malloc(sizeof(int));
		*ip = i;
		ListAppend(l, (void*)ip, sizeof(int));
		if (i==5)
			todelete = ip;
		printf("List element appended %d\n",  *((int*)(l->last->content)));
	}

	printf("List contents:\n");
	current = NULL;
	while (ListNextElement(l, &current) != NULL)
		printf("List element: %d\n", *((int*)(current->content)));

	printf("List contents in reverse order:\n");
	current = NULL;
	while (ListPrevElement(l, &current) != NULL)
		printf("List element: %d\n", *((int*)(current->content)));

	/* if ListFindItem(l, *ip, intcompare)->content */

	printf("List contents having deleted element %d:\n", *todelete);
	ListRemove(l, todelete);
	current = NULL;
	while (ListNextElement(l, &current) != NULL)
		printf("List element: %d\n", *((int*)(current->content)));

	i = 9;
	ListRemoveItem(l, &i, intcompare);
	printf("List contents having deleted another element, %d, size now %d:\n", i, l->size);
	current = NULL;
	while (ListNextElement(l, &current) != NULL)
		printf("List element: %d\n", *((int*)(current->content)));

	ListFree(l);
	printf("List freed\n");
}

#endif





