/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/* SPDX-License-Identifier: GPL-2.0 */
#ifndef _TOD_LIST_H
#define _TOD_LIST_H

#include "macros.h"

#include <stdbool.h>
#include <stddef.h>

EXTERN_C_BEGIN

struct tod_list_head {
  struct tod_list_head *next, *prev;
};

struct tod_hlist_head {
  struct tod_hlist_node *first;
};

struct tod_hlist_node {
  struct tod_hlist_node *next, **pprev;
};

// we know this is not thread safe
#define READ_ONCE(x) (x)
#define WRITE_ONCE(a, b)      a = (b)
// make sure this would result in page fault with different `bad address`
#define TOD_LIST_POISON1  ((void *) 0x100)
#define TOD_LIST_POISON2  ((void *) 0x122)

/**
 * container_of - cast a member of a structure out to the containing structure
 * @ptr:  the pointer to the member.
 * @type: the type of the container struct this is embedded in.
 * @member: the name of the member within the struct.
 *
 */
#define container_of(ptr, type, member)           \
  ((type *)(((unsigned char*)(ptr)) - offsetof(type, member)))

/*
 * Circular doubly linked tod_list implementation.
 *
 * Some of the internal functions ("__xxx") are useful when
 * manipulating whole tod_lists rather than single entries, as
 * sometimes we already know the next/prev entries and we can
 * generate better code by using them directly rather than
 * using the generic single-entry routines.
 */

#define TOD_LIST_HEAD_INIT(name) { &(name), &(name) }

#define TOD_LIST_HEAD(name) \
  struct tod_list_head name = TOD_LIST_HEAD_INIT(name)

/**
 * INIT_TOD_LIST_HEAD - Initialize a tod_list_head structure
 * @tod_list: tod_list_head structure to be initialized.
 *
 * Initializes the tod_list_head to point to itself.  If it is a tod_list header,
 * the result is an empty tod_list.
 */
static inline void INIT_TOD_LIST_HEAD(struct tod_list_head *tod_list)
{
  WRITE_ONCE(tod_list->next, tod_list);
  tod_list->prev = tod_list;
}

/*
 * Insert a new entry between two known consecutive entries.
 *
 * This is only for internal tod_list manipulation where we know
 * the prev/next entries already!
 */
static inline void __tod_list_add(struct tod_list_head *new_,
            struct tod_list_head *prev,
            struct tod_list_head *next)
{
  next->prev = new_;
  new_->next = next;
  new_->prev = prev;
  WRITE_ONCE(prev->next, new_);
}

/**
 * tod_list_add - add a new entry
 * @new: new entry to be added
 * @head: tod_list head to add it after
 *
 * Insert a new entry after the specified head.
 * This is good for implementing stacks.
 */
static inline void tod_list_add(struct tod_list_head *new_, struct tod_list_head *head)
{
  __tod_list_add(new_, head, head->next);
}


/**
 * tod_list_add_tail - add a new entry
 * @new: new entry to be added
 * @head: tod_list head to add it before
 *
 * Insert a new entry before the specified head.
 * This is useful for implementing queues.
 */
static inline void tod_list_add_tail(struct tod_list_head *new_, struct tod_list_head *head)
{
  __tod_list_add(new_, head->prev, head);
}

/*
 * Delete a tod_list entry by making the prev/next entries
 * point to each other.
 *
 * This is only for internal tod_list manipulation where we know
 * the prev/next entries already!
 */
static inline void __tod_list_del(struct tod_list_head * prev, struct tod_list_head * next)
{
  next->prev = prev;
  WRITE_ONCE(prev->next, next);
}

/*
 * Delete a tod_list entry and clear the 'prev' pointer.
 *
 * This is a special-purpose tod_list clearing method used in the networking code
 * for tod_lists allocated as per-cpu, where we don't want to incur the extra
 * WRITE_ONCE() overhead of a regular tod_list_del_init(). The code that uses this
 * needs to check the node 'prev' pointer instead of calling tod_list_empty().
 */
static inline void __tod_list_del_clearprev(struct tod_list_head *entry)
{
  __tod_list_del(entry->prev, entry->next);
  entry->prev = NULL;
}

static inline void __tod_list_del_entry(struct tod_list_head *entry)
{
  __tod_list_del(entry->prev, entry->next);
}

/**
 * tod_list_del - deletes entry from tod_list.
 * @entry: the element to delete from the tod_list.
 * Note: tod_list_empty() on entry does not return true after this, the entry is
 * in an undefined state.
 */
static inline void tod_list_del(struct tod_list_head *entry)
{
  __tod_list_del_entry(entry);
  entry->next = (struct tod_list_head*)TOD_LIST_POISON1;
  entry->prev = (struct tod_list_head*)TOD_LIST_POISON2;
}

/**
 * tod_list_replace - replace old entry by new one
 * @old : the element to be replaced
 * @new : the new element to insert
 *
 * If @old was empty, it will be overwritten.
 */
static inline void tod_list_replace(struct tod_list_head *old,
        struct tod_list_head *new_)
{
  new_->next = old->next;
  new_->next->prev = new_;
  new_->prev = old->prev;
  new_->prev->next = new_;
}

/**
 * tod_list_replace_init - replace old entry by new one and initialize the old one
 * @old : the element to be replaced
 * @new : the new element to insert
 *
 * If @old was empty, it will be overwritten.
 */
static inline void tod_list_replace_init(struct tod_list_head *old,
             struct tod_list_head *new_)
{
  tod_list_replace(old, new_);
  INIT_TOD_LIST_HEAD(old);
}

/**
 * tod_list_swap - replace entry1 with entry2 and re-add entry1 at entry2's position
 * @entry1: the location to place entry2
 * @entry2: the location to place entry1
 */
static inline void tod_list_swap(struct tod_list_head *entry1,
           struct tod_list_head *entry2)
{
  struct tod_list_head *pos = entry2->prev;

  tod_list_del(entry2);
  tod_list_replace(entry1, entry2);
  if (pos == entry1)
    pos = entry2;
  tod_list_add(entry1, pos);
}

/**
 * tod_list_del_init - deletes entry from tod_list and reinitialize it.
 * @entry: the element to delete from the tod_list.
 */
static inline void tod_list_del_init(struct tod_list_head *entry)
{
  __tod_list_del_entry(entry);
  INIT_TOD_LIST_HEAD(entry);
}

/**
 * tod_list_move - delete from one tod_list and add as another's head
 * @tod_list: the entry to move
 * @head: the head that will precede our entry
 */
static inline void tod_list_move(struct tod_list_head *tod_list, struct tod_list_head *head)
{
  __tod_list_del_entry(tod_list);
  tod_list_add(tod_list, head);
}

/**
 * tod_list_move_tail - delete from one tod_list and add as another's tail
 * @tod_list: the entry to move
 * @head: the head that will follow our entry
 */
static inline void tod_list_move_tail(struct tod_list_head *tod_list,
          struct tod_list_head *head)
{
  __tod_list_del_entry(tod_list);
  tod_list_add_tail(tod_list, head);
}

/**
 * tod_list_bulk_move_tail - move a subsection of a tod_list to its tail
 * @head: the head that will follow our entry
 * @first: first entry to move
 * @last: last entry to move, can be the same as first
 *
 * Move all entries between @first and including @last before @head.
 * All three entries must belong to the same linked tod_list.
 */
static inline void tod_list_bulk_move_tail(struct tod_list_head *head,
               struct tod_list_head *first,
               struct tod_list_head *last)
{
  first->prev->next = last->next;
  last->next->prev = first->prev;

  head->prev->next = first;
  first->prev = head->prev;

  last->next = head;
  head->prev = last;
}

/**
 * tod_list_is_first -- tests whether @tod_list is the first entry in tod_list @head
 * @tod_list: the entry to test
 * @head: the head of the tod_list
 */
static inline int tod_list_is_first(const struct tod_list_head *tod_list,
          const struct tod_list_head *head)
{
  return tod_list->prev == head;
}

/**
 * tod_list_is_last - tests whether @tod_list is the last entry in tod_list @head
 * @tod_list: the entry to test
 * @head: the head of the tod_list
 */
static inline int tod_list_is_last(const struct tod_list_head *tod_list,
        const struct tod_list_head *head)
{
  return tod_list->next == head;
}

/**
 * tod_list_empty - tests whether a tod_list is empty
 * @head: the tod_list to test.
 */
static inline int tod_list_empty(const struct tod_list_head *head)
{
  return READ_ONCE(head->next) == head;
}

/**
 * tod_list_rotate_left - rotate the tod_list to the left
 * @head: the head of the tod_list
 */
static inline void tod_list_rotate_left(struct tod_list_head *head)
{
  struct tod_list_head *first;

  if (!tod_list_empty(head)) {
    first = head->next;
    tod_list_move_tail(first, head);
  }
}

/**
 * tod_list_rotate_to_front() - Rotate tod_list to specific item.
 * @tod_list: The desired new front of the tod_list.
 * @head: The head of the tod_list.
 *
 * Rotates tod_list so that @tod_list becomes the new front of the tod_list.
 */
static inline void tod_list_rotate_to_front(struct tod_list_head *tod_list,
          struct tod_list_head *head)
{
  /*
   * Deletes the tod_list head from the tod_list denoted by @head and
   * places it as the tail of @tod_list, this effectively rotates the
   * tod_list so that @tod_list is at the front.
   */
  tod_list_move_tail(head, tod_list);
}

/**
 * tod_list_is_singular - tests whether a tod_list has just one entry.
 * @head: the tod_list to test.
 */
static inline int tod_list_is_singular(const struct tod_list_head *head)
{
  return !tod_list_empty(head) && (head->next == head->prev);
}

static inline void __tod_list_cut_position(struct tod_list_head *tod_list,
    struct tod_list_head *head, struct tod_list_head *entry)
{
  struct tod_list_head *new_first = entry->next;
  tod_list->next = head->next;
  tod_list->next->prev = tod_list;
  tod_list->prev = entry;
  entry->next = tod_list;
  head->next = new_first;
  new_first->prev = head;
}

/**
 * tod_list_cut_position - cut a tod_list into two
 * @tod_list: a new tod_list to add all removed entries
 * @head: a tod_list with entries
 * @entry: an entry within head, could be the head itself
 *  and if so we won't cut the tod_list
 *
 * This helper moves the initial part of @head, up to and
 * including @entry, from @head to @tod_list. You should
 * pass on @entry an element you know is on @head. @tod_list
 * should be an empty tod_list or a tod_list you do not care about
 * losing its data.
 *
 */
static inline void tod_list_cut_position(struct tod_list_head *tod_list,
    struct tod_list_head *head, struct tod_list_head *entry)
{
  if (tod_list_empty(head))
    return;
  if (tod_list_is_singular(head) &&
    (head->next != entry && head != entry))
    return;
  if (entry == head)
    INIT_TOD_LIST_HEAD(tod_list);
  else
    __tod_list_cut_position(tod_list, head, entry);
}

/**
 * tod_list_cut_before - cut a tod_list into two, before given entry
 * @tod_list: a new tod_list to add all removed entries
 * @head: a tod_list with entries
 * @entry: an entry within head, could be the head itself
 *
 * This helper moves the initial part of @head, up to but
 * excluding @entry, from @head to @tod_list.  You should pass
 * in @entry an element you know is on @head.  @tod_list should
 * be an empty tod_list or a tod_list you do not care about losing
 * its data.
 * If @entry == @head, all entries on @head are moved to
 * @tod_list.
 */
static inline void tod_list_cut_before(struct tod_list_head *tod_list,
           struct tod_list_head *head,
           struct tod_list_head *entry)
{
  if (head->next == entry) {
    INIT_TOD_LIST_HEAD(tod_list);
    return;
  }
  tod_list->next = head->next;
  tod_list->next->prev = tod_list;
  tod_list->prev = entry->prev;
  tod_list->prev->next = tod_list;
  head->next = entry;
  entry->prev = head;
}

static inline void __tod_list_splice(const struct tod_list_head *tod_list,
         struct tod_list_head *prev,
         struct tod_list_head *next)
{
  struct tod_list_head *first = tod_list->next;
  struct tod_list_head *last = tod_list->prev;

  first->prev = prev;
  prev->next = first;

  last->next = next;
  next->prev = last;
}

/**
 * tod_list_splice - join two tod_lists, this is designed for stacks
 * @tod_list: the new tod_list to add.
 * @head: the place to add it in the first tod_list.
 */
static inline void tod_list_splice(const struct tod_list_head *tod_list,
        struct tod_list_head *head)
{
  if (!tod_list_empty(tod_list))
    __tod_list_splice(tod_list, head, head->next);
}

/**
 * tod_list_splice_tail - join two tod_lists, each tod_list being a queue
 * @tod_list: the new tod_list to add.
 * @head: the place to add it in the first tod_list.
 */
static inline void tod_list_splice_tail(struct tod_list_head *tod_list,
        struct tod_list_head *head)
{
  if (!tod_list_empty(tod_list))
    __tod_list_splice(tod_list, head->prev, head);
}

/**
 * tod_list_splice_init - join two tod_lists and reinitialise the emptied tod_list.
 * @tod_list: the new tod_list to add.
 * @head: the place to add it in the first tod_list.
 *
 * The tod_list at @tod_list is reinitialised
 */
static inline void tod_list_splice_init(struct tod_list_head *tod_list,
            struct tod_list_head *head)
{
  if (!tod_list_empty(tod_list)) {
    __tod_list_splice(tod_list, head, head->next);
    INIT_TOD_LIST_HEAD(tod_list);
  }
}

/**
 * tod_list_splice_tail_init - join two tod_lists and reinitialise the emptied tod_list
 * @tod_list: the new tod_list to add.
 * @head: the place to add it in the first tod_list.
 *
 * Each of the tod_lists is a queue.
 * The tod_list at @tod_list is reinitialised
 */
static inline void tod_list_splice_tail_init(struct tod_list_head *tod_list,
           struct tod_list_head *head)
{
  if (!tod_list_empty(tod_list)) {
    __tod_list_splice(tod_list, head->prev, head);
    INIT_TOD_LIST_HEAD(tod_list);
  }
}

/**
 * tod_list_entry - get the struct for this entry
 * @ptr:  the &struct tod_list_head pointer.
 * @type:  the type of the struct this is embedded in.
 * @member:  the name of the tod_list_head within the struct.
 */
#define tod_list_entry(ptr, type, member) \
  container_of(ptr, type, member)

/**
 * tod_list_first_entry - get the first element from a tod_list
 * @ptr:  the tod_list head to take the element from.
 * @type:  the type of the struct this is embedded in.
 * @member:  the name of the tod_list_head within the struct.
 *
 * Note, that tod_list is expected to be not empty.
 */
#define tod_list_first_entry(ptr, type, member) \
  tod_list_entry((ptr)->next, type, member)

/**
 * tod_list_last_entry - get the last element from a tod_list
 * @ptr:  the tod_list head to take the element from.
 * @type:  the type of the struct this is embedded in.
 * @member:  the name of the tod_list_head within the struct.
 *
 * Note, that tod_list is expected to be not empty.
 */
#define tod_list_last_entry(ptr, type, member) \
  tod_list_entry((ptr)->prev, type, member)

/**
 * tod_list_first_entry_or_null - get the first element from a tod_list
 * @ptr:  the tod_list head to take the element from.
 * @type:  the type of the struct this is embedded in.
 * @member:  the name of the tod_list_head within the struct.
 *
 * Note that if the tod_list is empty, it returns NULL.
 */
#define tod_list_first_entry_or_null(result, ptr, type, member) do {      \
  struct tod_list_head *_ptr = ptr;                                       \
  result = _ptr->next ? tod_list_first_entry(_ptr, type, member) : NULL;  \
} while (0)

/**
 * tod_list_next_entry - get the next element in tod_list
 * @pos:  the type * to cursor
 * @member:  the name of the tod_list_head within the struct.
 */
#define tod_list_next_entry(pos, type, member) \
  tod_list_entry((pos)->member.next, type, member)

/**
 * tod_list_prev_entry - get the prev element in tod_list
 * @pos:  the type * to cursor
 * @member:  the name of the tod_list_head within the struct.
 */
#define tod_list_prev_entry(pos, type, member) \
  tod_list_entry((pos)->member.prev, type, member)

/**
 * tod_list_for_each  -  iterate over a tod_list
 * @pos:  the &struct tod_list_head to use as a loop cursor.
 * @head:  the head for your tod_list.
 */
#define tod_list_for_each(pos, head) \
  for (pos = (head)->next; pos != (head); pos = pos->next)

/**
 * tod_list_for_each_continue - continue iteration over a tod_list
 * @pos:  the &struct tod_list_head to use as a loop cursor.
 * @head:  the head for your tod_list.
 *
 * Continue to iterate over a tod_list, continuing after the current position.
 */
#define tod_list_for_each_continue(pos, head) \
  for (pos = pos->next; pos != (head); pos = pos->next)

/**
 * tod_list_for_each_prev  -  iterate over a tod_list backwards
 * @pos:  the &struct tod_list_head to use as a loop cursor.
 * @head:  the head for your tod_list.
 */
#define tod_list_for_each_prev(pos, head) \
  for (pos = (head)->prev; pos != (head); pos = pos->prev)

/**
 * tod_list_for_each_safe - iterate over a tod_list safe against removal of tod_list entry
 * @pos:  the &struct tod_list_head to use as a loop cursor.
 * @n:    another &struct tod_list_head to use as temporary storage
 * @head:  the head for your tod_list.
 */
#define tod_list_for_each_safe(pos, n, head)             \
  for (pos = (head)->next, n = pos->next;                \
       pos != (head);                                    \
       pos = n, n = pos->next)

/**
 * tod_list_for_each_prev_safe - iterate over a tod_list backwards safe against removal of tod_list entry
 * @pos:  the &struct tod_list_head to use as a loop cursor.
 * @n:    another &struct tod_list_head to use as temporary storage
 * @head:  the head for your tod_list.
 */
#define tod_list_for_each_prev_safe(pos, n, head) \
  for (pos = (head)->prev, n = pos->prev;         \
       pos != (head);                             \
       pos = n, n = pos->prev)

/**
 * tod_list_entry_is_head - test if the entry points to the head of the tod_list
 * @pos:  the type * to cursor
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_list_head within the struct.
 */
#define tod_list_entry_is_head(pos, head, type, member)        \
  (&(pos)->member == (head))

/**
 * tod_list_for_each_entry  -  iterate over tod_list of given type
 * @pos:  the type * to use as a loop cursor.
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_list_head within the struct.
 */
#define tod_list_for_each_entry(pos, head, type, member)        \
  for (pos = tod_list_first_entry(head, type, member);          \
       !tod_list_entry_is_head(pos, head, type, member);        \
       pos = tod_list_next_entry(pos, type, member))

/**
 * tod_list_for_each_entry_reverse - iterate backwards over tod_list of given type.
 * @pos:  the type * to use as a loop cursor.
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_list_head within the struct.
 */
#define tod_list_for_each_entry_reverse(pos, head, type, member)    \
  for (pos = tod_list_last_entry(head, type, member);               \
       !tod_list_entry_is_head(pos, head, type, member);            \
       pos = tod_list_prev_entry(pos, type, member))

/**
 * tod_list_for_each_entry_continue - continue iteration over tod_list of given type
 * @pos:  the type * to use as a loop cursor.
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_list_head within the struct.
 *
 * Continue to iterate over tod_list of given type, continuing after
 * the current position.
 */
#define tod_list_for_each_entry_continue(pos, head, type, member)     \
  for (pos = tod_list_next_entry(pos, type, member);                  \
       !tod_list_entry_is_head(pos, head, type, member);              \
       pos = tod_list_next_entry(pos, type, member))

/**
 * tod_list_for_each_entry_continue_reverse - iterate backwards from the given point
 * @pos:  the type * to use as a loop cursor.
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_list_head within the struct.
 *
 * Start to iterate over tod_list of given type backwards, continuing after
 * the current position.
 */
#define tod_list_for_each_entry_continue_reverse(pos, head, type, member)    \
  for (pos = tod_list_prev_entry(pos, type, member);                         \
       !tod_list_entry_is_head(pos, head, type, member);                     \
       pos = tod_list_prev_entry(pos, type, member))

/**
 * tod_list_for_each_entry_from - iterate over tod_list of given type from the current point
 * @pos:  the type * to use as a loop cursor.
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_list_head within the struct.
 *
 * Iterate over tod_list of given type, continuing from current position.
 */
#define tod_list_for_each_entry_from(pos, head, type, member)       \
  for (; !tod_list_entry_is_head(pos, head, type, member);          \
       pos = tod_list_next_entry(pos, type, member))

/**
 * tod_list_for_each_entry_from_reverse - iterate backwards over tod_list of given type
 *                                    from the current point
 * @pos:  the type * to use as a loop cursor.
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_list_head within the struct.
 *
 * Iterate backwards over tod_list of given type, continuing from current position.
 */
#define tod_list_for_each_entry_from_reverse(pos, head, type, member)    \
  for (; !tod_list_entry_is_head(pos, head, type, member);               \
       pos = tod_list_prev_entry(pos, type, member))

/**
 * tod_list_for_each_entry_safe - iterate over tod_list of given type safe against removal of tod_list entry
 * @pos:  the type * to use as a loop cursor.
 * @n:    another type * to use as temporary storage
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_list_head within the struct.
 */
#define tod_list_for_each_entry_safe(pos, n, head, type, member)      \
  for (pos = tod_list_first_entry(head, type, member),                \
    n = tod_list_next_entry(pos, type, member);                       \
       !tod_list_entry_is_head(pos, head, type, member);              \
       pos = n, n = tod_list_next_entry(n, type, member))

/**
 * tod_list_for_each_entry_safe_continue - continue tod_list iteration safe against removal
 * @pos:  the type * to use as a loop cursor.
 * @n:    another type * to use as temporary storage
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_list_head within the struct.
 *
 * Iterate over tod_list of given type, continuing after current point,
 * safe against removal of tod_list entry.
 */
#define tod_list_for_each_entry_safe_continue(pos, n, head, type, member)     \
  for (pos = tod_list_next_entry(pos, type, member),                          \
    n = tod_list_next_entry(pos, type, member);                               \
       !tod_list_entry_is_head(pos, head, type, member);                      \
       pos = n, n = tod_list_next_entry(n, type, member))

/**
 * tod_list_for_each_entry_safe_from - iterate over tod_list from current point safe against removal
 * @pos:  the type * to use as a loop cursor.
 * @n:    another type * to use as temporary storage
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_list_head within the struct.
 *
 * Iterate over tod_list of given type from current point, safe against
 * removal of tod_list entry.
 */
#define tod_list_for_each_entry_safe_from(pos, n, head, type, member)       \
  for (n = tod_list_next_entry(pos, type, member);                          \
       !tod_list_entry_is_head(pos, head, type, member);                    \
       pos = n, n = tod_list_next_entry(n, type, member))

/**
 * tod_list_for_each_entry_safe_reverse - iterate backwards over tod_list safe against removal
 * @pos:  the type * to use as a loop cursor.
 * @n:    another type * to use as temporary storage
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_list_head within the struct.
 *
 * Iterate backwards over tod_list of given type, safe against removal
 * of tod_list entry.
 */
#define tod_list_for_each_entry_safe_reverse(pos, n, head, type, member)    \
  for (pos = tod_list_last_entry(head, type, member),                       \
    n = tod_list_prev_entry(pos, type, member);                             \
       !tod_list_entry_is_head(pos, head, type, member);                    \
       pos = n, n = tod_list_prev_entry(n, type, member))

/**
 * tod_list_safe_reset_next - reset a stale tod_list_for_each_entry_safe loop
 * @pos:  the loop cursor used in the tod_list_for_each_entry_safe loop
 * @n:    temporary storage used in tod_list_for_each_entry_safe
 * @member:  the name of the tod_list_head within the struct.
 *
 * tod_list_safe_reset_next is not safe to use in general if the tod_list may be
 * modified concurrently (eg. the lock is dropped in the loop body). An
 * exception to this is if the cursor element (pos) is pinned in the tod_list,
 * and tod_list_safe_reset_next is called after re-taking the lock and before
 * completing the current iteration of the loop body.
 */
#define tod_list_safe_reset_next(pos, n, type, member)        \
  n = tod_list_next_entry(pos, type, member)

/*
 * Double linked tod_lists with a single pointer tod_list head.
 * Mostly useful for hash tables where the two pointer tod_list head is
 * too wasteful.
 * You lose the ability to access the tail in O(1).
 */

#define TOD_HLIST_HEAD_INIT { .first = NULL }
#define TOD_HLIST_HEAD(name) struct tod_hlist_head name = {  .first = NULL }
#define INIT_TOD_HLIST_HEAD(ptr) ((ptr)->first = NULL)
static inline void INIT_TOD_HLIST_NODE(struct tod_hlist_node *h)
{
  h->next = NULL;
  h->pprev = NULL;
}

/**
 * tod_hlist_unhashed - Has node been removed from tod_list and reinitialized?
 * @h: Node to be checked
 *
 * Not that not all removal functions will leave a node in unhashed
 * state.  For example, tod_hlist_nulls_del_init_rcu() does leave the
 * node in unhashed state, but tod_hlist_nulls_del() does not.
 */
static inline int tod_hlist_unhashed(const struct tod_hlist_node *h)
{
  return !h->pprev;
}

/**
 * tod_hlist_unhashed_lockless - Version of tod_hlist_unhashed for lockless use
 * @h: Node to be checked
 *
 * This variant of tod_hlist_unhashed() must be used in lockless contexts
 * to avoid potential load-tearing.  The READ_ONCE() is paired with the
 * various WRITE_ONCE() in tod_hlist helpers that are defined below.
 */
static inline int tod_hlist_unhashed_lockless(const struct tod_hlist_node *h)
{
  return !READ_ONCE(h->pprev);
}

/**
 * tod_hlist_empty - Is the specified tod_hlist_head structure an empty tod_hlist?
 * @h: Structure to check.
 */
static inline int tod_hlist_empty(const struct tod_hlist_head *h)
{
  return !READ_ONCE(h->first);
}

static inline void __tod_hlist_del(struct tod_hlist_node *n)
{
  struct tod_hlist_node *next = n->next;
  struct tod_hlist_node **pprev = n->pprev;

  WRITE_ONCE(*pprev, next);
  if (next)
    WRITE_ONCE(next->pprev, pprev);
}

/**
 * tod_hlist_del - Delete the specified tod_hlist_node from its tod_list
 * @n: Node to delete.
 *
 * Note that this function leaves the node in hashed state.  Use
 * tod_hlist_del_init() or similar instead to unhash @n.
 */
static inline void tod_hlist_del(struct tod_hlist_node *n)
{
  __tod_hlist_del(n);
  n->next  = (struct tod_hlist_node*)TOD_LIST_POISON1;
  n->pprev = (struct tod_hlist_node**)TOD_LIST_POISON2;
}

/**
 * tod_hlist_del_init - Delete the specified tod_hlist_node from its tod_list and initialize
 * @n: Node to delete.
 *
 * Note that this function leaves the node in unhashed state.
 */
static inline void tod_hlist_del_init(struct tod_hlist_node *n)
{
  if (!tod_hlist_unhashed(n)) {
    __tod_hlist_del(n);
    INIT_TOD_HLIST_NODE(n);
  }
}

/**
 * tod_hlist_add_head - add a new entry at the beginning of the tod_hlist
 * @n: new entry to be added
 * @h: tod_hlist head to add it after
 *
 * Insert a new entry after the specified head.
 * This is good for implementing stacks.
 */
static inline void tod_hlist_add_head(struct tod_hlist_node *n, struct tod_hlist_head *h)
{
  struct tod_hlist_node *first = h->first;
  WRITE_ONCE(n->next, first);
  if (first)
    WRITE_ONCE(first->pprev, &n->next);
  WRITE_ONCE(h->first, n);
  WRITE_ONCE(n->pprev, &h->first);
}

/**
 * tod_hlist_add_before - add a new entry before the one specified
 * @n: new entry to be added
 * @next: tod_hlist node to add it before, which must be non-NULL
 */
static inline void tod_hlist_add_before(struct tod_hlist_node *n,
            struct tod_hlist_node *next)
{
  WRITE_ONCE(n->pprev, next->pprev);
  WRITE_ONCE(n->next, next);
  WRITE_ONCE(next->pprev, &n->next);
  WRITE_ONCE(*(n->pprev), n);
}

/**
 * tod_hlist_add_behind - add a new entry after the one specified
 * @n: new entry to be added
 * @prev: tod_hlist node to add it after, which must be non-NULL
 */
static inline void tod_hlist_add_behind(struct tod_hlist_node *n,
            struct tod_hlist_node *prev)
{
  WRITE_ONCE(n->next, prev->next);
  WRITE_ONCE(prev->next, n);
  WRITE_ONCE(n->pprev, &prev->next);

  if (n->next)
    WRITE_ONCE(n->next->pprev, &n->next);
}

/**
 * tod_hlist_add_fake - create a fake tod_hlist consisting of a single headless node
 * @n: Node to make a fake tod_list out of
 *
 * This makes @n appear to be its own predecessor on a headless tod_hlist.
 * The point of this is to allow things like tod_hlist_del() to work correctly
 * in cases where there is no tod_list.
 */
static inline void tod_hlist_add_fake(struct tod_hlist_node *n)
{
  n->pprev = &n->next;
}

/**
 * tod_hlist_fake: Is this node a fake tod_hlist?
 * @h: Node to check for being a self-referential fake tod_hlist.
 */
static inline bool tod_hlist_fake(struct tod_hlist_node *h)
{
  return h->pprev == &h->next;
}

/**
 * tod_hlist_is_singular_node - is node the only element of the specified tod_hlist?
 * @n: Node to check for singularity.
 * @h: Header for potentially singular tod_list.
 *
 * Check whether the node is the only node of the head without
 * accessing head, thus avoiding unnecessary cache misses.
 */
static inline bool
tod_hlist_is_singular_node(struct tod_hlist_node *n, struct tod_hlist_head *h)
{
  return !n->next && n->pprev == &h->first;
}

/**
 * tod_hlist_move_tod_list - Move an tod_hlist
 * @old: tod_hlist_head for old tod_list.
 * @new: tod_hlist_head for new tod_list.
 *
 * Move a tod_list from one tod_list head to another. Fixup the pprev
 * reference of the first entry if it exists.
 */
static inline void tod_hlist_move_tod_list(struct tod_hlist_head *old,
           struct tod_hlist_head *new_)
{
  new_->first = old->first;
  if (new_->first)
    new_->first->pprev = &new_->first;
  old->first = NULL;
}

#define tod_hlist_entry(ptr, type, member) container_of(ptr,type,member)

#define tod_hlist_for_each(pos, head) \
  for (pos = (head)->first; pos ; pos = pos->next)

#define tod_hlist_for_each_safe(pos, n, head)               \
  for (pos = (head)->first, n = pos ? pos : NULL;           \
       pos;                                                 \
       n = pos->next, pos = n)


#define tod_hlist_entry_safe(result, ptr, type, member)  do {             \
  type *_ptr = ptr;                                                       \
  result = _ptr ? tod_hlist_entry(_ptr, type, member) : NULL;             \
} while (0)

/**
 * tod_hlist_for_each_entry  - iterate over tod_list of given type
 * @pos:  the type * to use as a loop cursor.
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_hlist_node within the struct.
 */
#define tod_hlist_for_each_entry(pos, head, type, member)                      \
  for (pos = tod_hlist_entry_safe((head)->first, type, member);                \
       pos;                                                                    \
       pos = tod_hlist_entry_safe((pos)->member.next, type, member))

/**
 * tod_hlist_for_each_entry_continue - iterate over a tod_hlist continuing after current point
 * @pos:  the type * to use as a loop cursor.
 * @member:  the name of the tod_hlist_node within the struct.
 */
#define tod_hlist_for_each_entry_continue(pos, type, member)                   \
  for (pos = tod_hlist_entry_safe((pos)->member.next, type, member);           \
       pos;                                                                    \
       pos = tod_hlist_entry_safe((pos)->member.next, type, member))

/**
 * tod_hlist_for_each_entry_from - iterate over a tod_hlist continuing from current point
 * @pos:  the type * to use as a loop cursor.
 * @member:  the name of the tod_hlist_node within the struct.
 */
#define tod_hlist_for_each_entry_from(pos, type, member)                       \
  for (; pos;                                                                  \
       pos = tod_hlist_entry_safe((pos)->member.next, type, member))

/**
 * tod_hlist_for_each_entry_safe - iterate over tod_list of given type safe against removal of tod_list entry
 * @pos:  the type * to use as a loop cursor.
 * @n:    a &struct tod_hlist_node to use as temporary storage
 * @head:  the head for your tod_list.
 * @member:  the name of the tod_hlist_node within the struct.
 */
#define tod_hlist_for_each_entry_safe(pos, n, head, type, member)        \
  for (pos = tod_hlist_entry_safe((head)->first, type, member);          \
       pos && { n = pos->member.next; 1; };                              \
       pos = tod_hlist_entry_safe(n, type, member))

EXTERN_C_END

#endif // _TOD_LIST_H

