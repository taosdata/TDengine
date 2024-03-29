
/*-------------------------------------------------------------------------*/
/**
   @file    dictionary.h
   @author  N. Devillard
   @brief   Implements a dictionary for string variables.

   This module implements a simple dictionary object, i.e. a list
   of string/string associations. This object is useful to store e.g.
   informations retrieved from a configuration file (ini files).
*/
/*--------------------------------------------------------------------------*/

#ifndef _DICTIONARY_H_
#define _DICTIONARY_H_

/*---------------------------------------------------------------------------
                                Includes
 ---------------------------------------------------------------------------*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*---------------------------------------------------------------------------
                                New types
 ---------------------------------------------------------------------------*/

#ifdef __cplusplus
extern "C" {
#endif

/*-------------------------------------------------------------------------*/
/**
  @brief    Dictionary object

  This object contains a list of string/string associations. Each
  association is identified by a unique string key. Looking up values
  in the dictionary is speeded up by the use of a (hopefully collision-free)
  hash function.
 */
/*-------------------------------------------------------------------------*/
typedef struct _dictionary_ {
    int             n ;     /** Number of entries in dictionary */
    int             size ;  /** Storage size */
    char        **  val ;   /** List of string values */
    char        **  key ;   /** List of string keys */
    unsigned     *  hash ;  /** List of hash values for keys */
} dictionary ;


/*---------------------------------------------------------------------------
                            Function prototypes
 ---------------------------------------------------------------------------*/

/*-------------------------------------------------------------------------*/
/**
  @brief    Compute the hash key for a string.
  @param    key     Character string to use for key.
  @return   1 unsigned int on at least 32 bits.

  This hash function has been taken from an Article in Dr Dobbs Journal.
  This is normally a collision-free function, distributing keys evenly.
  The key is stored anyway in the struct so that collision can be avoided
  by comparing the key itself in last resort.
 */
/*--------------------------------------------------------------------------*/
unsigned dictionary_hash(const char * key);

/*-------------------------------------------------------------------------*/
/**
  @brief    Create a new dictionary object.
  @param    size    Optional initial size of the dictionary.
  @return   1 newly allocated dictionary objet.

  This function allocates a new dictionary object of given size and returns
  it. If you do not know in advance (roughly) the number of entries in the
  dictionary, give size=0.
 */
/*--------------------------------------------------------------------------*/
dictionary * dictionary_new(int size);

/*-------------------------------------------------------------------------*/
/**
  @brief    Delete a dictionary object
  @param    d   dictionary object to deallocate.
  @return   void

  Deallocate a dictionary object and all memory associated to it.
 */
/*--------------------------------------------------------------------------*/
void dictionary_del(dictionary * vd);

/*-------------------------------------------------------------------------*/
/**
  @brief    Get a value from a dictionary.
  @param    d       dictionary object to search.
  @param    key     Key to look for in the dictionary.
  @param    def     Default value to return if key not found.
  @return   1 pointer to internally allocated character string.

  This function locates a key in a dictionary and returns a pointer to its
  value, or the passed 'def' pointer if no such key can be found in
  dictionary. The returned character pointer points to data internal to the
  dictionary object, you should not try to free it or modify it.
 */
/*--------------------------------------------------------------------------*/
char * dictionary_get(dictionary * d, const char * key, char * def);


/*-------------------------------------------------------------------------*/
/**
  @brief    Set a value in a dictionary.
  @param    d       dictionary object to modify.
  @param    key     Key to modify or add.
  @param    val     Value to add.
  @return   int     0 if Ok, anything else otherwise

  If the given key is found in the dictionary, the associated value is
  replaced by the provided one. If the key cannot be found in the
  dictionary, it is added to it.

  It is Ok to provide a NULL value for val, but NULL values for the dictionary
  or the key are considered as errors: the function will return immediately
  in such a case.

  Notice that if you dictionary_set a variable to NULL, a call to
  dictionary_get will return a NULL value: the variable will be found, and
  its value (NULL) is returned. In other words, setting the variable
  content to NULL is equivalent to deleting the variable from the
  dictionary. It is not possible (in this implementation) to have a key in
  the dictionary without value.

  This function returns non-zero in case of failure.
 */
/*--------------------------------------------------------------------------*/
int dictionary_set(dictionary * vd, const char * key, const char * val);

/*-------------------------------------------------------------------------*/
/**
  @brief    Delete a key in a dictionary
  @param    d       dictionary object to modify.
  @param    key     Key to remove.
  @return   void

  This function deletes a key in a dictionary. Nothing is done if the
  key cannot be found.
 */
/*--------------------------------------------------------------------------*/
void dictionary_unset(dictionary * d, const char * key);


/*-------------------------------------------------------------------------*/
/**
  @brief    Dump a dictionary to an opened file pointer.
  @param    d   Dictionary to dump
  @param    f   Opened file pointer.
  @return   void

  Dumps a dictionary onto an opened file pointer. Key pairs are printed out
  as @c [Key]=[Value], one per line. It is Ok to provide stdout or stderr as
  output file pointers.
 */
/*--------------------------------------------------------------------------*/
void dictionary_dump(dictionary * d, FILE * out);

#ifdef __cplusplus
}
#endif

#endif
