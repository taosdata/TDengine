/*****************************************************************************
 * dirent.h - dirent API for Microsoft Visual Studio
 *
 * Copyright (C) 2006 Toni Ronkko
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * ``Software''), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED ``AS IS'', WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL TONI RONKKO BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * Mar 15, 2011, Toni Ronkko
 * Defined FILE_ATTRIBUTE_DEVICE for MSVC 6.0.
 *
 * Aug 11, 2010, Toni Ronkko
 * Added d_type and d_namlen fields to dirent structure.  The former is
 * especially useful for determining whether directory entry represents a
 * file or a directory.  For more information, see
 * http://www.delorie.com/gnu/docs/glibc/libc_270.html
 *
 * Aug 11, 2010, Toni Ronkko
 * Improved conformance to the standards.  For example, errno is now set
 * properly on failure and assert() is never used.  Thanks to Peter Brockam
 * for suggestions.
 *
 * Aug 11, 2010, Toni Ronkko
 * Fixed a bug in rewinddir(): when using relative directory names, change
 * of working directory no longer causes rewinddir() to fail.
 *
 * Dec 15, 2009, John Cunningham
 * Added rewinddir member function
 *
 * Jan 18, 2008, Toni Ronkko
 * Using FindFirstFileA and WIN32_FIND_DATAA to avoid converting string
 * between multi-byte and unicode representations.  This makes the
 * code simpler and also allows the code to be compiled under MingW.  Thanks
 * to Azriel Fasten for the suggestion.
 *
 * Mar 4, 2007, Toni Ronkko
 * Bug fix: due to the strncpy_s() function this file only compiled in
 * Visual Studio 2005.  Using the new string functions only when the
 * compiler version allows.
 *
 * Nov  2, 2006, Toni Ronkko
 * Major update: removed support for Watcom C, MS-DOS and Turbo C to
 * simplify the file, updated the code to compile cleanly on Visual
 * Studio 2005 with both unicode and multi-byte character strings,
 * removed rewinddir() as it had a bug.
 *
 * Aug 20, 2006, Toni Ronkko
 * Removed all remarks about MSVC 1.0, which is antiqued now.  Simplified
 * comments by removing SGML tags.
 *
 * May 14 2002, Toni Ronkko
 * Embedded the function definitions directly to the header so that no
 * source modules need to be included in the Visual Studio project.  Removed
 * all the dependencies to other projects so that this very header can be
 * used independently.
 *
 * May 28 1998, Toni Ronkko
 * First version.
 *****************************************************************************/
#ifndef DIRENT_H
#define DIRENT_H

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

/* Entries missing from MSVC 6.0 */
#if !defined(FILE_ATTRIBUTE_DEVICE)
# define FILE_ATTRIBUTE_DEVICE 0x40
#endif

/* File type and permission flags for stat() */
#if defined(_MSC_VER)  &&  !defined(S_IREAD)
# define S_IFMT   _S_IFMT                      /* file type mask */
# define S_IFDIR  _S_IFDIR                     /* directory */
# define S_IFCHR  _S_IFCHR                     /* character device */
# define S_IFFIFO _S_IFFIFO                    /* pipe */
# define S_IFREG  _S_IFREG                     /* regular file */
# define S_IREAD  _S_IREAD                     /* read permission */
# define S_IWRITE _S_IWRITE                    /* write permission */
# define S_IEXEC  _S_IEXEC                     /* execute permission */
#endif
#define S_IFBLK   0                            /* block device */
#define S_IFLNK   0                            /* link */
#define S_IFSOCK  0                            /* socket */

#if defined(_MSC_VER)
# define S_IRUSR  S_IREAD                      /* read, user */
# define S_IWUSR  S_IWRITE                     /* write, user */
# define S_IXUSR  0                            /* execute, user */
# define S_IRGRP  0                            /* read, group */
# define S_IWGRP  0                            /* write, group */
# define S_IXGRP  0                            /* execute, group */
# define S_IROTH  0                            /* read, others */
# define S_IWOTH  0                            /* write, others */
# define S_IXOTH  0                            /* execute, others */
#endif

/* Indicates that d_type field is available in dirent structure */
#define _DIRENT_HAVE_D_TYPE

/* File type flags for d_type */
#define DT_UNKNOWN  0
#define DT_REG      S_IFREG
#define DT_DIR      S_IFDIR
#define DT_FIFO     S_IFFIFO
#define DT_SOCK     S_IFSOCK
#define DT_CHR      S_IFCHR
#define DT_BLK      S_IFBLK

/* Macros for converting between st_mode and d_type */
#define IFTODT(mode) ((mode) & S_IFMT)
#define DTTOIF(type) (type)

/*
 * File type macros.  Note that block devices, sockets and links cannot be
 * distinguished on Windows and the macros S_ISBLK, S_ISSOCK and S_ISLNK are
 * only defined for compatibility.  These macros should always return false
 * on Windows.
 */
#define	S_ISFIFO(mode) (((mode) & S_IFMT) == S_IFFIFO)
#define	S_ISDIR(mode)  (((mode) & S_IFMT) == S_IFDIR)
#define	S_ISREG(mode)  (((mode) & S_IFMT) == S_IFREG)
#define	S_ISLNK(mode)  (((mode) & S_IFMT) == S_IFLNK)
#define	S_ISSOCK(mode) (((mode) & S_IFMT) == S_IFSOCK)
#define	S_ISCHR(mode)  (((mode) & S_IFMT) == S_IFCHR)
#define	S_ISBLK(mode)  (((mode) & S_IFMT) == S_IFBLK)

#ifdef __cplusplus
extern "C" {
#endif


typedef struct dirent
{
   char d_name[MAX_PATH + 1];                  /* File name */
   size_t d_namlen;                            /* Length of name without \0 */
   int d_type;                                 /* File type */
} dirent;


typedef struct DIR
{
   dirent           curentry;                  /* Current directory entry */
   WIN32_FIND_DATAA find_data;                 /* Private file data */
   int              cached;                    /* True if data is valid */
   HANDLE           search_handle;             /* Win32 search handle */
   char             patt[MAX_PATH + 3];        /* Initial directory name */
} DIR;


/* Forward declarations */
static DIR *opendir(const char *dirname);
static struct dirent *readdir(DIR *dirp);
static int closedir(DIR *dirp);
static void rewinddir(DIR* dirp);


/* Use the new safe string functions introduced in Visual Studio 2005 */
#if defined(_MSC_VER) && _MSC_VER >= 1400
# define DIRENT_STRNCPY(dest,src,size) strncpy_s((dest),(size),(src),_TRUNCATE)
#else
# define DIRENT_STRNCPY(dest,src,size) strncpy((dest),(src),(size))
#endif

/* Set errno variable */
#if defined(_MSC_VER)
#define DIRENT_SET_ERRNO(x) _set_errno (x)
#else
#define DIRENT_SET_ERRNO(x) (errno = (x))
#endif


/*****************************************************************************
 * Open directory stream DIRNAME for read and return a pointer to the
 * internal working area that is used to retrieve individual directory
 * entries.
 */
static DIR *opendir(const char *dirname)
{
   DIR *dirp;

   /* ensure that the resulting search pattern will be a valid file name */
   if (dirname == NULL) {
      DIRENT_SET_ERRNO (ENOENT);
      return NULL;
   }
   if (strlen (dirname) + 3 >= MAX_PATH) {
      DIRENT_SET_ERRNO (ENAMETOOLONG);
      return NULL;
   }

   /* construct new DIR structure */
   dirp = (DIR*) malloc (sizeof (struct DIR));
   if (dirp != NULL) {
      int error;

      /*
       * Convert relative directory name to an absolute one.  This
       * allows rewinddir() to function correctly when the current working
       * directory is changed between opendir() and rewinddir().
       */
      if (GetFullPathNameA (dirname, MAX_PATH, dirp->patt, NULL)) {
         char *p;

         /* append the search pattern "\\*\0" to the directory name */
         p = strchr (dirp->patt, '\0');
         if (dirp->patt < p  &&  *(p-1) != '\\'  &&  *(p-1) != ':') {
           *p++ = '\\';
         }
         *p++ = '*';
         *p = '\0';

         /* open directory stream and retrieve the first entry */
         dirp->search_handle = FindFirstFileA (dirp->patt, &dirp->find_data);
         if (dirp->search_handle != INVALID_HANDLE_VALUE) {
            /* a directory entry is now waiting in memory */
            dirp->cached = 1;
            error = 0;
         } else {
            /* search pattern is not a directory name? */
            DIRENT_SET_ERRNO (ENOENT);
            error = 1;
         }
      } else {
         /* buffer too small */
         DIRENT_SET_ERRNO (ENOMEM);
         error = 1;
      }

      if (error) {
         free (dirp);
         dirp = NULL;
      }
   }

   return dirp;
}


/*****************************************************************************
 * Read a directory entry, and return a pointer to a dirent structure
 * containing the name of the entry in d_name field.  Individual directory
 * entries returned by this very function include regular files,
 * sub-directories, pseudo-directories "." and "..", but also volume labels,
 * hidden files and system files may be returned.
 */
static struct dirent *readdir(DIR *dirp)
{
   DWORD attr;
   if (dirp == NULL) {
      /* directory stream did not open */
      DIRENT_SET_ERRNO (EBADF);
      return NULL;
   }

   /* get next directory entry */
   if (dirp->cached != 0) {
      /* a valid directory entry already in memory */
      dirp->cached = 0;
   } else {
      /* get the next directory entry from stream */
      if (dirp->search_handle == INVALID_HANDLE_VALUE) {
         return NULL;
      }
      if (FindNextFileA (dirp->search_handle, &dirp->find_data) == FALSE) {
         /* the very last entry has been processed or an error occured */
         FindClose (dirp->search_handle);
         dirp->search_handle = INVALID_HANDLE_VALUE;
         return NULL;
      }
   }

   /* copy as a multibyte character string */
   DIRENT_STRNCPY ( dirp->curentry.d_name,
             dirp->find_data.cFileName,
             sizeof(dirp->curentry.d_name) );
   dirp->curentry.d_name[MAX_PATH] = '\0';

   /* compute the length of name */
   dirp->curentry.d_namlen = strlen (dirp->curentry.d_name);

   /* determine file type */
   attr = dirp->find_data.dwFileAttributes;
   if ((attr & FILE_ATTRIBUTE_DEVICE) != 0) {
      dirp->curentry.d_type = DT_CHR;
   } else if ((attr & FILE_ATTRIBUTE_DIRECTORY) != 0) {
      dirp->curentry.d_type = DT_DIR;
   } else {
      dirp->curentry.d_type = DT_REG;
   }
   return &dirp->curentry;
}


/*****************************************************************************
 * Close directory stream opened by opendir() function.  Close of the
 * directory stream invalidates the DIR structure as well as any previously
 * read directory entry.
 */
static int closedir(DIR *dirp)
{
   if (dirp == NULL) {
      /* invalid directory stream */
      DIRENT_SET_ERRNO (EBADF);
      return -1;
   }

   /* release search handle */
   if (dirp->search_handle != INVALID_HANDLE_VALUE) {
      FindClose (dirp->search_handle);
      dirp->search_handle = INVALID_HANDLE_VALUE;
   }

   /* release directory structure */
   free (dirp);
   return 0;
}


/*****************************************************************************
 * Resets the position of the directory stream to which dirp refers to the
 * beginning of the directory.  It also causes the directory stream to refer
 * to the current state of the corresponding directory, as a call to opendir()
 * would have done.  If dirp does not refer to a directory stream, the effect
 * is undefined.
 */
static void rewinddir(DIR* dirp)
{
   if (dirp != NULL) {
      /* release search handle */
      if (dirp->search_handle != INVALID_HANDLE_VALUE) {
         FindClose (dirp->search_handle);
      }

      /* open new search handle and retrieve the first entry */
      dirp->search_handle = FindFirstFileA (dirp->patt, &dirp->find_data);
      if (dirp->search_handle != INVALID_HANDLE_VALUE) {
         /* a directory entry is now waiting in memory */
         dirp->cached = 1;
      } else {
         /* failed to re-open directory: no directory entry in memory */
         dirp->cached = 0;
      }
   }
}


#ifdef __cplusplus
}
#endif
#endif /*DIRENT_H*/
