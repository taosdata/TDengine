/***************************************************************************
 * I/O handling routines, for files and URLs.
 *
 * This file is part of the miniSEED Library.
 *
 * Copyright (c) 2020 Chad Trabant, IRIS Data Management Center
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************/

/* Define _LARGEFILE_SOURCE to get ftello/fseeko on some systems (Linux) */
#define _LARGEFILE_SOURCE 1

#include <errno.h>

#include "msio.h"

/* Include libcurl library header if URL supported is requested */
#if defined(LIBMSEED_URL)

#include <curl/curl.h>

/* Control for enabling debugging information */
int libmseed_url_debug = -1;

/* Control for SSL peer and host verification */
long libmseed_ssl_noverify = -1;

/* A global libcurl easy handle for configuration options */
CURL *gCURLeasy = NULL;

/* A global libcurl list of headers */
struct curl_slist *gCURLheaders = NULL;

/* Receving callback parameters */
struct recv_callback_parameters
{
  char *buffer;
  size_t size;
  int is_paused;
};

/* Header callback parameters */
struct header_callback_parameters
{
  int64_t *startoffset;
  int64_t *endoffset;
};

/*********************************************************************
 * Callback fired when recv'ing data using libcurl.
 *
 * The destination buffer pointer and size in the callback parameters
 * are adjusted as data are added.
 *
 * Returns number of bytes added to the destination buffer.
 *********************************************************************/
static size_t
recv_callback (char *buffer, size_t size, size_t num, void *userdata)
{
  struct recv_callback_parameters *rcp = (struct recv_callback_parameters *)userdata;

  if (!buffer || !userdata)
    return 0;

  size *= num;

  /* Pause connection if passed data does not fit into destination buffer */
  if (size > rcp->size)
  {
    rcp->is_paused = 1;
    return CURL_WRITEFUNC_PAUSE;
  }
  /* Otherwise, copy data to destination buffer */
  else
  {
    memcpy (rcp->buffer, buffer, size);
    rcp->buffer += size;
    rcp->size -= size;
  }

  return size;
}

/*********************************************************************
 * Callback fired when receiving headers using libcurl.
 *
 * Returns number of bytes processed for success.
 *********************************************************************/
static size_t
header_callback (char *buffer, size_t size, size_t num, void *userdata)
{
  struct header_callback_parameters *hcp = (struct header_callback_parameters *)userdata;

  char startstr[21] = {0}; /* Maximum of 20 digit value */
  char endstr[21]   = {0}; /* Maximum of 20 digit value */
  int startdigits   = 0;
  int enddigits     = 0;
  char *dash        = NULL;
  char *ptr;

  if (!buffer || !userdata)
    return 0;

  size *= num;

  /* Parse and store: "Content-Range: bytes START-END/TOTAL"
   * e.g. Content-Range: bytes 512-1023/4096 */
  if (size > 22 && strncasecmp (buffer, "Content-Range: bytes", 20) == 0)
  {
    /* Process each character, starting just afer "bytes" unit */
    for (ptr = buffer + 20; *ptr != '\0' && (ptr - buffer) < size; ptr++)
    {
      /* Skip spaces before start of range */
      if (*ptr == ' ' && startdigits == 0)
        continue;
      /* Digits before dash, part of start */
      else if (isdigit (*ptr) && dash == NULL)
        startstr[startdigits++] = *ptr;
      /* Digits after dash, part of end */
      else if (isdigit (*ptr) && dash != NULL)
        endstr[enddigits++] = *ptr;
      /* If first dash found, store pointer */
      else if (*ptr == '-' && dash == NULL)
        dash = ptr;
      /* Nothing else is part of the range */
      else
        break;

      /* If digit sequences have exceeded limits, not a valid range */
      if (startdigits >= sizeof (startstr) || enddigits >= sizeof (endstr))
      {
        startdigits = 0;
        enddigits   = 0;
        break;
      }
    }

    /* Convert start and end values to numbers if non-zero length */
    if (hcp->startoffset && startdigits)
      *hcp->startoffset = (int64_t) strtoull (startstr, NULL, 10);

    if (hcp->endoffset && enddigits)
      *hcp->endoffset = (int64_t) strtoull (endstr, NULL, 10);
  }

  return size;
}

#endif /* defined(LIBMSEED_URL) */


/***************************************************************************
 * msio_fopen:
 *
 * Determine if requested path is a regular file or a URL and open or
 * initialize as appropriate.
 *
 * The 'mode' argument is only for file-system paths and ignored for
 * URLs.  If 'mode' is set to NULL, default is 'rb' mode.
 *
 * If 'startoffset' or 'endoffset' are non-zero they will be used to
 * position the stream for reading, either setting the read position
 * of a file or requesting a range via HTTP.  These will be set to the
 * actual range if reported via HTTP, which may be different than
 * requested.
 *
 * Return 0 on success and -1 on error.
 *
 * \ref MessageOnError - this function logs a message on error
 ***************************************************************************/
int
msio_fopen (LMIO *io, const char *path, const char *mode,
          int64_t *startoffset, int64_t *endoffset)
{
  int knownfile = 0;

  if (!io || !path)
    return -1;

  if (!mode)
    mode = "rb";

  /* Treat "file://" specifications as local files by removing the scheme */
  if (!strncasecmp (path, "file://", 7))
  {
    path += 7;
    knownfile = 1;
  }

  /* Test for URL scheme via "://" */
  if (!knownfile && strstr (path, "://"))
  {
#if !defined(LIBMSEED_URL)
    ms_log (2, "URL support not included in library for %s\n", path);
    return -1;
#else
    long response_code;
    struct header_callback_parameters hcp;

    io->type = LMIO_URL;

    /* Check for URL debugging environment variable */
    if (libmseed_url_debug < 0)
    {
      if (getenv ("LIBMSEED_URL_DEBUG"))
        libmseed_url_debug = 1;
      else
        libmseed_url_debug = 0;
    }

    /* Check for SSL peer/host verify environment variable */
    if (libmseed_ssl_noverify < 0)
    {
      if (getenv ("LIBMSEED_SSL_NOVERIFY"))
        libmseed_ssl_noverify = 1;
      else
        libmseed_ssl_noverify = 0;
    }

    /* Configure the libcurl easy handle, duplicate global options if present */
    io->handle = (gCURLeasy) ? curl_easy_duphandle (gCURLeasy) : curl_easy_init ();

    if (io->handle == NULL)
    {
      ms_log (2, "Cannot initialize CURL handle\n");
      return -1;
    }

    /* URL debug */
    if (libmseed_url_debug && curl_easy_setopt (io->handle, CURLOPT_VERBOSE, 1L) != CURLE_OK)
    {
      ms_log (2, "Cannot set CURLOPT_VERBOSE\n");
      return -1;
    }

    /* SSL peer and host verification */
    if (libmseed_ssl_noverify &&
        (curl_easy_setopt (io->handle, CURLOPT_SSL_VERIFYPEER, 0L) != CURLE_OK ||
         curl_easy_setopt (io->handle, CURLOPT_SSL_VERIFYHOST, 0L) != CURLE_OK))
    {
      ms_log (2, "Cannot set CURLOPT_SSL_VERIFYPEER and/or CURLOPT_SSL_VERIFYHOST\n");
      return -1;
    }

    /* Set URL */
    if (curl_easy_setopt (io->handle, CURLOPT_URL, path) != CURLE_OK)
    {
      ms_log (2, "Cannot set CURLOPT_URL\n");
      return -1;
    }

    /* Set default User-Agent header, can be overridden via custom header */
    if (curl_easy_setopt (io->handle, CURLOPT_USERAGENT,
                          "libmseed/" LIBMSEED_VERSION " libcurl/" LIBCURL_VERSION) != CURLE_OK)
    {
      ms_log (2, "Cannot set default CURLOPT_USERAGENT\n");
      return -1;
    }

    /* Disable signals */
    if (curl_easy_setopt (io->handle, CURLOPT_NOSIGNAL, 1L) != CURLE_OK)
    {
      ms_log (2, "Cannot set CURLOPT_NOSIGNAL\n");
      return -1;
    }

    /* Return failure codes on errors */
    if (curl_easy_setopt (io->handle, CURLOPT_FAILONERROR, 1L) != CURLE_OK)
    {
      ms_log (2, "Cannot set CURLOPT_FAILONERROR\n");
      return -1;
    }

    /* Follow HTTP redirects */
    if (curl_easy_setopt (io->handle, CURLOPT_FOLLOWLOCATION, 1L) != CURLE_OK)
    {
      ms_log (2, "Cannot set CURLOPT_FOLLOWLOCATION\n");
      return -1;
    }

    /* Configure write callback for recv'ed data */
    if (curl_easy_setopt (io->handle, CURLOPT_WRITEFUNCTION, recv_callback) != CURLE_OK)
    {
      ms_log (2, "Cannot set CURLOPT_WRITEFUNCTION\n");
      return -1;
    }

    /* Configure the libcurl multi handle, for use with the asynchronous interface */
    if ((io->handle2 = curl_multi_init ()) == NULL)
    {
      ms_log (2, "Cannot initialize CURL multi handle\n");
      return -1;
    }

    if (curl_multi_add_handle (io->handle2, io->handle) != CURLM_OK)
    {
      ms_log (2, "Cannot add CURL handle to multi handle\n");
      return -1;
    }

    /* Set byte ranging */
    if ((startoffset && *startoffset > 0) || (endoffset && *endoffset > 0))
    {
      char startstr[21] = {0};
      char endstr[21]   = {0};
      char rangestr[42];

      /* Build Range header value, either of the range ends are optional */
      if (*startoffset > 0)
        snprintf (startstr, sizeof (startstr), "%" PRId64, *startoffset);
      if (*endoffset > 0)
        snprintf (endstr, sizeof (endstr), "%" PRId64, *endoffset);

      snprintf (rangestr, sizeof (rangestr), "%s-%s", startstr, endstr);

      /* Set Range header */
      if (curl_easy_setopt (io->handle, CURLOPT_RANGE, rangestr) != CURLE_OK)
      {
        ms_log (2, "Cannot set CURLOPT_RANGE to '%s'\n", rangestr);
        return -1;
      }
    }

    /* Set up header callback */
    if (startoffset || endoffset)
    {
      hcp.startoffset = startoffset;
      hcp.endoffset = endoffset;

      /* Configure header callback */
      if (curl_easy_setopt (io->handle, CURLOPT_HEADERFUNCTION, header_callback) != CURLE_OK)
      {
        ms_log (2, "Cannot set CURLOPT_HEADERFUNCTION\n");
        return -1;
      }

      if (curl_easy_setopt (io->handle, CURLOPT_HEADERDATA, (void *)&hcp) != CURLE_OK)
      {
        ms_log (2, "Cannot set CURLOPT_HEADERDATA\n");
        return -1;
      }
    }

    /* Set custom headers */
    if (gCURLheaders && curl_easy_setopt (io->handle, CURLOPT_HTTPHEADER, gCURLheaders) != CURLE_OK)
    {
      ms_log (2, "Cannot set CURLOPT_HTTPHEADER\n");
      return -1;
    }

    /* Set connection as still running */
    io->still_running = 1;

    /* Start connection, get status & headers, without consuming any data */
    msio_fread (io, NULL, 0);

    curl_easy_getinfo (io->handle, CURLINFO_RESPONSE_CODE, &response_code);

    if (response_code == 404)
    {
      ms_log (2, "Cannot open %s: Not Found (404)\n", path);
      return -1;
    }
    else if (response_code >= 400 && response_code < 600)
    {
      ms_log (2, "Cannot open %s: response code %ld\n", path, response_code);
      return -1;
    }
#endif /* defined(LIBMSEED_URL) */
  }
  else
  {
    io->type = LMIO_FILE;

    if ((io->handle = fopen (path, mode)) == NULL)
    {
      ms_log (2, "Cannot open: %s (%s)\n", path, strerror(errno));
      return -1;
    }

    /* Seek to position if start offset is provided */
    if (startoffset && *startoffset > 0)
    {
      if (lmp_fseek64 (io->handle, *startoffset, SEEK_SET))
      {
        ms_log (2, "Cannot seek in %s to offset %" PRId64 "\n", path, *startoffset);
        return -1;
      }
    }
  }

  return 0;
}  /* End of msio_fopen() */

/*********************************************************************
 * msio_fclose:
 *
 * Close an IO handle.
 *
 * Returns 0 on success and negative value on error.
 *
 * \ref MessageOnError - this function logs a message on error
 *********************************************************************/
int
msio_fclose (LMIO *io)
{
  int rv;

  if (!io)
  {
    ms_log (2, "Required argument not defined: 'io'\n");
    return -1;
  }

  if (io->handle == NULL || io->type == LMIO_NULL)
    return 0;

  if (io->type == LMIO_FILE)
  {
    rv = fclose (io->handle);

    if (rv)
    {
      ms_log (2, "Error closing file (%s)\n", strerror(errno));
      return -1;
    }
  }
  else if (io->type == LMIO_URL)
  {
#if !defined(LIBMSEED_URL)
    ms_log (2, "URL support not included in library\n");
    return -1;
#else
    curl_multi_remove_handle (io->handle2, io->handle);
    curl_easy_cleanup (io->handle);
    curl_multi_cleanup (io->handle2);
#endif
  }

  io->type = LMIO_NULL;
  io->handle = NULL;
  io->handle2 = NULL;

  return 0;
} /* End of msio_fclose() */


/*********************************************************************
 * msio_fread:
 *
 * Read data from the identified IO handle into the specified buffer.
 * Up to the requested 'size' bytes are read.
 *
 * For URL support, with defined(LIBMSEED_URL), the destination
 * receive buffer MUST be at least as big as the curl receive buffer
 * (CURLOPT_BUFFERSIZE, which defaults to CURL_MAX_WRITE_SIZE of 16kB)
 * or the maximum size of a retrieved object if less than
 * CURL_MAX_WRITE_SIZE.  The caller must ensure this.
 *
 * Returns the number of bytes read on success and a negative value on
 * error.
 *********************************************************************/
size_t
msio_fread (LMIO *io, void *buffer, size_t size)
{
  size_t read = 0;

  if (!io)
    return -1;

  if (!buffer && size > 0)
  {
    ms_log (2, "No buffer specified for size is > 0\n");
    return -1;
  }

  /* Read from regular file stream */
  if (io->type == LMIO_FILE)
  {
    read = fread (buffer, 1, size, io->handle);
  }
  /* Read from URL stream */
  else if (io->type == LMIO_URL)
  {
#if !defined(LIBMSEED_URL)
    ms_log (2, "URL support not included in library\n");
    return -1;
#else
    struct recv_callback_parameters rcp;
    struct timeval timeout;
    fd_set fdread;
    fd_set fdwrite;
    fd_set fdexcep;
    long curl_timeo = -1;
    int maxfd       = -1;
    int rc;

    if (!io->still_running)
      return 0;

    /* Set up destination buffer in write callback parameters */
    rcp.buffer = buffer;
    rcp.size   = size;
    if (curl_easy_setopt (io->handle, CURLOPT_WRITEDATA, (void *)&rcp) != CURLE_OK)
    {
      ms_log (2, "Cannot set CURLOPT_WRITEDATA\n");
      return -1;
    }

    /* Unpause connection */
    curl_easy_pause (io->handle, CURLPAUSE_CONT);
    rcp.is_paused = 0;

    /* Receive data while connection running, destination space available
     * and connection is not paused. */
    do
    {
      /* Default timeout for read failure */
      timeout.tv_sec  = 60;
      timeout.tv_usec = 0;

      curl_multi_timeout (io->handle2, &curl_timeo);

      /* Tailor timeout based on maximum suggested by libcurl */
      if (curl_timeo >= 0)
      {
        timeout.tv_sec = curl_timeo / 1000;
        if (timeout.tv_sec > 1)
          timeout.tv_sec = 1;
        else
          timeout.tv_usec = (curl_timeo % 1000) * 1000;
      }

      FD_ZERO (&fdread);
      FD_ZERO (&fdwrite);
      FD_ZERO (&fdexcep);

      /* Extract descriptors from the multi-handle */
      if (curl_multi_fdset (io->handle2, &fdread, &fdwrite, &fdexcep, &maxfd) != CURLM_OK)
      {
        ms_log (2, "Error with curl_multi_fdset()\n");
        return -1;
      }

      /* libcurl/system needs time to work, sleep 100 milliseconds */
      if (maxfd == -1)
      {
        lmp_nanosleep (100000000);
        rc = 0;
      }
      else
      {
        rc = select (maxfd + 1, &fdread, &fdwrite, &fdexcep, &timeout);
      }

      /* Receive data */
      if (rc >= 0)
      {
        curl_multi_perform (io->handle2, &io->still_running);
      }
    } while (io->still_running > 0 && !rcp.is_paused && (rcp.size > 0 || rcp.buffer == NULL));

    read = size - rcp.size;

#endif /* defined(LIBMSEED_URL) */
  }

  return read;
} /* End of msio_fread() */

/*********************************************************************
 * msio_feof:
 *
 * Test if end-of-stream.
 *
 * Returns 1 when stream is at end, 0 if not, and -1 on error.
 *********************************************************************/
int
msio_feof (LMIO *io)
{
  if (!io)
    return 0;

  if (io->handle == NULL || io->type == LMIO_NULL)
    return 0;

  if (io->type == LMIO_FILE)
  {
    if (feof ((FILE *)io->handle))
      return 1;
  }
  else if (io->type == LMIO_URL)
  {
#if !defined(LIBMSEED_URL)
    ms_log (2, "URL support not included in library\n");
    return -1;
#else
    /* The still_running flag is only changed by curl_multi_perform()
     * and indicates current "transfers in progress".  Presumably no data
     * are in internal libcurl buffers either. */
    if (!io->still_running)
      return 1;
#endif
  }

  return 0;
} /* End of msio_feof() */

/*********************************************************************
 * msio_url_useragent:
 *
 * Set global User-Agent header for URL-based IO.
 *
 * The header is built as "PROGRAM/VERSION libmseed/version libcurl/version"
 * where VERSION is optional.
 *
 * Returns 0 on succes non-zero otherwise.
 *
 * \ref MessageOnError - this function logs a message on error
 *********************************************************************/
int
msio_url_useragent (const char *program, const char *version)
{
  if (!program)
  {
    ms_log (2, "Required argument not defined: 'program'\n");
    return -1;
  }

#if !defined(LIBMSEED_URL)
  ms_log (2, "URL support not included in library\n");
  return -1;
#else
  char header[1024];

  /* Build User-Agent header and add internal versions */
  snprintf (header, sizeof (header),
            "User-Agent: %s%s%s libmseed/" LIBMSEED_VERSION " libcurl/" LIBCURL_VERSION,
            program,
            (version) ? "/" : "",
            (version) ? version : "");

  return msio_url_addheader (header);
#endif

  return 0;
} /* End of msio_url_useragent() */

/*********************************************************************
 * msio_url_userpassword:
 *
 * Set global user-password credentials for URL-based IO.
 *
 * Returns 0 on succes non-zero otherwise.
 *
 * \ref MessageOnError - this function logs a message on error
 *********************************************************************/
int
msio_url_userpassword (const char *userpassword)
{
  if (!userpassword)
  {
    ms_log (2, "Required argument not defined: 'userpassword'\n");
    return -1;
  }

#if !defined(LIBMSEED_URL)
  ms_log (2, "URL support not included in library\n");
  return -1;
#else
  if (gCURLeasy == NULL && (gCURLeasy = curl_easy_init ()) == NULL)
    return -1;

  /* Allow any authentication, libcurl will pick the most secure */
  if (curl_easy_setopt (gCURLeasy, CURLOPT_HTTPAUTH, CURLAUTH_ANY) != CURLE_OK)
  {
    ms_log (2, "Cannot set CURLOPT_HTTPAUTH\n");
    return -1;
  }

  if (curl_easy_setopt (gCURLeasy, CURLOPT_USERPWD, userpassword) != CURLE_OK)
  {
    ms_log (2, "Cannot set CURLOPT_USERPWD\n");
    return -1;
  }
#endif

  return 0;
} /* End of msio_url_userpassword() */

/*********************************************************************
 * msio_url_addheader:
 *
 * Add header to global list for URL-based IO.
 *
 * Returns 0 on succes non-zero otherwise.
 *
 * \ref MessageOnError - this function logs a message on error
 *********************************************************************/
int
msio_url_addheader (const char *header)
{
  if (!header)
  {
    ms_log (2, "Required argument not defined: 'header'\n");
    return -1;
  }

#if !defined(LIBMSEED_URL)
  ms_log (2, "URL support not included in library\n");
  return -1;
#else
  struct curl_slist *slist = NULL;

  slist = curl_slist_append (gCURLheaders, header);

  if (slist == NULL)
  {
    ms_log (2, "Error adding header to list: %s\n", header);
    return -1;
  }

  gCURLheaders = slist;
#endif

  return 0;
} /* End of msio_url_addheader() */

/*********************************************************************
 * msio_url_freeheaders:
 *
 * Free the global list of headers for URL-based IO.
 *********************************************************************/
void
msio_url_freeheaders (void)
{
#if !defined(LIBMSEED_URL)
  ms_log (2, "URL support not included in library\n");
  return;
#else
  if (gCURLheaders != NULL)
  {
    curl_slist_free_all (gCURLheaders);
    gCURLheaders = NULL;
  }
#endif
} /* End of msio_url_freeheaders() */

/***************************************************************************
 * lmp_ftell64:
 *
 * Return the current file position for the specified descriptor using
 * the system's closest match to the POSIX ftello().
 ***************************************************************************/
int64_t
lmp_ftell64 (FILE *stream)
{
#if defined(LMP_WIN)
  return (int64_t)_ftelli64 (stream);
#else
  return (int64_t)ftello (stream);
#endif
} /* End of lmp_ftell64() */


/***************************************************************************
 * lmp_fseek64:
 *
 * Seek to a specific file position for the specified descriptor using
 * the system's closest match to the POSIX fseeko().
 ***************************************************************************/
int
lmp_fseek64 (FILE *stream, int64_t offset, int whence)
{
#if defined(LMP_WIN)
  return (int)_fseeki64 (stream, offset, whence);
#else
  return (int)fseeko (stream, offset, whence);
#endif
} /* End of lmp_fseeko() */


/***************************************************************************
 * @brief Sleep for a specified number of nanoseconds
 *
 * Sleep for a given number of nanoseconds.  Under WIN use SleepEx()
 * and is limited to millisecond resolution.  For all others use the
 * POSIX.4 nanosleep(), which can be interrupted by signals.
 *
 * @param nanoseconds Nanoseconds to sleep
 *
 * @return On non-WIN: the remaining nanoseconds are returned if the
 * requested interval is interrupted.
 ***************************************************************************/
uint64_t
lmp_nanosleep (uint64_t nanoseconds)
{
#if defined(LMP_WIN)

  /* SleepEx is limited to milliseconds */
  SleepEx ((DWORD) (nanoseconds / 1e6), 1);

  return 0;
#else

  struct timespec treq, trem;

  treq.tv_sec  = (time_t) (nanoseconds / 1e9);
  treq.tv_nsec = (long)(nanoseconds - (uint64_t)treq.tv_sec * 1e9);

  nanosleep (&treq, &trem);

  return trem.tv_sec * 1e9 + trem.tv_nsec;

#endif
} /* End of lmp_nanosleep() */
