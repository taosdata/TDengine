/***************************************************************************
 * Common logging facility.
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

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libmseed.h"

void rloginit_int (MSLogParam *logp,
                   void (*log_print) (const char *), const char *logprefix,
                   void (*diag_print) (const char *), const char *errprefix,
                   int maxmessages);

int rlog_int (MSLogParam *logp, const char *function, int level,
              const char *format, va_list *varlist);

int add_message_int (MSLogRegistry *logreg, const char *function, int level,
                     const char *message);
void print_message_int (MSLogParam *logp, int level, const char *message,
                        char *terminator);

/* Initialize the global logging parameters */
MSLogParam gMSLogParam = MSLogParam_INITIALIZER;

/**********************************************************************/ /**
 * @brief Initialize the global logging parameters.
 *
 * Any message printing functions indicated must except a single
 * argument, namely a string \c (const char *) that will contain the log
 * message.  If the function pointers are NULL, defaults will be used.
 *
 * If the log and error prefixes have been set they will be pre-pended
 * to the message.  If the prefixes are NULL, defaults will be used.
 *
 * If \a maxmessages is greater than zero, warning and error (level >=
 * 1) messages will be accumulated in a message registry.  Once the
 * maximum number of messages have accumulated, the oldest messages
 * are discarded.  Messages in the registry can be printed using
 * ms_rlog_emit() or cleared using ms_rlog_free().
 *
 * @param[in] log_print Function to print log messages
 * @param[in] logprefix Prefix to add to log and diagnostic messages
 * @param[in] diag_print Function to print diagnostic and error messages
 * @param[in] errprefix Prefix to add to error messages
 * @param[in] maxmessages Maximum number of error/warning messages to store in registry
 *
 * \sa ms_rlog()
 * \sa ms_rlog_emit()
 * \sa ms_rlog_free()
 ***************************************************************************/
void
ms_rloginit (void (*log_print) (const char *), const char *logprefix,
             void (*diag_print) (const char *), const char *errprefix,
             int maxmessages)
{
  rloginit_int (&gMSLogParam, log_print, logprefix,
                diag_print, errprefix, maxmessages);
} /* End of ms_rloginit() */

/**********************************************************************/ /**
 * @brief Initialize specified ::MSLogParam logging parameters.
 *
 * If the logging parameters have not been initialized (logp == NULL),
 * new parameter space will be allocated.
 *
 * Any message printing functions indicated must except a single
 * argument, namely a string \c (const char *) that will contain the log
 * message.  If the function pointers are NULL, defaults will be used.
 *
 * If the log and error prefixes have been set they will be pre-pended
 * to the message.  If the prefixes are NULL, defaults will be used.
 *
 * If \a maxmessages is greater than zero, warning and error (level >=
 * 1) messages will be accumulated in a message registry.  Once the
 * maximum number of messages have accumulated, the oldest messages
 * are discarded.  Messages in the registry can be printed using
 * ms_rlog_emit() or cleared using ms_rlog_free().
 *
 * @param[in] logp ::MSLogParam logging parameters
 * @param[in] log_print Function to print log messages
 * @param[in] logprefix Prefix to add to log and diagnostic messages
 * @param[in] diag_print Function to print diagnostic and error messages
 * @param[in] errprefix Prefix to add to error messages
 * @param[in] maxmessages Maximum number of error/warning messages to store in registry
 *
 * @returns a pointer to the created/re-initialized MSLogParam struct
 * on success and NULL on error.
 *
 * \sa ms_rlog()
 * \sa ms_rlog_emit()
 * \sa ms_rlog_free()
 ***************************************************************************/
MSLogParam *
ms_rloginit_l (MSLogParam *logp,
               void (*log_print) (const char *), const char *logprefix,
               void (*diag_print) (const char *), const char *errprefix,
               int maxmessages)
{
  MSLogParam *llog;

  if (logp == NULL)
  {
    llog = (MSLogParam *)libmseed_memory.malloc (sizeof (MSLogParam));

    if (llog == NULL)
    {
      ms_log (2, "Cannot allocate memory");
      return NULL;
    }

    llog->log_print = NULL;
    llog->logprefix = NULL;
    llog->diag_print = NULL;
    llog->errprefix = NULL;
    llog->registry.maxmessages = 0;
    llog->registry.messagecnt = 0;
    llog->registry.messages = NULL;
  }
  else
  {
    llog = logp;
  }

  rloginit_int (llog, log_print, logprefix,
                diag_print, errprefix, maxmessages);

  return llog;
} /* End of ms_rloginit_l() */

/**********************************************************************/ /**
 * @brief Initialize the logging subsystem.
 *
 * This function modifies the logging parameters in the supplied
 * ::MSLogParam. Use NULL for the function pointers or the prefixes if
 * they should not be changed from previously set or default values.
 *
 ***************************************************************************/
void
rloginit_int (MSLogParam *logp,
              void (*log_print) (const char *), const char *logprefix,
              void (*diag_print) (const char *), const char *errprefix,
              int maxmessages)
{
  if (!logp)
    return;

  if (log_print)
    logp->log_print = log_print;

  if (logprefix)
  {
    if (strlen (logprefix) >= MAX_LOG_MSG_LENGTH)
    {
      ms_log_l (logp, 2, "%s", "log message prefix is too large");
    }
    else
    {
      logp->logprefix = logprefix;
    }
  }

  if (diag_print)
    logp->diag_print = diag_print;

  if (errprefix)
  {
    if (strlen (errprefix) >= MAX_LOG_MSG_LENGTH)
    {
      ms_log_l (logp, 2, "%s", "error message prefix is too large");
    }
    else
    {
      logp->errprefix = errprefix;
    }
  }

  if (maxmessages >= 0)
  {
    logp->registry.maxmessages = maxmessages;
  }

  return;
} /* End of rloginit_int() */

/**********************************************************************/ /**
 * @brief Register log message using global logging parameters.
 *
 * It is convenient to call this function via the ms_log() macro,
 * which sets the calling function automatically.
 *
 * Three message levels are recognized, see @ref logging-levels for
 * more information.
 *
 * This function builds the log/error message and passes to it to the
 * appropriate print function.  If custom printing functions have not
 * been defined, messages will be printed with \c fprintf(), log
 * messages to \c stdout and error messages to \c stderr.
 *
 * If the log/error prefixes have been set they will be pre-pended to
 * the message.  If no custom log prefix is set none will be included.
 * If no custom error prefix is set \c "Error: " will be included.
 *
 * A trailing newline character is for error messages is removed if
 * the message is added to the log registry.
 *
 * All messages will be truncated at the ::MAX_LOG_MSG_LENGTH, this
 * includes any prefix.
 *
 * @param[in] function Name of function registering log message
 * @param[in] level Message level
 * @param[in] format Message format in printf() style
 * @param[in] ... Message format variables
 *
 * @returns The number of characters formatted on success, and a
 * negative value on error..
 ***************************************************************************/
int
ms_rlog (const char *function, int level, const char *format, ...)
{
  int retval;
  va_list varlist;

  va_start (varlist, format);

  retval = rlog_int (&gMSLogParam, function, level, format, &varlist);

  va_end (varlist);

  return retval;
} /* End of ms_rlog() */

/**********************************************************************/ /**
 * @brief Register log message using specified logging parameters.
 *
 * It is convenient to call this function via the ms_log_l() macro,
 * which sets the calling function automatically.
 *
 * The function uses logging parameters specified in the supplied
 * ::MSLogParam.  This reentrant capability allows using different
 * parameters in different parts of a program or different threads.
 *
 * Three message levels are recognized, see @ref logging-levels for
 * more information.
 *
 * This function builds the log/error message and passes to it to the
 * appropriate print function.  If custom printing functions have not
 * been defined, messages will be printed with \c fprintf(), log
 * messages to \c stdout and error messages to \c stderr.
 *
 * If the log/error prefixes have been set they will be pre-pended to
 * the message.  If no custom log prefix is set none will be included.
 * If no custom error prefix is set \c "Error: " will be included.
 *
 * A trailing newline character is for error messages is removed if
 * the message is added to the log registry.
 *
 * All messages will be truncated at the ::MAX_LOG_MSG_LENGTH, this
 * includes any prefix.
 *
 * @param[in] logp Pointer to ::MSLogParam to use for this message
 * @param[in] function Name of function registering log message
 * @param[in] level Message level
 * @param[in] format Message format in printf() style
 * @param[in] ... Message format variables
 *
 * @returns The number of characters formatted on success, and a
 * negative value on error.
 ***************************************************************************/
int
ms_rlog_l (MSLogParam *logp, const char *function, int level, const char *format, ...)
{
  int retval;
  va_list varlist;

  if (!logp)
    logp = &gMSLogParam;

  va_start (varlist, format);

  retval = rlog_int (logp, function, level, format, &varlist);

  va_end (varlist);

  return retval;
} /* End of ms_rlog_l() */

/**********************************************************************/ /**
 * @brief Log message using specified logging parameters and \c va_list
 *
 * Trailing newline character is removed when added messages to the
 * registry.
 *
 * @param[in] logp Pointer to ::MSLogParam to use for this message
 * @param[in] function Name of function registering log message
 * @param[in] level Message level
 * @param[in] varlist Message in a \c va_list in printf() style
 *
 * @returns The number of characters formatted on success, and a
 * negative value on error.
 ***************************************************************************/
int
rlog_int (MSLogParam *logp, const char *function, int level,
          const char *format, va_list *varlist)
{
  static char message[MAX_LOG_MSG_LENGTH];
  int presize = 0;
  int printed = 0;

  if (!logp)
  {
    fprintf (stderr, "%s() called without specifying log parameters", __func__);
    return -1;
  }

  message[0] = '\0';

  if (level >= 2) /* Error message */
  {
    if (logp->errprefix != NULL)
    {
      strncpy (message, logp->errprefix, MAX_LOG_MSG_LENGTH);
      message[MAX_LOG_MSG_LENGTH - 1] = '\0';
    }
    else
    {
      strncpy (message, "Error: ", MAX_LOG_MSG_LENGTH);
    }

    presize = strlen (message);
    printed = vsnprintf (&message[presize],
                         MAX_LOG_MSG_LENGTH - presize,
                         format, *varlist);

    message[MAX_LOG_MSG_LENGTH - 1] = '\0';
  }
  else if (level == 1) /* Diagnostic message */
  {
    if (logp->logprefix != NULL)
    {
      strncpy (message, logp->logprefix, MAX_LOG_MSG_LENGTH);
      message[MAX_LOG_MSG_LENGTH - 1] = '\0';
    }

    presize = strlen (message);
    printed = vsnprintf (&message[presize],
                         MAX_LOG_MSG_LENGTH - presize,
                         format, *varlist);

    message[MAX_LOG_MSG_LENGTH - 1] = '\0';
  }
  else if (level == 0) /* Normal log message */
  {
    if (logp->logprefix != NULL)
    {
      strncpy (message, logp->logprefix, MAX_LOG_MSG_LENGTH);
      message[MAX_LOG_MSG_LENGTH - 1] = '\0';
    }

    presize = strlen (message);
    printed = vsnprintf (&message[presize],
                         MAX_LOG_MSG_LENGTH - presize,
                         format, *varlist);

    message[MAX_LOG_MSG_LENGTH - 1] = '\0';
  }

  printed += presize;

  /* Add warning/error message to registry if enabled */
  if (level >= 1 && logp->registry.maxmessages > 0)
  {
    /* Remove trailing newline if present */
    if (message[printed - 1] == '\n')
    {
      message[printed - 1] = '\0';
      printed -= 1;
    }

    add_message_int (&logp->registry, function, level, message);
  }
  else
  {
    print_message_int (logp, level, message, NULL);
  }

  return printed;
} /* End of rlog_int() */

/**********************************************************************/ /**
 * @brief Add message to registry
 *
 * Add a message to the specified log registry.  Earliest entries are
 * removed to remain within the specified maximum number of messsages.
 *
 * @param[in] logreg Pointer to ::MSLogRegistry to use for this message
 * @param[in] function Name of function generating the message
 * @param[in] level Message level
 * @param[in] message Message text
 *
 * @returns Zero on sucess and non-zero on error
 ***************************************************************************/
int
add_message_int (MSLogRegistry *logreg, const char *function, int level,
                 const char *message)
{
  MSLogEntry *logentry = NULL;
  MSLogEntry *lognext = NULL;
  int count;

  if (!logreg || !message)
    return -1;

  /* Allocate new entry */
  logentry = (MSLogEntry *)libmseed_memory.malloc (sizeof (MSLogEntry));

  if (logentry == NULL)
  {
    fprintf (stderr, "%s(): Cannot allocate memory for log message\n", __func__);
    return -1;
  }

  /* Populate new entry */
  logentry->level = level;
  if (function)
  {
    strncpy (logentry->function, function, sizeof (logentry->function));
    logentry->function[sizeof (logentry->function) - 1] = '\0';
  }
  else
  {
    logentry->function[0] = '\0';
  }
  strncpy (logentry->message, message, sizeof (logentry->message));
  logentry->message[sizeof (logentry->message) - 1] = '\0';

  /* Add entry to registry */
  logentry->next = logreg->messages;
  logreg->messages = logentry;
  logreg->messagecnt += 1;

  /* Remove earliest messages if more than maximum allowed */
  if (logreg->messagecnt > logreg->maxmessages)
  {
    count = 0;
    logentry = logreg->messages;
    while (logentry)
    {
      lognext = logentry->next;
      count++;

      if (count == logreg->maxmessages)
        logentry->next = NULL;

      if (count > logreg->maxmessages)
        free (logentry);

      logentry = lognext;
    }
  }

  return 0;
} /* End of add_message_int() */


/**********************************************************************/ /**
 * @brief Send message to print functions
 *
 * @param[in] logp Pointer to ::MSLogParam appropriate for this message
 * @param[in] level Message level
 * @param[in] message Message to print
 *
 * @returns Zero on success, and a negative value on error.
 ***************************************************************************/
void
print_message_int (MSLogParam *logp, int level, const char *message,
                   char *terminator)
{
  if (!logp || !message)
    return;

  if (level >= 1) /* Error or warning message */
  {
    if (logp->diag_print != NULL)
    {
      logp->diag_print (message);
    }
    else
    {
      fprintf (stderr, "%s%s", message, (terminator) ? terminator : "");
    }
  }
  else if (level == 0) /* Normal log message */
  {
    if (logp->log_print != NULL)
    {
      logp->log_print (message);
    }
    else
    {
      fprintf (stdout, "%s%s", message, (terminator) ? terminator : "");
    }
  }
} /* End of print_message_int() */


/**********************************************************************/ /**
 * @brief Emit, aka send to print functions, messages from log registry
 *
 * Emit messages from the log registry, using the printing functions
 * identified by the ::MSLogParam.
 *
 * Messages are printed in order from earliest to latest.
 *
 * The maximum number messages to emit, from most recent to earliest,
 * can be limited using \a count.  If the value is 0 all messages are
 * emitted.  If this limit is used and messages are left in the
 * registry, it is highly recommended to either emit them soon or
 * clear them with ms_rlog_free().  A common pattern would be to emit
 * the last message (e.g. \a count of 1) and immediately free
 * (discard) any remaining messages.
 *
 * @param[in] logp ::MSLogParam for this message or NULL for global parameters
 * @param[in] count Number of messages to emit, 0 to emit all messages
 * @param[in] context If non-zero include context by prefixing the function name (if available)
 *
 * @returns The number of message emitted on success, and a negative
 * value on error.
 *
 * \sa ms_rloginit()
 * \sa ms_rlog_free()
 ***************************************************************************/
int
ms_rlog_emit (MSLogParam *logp, int count, int context)
{
  MSLogEntry *logentry = NULL;
  MSLogEntry *logprint = NULL;
  char local_message[MAX_LOG_MSG_LENGTH];
  char *message = NULL;
  int emit = (count > 0) ? count : -1;

  if (!logp)
    logp = &gMSLogParam;

  /* Pop off count entries (or all if count <= 0), and invert into print list */
  logentry = logp->registry.messages;
  while (logentry && emit)
  {
    logp->registry.messages = logentry->next;

    logentry->next = logprint;
    logprint = logentry;

    if (emit > 0)
      emit--;

    logentry = logp->registry.messages;
  }

  /* Print and free entries */
  logentry = logprint;
  while (logprint)
  {
    /* Add function name to message if requested and present */
    if (context && logprint->function[0] != '\0')
    {
      snprintf (local_message, sizeof(local_message), "%s() %.*s",
                logprint->function,
                (int)(MAX_LOG_MSG_LENGTH - sizeof(logprint->function) - 3),
                logprint->message);
      message = local_message;
    }
    else
    {
      message = logprint->message;
    }

    print_message_int (logp, logprint->level, message, "\n");

    logentry = logprint->next;
    free (logprint);
    logprint = logentry;
  }

  return 0;
} /* End of ms_rlog_emit() */


/**********************************************************************/ /**
 * @brief Free, without emitting, all messages from log registry
 *
 * @param[in] logp ::MSLogParam for this message or NULL for global parameters
 *
 * @returns The number of message freed on success, and a negative
 * value on error.
 ***************************************************************************/
int
ms_rlog_free (MSLogParam *logp)
{
  MSLogEntry *logentry = NULL;
  int freed = 0;

  if (!logp)
    logp = &gMSLogParam;

  logentry = logp->registry.messages;

  while (logentry)
  {
    freed++;

    logp->registry.messages = logentry->next;
    free (logentry);
    logentry = logp->registry.messages;
  }

  return freed;
} /* End of ms_rlog_free() */
