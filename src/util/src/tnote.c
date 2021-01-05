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

#define _DEFAULT_SOURCE
#include "os.h"
#include "tutil.h"
#include "tglobal.h"
#include "tnote.h"

SNoteObj tsHttpNote;
SNoteObj tsTscNote;
SNoteObj tsInfoNote;

static int32_t taosOpenNoteWithMaxLines(char *fn, int32_t maxLines, int32_t maxNoteNum, SNoteObj *pNote);
static void    taosCloseNoteByFd(int32_t oldFd, SNoteObj *pNote);

static void taosInitNote(int32_t numOfLines, int32_t maxNotes, SNoteObj *pNote, char *name) {
  memset(pNote, 0, sizeof(SNoteObj));
  pNote->fileNum = 1;
  pNote->fd = -1;

  if (taosOpenNoteWithMaxLines(name, numOfLines, maxNotes, pNote) < 0) {
    fprintf(stderr, "failed to init note file\n");
  }

  taosNotePrint(pNote, "==================================================");
  taosNotePrint(pNote, "===================  new note  ===================");
  taosNotePrint(pNote, "==================================================");
}

void taosInitNotes() {
  char name[TSDB_FILENAME_LEN * 2] = {0};

  if (tsTscEnableRecordSql) {
    snprintf(name, TSDB_FILENAME_LEN * 2, "%s/tscsql-%d", tsLogDir, taosGetPId());
    taosInitNote(tsNumOfLogLines, 1, &tsTscNote, name);
  }

  if (tsHttpEnableRecordSql) {
    snprintf(name, TSDB_FILENAME_LEN * 2, "%s/httpsql", tsLogDir);
    taosInitNote(tsNumOfLogLines, 1, &tsHttpNote, name);
  }

  if (tscEmbedded == 1) {
    snprintf(name, TSDB_FILENAME_LEN * 2, "%s/taosinfo", tsLogDir);
    taosInitNote(tsNumOfLogLines, 1, &tsInfoNote, name);
  }
}

static bool taosLockNote(int32_t fd, SNoteObj *pNote) {
  if (fd < 0) return false;

  if (pNote->fileNum > 1) {
    int32_t ret = (int32_t)(flock(fd, LOCK_EX | LOCK_NB));
    if (ret == 0) {
      return true;
    }
  }

  return false;
}

static void taosUnLockNote(int32_t fd, SNoteObj *pNote) {
  if (fd < 0) return;

  if (pNote->fileNum > 1) {
    flock(fd, LOCK_UN | LOCK_NB);
  }
}

static void *taosThreadToOpenNewNote(void *param) {
  char      name[NOTE_FILE_NAME_LEN * 2];
  SNoteObj *pNote = (SNoteObj *)param;

  pNote->flag ^= 1;
  pNote->lines = 0;
  sprintf(name, "%s.%d", pNote->name, pNote->flag);

  umask(0);

  int32_t fd = open(name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    return NULL;
  }

  taosLockNote(fd, pNote);
  (void)lseek(fd, 0, SEEK_SET);

  int32_t oldFd = pNote->fd;
  pNote->fd = fd;
  pNote->lines = 0;
  pNote->openInProgress = 0;
  taosNotePrint(pNote, "===============  new note is opened  =============");

  taosCloseNoteByFd(oldFd, pNote);
  return NULL;
}

static int32_t taosOpenNewNote(SNoteObj *pNote) {
  pthread_mutex_lock(&pNote->mutex);

  if (pNote->lines > pNote->maxLines && pNote->openInProgress == 0) {
    pNote->openInProgress = 1;

    taosNotePrint(pNote, "===============  open new note  ==================");
    pthread_t      pattern;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    pthread_create(&pattern, &attr, taosThreadToOpenNewNote, (void *)pNote);
    pthread_attr_destroy(&attr);
  }

  pthread_mutex_unlock(&pNote->mutex);

  return pNote->fd;
}

static bool taosCheckNoteIsOpen(char *noteName, SNoteObj *pNote) {
  int32_t fd = open(noteName, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    fprintf(stderr, "failed to open note:%s reason:%s\n", noteName, strerror(errno));
    return true;
  }

  if (taosLockNote(fd, pNote)) {
    taosUnLockNote(fd, pNote);
    close(fd);
    return false;
  } else {
    close(fd);
    return true;
  }
}

static void taosGetNoteName(char *fn, SNoteObj *pNote) {
  if (pNote->fileNum > 1) {
    for (int32_t i = 0; i < pNote->fileNum; i++) {
      char fileName[NOTE_FILE_NAME_LEN];

      sprintf(fileName, "%s%d.0", fn, i);
      bool file1open = taosCheckNoteIsOpen(fileName, pNote);

      sprintf(fileName, "%s%d.1", fn, i);
      bool file2open = taosCheckNoteIsOpen(fileName, pNote);

      if (!file1open && !file2open) {
        sprintf(pNote->name, "%s%d", fn, i);
        return;
      }
    }
  }

  if (strlen(fn) < NOTE_FILE_NAME_LEN) {
    strcpy(pNote->name, fn);
  }
}

static int32_t taosOpenNoteWithMaxLines(char *fn, int32_t maxLines, int32_t maxNoteNum, SNoteObj *pNote) {
  char    name[NOTE_FILE_NAME_LEN * 2] = {0};
  int32_t size;
  struct stat logstat0, logstat1;

  pNote->maxLines = maxLines;
  pNote->fileNum = maxNoteNum;
  taosGetNoteName(fn, pNote);

  if (strlen(fn) < NOTE_FILE_NAME_LEN + 50 - 2) {
    strcpy(name, fn);
    strcat(name, ".0");
  }
  bool log0Exist = stat(name, &logstat0) >= 0;

  if (strlen(fn) < NOTE_FILE_NAME_LEN + 50 - 2) {
    strcpy(name, fn);
    strcat(name, ".1");
  }
  bool log1Exist = stat(name, &logstat1) >= 0;

  if (!log0Exist && !log1Exist) {
    pNote->flag = 0;
  } else if (!log1Exist) {
    pNote->flag = 0;
  } else if (!log0Exist) {
    pNote->flag = 1;
  } else {
    pNote->flag = (logstat0.st_mtime > logstat1.st_mtime) ? 0 : 1;
  }

  char noteName[NOTE_FILE_NAME_LEN * 2] = {0};
  sprintf(noteName, "%s.%d", pNote->name, pNote->flag);
  pthread_mutex_init(&pNote->mutex, NULL);

  umask(0);
  pNote->fd = open(noteName, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

  if (pNote->fd < 0) {
    fprintf(stderr, "failed to open note file:%s reason:%s\n", noteName, strerror(errno));
    return -1;
  }
  taosLockNote(pNote->fd, pNote);

  // only an estimate for number of lines
  struct stat filestat;
  if (fstat(pNote->fd, &filestat) < 0) {
    fprintf(stderr, "failed to fstat note file:%s reason:%s\n", noteName, strerror(errno));
    return -1;
  }
  size = (int32_t)filestat.st_size;
  pNote->lines = size / 60;

  lseek(pNote->fd, 0, SEEK_END);

  return 0;
}

void taosNotePrintBuffer(SNoteObj *pNote, char *buffer, int32_t len) {
  if (pNote->fd <= 0) return;
  taosWrite(pNote->fd, buffer, len);

  if (pNote->maxLines > 0) {
    pNote->lines++;
    if ((pNote->lines > pNote->maxLines) && (pNote->openInProgress == 0)) taosOpenNewNote(pNote);
  }
}

void taosNotePrint(SNoteObj *pNote, const char *const format, ...) {
  va_list        argpointer;
  char           buffer[MAX_NOTE_LINE_SIZE + 2];
  int32_t        len;
  struct tm      Tm, *ptm;
  struct timeval timeSecs;
  time_t         curTime;

  gettimeofday(&timeSecs, NULL);
  curTime = timeSecs.tv_sec;
  ptm = localtime_r(&curTime, &Tm);
  len = sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d 0x%08" PRIx64 " ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour,
                ptm->tm_min, ptm->tm_sec, (int32_t)timeSecs.tv_usec, taosGetSelfPthreadId());
  va_start(argpointer, format);
  len += vsnprintf(buffer + len, MAX_NOTE_LINE_SIZE - len, format, argpointer);
  va_end(argpointer);

  if (len >= MAX_NOTE_LINE_SIZE) len = MAX_NOTE_LINE_SIZE - 2;

  buffer[len++] = '\n';
  buffer[len] = 0;

  taosNotePrintBuffer(pNote, buffer, len);
}

// static void taosCloseNote(SNoteObj *pNote) { taosCloseNoteByFd(pNote->fd, pNote); }

static void taosCloseNoteByFd(int32_t fd, SNoteObj *pNote) {
  if (fd >= 0) {
    taosUnLockNote(fd, pNote);
    close(fd);
  }
}
