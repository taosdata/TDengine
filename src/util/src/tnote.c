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

#include "os.h"
#include "tnote.h"

taosNoteInfo  m_HttpNote;
taosNoteInfo  m_TscNote;

int taosOpenNoteWithMaxLines(char *fn, int maxLines, int maxNoteNum, taosNoteInfo * pNote);

void taosInitNote(int numOfNoteLines, int maxNotes, char* lable)
{
    taosNoteInfo * pNote = NULL;
    char temp[128] = { 0 };

    if (strcasecmp(lable, "http_note") == 0) {
        pNote = &m_HttpNote;
        sprintf(temp, "%s/httpnote", tsLogDir);
    } else if (strcasecmp(lable, "tsc_note") == 0) {
        pNote = &m_TscNote;        
        sprintf(temp, "%s/tscnote-%d", tsLogDir, getpid());
    } else {
        return;
    }

    memset(pNote, 0, sizeof(taosNoteInfo));
    pNote->taosNoteFileNum        = 1;
    //pNote->taosNoteMaxLines       = 0;
    //pNote->taosNoteLines          = 0;
    //pNote->taosNoteFlag           = 0;
    pNote->taosNoteFd             = -1;
    //pNote->taosNoteOpenInProgress = 0;

    if (taosOpenNoteWithMaxLines(temp, numOfNoteLines, maxNotes, pNote) < 0)
        fprintf(stderr, "failed to init note file\n");

    taosNotePrint(pNote, "==================================================");
    taosNotePrint(pNote, "===================  new note  ===================");
    taosNotePrint(pNote, "==================================================");
}

void taosCloseNoteByFd(int oldFd, taosNoteInfo * pNote);
bool taosLockNote(int fd, taosNoteInfo * pNote)
{
    if (fd < 0) return false;

    if (pNote->taosNoteFileNum > 1) {
        int ret = (int)(flock(fd, LOCK_EX | LOCK_NB));
        if (ret == 0) {
            return true;
        }
    }

    return false;
}

void taosUnLockNote(int fd, taosNoteInfo * pNote)
{
    if (fd < 0) return;

    if (pNote->taosNoteFileNum > 1) {
        flock(fd, LOCK_UN | LOCK_NB);
    }
}

void *taosThreadToOpenNewNote(void *param)
{
    char name[NOTE_FILE_NAME_LEN * 2];
    taosNoteInfo * pNote = (taosNoteInfo *)param;

    pNote->taosNoteFlag ^= 1;
    pNote->taosNoteLines = 0;
    sprintf(name, "%s.%d", pNote->taosNoteName, pNote->taosNoteFlag);

    umask(0);

    int fd = open(name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
    if (fd < 0) {
      return NULL;
    }

    taosLockNote(fd, pNote);
    (void)lseek(fd, 0, SEEK_SET);

    int oldFd = pNote->taosNoteFd;
    pNote->taosNoteFd = fd;
    pNote->taosNoteLines = 0;
    pNote->taosNoteOpenInProgress = 0;
    taosNotePrint(pNote, "===============  new note is opened  =============");

    taosCloseNoteByFd(oldFd, pNote);
    return NULL;
}

int taosOpenNewNote(taosNoteInfo * pNote)
{
    pthread_mutex_lock(&pNote->taosNoteMutex);

    if (pNote->taosNoteLines > pNote->taosNoteMaxLines && pNote->taosNoteOpenInProgress == 0) {
        pNote->taosNoteOpenInProgress = 1;

        taosNotePrint(pNote, "===============  open new note  ==================");
        pthread_t pattern;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

        pthread_create(&pattern, &attr, taosThreadToOpenNewNote, (void*)pNote);
        pthread_attr_destroy(&attr);
    }

    pthread_mutex_unlock(&pNote->taosNoteMutex);

    return pNote->taosNoteFd;
}

bool taosCheckNoteIsOpen(char *noteName, taosNoteInfo * pNote)
{
    /*
    int exist = access(noteName, F_OK);
    if (exist != 0) {
        return false;
    }
    */

    int fd = open(noteName, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    if (fd < 0) {
        fprintf(stderr, "failed to open note:%s reason:%s\n", noteName, strerror(errno));
        return true;
    }

    if (taosLockNote(fd, pNote)) {
        taosUnLockNote(fd, pNote);
        close(fd);
        return false;
    }
    else {
        close(fd);
        return true;
    }
}

void taosGetNoteName(char *fn, taosNoteInfo * pNote)
{
    if (pNote->taosNoteFileNum > 1) {
        for (int i = 0; i < pNote->taosNoteFileNum; i++) {
            char fileName[NOTE_FILE_NAME_LEN];

            sprintf(fileName, "%s%d.0", fn, i);
            bool file1open = taosCheckNoteIsOpen(fileName, pNote);

            sprintf(fileName, "%s%d.1", fn, i);
            bool file2open = taosCheckNoteIsOpen(fileName, pNote);

            if (!file1open && !file2open) {
                sprintf(pNote->taosNoteName, "%s%d", fn, i);
                return;
            }
        }
    }

    if (strlen(fn) < NOTE_FILE_NAME_LEN) {
      strcpy(pNote->taosNoteName, fn);
    }
}

int taosOpenNoteWithMaxLines(char *fn, int maxLines, int maxNoteNum, taosNoteInfo * pNote)
{
    char name[NOTE_FILE_NAME_LEN * 2] = "\0";
    struct stat  notestat0, notestat1;
    int size;

    pNote->taosNoteMaxLines = maxLines;
    pNote->taosNoteFileNum = maxNoteNum;
    taosGetNoteName(fn, pNote);

    if (strlen(fn) > NOTE_FILE_NAME_LEN * 2 - 2) {
      fprintf(stderr, "the len of file name overflow:%s\n", fn);
      return -1;
    } 

    strcpy(name, fn);
    strcat(name, ".0");

    // if none of the note files exist, open 0, if both exists, open the old one
    if (stat(name, &notestat0) < 0) {
        pNote->taosNoteFlag = 0;
    } else {
        strcpy(name, fn);
        strcat(name, ".1");
        if (stat(name, &notestat1) < 0) {
            pNote->taosNoteFlag = 1;
        }
        else {
            pNote->taosNoteFlag = (notestat0.st_mtime > notestat1.st_mtime) ? 0 : 1;
        }
    }

    char noteName[NOTE_FILE_NAME_LEN * 2] = "\0";
    sprintf(noteName, "%s.%d", pNote->taosNoteName, pNote->taosNoteFlag);
    pthread_mutex_init(&pNote->taosNoteMutex, NULL);

    umask(0);
    pNote->taosNoteFd = open(noteName, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

    if (pNote->taosNoteFd < 0) {
        fprintf(stderr, "failed to open note file:%s reason:%s\n", noteName, strerror(errno));
        return -1;
    }
    taosLockNote(pNote->taosNoteFd, pNote);

    // only an estimate for number of lines
    struct stat filestat;
    if (fstat(pNote->taosNoteFd, &filestat) < 0) {
      fprintf(stderr, "failed to fstat note file:%s reason:%s\n", noteName, strerror(errno));
      return -1;
    }    
    size = (int)filestat.st_size;
    pNote->taosNoteLines = size / 60;

    lseek(pNote->taosNoteFd, 0, SEEK_END);

    return 0;
}

void taosNotePrint(taosNoteInfo * pNote, const char * const format, ...)
{
    va_list argpointer;
    char    buffer[MAX_NOTE_LINE_SIZE+2];
    int     len;
    struct  tm      Tm, *ptm;
    struct  timeval timeSecs;
    time_t  curTime;

    gettimeofday(&timeSecs, NULL);
    curTime = timeSecs.tv_sec;
    ptm = localtime_r(&curTime, &Tm);
#ifndef LINUX
  len = sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d 0x%lld ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour,
                ptm->tm_min, ptm->tm_sec, (int)timeSecs.tv_usec, taosGetPthreadId());
#else
  len = sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d %lx ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min,
                ptm->tm_sec, (int)timeSecs.tv_usec, (unsigned long int)pthread_self());
#endif
    va_start(argpointer, format);
    len += vsnprintf(buffer + len, MAX_NOTE_LINE_SIZE - len, format, argpointer);
    va_end(argpointer);

    if (len >= MAX_NOTE_LINE_SIZE) len = MAX_NOTE_LINE_SIZE - 2;

    buffer[len++] = '\n';
    buffer[len] = 0;

    if (pNote->taosNoteFd >= 0)  {
        taosWrite(pNote->taosNoteFd, buffer, (unsigned int)len);

        if (pNote->taosNoteMaxLines > 0) {
            pNote->taosNoteLines++;
            if ((pNote->taosNoteLines > pNote->taosNoteMaxLines) && (pNote->taosNoteOpenInProgress == 0))
                taosOpenNewNote(pNote);
        }
    }
}

void taosCloseNote(taosNoteInfo * pNote)
{
    taosCloseNoteByFd(pNote->taosNoteFd, pNote);
}

void taosCloseNoteByFd(int fd, taosNoteInfo * pNote)
{
    if (fd >= 0) {
        taosUnLockNote(fd, pNote);
        close(fd);
    }
}
