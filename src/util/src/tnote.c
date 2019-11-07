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

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <stdarg.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <semaphore.h>

#include "os.h"
#include "tutil.h"
#include "tglobalcfg.h"

#define MAX_NOTE_LINE_SIZE 66000
#define NOTE_FILE_NAME_LEN 300

static int  taosNoteFileNum = 1;
static int  taosNoteMaxLines = 0;
static int  taosNoteLines = 0;
static char taosNoteName[NOTE_FILE_NAME_LEN];
static int  taosNoteFlag = 0;
static int  taosNoteFd = -1;
static int  taosNoteOpenInProgress = 0;
static pthread_mutex_t taosNoteMutex;
void taosNotePrint(const char * const format, ...);
int taosOpenNoteWithMaxLines(char *fn, int maxLines, int maxNoteNum);

void taosInitNote(int numOfNoteLines, int maxNotes)
{
    char temp[128] = { 0 };
    sprintf(temp, "%s/taosnote", logDir);
    if (taosOpenNoteWithMaxLines(temp, numOfNoteLines, maxNotes) < 0)
        fprintf(stderr, "failed to init note file\n");

    taosNotePrint("==================================================");
    taosNotePrint("===================  new note  ===================");
    taosNotePrint("==================================================");
}

void taosCloseNoteByFd(int oldFd);
bool taosLockNote(int fd)
{
    if (fd < 0) return false;

    if (taosNoteFileNum > 1) {
        int ret = (int)(flock(fd, LOCK_EX | LOCK_NB));
        if (ret == 0) {
            return true;
        }
    }

    return false;
}

void taosUnLockNote(int fd)
{
    if (fd < 0) return;

    if (taosNoteFileNum > 1) {
        flock(fd, LOCK_UN | LOCK_NB);
    }
}

void *taosThreadToOpenNewNote(void *param)
{
    char name[NOTE_FILE_NAME_LEN];

    taosNoteFlag ^= 1;
    taosNoteLines = 0;
    sprintf(name, "%s.%d", taosNoteName, taosNoteFlag);

    umask(0);

    int fd = open(name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
    taosLockNote(fd);
    lseek(fd, 0, SEEK_SET);

    int oldFd = taosNoteFd;
    taosNoteFd = fd;
    taosNoteLines = 0;
    taosNoteOpenInProgress = 0;
    taosNotePrint("===============  new note is opened  =============");

    taosCloseNoteByFd(oldFd);
    return NULL;
}

int taosOpenNewNote()
{
    pthread_mutex_lock(&taosNoteMutex);

    if (taosNoteLines > taosNoteMaxLines && taosNoteOpenInProgress == 0) {
        taosNoteOpenInProgress = 1;

        taosNotePrint("===============  open new note  ==================");
        pthread_t pattern;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

        pthread_create(&pattern, &attr, taosThreadToOpenNewNote, NULL);
        pthread_attr_destroy(&attr);
    }

    pthread_mutex_unlock(&taosNoteMutex);

    return taosNoteFd;
}

bool taosCheckNoteIsOpen(char *noteName)
{
    int exist = access(noteName, F_OK);
    if (exist != 0) {
        return false;
    }

    int fd = open(noteName, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    if (fd < 0) {
        fprintf(stderr, "failed to open note:%s reason:%s\n", noteName, strerror(errno));
        return true;
    }

    if (taosLockNote(fd)) {
        taosUnLockNote(fd);
        close(fd);
        return false;
    }
    else {
        close(fd);
        return true;
    }
}

void taosGetNoteName(char *fn)
{
    if (taosNoteFileNum > 1) {
        for (int i = 0; i < taosNoteFileNum; i++) {
            char fileName[NOTE_FILE_NAME_LEN];

            sprintf(fileName, "%s%d.0", fn, i);
            bool file1open = taosCheckNoteIsOpen(fileName);

            sprintf(fileName, "%s%d.1", fn, i);
            bool file2open = taosCheckNoteIsOpen(fileName);

            if (!file1open && !file2open) {
                sprintf(taosNoteName, "%s%d", fn, i);
                return;
            }
        }
    }

    strcpy(taosNoteName, fn);
}

int taosOpenNoteWithMaxLines(char *fn, int maxLines, int maxNoteNum)
{
    char name[NOTE_FILE_NAME_LEN] = "\0";
    struct stat  notestat0, notestat1;
    int size;

    taosNoteMaxLines = maxLines;
    taosNoteFileNum = maxNoteNum;
    taosGetNoteName(fn);

    strcpy(name, fn);
    strcat(name, ".0");

    // if none of the note files exist, open 0, if both exists, open the old one
    if (stat(name, &notestat0) < 0) {
        taosNoteFlag = 0;
    }
    else {
        strcpy(name, fn);
        strcat(name, ".1");
        if (stat(name, &notestat1) < 0) {
            taosNoteFlag = 1;
        }
        else {
            taosNoteFlag = (notestat0.st_mtime > notestat1.st_mtime) ? 0 : 1;
        }
    }

    sprintf(name, "%s.%d", taosNoteName, taosNoteFlag);
    pthread_mutex_init(&taosNoteMutex, NULL);

    umask(0);
    taosNoteFd = open(name, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

    if (taosNoteFd < 0) {
        fprintf(stderr, "failed to open note file:%s reason:%s\n", name, strerror(errno));
        return -1;
    }
    taosLockNote(taosNoteFd);

    // only an estimate for number of lines
    struct stat filestat;
    fstat(taosNoteFd, &filestat);
    size = (int)filestat.st_size;
    taosNoteLines = size / 60;

    lseek(taosNoteFd, 0, SEEK_END);

    return 0;
}

void taosNotePrint(const char * const format, ...)
{
    va_list argpointer;
    char    buffer[MAX_NOTE_LINE_SIZE];
    int     len;
    struct  tm      Tm, *ptm;
    struct  timeval timeSecs;
    time_t  curTime;

    gettimeofday(&timeSecs, NULL);
    curTime = timeSecs.tv_sec;
    ptm = localtime_r(&curTime, &Tm);
    len = sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec, (int)timeSecs.tv_usec);

    va_start(argpointer, format);
    len += vsnprintf(buffer + len, MAX_NOTE_LINE_SIZE - len, format, argpointer);
    va_end(argpointer);

    if (len >= MAX_NOTE_LINE_SIZE) len = MAX_NOTE_LINE_SIZE - 2;

    buffer[len++] = '\n';
    buffer[len] = 0;

    if (taosNoteFd >= 0)  {
        twrite(taosNoteFd, buffer, (unsigned int)len);

        if (taosNoteMaxLines > 0) {
            taosNoteLines++;
            if ((taosNoteLines > taosNoteMaxLines) && (taosNoteOpenInProgress == 0))
                taosOpenNewNote();
        }
    }
}

void taosCloseNote()
{
    taosCloseNoteByFd(taosNoteFd);
}

void taosCloseNoteByFd(int fd)
{
    if (fd >= 0) {
        taosUnLockNote(fd);
        close(fd);
    }
}
