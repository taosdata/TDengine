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

#ifndef TDENGINE_TNOTE_H
#define TDENGINE_TNOTE_H

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_NOTE_LINE_SIZE 66000
#define NOTE_FILE_NAME_LEN 300

typedef struct {
  int32_t fileNum;
  int32_t maxLines;
  int32_t lines;
  int32_t flag;
  int32_t fd;
  int32_t openInProgress;
  char    name[NOTE_FILE_NAME_LEN];
  pthread_mutex_t mutex;
} SNoteObj;

extern SNoteObj tsHttpNote;
extern SNoteObj tsTscNote;
extern SNoteObj tsInfoNote;

void taosInitNotes();
void taosNotePrint(SNoteObj* pNote, const char* const format, ...);
void taosNotePrintBuffer(SNoteObj *pNote, char *buffer, int32_t len);

#define nPrintHttp(...)                      \
  if (tsHttpEnableRecordSql) {               \
    taosNotePrint(&tsHttpNote, __VA_ARGS__); \
  }

#define nPrintTsc(...)                      \
  if (tsTscEnableRecordSql) {               \
    taosNotePrint(&tsTscNote, __VA_ARGS__); \
  }

#define nInfo(buffer, len) taosNotePrintBuffer(&tsInfoNote, buffer, len);

#ifdef __cplusplus
}
#endif

#endif
