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

#include "os.h"
#include "tutil.h"
#include "tglobal.h"

#define MAX_NOTE_LINE_SIZE 66000
#define NOTE_FILE_NAME_LEN 300
  
typedef struct _taosNoteInfo {
  int  taosNoteFileNum ;
  int  taosNoteMaxLines;
  int  taosNoteLines;
  char taosNoteName[NOTE_FILE_NAME_LEN];
  int  taosNoteFlag;
  int  taosNoteFd;
  int  taosNoteOpenInProgress;
  pthread_mutex_t taosNoteMutex;
}taosNoteInfo;
  
void taosNotePrint(taosNoteInfo * pNote, const char * const format, ...);

extern taosNoteInfo  m_HttpNote;
extern taosNoteInfo  m_TscNote;

extern int   tsHttpEnableRecordSql;
extern int   tsTscEnableRecordSql;

#define taosNotePrintHttp(...)               \
  if (tsHttpEnableRecordSql) {               \
    taosNotePrint(&m_HttpNote, __VA_ARGS__); \
  }
    
#define taosNotePrintTsc(...)                 \
    if (tsTscEnableRecordSql) {               \
      taosNotePrint(&m_TscNote, __VA_ARGS__); \
    }

#ifdef __cplusplus
}
#endif

#endif
