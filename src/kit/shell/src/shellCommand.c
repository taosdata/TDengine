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

#define __USE_XOPEN

#include "os.h"
#include "shell.h"
#include "shellCommand.h"

extern int wcwidth(wchar_t c);
extern int wcswidth(const wchar_t *s, size_t n);
typedef struct {
  char widthInString;
  char widthOnScreen;
} UTFCodeInfo;

int countPrefixOnes(unsigned char c) {
  unsigned char mask = 127;
  mask = ~mask;
  int ret = 0;
  while ((c & mask) != 0) {
    ret++;
    c <<= 1;
  }

  return ret;
}

void getPrevCharSize(const char *str, int pos, int *size, int *width) {
  assert(pos > 0);

  wchar_t wc;
  *size = 0;
  *width = 0;

  while (--pos >= 0) {
    *size += 1;

    if (str[pos] > 0 || countPrefixOnes((unsigned char )str[pos]) > 1) break;
  }

  int rc = mbtowc(&wc, str + pos, MB_CUR_MAX);
  assert(rc == *size);

  *width = wcwidth(wc);
}

void getNextCharSize(const char *str, int pos, int *size, int *width) {
  assert(pos >= 0);

  wchar_t wc;
  *size = mbtowc(&wc, str + pos, MB_CUR_MAX);
  *width = wcwidth(wc);
}

void insertChar(Command *cmd, char *c, int size) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  wchar_t wc;
  if (mbtowc(&wc, c, size) < 0) return;

  clearScreen(cmd->endOffset + prompt_size, cmd->screenOffset + prompt_size);
  /* update the buffer */
  memmove(cmd->command + cmd->cursorOffset + size, cmd->command + cmd->cursorOffset,
          cmd->commandSize - cmd->cursorOffset);
  memcpy(cmd->command + cmd->cursorOffset, c, size);
  /* update the values */
  cmd->commandSize += size;
  cmd->cursorOffset += size;
  cmd->screenOffset += wcwidth(wc);
  cmd->endOffset += wcwidth(wc);
  showOnScreen(cmd);
}

void backspaceChar(Command *cmd) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  if (cmd->cursorOffset > 0) {
    clearScreen(cmd->endOffset + prompt_size, cmd->screenOffset + prompt_size);
    int size = 0;
    int width = 0;
    getPrevCharSize(cmd->command, cmd->cursorOffset, &size, &width);
    memmove(cmd->command + cmd->cursorOffset - size, cmd->command + cmd->cursorOffset,
            cmd->commandSize - cmd->cursorOffset);
    cmd->commandSize -= size;
    cmd->cursorOffset -= size;
    cmd->screenOffset -= width;
    cmd->endOffset -= width;
    showOnScreen(cmd);
  }
}

void clearLineBefore(Command *cmd) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  clearScreen(cmd->endOffset + prompt_size, cmd->screenOffset + prompt_size);
  memmove(cmd->command, cmd->command + cmd->cursorOffset,
          cmd->commandSize - cmd->cursorOffset);
  cmd->commandSize -= cmd->cursorOffset;
  cmd->cursorOffset = 0;
  cmd->screenOffset = 0;
  cmd->endOffset = cmd->commandSize;
  showOnScreen(cmd);
}

void clearLineAfter(Command *cmd) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  clearScreen(cmd->endOffset + prompt_size, cmd->screenOffset + prompt_size);
  cmd->commandSize -= cmd->endOffset - cmd->cursorOffset;
  cmd->endOffset = cmd->cursorOffset;
  showOnScreen(cmd);
}

void deleteChar(Command *cmd) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  if (cmd->cursorOffset < cmd->commandSize) {
    clearScreen(cmd->endOffset + prompt_size, cmd->screenOffset + prompt_size);
    int size = 0;
    int width = 0;
    getNextCharSize(cmd->command, cmd->cursorOffset, &size, &width);
    memmove(cmd->command + cmd->cursorOffset, cmd->command + cmd->cursorOffset + size,
            cmd->commandSize - cmd->cursorOffset - size);
    cmd->commandSize -= size;
    cmd->endOffset -= width;
    showOnScreen(cmd);
  }
}

void moveCursorLeft(Command *cmd) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  if (cmd->cursorOffset > 0) {
    clearScreen(cmd->endOffset + prompt_size, cmd->screenOffset + prompt_size);
    int size = 0;
    int width = 0;
    getPrevCharSize(cmd->command, cmd->cursorOffset, &size, &width);
    cmd->cursorOffset -= size;
    cmd->screenOffset -= width;
    showOnScreen(cmd);
  }
}

void moveCursorRight(Command *cmd) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  if (cmd->cursorOffset < cmd->commandSize) {
    clearScreen(cmd->endOffset + prompt_size, cmd->screenOffset + prompt_size);
    int size = 0;
    int width = 0;
    getNextCharSize(cmd->command, cmd->cursorOffset, &size, &width);
    cmd->cursorOffset += size;
    cmd->screenOffset += width;
    showOnScreen(cmd);
  }
}

void positionCursorHome(Command *cmd) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  if (cmd->cursorOffset > 0) {
    clearScreen(cmd->endOffset + prompt_size, cmd->screenOffset + prompt_size);
    cmd->cursorOffset = 0;
    cmd->screenOffset = 0;
    showOnScreen(cmd);
  }
}

void positionCursorEnd(Command *cmd) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  if (cmd->cursorOffset < cmd->commandSize) {
    clearScreen(cmd->endOffset + prompt_size, cmd->screenOffset + prompt_size);
    cmd->cursorOffset = cmd->commandSize;
    cmd->screenOffset = cmd->endOffset;
    showOnScreen(cmd);
  }
}

void printChar(char c, int times) {
  for (int i = 0; i < times; i++) {
    fprintf(stdout, "%c", c);
  }
  fflush(stdout);
}

void positionCursor(int step, int direction) {
  if (step > 0) {
    if (direction == LEFT) {
      fprintf(stdout, "\033[%dD", step);
    } else if (direction == RIGHT) {
      fprintf(stdout, "\033[%dC", step);
    } else if (direction == UP) {
      fprintf(stdout, "\033[%dA", step);
    } else if (direction == DOWN) {
      fprintf(stdout, "\033[%dB", step);
    }
    fflush(stdout);
  }
}

void updateBuffer(Command *cmd) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  if (regex_match(cmd->buffer, "(\\s+$)|(^$)", REG_EXTENDED)) strcat(cmd->command, " ");
  strcat(cmd->buffer, cmd->command);
  cmd->bufferSize += cmd->commandSize;

  memset(cmd->command, 0, MAX_COMMAND_SIZE);
  cmd->cursorOffset = 0;
  cmd->screenOffset = 0;
  cmd->commandSize = 0;
  cmd->endOffset = 0;
  showOnScreen(cmd);
}

int isReadyGo(Command *cmd) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  char *total = (char *)calloc(1, MAX_COMMAND_SIZE);
  memset(cmd->command + cmd->commandSize, 0, MAX_COMMAND_SIZE - cmd->commandSize);
  sprintf(total, "%s%s", cmd->buffer, cmd->command);

  char *reg_str =
    "(^.*;\\s*$)|(^\\s*$)|(^\\s*exit\\s*$)|(^\\s*q\\s*$)|(^\\s*quit\\s*$)|(^"
    "\\s*clear\\s*$)";
  if (regex_match(total, reg_str, REG_EXTENDED | REG_ICASE)) {
    free(total);
    return 1;
  }

  free(total);
  return 0;
}

void getMbSizeInfo(const char *str, int *size, int *width) {
  wchar_t *wc = (wchar_t *)calloc(sizeof(wchar_t), MAX_COMMAND_SIZE);
  *size = strlen(str);
  mbstowcs(wc, str, MAX_COMMAND_SIZE);
  *width = wcswidth(wc, MAX_COMMAND_SIZE);
  free(wc);
}

void resetCommand(Command *cmd, const char s[]) {
  assert(cmd->cursorOffset <= cmd->commandSize && cmd->endOffset >= cmd->screenOffset);

  clearScreen(cmd->endOffset + prompt_size, cmd->screenOffset + prompt_size);
  memset(cmd->buffer, 0, MAX_COMMAND_SIZE);
  memset(cmd->command, 0, MAX_COMMAND_SIZE);
  strncpy(cmd->command, s, MAX_COMMAND_SIZE);
  int size = 0;
  int width = 0;
  getMbSizeInfo(s, &size, &width);
  cmd->bufferSize = 0;
  cmd->commandSize = size;
  cmd->cursorOffset = size;
  cmd->screenOffset = width;
  cmd->endOffset = width;
  showOnScreen(cmd);
}
