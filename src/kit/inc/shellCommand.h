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

#ifndef __COMMAND_STRUCT__
#define __COMMAND_STRUCT__

#include "shell.h"

#define LEFT  1
#define RIGHT 2
#define UP    3
#define DOWN  4

typedef struct Command Command;
struct Command {
  char *   buffer;
  char *   command;
  unsigned commandSize;
  unsigned bufferSize;
  unsigned cursorOffset;
  unsigned screenOffset;
  unsigned endOffset;
};

extern void insertChar(Command *cmd, char *c, int size);
extern void backspaceChar(Command *cmd);
extern void deleteChar(Command *cmd);
extern void moveCursorLeft(Command *cmd);
extern void moveCursorRight(Command *cmd);
extern void positionCursorHome(Command *cmd);
extern void positionCursorEnd(Command *cmd);
extern void showOnScreen(Command *cmd);
extern void updateBuffer(Command *cmd);
extern int isReadyGo(Command *cmd);
extern void resetCommand(Command *cmd, const char s[]);

int countPrefixOnes(char c);
void clearScreen(int ecmd_pos, int cursor_pos);
void printChar(char c, int times);
void positionCursor(int step, int direction);

#endif
