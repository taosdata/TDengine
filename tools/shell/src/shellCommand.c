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
#include "shellInt.h"
#include "shellAuto.h"

#define LEFT  1
#define RIGHT 2
#define UP    3
#define DOWN  4
#define PSIZE shell.info.promptSize
#define SHELL_INPUT_MAX_COMMAND_SIZE 10000


static int32_t shellCountPrefixOnes(uint8_t c);

static void    shellGetNextCharSize(const char *str, int32_t pos, int32_t *size, int32_t *width);

static void    shellBackspaceChar(SShellCmd *cmd);
static void    shellClearLineBefore(SShellCmd *cmd);
static void    shellClearLineAfter(SShellCmd *cmd);
static void    shellDeleteChar(SShellCmd *cmd);
static void    shellMoveCursorLeft(SShellCmd *cmd);
static void    shellMoveCursorRight(SShellCmd *cmd);
static void    shellPositionCursorHome(SShellCmd *cmd);
static void    shellPositionCursorEnd(SShellCmd *cmd);
static void    shellPrintChar(char c, int32_t times);
static void    shellPositionCursor(int32_t step, int32_t direction);
static void    shellUpdateBuffer(SShellCmd *cmd);
static bool    shellIsReadyGo(SShellCmd *cmd);
static void    shellGetMbSizeInfo(const char *str, int32_t *size, int32_t *width);
static void    shellResetCommand(SShellCmd *cmd, const char s[]);
void           shellClearScreen(int32_t ecmd_pos, int32_t cursor_pos);
void           shellShowOnScreen(SShellCmd *cmd);
void           shellGetPrevCharSize(const char *str, int32_t pos, int32_t *size, int32_t *width);
void           shellInsertChar(SShellCmd *cmd, char *c, int size);
void           shellInsertString(SShellCmd *cmd, char *str, int size);

int32_t shellCountPrefixOnes(uint8_t c) {
  uint8_t mask = 127;
  mask = ~mask;
  int32_t ret = 0;
  while ((c & mask) != 0) {
    ret++;
    c <<= 1;
  }

  return ret;
}

void shellGetPrevCharSize(const char *str, int32_t pos, int32_t *size, int32_t *width) {
  if (pos <= 0) return;

  TdWchar wc;
  *size = 0;
  *width = 0;

  while (--pos >= 0) {
    *size += 1;

    if (str[pos] > 0 || shellCountPrefixOnes((uint8_t)str[pos]) > 1) break;
  }

  taosMbToWchar(&wc, str + pos, MB_CUR_MAX);
  // ASSERT(rc == *size); // it will be core, if str is encode by utf8 and taos charset is gbk

  *width = taosWcharWidth(wc);
}

void shellGetNextCharSize(const char *str, int32_t pos, int32_t *size, int32_t *width) {
  if(pos < 0) return;

  TdWchar wc;
  *size = taosMbToWchar(&wc, str + pos, MB_CUR_MAX);
  *width = taosWcharWidth(wc);
}

void shellInsertChar(SShellCmd *cmd, char *c, int32_t size) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return;

  TdWchar wc;
  if (taosMbToWchar(&wc, c, size) < 0) return;

  shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
  /* update the buffer */
  memmove(cmd->command + cmd->cursorOffset + size, cmd->command + cmd->cursorOffset,
          cmd->commandSize - cmd->cursorOffset);
  memcpy(cmd->command + cmd->cursorOffset, c, size);
  /* update the values */
  cmd->commandSize += size;
  cmd->cursorOffset += size;
  cmd->screenOffset += taosWcharWidth(wc);
  cmd->endOffset += taosWcharWidth(wc);

  // set string end
  cmd->command[cmd->commandSize] = 0;
#ifdef WINDOWS
#else
  shellShowOnScreen(cmd);
#endif
}

// insert string . count is str char count
void shellInsertStr(SShellCmd *cmd, char *str, int32_t size) {
  shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
  /* update the buffer */
  memmove(cmd->command + cmd->cursorOffset + size, cmd->command + cmd->cursorOffset,
          cmd->commandSize - cmd->cursorOffset);
  memcpy(cmd->command + cmd->cursorOffset, str, size);
  /* update the values */
  cmd->commandSize += size;
  cmd->cursorOffset += size;
  cmd->screenOffset += size;
  cmd->endOffset += size;

  // set string end
  cmd->command[cmd->commandSize] = 0;
#ifdef WINDOWS
#else
  shellShowOnScreen(cmd);
#endif
}

void shellBackspaceChar(SShellCmd *cmd) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return;

  if (cmd->cursorOffset > 0) {
    shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
    int32_t size = 0;
    int32_t width = 0;
    shellGetPrevCharSize(cmd->command, cmd->cursorOffset, &size, &width);
    memmove(cmd->command + cmd->cursorOffset - size, cmd->command + cmd->cursorOffset,
            cmd->commandSize - cmd->cursorOffset);
    cmd->commandSize -= size;
    cmd->cursorOffset -= size;
    cmd->screenOffset -= width;
    cmd->endOffset -= width;
    // set string end
    cmd->command[cmd->commandSize] = 0;
    shellShowOnScreen(cmd);
  }
}

void shellClearLineBefore(SShellCmd *cmd) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return;

  shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
  memmove(cmd->command, cmd->command + cmd->cursorOffset, cmd->commandSize - cmd->cursorOffset);
  cmd->commandSize -= cmd->cursorOffset;
  cmd->cursorOffset = 0;
  cmd->screenOffset = 0;
  cmd->endOffset = cmd->commandSize;
  // set string end
  cmd->command[cmd->commandSize] = 0;
  shellShowOnScreen(cmd);
}

void shellClearLineAfter(SShellCmd *cmd) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return;

  shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
  cmd->commandSize -= cmd->endOffset - cmd->cursorOffset;
  cmd->endOffset = cmd->cursorOffset;
  shellShowOnScreen(cmd);
}

void shellDeleteChar(SShellCmd *cmd) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return;

  if (cmd->cursorOffset < cmd->commandSize) {
    shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
    int32_t size = 0;
    int32_t width = 0;
    shellGetNextCharSize(cmd->command, cmd->cursorOffset, &size, &width);
    memmove(cmd->command + cmd->cursorOffset, cmd->command + cmd->cursorOffset + size,
            cmd->commandSize - cmd->cursorOffset - size);
    cmd->commandSize -= size;
    cmd->endOffset -= width;
    // set string end
    cmd->command[cmd->commandSize] = 0;
    shellShowOnScreen(cmd);
  }
}

void shellMoveCursorLeft(SShellCmd *cmd) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return;

  if (cmd->cursorOffset > 0) {
    shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
    int32_t size = 0;
    int32_t width = 0;
    shellGetPrevCharSize(cmd->command, cmd->cursorOffset, &size, &width);
    cmd->cursorOffset -= size;
    cmd->screenOffset -= width;
    shellShowOnScreen(cmd);
  }
}

void shellMoveCursorRight(SShellCmd *cmd) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return;

  if (cmd->cursorOffset < cmd->commandSize) {
    shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
    int32_t size = 0;
    int32_t width = 0;
    shellGetNextCharSize(cmd->command, cmd->cursorOffset, &size, &width);
    cmd->cursorOffset += size;
    cmd->screenOffset += width;
    shellShowOnScreen(cmd);
  }
}

void shellPositionCursorHome(SShellCmd *cmd) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return;

  if (cmd->cursorOffset > 0) {
    shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
    cmd->cursorOffset = 0;
    cmd->screenOffset = 0;
    shellShowOnScreen(cmd);
  }
}

void positionCursorMiddle(SShellCmd *cmd) {
  if (cmd->endOffset > 0) {
    shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
    cmd->cursorOffset = cmd->commandSize/2;
    cmd->screenOffset = cmd->endOffset/2;
    shellShowOnScreen(cmd);
  }
}

void shellPositionCursorEnd(SShellCmd *cmd) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return;

  if (cmd->cursorOffset < cmd->commandSize) {
    shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
    cmd->cursorOffset = cmd->commandSize;
    cmd->screenOffset = cmd->endOffset;
    shellShowOnScreen(cmd);
  }
}

void shellPrintChar(char c, int32_t times) {
  for (int32_t i = 0; i < times; i++) {
    fprintf(stdout, "%c", c);
  }
  fflush(stdout);
}

void shellPositionCursor(int32_t step, int32_t direction) {
#ifndef WINDOWS
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
#endif
}

void shellUpdateBuffer(SShellCmd *cmd) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return;

  if (shellRegexMatch(cmd->buffer, "(\\s+$)|(^$)", REG_EXTENDED)) strcat(cmd->command, " ");
  strcat(cmd->buffer, cmd->command);
  cmd->bufferSize += cmd->commandSize;

  memset(cmd->command, 0, SHELL_MAX_COMMAND_SIZE);
  cmd->cursorOffset = 0;
  cmd->screenOffset = 0;
  cmd->commandSize = 0;
  cmd->endOffset = 0;
  shellShowOnScreen(cmd);
}

bool shellIsReadyGo(SShellCmd *cmd) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return false;

  char *total = (char *)taosMemoryCalloc(1, SHELL_MAX_COMMAND_SIZE);
  memset(cmd->command + cmd->commandSize, 0, SHELL_MAX_COMMAND_SIZE - cmd->commandSize);
  sprintf(total, "%s%s", cmd->buffer, cmd->command);

  char *reg_str =
      "(^.*;\\s*$)|(^\\s*$)|(^\\s*exit\\s*$)|(^\\s*q\\s*$)|(^\\s*quit\\s*$)|(^"
      "\\s*clear\\s*$)";
  if (shellRegexMatch(total, reg_str, REG_EXTENDED | REG_ICASE)) {
    taosMemoryFree(total);
    return true;
  }

  taosMemoryFree(total);
  return false;
}

void shellGetMbSizeInfo(const char *str, int32_t *size, int32_t *width) {
  TdWchar *wc = (TdWchar *)taosMemoryCalloc(sizeof(TdWchar), SHELL_MAX_COMMAND_SIZE);
  *size = strlen(str);
  taosMbsToWchars(wc, str, SHELL_MAX_COMMAND_SIZE);
  *width = taosWcharsWidth(wc, SHELL_MAX_COMMAND_SIZE);
  taosMemoryFree(wc);
}

void shellResetCommand(SShellCmd *cmd, const char s[]) {
  if(cmd->cursorOffset > cmd->commandSize || cmd->endOffset < cmd->screenOffset) return;

  shellClearScreen(cmd->endOffset + PSIZE, cmd->screenOffset + PSIZE);
  memset(cmd->buffer, 0, SHELL_MAX_COMMAND_SIZE);
  memset(cmd->command, 0, SHELL_MAX_COMMAND_SIZE);
  strncpy(cmd->command, s, SHELL_MAX_COMMAND_SIZE);
  int32_t size = 0;
  int32_t width = 0;
  shellGetMbSizeInfo(s, &size, &width);
  cmd->bufferSize = 0;
  cmd->commandSize = size;
  cmd->cursorOffset = size;
  cmd->screenOffset = width;
  cmd->endOffset = width;
  shellShowOnScreen(cmd);
}


void shellGetScreenSize(int32_t *ws_col, int32_t *ws_row) {
#ifdef WINDOWS
  CONSOLE_SCREEN_BUFFER_INFO csbi;
  GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi);
  if (ws_col != NULL) *ws_col = csbi.srWindow.Right - csbi.srWindow.Left + 1;
  if (ws_row != NULL) *ws_row = csbi.srWindow.Bottom - csbi.srWindow.Top + 1;
#else
  struct winsize w;
  if (ioctl(0, TIOCGWINSZ, &w) < 0 || w.ws_col == 0 || w.ws_row == 0) {
    // fprintf(stderr, "No stream device, and use default value(col 120, row 30)\r\n");
    if (ws_col != NULL) *ws_col = 120;
    if (ws_row != NULL) *ws_row = 30;
  } else {
    if (ws_col != NULL) *ws_col = w.ws_col;
    if (ws_row != NULL) *ws_row = w.ws_row;
  }
#endif
}

void shellClearScreen(int32_t ecmd_pos, int32_t cursor_pos) {
  int32_t ws_col;
  shellGetScreenSize(&ws_col, NULL);

  int32_t cursor_x = cursor_pos / ws_col;
  int32_t cursor_y = cursor_pos % ws_col;
  int32_t command_x = ecmd_pos / ws_col;
  shellPositionCursor(cursor_y, LEFT);
  shellPositionCursor(command_x - cursor_x, DOWN);
#ifndef WINDOWS
  fprintf(stdout, "\033[2K");
#endif
  for (int32_t i = 0; i < command_x; i++) {
    shellPositionCursor(1, UP);
  #ifndef WINDOWS
    fprintf(stdout, "\033[2K");
  #endif
  }
  fflush(stdout);
}

void shellShowOnScreen(SShellCmd *cmd) {
  int32_t ws_col;
  shellGetScreenSize(&ws_col, NULL);

  TdWchar wc;
  int32_t size = 0;

  // Print out the command.
  char *total_string = taosMemoryMalloc(SHELL_MAX_COMMAND_SIZE);
  memset(total_string, '\0', SHELL_MAX_COMMAND_SIZE);
  if (strcmp(cmd->buffer, "") == 0) {
    sprintf(total_string, "%s%s", shell.info.promptHeader, cmd->command);
  } else {
    sprintf(total_string, "%s%s", shell.info.promptContinue, cmd->command);
  }
  int32_t remain_column = ws_col;
  for (char *str = total_string; size < cmd->commandSize + PSIZE;) {
    int32_t ret = taosMbToWchar(&wc, str, MB_CUR_MAX);
    if (ret < 0) break;
    size += ret;
    /* ASSERT(size >= 0); */
    int32_t width = taosWcharWidth(wc);
    if (remain_column > width) {
      printf("%lc", wc);
      remain_column -= width;
    } else {
      if (remain_column == width) {
        printf("%lc\n\r", wc);
        remain_column = ws_col;
      } else {
        printf("\n\r%lc", wc);
        remain_column = ws_col - width;
      }
    }

    str = total_string + size;
  }

  taosMemoryFree(total_string);
  // Position the cursor
  int32_t cursor_pos = cmd->screenOffset + PSIZE;
  int32_t ecmd_pos = cmd->endOffset + PSIZE;

  int32_t cursor_x = cursor_pos / ws_col;
  int32_t cursor_y = cursor_pos % ws_col;
  // int32_t cursor_y = cursor % ws_col;
  int32_t command_x = ecmd_pos / ws_col;
  int32_t command_y = ecmd_pos % ws_col;
  // int32_t command_y = (command.size() + PSIZE) % ws_col;
  shellPositionCursor(command_y, LEFT);
  shellPositionCursor(command_x, UP);
  shellPositionCursor(cursor_x, DOWN);
  shellPositionCursor(cursor_y, RIGHT);
  fflush(stdout);
}

char taosGetConsoleChar() {
#ifdef WINDOWS
  static void *console = NULL;
  if (console == NULL) {
    console = GetStdHandle(STD_INPUT_HANDLE);
  }
  static TdWchar buf[SHELL_INPUT_MAX_COMMAND_SIZE];
  static char mbStr[5];
  static unsigned long bufLen = 0;
  static uint16_t bufIndex = 0, mbStrIndex = 0, mbStrLen = 0;
  CONSOLE_READCONSOLE_CONTROL inputControl={ sizeof(CONSOLE_READCONSOLE_CONTROL), 0, 1<<TAB_KEY, 0 };
  while (bufLen == 0) {
    ReadConsoleW(console, buf, SHELL_INPUT_MAX_COMMAND_SIZE, &bufLen, &inputControl);
    if (bufLen > 0 && buf[0] == 0) bufLen = 0;
    bufIndex = 0;
  }
  if (mbStrLen == 0){
    if (buf[bufIndex] == '\r') {
      bufIndex++;
    }
    mbStrLen = WideCharToMultiByte(CP_UTF8, 0, &buf[bufIndex], 1, mbStr, sizeof(mbStr), NULL, NULL);
    mbStrIndex = 0;
    bufIndex++;
  }
  mbStrIndex++;
  if (mbStrIndex == mbStrLen) {
    mbStrLen = 0;
    if (bufIndex == bufLen) {
      bufLen = 0;
    }
  }
  return mbStr[mbStrIndex-1];
#else
  return (char)getchar();  // getchar() return an 'int32_t' value
#endif
}

int32_t shellReadCommand(char *command) {
  SShellHistory *pHistory = &shell.history;
  SShellCmd      cmd = {0};
  uint32_t       hist_counter = pHistory->hend;
  char           utf8_array[10] = "\0";

  cmd.buffer = (char *)taosMemoryCalloc(1, SHELL_MAX_COMMAND_SIZE);
  cmd.command = (char *)taosMemoryCalloc(1, SHELL_MAX_COMMAND_SIZE);
  shellShowOnScreen(&cmd);

  // Read input.
  char c;
  while (1) {
    c = taosGetConsoleChar();

    if (c == (char)EOF) {
      return c;
    }

    if (c < 0) {  // For UTF-8
      int32_t count = shellCountPrefixOnes(c);
      utf8_array[0] = c;
      for (int32_t k = 1; k < count; k++) {
        c = taosGetConsoleChar();
        utf8_array[k] = c;
      }
      shellInsertChar(&cmd, utf8_array, count);
      pressOtherKey(c);
#ifndef WINDOWS
    } else if (c == TAB_KEY) {
      // press TAB key
      pressTabKey(&cmd);
#endif
    } else if (c < '\033') {
      pressOtherKey(c);      
      // Ctrl keys.  TODO: Implement ctrl combinations
      switch (c) {
        case 0:
          break;
        case 1:  // ctrl A
          shellPositionCursorHome(&cmd);
          break;
        case 3:
          printf("\r\n");
          shellResetCommand(&cmd, "");
          #ifdef WINDOWS
            raise(SIGINT);
          #else
            kill(0, SIGINT);
          #endif
          break;
        case 4:  // EOF or Ctrl+D
          taosResetTerminalMode();
          printf("\r\n");
          return -1;
        case 5:  // ctrl E
          shellPositionCursorEnd(&cmd);
          break;
        case 8:
          shellBackspaceChar(&cmd);
          break;
        case '\n':
        case '\r':
        #ifdef WINDOWS 
        #else
          printf("\r\n");
        #endif
          if (shellIsReadyGo(&cmd)) {
            sprintf(command, "%s%s", cmd.buffer, cmd.command);
            taosMemoryFreeClear(cmd.buffer);
            taosMemoryFreeClear(cmd.command);
            return 0;
          } else {
            shellUpdateBuffer(&cmd);
          }
          break;
        case 11:  // Ctrl + K;
          shellClearLineAfter(&cmd);
          break;
        case 12:  // Ctrl + L;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
          system("clear");
#pragma GCC diagnostic pop
          shellShowOnScreen(&cmd);
          break;
        case 21:  // Ctrl + U;
          shellClearLineBefore(&cmd);
          break;
        case 23:  // Ctrl + W;
          positionCursorMiddle(&cmd);
          break;          
      }
    } else if (c == '\033') {
      pressOtherKey(c);
      c = taosGetConsoleChar();
      switch (c) {
        case '[':
          c = taosGetConsoleChar();
          switch (c) {
            case 'A':  // Up arrow
              hist_counter = (hist_counter + SHELL_MAX_HISTORY_SIZE - 1) % SHELL_MAX_HISTORY_SIZE;
              if (pHistory->hist[hist_counter] == NULL) {
                hist_counter = (hist_counter + SHELL_MAX_HISTORY_SIZE + 1) % SHELL_MAX_HISTORY_SIZE;
              } else {
                shellResetCommand(&cmd, pHistory->hist[hist_counter]);
              }
              break;
            case 'B':  // Down arrow
              if (hist_counter != pHistory->hend) {
                int32_t next_hist = (hist_counter + 1) % SHELL_MAX_HISTORY_SIZE;

                if (next_hist != pHistory->hend) {
                  shellResetCommand(&cmd, (pHistory->hist[next_hist] == NULL) ? "" : pHistory->hist[next_hist]);
                } else {
                  shellResetCommand(&cmd, "");
                }
                hist_counter = next_hist;
              }
              break;
            case 'C':  // Right arrow
              shellMoveCursorRight(&cmd);
              break;
            case 'D':  // Left arrow
              shellMoveCursorLeft(&cmd);
              break;
            case '1':
              if ((c = taosGetConsoleChar()) == '~') {
                // Home key
                shellPositionCursorHome(&cmd);
              }
              break;
            case '2':
              if ((c = taosGetConsoleChar()) == '~') {
                // Insert key
              }
              break;
            case '3':
              if ((c = taosGetConsoleChar()) == '~') {
                // Delete key
                shellDeleteChar(&cmd);
              }
              break;
            case '4':
              if ((c = taosGetConsoleChar()) == '~') {
                // End key
                shellPositionCursorEnd(&cmd);
              }
              break;
            case '5':
              if ((c = taosGetConsoleChar()) == '~') {
                // Page up key
              }
              break;
            case '6':
              if ((c = taosGetConsoleChar()) == '~') {
                // Page down key
              }
              break;
            case 72:
              // Home key
              shellPositionCursorHome(&cmd);
              break;
            case 70:
              // End key
              shellPositionCursorEnd(&cmd);
              break;
          }
          break;
      }
    } else if (c == 0x7f) {
      pressOtherKey(c);
      // press delete key
      shellBackspaceChar(&cmd);
    } else {
      pressOtherKey(c);
      shellInsertChar(&cmd, &c, 1);
    }
  }

  return 0;
}
