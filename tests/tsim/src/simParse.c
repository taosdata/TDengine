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

/* Thi file is to parse the simScriptPos, function file and default file,
 *
 * syntax
 *
 *    expression is format like:
 *       $var [=|<|>|==] var
 *     or:
 *       $var = var [+|-|*|/|.] var
 *
 *    if expression
 *      command
 *
 *    if expression then
 *      commands
 *    elif expression
 *      commands
 *    elif expression
 *      commands
 *    else
 *      commands
 *    endi
 *
 *    while expression
 *      commands
 *      continue
 *      break
 *    endw
 *
 *    switch expression
 *      case 1
 *        commands
 *        break
 *      case 2
 *        commands
 *        break
 *      default
 *        commands
 *    ends
 *
 *    label:
 *    goto label
 *
 */

#define _DEFAULT_SOURCE
#include "os.h"
#include "sim.h"
#include "simParse.h"
#include "tutil.h"
#undef TAOS_MEM_CHECK

static SCommand *cmdHashList[MAX_NUM_CMD];
static SCmdLine  cmdLine[MAX_CMD_LINES];
static char      parseErr[MAX_ERROR_LEN];
static char      optionBuffer[MAX_OPTION_BUFFER];
static int32_t   numOfLines, optionOffset;
static SLabel    label, dest;
static SBlock    block;

int32_t simHashCmd(char *token, int32_t tokenLen) {
  int32_t i;
  int32_t hash = 0;

  for (i = 0; i < tokenLen; ++i) hash += token[i];

  hash = hash % MAX_NUM_CMD;

  return hash;
}

SCommand *simCheckCmd(char *token, int32_t tokenLen) {
  int32_t   hash;
  SCommand *node;

  hash = simHashCmd(token, tokenLen);

  node = cmdHashList[hash];

  while (node) {
    if (node->nlen == tokenLen && strncmp(node->name, token, tokenLen) == 0) {
      return node;
    } else {
      node = node->next;
    }
  }

  return NULL;
}

void simAddCmdIntoHash(SCommand *pCmd) {
  int32_t   hash;
  SCommand *node;

  hash = simHashCmd(pCmd->name, (int32_t)strlen(pCmd->name));
  node = cmdHashList[hash];
  pCmd->next = node;
  cmdHashList[hash] = pCmd;
}

void simResetParser() {
  optionOffset = 4;
  numOfLines = 0;
  memset(cmdLine, 0, sizeof(cmdLine));
  memset(optionBuffer, 0, sizeof(optionBuffer));
  memset(&label, 0, sizeof(label));
  memset(&block, 0, sizeof(block));
  memset(&dest, 0, sizeof(dest));
}

SScript *simBuildScriptObj(char *fileName) {
  int32_t i, destPos;

  /* process labels */

  cmdLine[numOfLines].cmdno = SIM_CMD_RETURN;
  numOfLines++;

  for (i = 0; i < numOfLines; ++i) {
    cmdLine[i].errorJump = SQL_JUMP_FALSE;
  }

  for (--dest.top; dest.top >= 0; --dest.top) {
    for (i = 0; i < label.top; ++i) {
      if (strcmp(label.label[i], dest.label[(uint8_t)dest.top]) == 0) break;
    }

    if (i == label.top) {
      sprintf(parseErr, "label:%s not defined", dest.label[(uint8_t)dest.top]);
      return NULL;
    }

    destPos = dest.pos[(uint8_t)dest.top];
    cmdLine[destPos].jump = label.pos[i];
    if (cmdLine[destPos].cmdno == SIM_CMD_SQL) {
      cmdLine[destPos].errorJump = SQL_JUMP_TRUE;
    }
  }

  if (block.top != 0) {
    sprintf(parseErr, "mismatched block");
    return NULL;
  }

  for (i = 0; i < numOfLines; ++i) {
    if (cmdLine[i].jump == 0) cmdLine[i].jump = numOfLines;
  }

  SScript *script = malloc(sizeof(SScript));
  memset(script, 0, sizeof(SScript));

  script->type = SIM_SCRIPT_TYPE_MAIN;
  script->numOfLines = numOfLines;
  tstrncpy(script->fileName, fileName, sizeof(script->fileName));

  script->optionBuffer = malloc(optionOffset);
  memcpy(script->optionBuffer, optionBuffer, optionOffset);

  script->lines = malloc(sizeof(SCmdLine) * numOfLines);
  memcpy(script->lines, cmdLine, sizeof(SCmdLine) * numOfLines);

  return script;
}

SScript *simParseScript(char *fileName) {
  FILE *    fd;
  int32_t   tokenLen, lineNum = 0;
  char      buffer[MAX_LINE_LEN], name[128], *token, *rest;
  SCommand *pCmd;
  SScript * script;

  if ((fileName[0] == '.') || (fileName[0] == '/')) {
    strcpy(name, fileName);
  } else {
    sprintf(name, "%s/%s", tsScriptDir, fileName);
  }

  if ((fd = fopen(name, "r")) == NULL) {
    simError("failed to open file:%s", name);
    return NULL;
  }

  simResetParser();

  while (!feof(fd)) {
    if (fgets(buffer, sizeof(buffer), fd) == NULL) continue;

    lineNum++;
    int32_t cmdlen = (int32_t)strlen(buffer);
    if (buffer[cmdlen - 1] == '\r' || buffer[cmdlen - 1] == '\n') {
      buffer[cmdlen - 1] = 0;
    }
    rest = buffer;

    for (int32_t i = 0; i < cmdlen; ++i) {
      if (buffer[i] == '\r' || buffer[i] == '\n') {
        buffer[i] = ' ';
      }
    }

  again:
    rest = paGetToken(rest, &token, &tokenLen);
    if (tokenLen == 0) continue;

    if (token[tokenLen - 1] == ':') {
      strncpy(label.label[(uint8_t)label.top], token, tokenLen - 1);
      label.pos[(uint8_t)label.top] = numOfLines;
      label.top++;
      goto again;
    }

    if (token[0] == '$') {
      if (!simParseExpression(token, lineNum)) {
        simError("script:%s line:%d %s", fileName, lineNum, parseErr);
        return NULL;
      }
      continue;
    }

    if ((pCmd = simCheckCmd(token, tokenLen)) == NULL) {
      token[tokenLen] = 0;
      simError("script:%s line:%d invalid cmd:%s", fileName, lineNum, token);
      return NULL;
    }

    if (!pCmd->parseCmd(rest, pCmd, lineNum)) {
      simError("script:%s line:%d %s", fileName, lineNum, parseErr);
      return NULL;
    }
  }

  fclose(fd);

  script = simBuildScriptObj(fileName);
  if (script == NULL) simError("script:%s %s", fileName, parseErr);

  return script;
}

int32_t simCheckExpression(char *exp) {
  char *  op1, *op2, *op, *rest;
  int32_t op1Len, op2Len, opLen;

  rest = paGetToken(exp, &op1, &op1Len);
  if (op1Len == 0) {
    sprintf(parseErr, "expression is required");
    return -1;
  }

  rest = paGetToken(rest, &op, &opLen);
  if (opLen == 0) {
    sprintf(parseErr, "operator is missed");
    return -1;
  }

  rest = paGetToken(rest, &op2, &op2Len);
  if (op2Len == 0) {
    sprintf(parseErr, "operand is missed");
    return -1;
  }

  if (opLen == 1) {
    if (op[0] != '=' && op[0] != '<' && op[0] != '>') {
      sprintf(parseErr, "invalid operator:%s", op);
      return -1;
    }

    if (op[0] == '=' && op1[0] != '$') {
      sprintf(parseErr, "left side of assignment must be variable");
      return -1;
    }
  } else if (opLen == 2) {
    if (op[1] != '=' || (op[0] != '=' && op[0] != '<' && op[0] != '>' && op[0] != '!')) {
      sprintf(parseErr, "left side of assignment must be variable");
      return -1;
    }
  } else {
    sprintf(parseErr, "invalid operator:%s", op);
    return -1;
  }

  rest = paGetToken(rest, &op, &opLen);

  if (opLen == 0) return (int32_t)(rest - exp);

  /* if it is key word "then" */
  if (strncmp(op, "then", 4) == 0) return (int32_t)(op - exp);

  rest = paGetToken(rest, &op2, &op2Len);
  if (op2Len == 0) {
    sprintf(parseErr, "operand is missed");
    return -1;
  }

  if (opLen > 1) {
    sprintf(parseErr, "invalid operator:%s", op);
    return -1;
  }

  if (op[0] == '+' || op[0] == '-' || op[0] == '*' || op[0] == '/' || op[0] == '.') {
    return (int32_t)(rest - exp);
  }

  return -1;
}

bool simParseExpression(char *token, int32_t lineNum) {
  int32_t expLen;

  expLen = simCheckExpression(token);
  if (expLen <= 0) return -1;

  cmdLine[numOfLines].cmdno = SIM_CMD_EXP;
  cmdLine[numOfLines].lineNum = lineNum;
  cmdLine[numOfLines].optionOffset = optionOffset;
  memcpy(optionBuffer + optionOffset, token, expLen);
  optionOffset += expLen + 1;
  *(optionBuffer + optionOffset - 1) = 0;

  numOfLines++;
  return true;
}

bool simParseIfCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  char *  ret;
  int32_t expLen;

  expLen = simCheckExpression(rest);

  if (expLen <= 0) return -1;

  ret = rest + expLen;

  if (strncmp(ret, "then", 4) == 0) {
    block.type[(uint8_t)block.top] = BLOCK_IF;
    block.pos[(uint8_t)block.top] = &cmdLine[numOfLines].jump;
    block.top++;
  } else {
    cmdLine[numOfLines].jump = numOfLines + 2;
  }

  cmdLine[numOfLines].optionOffset = optionOffset;
  memcpy(optionBuffer + optionOffset, rest, expLen);
  optionOffset += expLen + 1;
  *(optionBuffer + optionOffset - 1) = 0;
  cmdLine[numOfLines].cmdno = SIM_CMD_TEST;
  cmdLine[numOfLines].lineNum = lineNum;

  numOfLines++;
  return true;
}

bool simParseElifCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  int32_t expLen;

  expLen = simCheckExpression(rest);

  if (expLen <= 0) return -1;

  if (block.top < 1) {
    sprintf(parseErr, "no matching if");
    return false;
  }

  if (block.type[block.top - 1] != BLOCK_IF) {
    sprintf(parseErr, "no matched if block");
    return false;
  }

  cmdLine[numOfLines].cmdno = SIM_CMD_GOTO;
  block.jump[block.top - 1][(uint8_t)block.numJump[block.top - 1]] = &(cmdLine[numOfLines].jump);
  block.numJump[block.top - 1]++;

  numOfLines++;

  *(block.pos[block.top - 1]) = numOfLines;
  block.pos[block.top - 1] = &cmdLine[numOfLines].jump;

  cmdLine[numOfLines].optionOffset = optionOffset;
  memcpy(optionBuffer + optionOffset, rest, expLen);
  optionOffset += expLen + 1;
  *(optionBuffer + optionOffset - 1) = 0;
  cmdLine[numOfLines].cmdno = SIM_CMD_TEST;
  cmdLine[numOfLines].lineNum = lineNum;

  numOfLines++;
  return true;
}

bool simParseElseCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  if (block.top < 1) {
    sprintf(parseErr, "no matching if");
    return false;
  }

  if (block.type[block.top - 1] != BLOCK_IF) {
    sprintf(parseErr, "no matched if block");
    return false;
  }

  cmdLine[numOfLines].cmdno = SIM_CMD_GOTO;
  block.jump[block.top - 1][(uint8_t)block.numJump[block.top - 1]] = &(cmdLine[numOfLines].jump);
  block.numJump[block.top - 1]++;

  numOfLines++;

  *(block.pos[block.top - 1]) = numOfLines;
  block.pos[block.top - 1] = NULL;

  return true;
}

bool simParseEndiCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  int32_t i;

  if (block.top < 1) {
    sprintf(parseErr, "no matching if");
    return false;
  }

  if (block.type[block.top - 1] != BLOCK_IF) {
    sprintf(parseErr, "no matched if block");
    return false;
  }

  if (block.pos[block.top - 1]) *(block.pos[block.top - 1]) = numOfLines;

  for (i = 0; i < block.numJump[block.top - 1]; ++i) {
    *(block.jump[block.top - 1][i]) = numOfLines;
  }

  block.numJump[block.top - 1] = 0;
  block.top--;

  return true;
}

bool simParseWhileCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  int32_t expLen;

  expLen = simCheckExpression(rest);

  if (expLen <= 0) return false;

  block.type[(uint8_t)block.top] = BLOCK_WHILE;
  block.pos[(uint8_t)block.top] = &(cmdLine[numOfLines].jump);
  block.back[(uint8_t)block.top] = numOfLines;
  block.top++;

  cmdLine[numOfLines].optionOffset = optionOffset;
  memcpy(optionBuffer + optionOffset, rest, expLen);
  optionOffset += expLen + 1;
  *(optionBuffer + optionOffset - 1) = 0;
  cmdLine[numOfLines].cmdno = SIM_CMD_TEST;
  cmdLine[numOfLines].lineNum = lineNum;

  numOfLines++;
  return true;
}

bool simParseEndwCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  int32_t i;

  if (block.top < 1) {
    sprintf(parseErr, "no matching while");
    return false;
  }

  if (block.type[block.top - 1] != BLOCK_WHILE) {
    sprintf(parseErr, "no matched while block");
    return false;
  }

  cmdLine[numOfLines].cmdno = SIM_CMD_GOTO;
  cmdLine[numOfLines].jump = block.back[block.top - 1];
  cmdLine[numOfLines].lineNum = lineNum;
  numOfLines++;

  *(block.pos[block.top - 1]) = numOfLines;

  for (i = 0; i < block.numJump[block.top - 1]; ++i) {
    *(block.jump[block.top - 1][i]) = numOfLines;
  }

  block.top--;

  return true;
}

bool simParseSwitchCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  char *  token;
  int32_t tokenLen;

  rest = paGetToken(rest, &token, &tokenLen);
  if (tokenLen == 0) {
    sprintf(parseErr, "switch should be followed by variable");
    return false;
  }

  if (token[0] != '$') {
    sprintf(parseErr, "switch must be followed by variable");
    return false;
  }

  memcpy(block.sexp[(uint8_t)block.top], token, tokenLen);
  block.sexpLen[(uint8_t)block.top] = tokenLen;
  block.type[(uint8_t)block.top] = BLOCK_SWITCH;
  block.top++;

  return true;
}

bool simParseCaseCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  char *  token;
  int32_t tokenLen;

  rest = paGetToken(rest, &token, &tokenLen);
  if (tokenLen == 0) {
    sprintf(parseErr, "case should be followed by value");
    return false;
  }

  if (block.top < 1) {
    sprintf(parseErr, "no matching switch");
    return false;
  }

  if (block.type[block.top - 1] != BLOCK_SWITCH) {
    sprintf(parseErr, "case not matched");
    return false;
  }

  if (block.pos[block.top - 1] != NULL) {
    *(block.pos[block.top - 1]) = numOfLines;
  }

  block.pos[block.top - 1] = &(cmdLine[numOfLines].jump);

  cmdLine[numOfLines].cmdno = SIM_CMD_TEST;
  cmdLine[numOfLines].lineNum = lineNum;
  cmdLine[numOfLines].optionOffset = optionOffset;
  memcpy(optionBuffer + optionOffset, block.sexp[block.top - 1], block.sexpLen[block.top - 1]);
  optionOffset += block.sexpLen[block.top - 1];
  *(optionBuffer + optionOffset++) = ' ';
  *(optionBuffer + optionOffset++) = '=';
  *(optionBuffer + optionOffset++) = '=';
  *(optionBuffer + optionOffset++) = ' ';
  memcpy(optionBuffer + optionOffset, token, tokenLen);
  optionOffset += tokenLen + 1;
  *(optionBuffer + optionOffset - 1) = 0;

  numOfLines++;
  return true;
}

bool simParseBreakCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  if (block.top < 1) {
    sprintf(parseErr, "no blcok exists");
    return false;
  }

  if (block.type[block.top - 1] != BLOCK_SWITCH && block.type[block.top - 1] != BLOCK_WHILE) {
    sprintf(parseErr, "not in switch or while block");
    return false;
  }

  block.jump[block.top - 1][(uint8_t)block.numJump[block.top - 1]] = &(cmdLine[numOfLines].jump);
  block.numJump[block.top - 1]++;

  cmdLine[numOfLines].cmdno = SIM_CMD_GOTO;
  cmdLine[numOfLines].lineNum = lineNum;

  numOfLines++;
  return true;
}

bool simParseDefaultCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  if (block.top < 1) {
    sprintf(parseErr, "no matching switch");
    return false;
  }

  if (block.type[block.top - 1] != BLOCK_SWITCH) {
    sprintf(parseErr, "default should be matched with switch");
    return false;
  }

  if (block.pos[block.top - 1] != NULL) {
    *(block.pos[block.top - 1]) = numOfLines;
  }

  return true;
}

bool simParseEndsCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  int32_t i;

  if (block.top < 1) {
    sprintf(parseErr, "no matching switch");
    return false;
  }

  if (block.type[block.top - 1] != BLOCK_SWITCH) {
    sprintf(parseErr, "ends should be matched with switch");
    return false;
  }

  for (i = 0; i < block.numJump[block.top - 1]; ++i) {
    *(block.jump[block.top - 1][i]) = numOfLines;
  }

  block.numJump[block.top - 1] = 0;
  block.top--;

  return true;
}

bool simParseContinueCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  if (block.top < 1) {
    sprintf(parseErr, "no matching while");
    return false;
  }

  if (block.type[block.top - 1] != BLOCK_WHILE) {
    sprintf(parseErr, "continue should be matched with while cmd");
    return false;
  }

  cmdLine[numOfLines].cmdno = SIM_CMD_GOTO;
  cmdLine[numOfLines].lineNum = lineNum;
  cmdLine[numOfLines].jump = block.back[block.top - 1];

  numOfLines++;
  return true;
}

bool simParsePrintCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  int32_t expLen;

  rest++;
  cmdLine[numOfLines].cmdno = SIM_CMD_PRINT;
  cmdLine[numOfLines].lineNum = lineNum;
  cmdLine[numOfLines].optionOffset = optionOffset;
  expLen = (int32_t)strlen(rest);
  memcpy(optionBuffer + optionOffset, rest, expLen);
  optionOffset += expLen + 1;
  *(optionBuffer + optionOffset - 1) = 0;

  numOfLines++;
  return true;
}

void simCheckSqlOption(char *rest) {
  int32_t valueLen;
  char *  value, *xpos;

  xpos = strstr(rest, " -x");  // need a blank
  if (xpos) {
    paGetToken(xpos + 3, &value, &valueLen);
    if (valueLen != 0) {
      memcpy(dest.label[(uint8_t)dest.top], value, valueLen);
      dest.label[(uint8_t)dest.top][valueLen] = 0;
      dest.pos[(uint8_t)dest.top] = numOfLines;
      dest.top++;

      *xpos = 0;
    }
  }
}

bool simParseSqlCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  int32_t expLen;

  rest++;
  simCheckSqlOption(rest);
  cmdLine[numOfLines].cmdno = SIM_CMD_SQL;
  cmdLine[numOfLines].lineNum = lineNum;
  cmdLine[numOfLines].optionOffset = optionOffset;
  expLen = (int32_t)strlen(rest);
  memcpy(optionBuffer + optionOffset, rest, expLen);
  optionOffset += expLen + 1;
  *(optionBuffer + optionOffset - 1) = 0;

  numOfLines++;
  return true;
}

bool simParseSqlErrorCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  int32_t expLen;

  rest++;
  cmdLine[numOfLines].cmdno = SIM_CMD_SQL_ERROR;
  cmdLine[numOfLines].lineNum = lineNum;
  cmdLine[numOfLines].optionOffset = optionOffset;
  expLen = (int32_t)strlen(rest);
  memcpy(optionBuffer + optionOffset, rest, expLen);
  optionOffset += expLen + 1;
  *(optionBuffer + optionOffset - 1) = 0;

  numOfLines++;
  return true;
}

bool simParseSqlSlowCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  simParseSqlCmd(rest, pCmd, lineNum);
  cmdLine[numOfLines - 1].cmdno = SIM_CMD_SQL_SLOW;
  return true;
}

bool simParseRestfulCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  simParseSqlCmd(rest, pCmd, lineNum);
  cmdLine[numOfLines - 1].cmdno = SIM_CMD_RESTFUL;
  return true;
}

bool simParseSystemCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  int32_t expLen;

  rest++;
  cmdLine[numOfLines].cmdno = SIM_CMD_SYSTEM;
  cmdLine[numOfLines].lineNum = lineNum;
  cmdLine[numOfLines].optionOffset = optionOffset;
  expLen = (int32_t)strlen(rest);
  memcpy(optionBuffer + optionOffset, rest, expLen);
  optionOffset += expLen + 1;
  *(optionBuffer + optionOffset - 1) = 0;

  numOfLines++;
  return true;
}

bool simParseSystemContentCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  simParseSystemCmd(rest, pCmd, lineNum);
  cmdLine[numOfLines - 1].cmdno = SIM_CMD_SYSTEM_CONTENT;
  return true;
}

bool simParseSleepCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  char *  token;
  int32_t tokenLen;

  cmdLine[numOfLines].cmdno = SIM_CMD_SLEEP;
  cmdLine[numOfLines].lineNum = lineNum;

  paGetToken(rest, &token, &tokenLen);
  if (tokenLen > 0) {
    cmdLine[numOfLines].optionOffset = optionOffset;
    memcpy(optionBuffer + optionOffset, token, tokenLen);
    optionOffset += tokenLen + 1;
    *(optionBuffer + optionOffset - 1) = 0;
  }

  numOfLines++;
  return true;
}

bool simParseReturnCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  char *  token;
  int32_t tokenLen;

  cmdLine[numOfLines].cmdno = SIM_CMD_RETURN;
  cmdLine[numOfLines].lineNum = lineNum;

  paGetToken(rest, &token, &tokenLen);
  if (tokenLen > 0) {
    cmdLine[numOfLines].optionOffset = optionOffset;
    memcpy(optionBuffer + optionOffset, token, tokenLen);
    optionOffset += tokenLen + 1;
    *(optionBuffer + optionOffset - 1) = 0;
  }

  numOfLines++;
  return true;
}

bool simParseGotoCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  char *  token;
  int32_t tokenLen;

  rest = paGetToken(rest, &token, &tokenLen);

  if (tokenLen == 0) {
    sprintf(parseErr, "label should be followed by goto cmd");
    return false;
  }

  memcpy(dest.label[(uint8_t)dest.top], token, tokenLen);
  dest.label[(uint8_t)dest.top][tokenLen] = 0;
  dest.pos[(uint8_t)dest.top] = numOfLines;
  dest.top++;

  cmdLine[numOfLines].cmdno = SIM_CMD_GOTO;
  cmdLine[numOfLines].lineNum = lineNum;

  numOfLines++;
  return true;
}

bool simParseRunCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  char *  token;
  int32_t tokenLen;

  rest = paGetToken(rest, &token, &tokenLen);

  if (tokenLen == 0) {
    sprintf(parseErr, "file name should be followed by run cmd");
    return false;
  }

  cmdLine[numOfLines].cmdno = SIM_CMD_RUN;
  cmdLine[numOfLines].lineNum = lineNum;
  cmdLine[numOfLines].optionOffset = optionOffset;
  memcpy(optionBuffer + optionOffset, token, tokenLen);
  optionOffset += tokenLen + 1;
  *(optionBuffer + optionOffset - 1) = 0;

  numOfLines++;
  return true;
}

bool simParseRunBackCmd(char *rest, SCommand *pCmd, int32_t lineNum) {
  simParseRunCmd(rest, pCmd, lineNum);
  cmdLine[numOfLines - 1].cmdno = SIM_CMD_RUN_BACK;
  return true;
}

void simInitsimCmdList() {
  int32_t cmdno;
  memset(simCmdList, 0, SIM_CMD_END * sizeof(SCommand));

  /* internal command */
  cmdno = SIM_CMD_EXP;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "exp");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = NULL;
  simCmdList[cmdno].executeCmd = simExecuteExpCmd;

  cmdno = SIM_CMD_IF;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "if");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseIfCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_ELIF;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "elif");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseElifCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_ELSE;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "else");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseElseCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_ENDI;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "endi");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseEndiCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_WHILE;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "while");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseWhileCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_ENDW;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "endw");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseEndwCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_SWITCH;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "switch");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseSwitchCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_CASE;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "case");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseCaseCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_DEFAULT;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "default");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseDefaultCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_BREAK;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "break");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseBreakCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_CONTINUE;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "continue");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseContinueCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_ENDS;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "ends");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseEndsCmd;
  simCmdList[cmdno].executeCmd = NULL;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_SLEEP;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "sleep");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseSleepCmd;
  simCmdList[cmdno].executeCmd = simExecuteSleepCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_GOTO;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "goto");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseGotoCmd;
  simCmdList[cmdno].executeCmd = simExecuteGotoCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_RUN;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "run");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseRunCmd;
  simCmdList[cmdno].executeCmd = simExecuteRunCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_RUN_BACK;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "run_back");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseRunBackCmd;
  simCmdList[cmdno].executeCmd = simExecuteRunBackCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_SYSTEM;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "system");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseSystemCmd;
  simCmdList[cmdno].executeCmd = simExecuteSystemCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_SYSTEM_CONTENT;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "system_content");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseSystemContentCmd;
  simCmdList[cmdno].executeCmd = simExecuteSystemContentCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_PRINT;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "print");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParsePrintCmd;
  simCmdList[cmdno].executeCmd = simExecutePrintCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_SQL;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "sql");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseSqlCmd;
  simCmdList[cmdno].executeCmd = simExecuteSqlCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_SQL_ERROR;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "sql_error");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseSqlErrorCmd;
  simCmdList[cmdno].executeCmd = simExecuteSqlErrorCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_SQL_SLOW;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "sql_slow");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseSqlSlowCmd;
  simCmdList[cmdno].executeCmd = simExecuteSqlSlowCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  cmdno = SIM_CMD_RESTFUL;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "restful");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseRestfulCmd;
  simCmdList[cmdno].executeCmd = simExecuteRestfulCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));

  /* test is only an internal command */
  cmdno = SIM_CMD_TEST;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "test");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = NULL;
  simCmdList[cmdno].executeCmd = simExecuteTestCmd;

  cmdno = SIM_CMD_RETURN;
  simCmdList[cmdno].cmdno = cmdno;
  strcpy(simCmdList[cmdno].name, "return");
  simCmdList[cmdno].nlen = (int16_t)strlen(simCmdList[cmdno].name);
  simCmdList[cmdno].parseCmd = simParseReturnCmd;
  simCmdList[cmdno].executeCmd = simExecuteReturnCmd;
  simAddCmdIntoHash(&(simCmdList[cmdno]));
}
