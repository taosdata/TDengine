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

#ifndef __SIM_PARSE_H__
#define __SIM_PARSE_H__

#define MAX_NUM_CMD 64
#define MAX_NUM_LABLES 100
#define MAX_LABEL_LEN 40
#define MAX_NUM_BLOCK 100
#define MAX_NUM_JUMP 100
#define MAX_LINE_LEN 3000
#define MAX_CMD_LINES 2048
#define MAX_OPTION_BUFFER 64000

enum {
  BLOCK_IF,
  BLOCK_WHILE,
  BLOCK_SWITCH,
};

/* label stack */
typedef struct {
  char    top;                                  /* number of labels */
  int16_t pos[MAX_NUM_LABLES];                  /* the position of the label */
  char    label[MAX_NUM_LABLES][MAX_LABEL_LEN]; /* name of the label */
} SLabel;

/* block definition */
typedef struct {
  char     top;                  /* the number of blocks stacked */
  char     type[MAX_NUM_BLOCK];  /* the block type */
  int16_t *pos[MAX_NUM_BLOCK];   /* position of the jump for if/elif/case */
  int16_t  back[MAX_NUM_BLOCK];  /* go back, endw and continue */
  char     numJump[MAX_NUM_BLOCK];
  int16_t *jump[MAX_NUM_BLOCK][MAX_NUM_JUMP]; /* break or elif */
  char     sexp[MAX_NUM_BLOCK][40];           /*switch expression */
  char     sexpLen[MAX_NUM_BLOCK];            /*switch expression length */
} SBlock;

bool simParseExpression(char *token, int32_t lineNum);

#endif