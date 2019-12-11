/*******************************************************************
 *           Copyright (c) 2001 by TAOS Networks, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Networks, Inc.
 *  No part of this file may be reproduced, stored, transmitted,
 *  disclosed or used in any form or by any means other than as
 *  expressly provided by the written permission from Jianhui Tao
 *
 * ****************************************************************/

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
  uint8_t top;                                  /* number of labels */
  short pos[MAX_NUM_LABLES];                 /* the position of the label */
  char label[MAX_NUM_LABLES][MAX_LABEL_LEN]; /* name of the label */
} SLabel;

/* block definition */
typedef struct {
  uint8_t top;                  /* the number of blocks stacked */
  char type[MAX_NUM_BLOCK];  /* the block type */
  short *pos[MAX_NUM_BLOCK]; /* position of the jump for if/elif/case */
  short back[MAX_NUM_BLOCK]; /* go back, endw and continue */
  uint8_t numJump[MAX_NUM_BLOCK];
  short *jump[MAX_NUM_BLOCK][MAX_NUM_JUMP]; /* break or elif */
  char sexp[MAX_NUM_BLOCK][40];             /*switch expression */
  char sexpLen[MAX_NUM_BLOCK];              /*switch expression length */
} SBlock;

bool simParseExpression(char *token, int lineNum);

#endif