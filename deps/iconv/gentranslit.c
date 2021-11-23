/* Copyright (C) 1999-2003, 2005 Free Software Foundation, Inc.
   This file is part of the GNU LIBICONV Library.

   The GNU LIBICONV Library is free software; you can redistribute it
   and/or modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either version 2
   of the License, or (at your option) any later version.

   The GNU LIBICONV Library is distributed in the hope that it will be
   useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.

   You should have received a copy of the GNU Library General Public
   License along with the GNU LIBICONV Library; see the file COPYING.LIB.
   If not, write to the Free Software Foundation, Inc., 51 Franklin Street,
   Fifth Floor, Boston, MA 02110-1301, USA.  */

/*
 * Generates a table of small strings, used for transliteration, from a table
 * containing lines of the form
 *   Unicode <tab> utf-8 replacement <tab> # comment
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

int main (int argc, char *argv[])
{
  unsigned int data[0x100000];
  int uni2index[0x110000];
  int deps_index;

  if (argc != 1)
    exit(1);

  printf("/*\n");
  printf(" * Copyright (C) 1999-2003 Free Software Foundation, Inc.\n");
  printf(" * This file is part of the GNU LIBICONV Library.\n");
  printf(" *\n");
  printf(" * The GNU LIBICONV Library is free software; you can redistribute it\n");
  printf(" * and/or modify it under the terms of the GNU Library General Public\n");
  printf(" * License as published by the Free Software Foundation; either version 2\n");
  printf(" * of the License, or (at your option) any later version.\n");
  printf(" *\n");
  printf(" * The GNU LIBICONV Library is distributed in the hope that it will be\n");
  printf(" * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of\n");
  printf(" * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU\n");
  printf(" * Library General Public License for more details.\n");
  printf(" *\n");
  printf(" * You should have received a copy of the GNU Library General Public\n");
  printf(" * License along with the GNU LIBICONV Library; see the file COPYING.LIB.\n");
  printf(" * If not, write to the Free Software Foundation, Inc., 51 Franklin Street,\n");
  printf(" * Fifth Floor, Boston, MA 02110-1301, USA.\n");
  printf(" */\n");
  printf("\n");
  printf("/*\n");
  printf(" * Transliteration table\n");
  printf(" */\n");
  printf("\n");
  {
    int c;
    int j;
    for (j = 0; j < 0x110000; j++)
      uni2index[j] = -1;
    deps_index = 0;
    for (;;) {
      c = getc(stdin);
      if (c == EOF)
        break;
      if (c == '#') {
        do { c = getc(stdin); } while (!(c == EOF || c == '\n'));
        continue;
      }
      ungetc(c,stdin);
      if (scanf("%x",&j) != 1)
        exit(1);
      c = getc(stdin);
      if (c != '\t')
        exit(1);
      for (;;) {
        c = getc(stdin);
        if (c == EOF || c == '\n')
          exit(1);
        if (c == '\t')
          break;
        if (uni2index[j] < 0) {
          uni2index[j] = deps_index;
          data[deps_index++] = 0;
        }
        if (c >= 0x80) {
          /* Finish reading an UTF-8 character. */
          if (c < 0xc0)
            exit(1);
          else {
            unsigned int i = (c < 0xe0 ? 2 : c < 0xf0 ? 3 : c < 0xf8 ? 4 : c < 0xfc ? 5 : 6);
            c &= (1 << (8-i)) - 1;
            while (--i > 0) {
              int cc = getc(stdin);
              if (!(cc >= 0x80 && cc < 0xc0))
                exit(1);
              c <<= 6; c |= (cc & 0x3f);
            }
          }
        }
        data[deps_index++] = (unsigned int) c;
      }
      if (uni2index[j] >= 0)
        data[uni2index[j]] = deps_index - uni2index[j] - 1;
      do { c = getc(stdin); } while (!(c == EOF || c == '\n'));
    }
  }
  printf("static const unsigned int translit_data[%d] = {",deps_index);
  {
    int i;
    for (i = 0; i < deps_index; i++) {
      if (data[i] < 32)
        printf("\n %3d,",data[i]);
      else if (data[i] == '\'')
        printf("'\\'',");
      else if (data[i] == '\\')
        printf("'\\\\',");
      else if (data[i] < 127)
        printf(" '%c',",data[i]);
      else if (data[i] < 256)
        printf("0x%02X,",data[i]);
      else
        printf("0x%04X,",data[i]);
    }
    printf("\n};\n");
  }
  printf("\n");
  {
    bool pages[0x1100];
    int line[0x22000];
    int tableno;
    struct { int minline; int maxline; int usecount; const char* suffix; } tables[0x2000];
    int i, j, p, j1, j2, t;

    for (p = 0; p < 0x1100; p++)
      pages[p] = false;
    for (j = 0; j < 0x110000; j++)
      if (uni2index[j] >= 0)
        pages[j>>8] = true;
    for (j1 = 0; j1 < 0x22000; j1++) {
      bool all_invalid = true;
      for (j2 = 0; j2 < 8; j2++) {
        j = 8*j1+j2;
        if (uni2index[j] >= 0)
          all_invalid = false;
      }
      if (all_invalid)
        line[j1] = -1;
      else
        line[j1] = 0;
    }
    tableno = 0;
    for (j1 = 0; j1 < 0x22000; j1++) {
      if (line[j1] >= 0) {
        if (tableno > 0
            && ((j1 > 0 && line[j1-1] == tableno-1)
                || ((tables[tableno-1].maxline >> 5) == (j1 >> 5)
                    && j1 - tables[tableno-1].maxline <= 8))) {
          line[j1] = tableno-1;
          tables[tableno-1].maxline = j1;
        } else {
          tableno++;
          line[j1] = tableno-1;
          tables[tableno-1].minline = tables[tableno-1].maxline = j1;
        }
      }
    }
    for (t = 0; t < tableno; t++) {
      tables[t].usecount = 0;
      j1 = 8*tables[t].minline;
      j2 = 8*(tables[t].maxline+1);
      for (j = j1; j < j2; j++)
        if (uni2index[j] >= 0)
          tables[t].usecount++;
    }
    for (t = 0, p = -1, i = 0; t < tableno; t++) {
      if (tables[t].usecount > 1) {
        char* s;
        if (p == tables[t].minline >> 5) {
          s = (char*) malloc(5+1);
          sprintf(s, "%02x_%d", p, ++i);
        } else {
          p = tables[t].minline >> 5;
          s = (char*) malloc(2+1);
          sprintf(s, "%02x", p);
        }
        tables[t].suffix = s;
      } else
        tables[t].suffix = NULL;
    }
    {
      p = -1;
      for (t = 0; t < tableno; t++)
        if (tables[t].usecount > 1) {
          p = 0;
          printf("static const short translit_page%s[%d] = {\n", tables[t].suffix, 8*(tables[t].maxline-tables[t].minline+1));
          for (j1 = tables[t].minline; j1 <= tables[t].maxline; j1++) {
            if ((j1 % 0x20) == 0 && j1 > tables[t].minline)
              printf("  /* 0x%04x */\n", 8*j1);
            printf(" ");
            for (j2 = 0; j2 < 8; j2++) {
              j = 8*j1+j2;
              printf(" %4d,", uni2index[j]);
            }
            printf(" /* 0x%02x-0x%02x */\n", 8*(j1 % 0x20), 8*(j1 % 0x20)+7);
          }
          printf("};\n");
        }
      if (p >= 0)
        printf("\n");
    }
    printf("#define translit_index(wc) \\\n  (");
    for (j1 = 0; j1 < 0x22000;) {
      t = line[j1];
      for (j2 = j1; j2 < 0x22000 && line[j2] == t; j2++);
      if (t >= 0) {
        if (j1 != tables[t].minline) abort();
        if (j2 > tables[t].maxline+1) abort();
        j2 = tables[t].maxline+1;
      }
      if (t == -1) {
      } else {
        if (t >= 0 && tables[t].usecount == 0) abort();
        if (t >= 0 && tables[t].usecount == 1) {
          if (j2 != j1+1) abort();
          for (j = 8*j1; j < 8*j2; j++)
            if (uni2index[j] >= 0) {
              printf("wc == 0x%04x ? %d", j, uni2index[j]);
              break;
            }
        } else {
          if (j1 == 0) {
            printf("wc < 0x%04x", 8*j2);
          } else {
            printf("wc >= 0x%04x && wc < 0x%04x", 8*j1, 8*j2);
          }
          printf(" ? translit_page%s[wc", tables[t].suffix);
          if (tables[t].minline > 0)
            printf("-0x%04x", 8*j1);
          printf("]");
        }
        printf(" : \\\n   ");
      }
      j1 = j2;
    }
    printf("-1)\n");
  }

  if (ferror(stdout) || fclose(stdout))
    exit(1);
  exit(0);
}
