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

#include "boost.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <pthread.h>

int boost_progress_init(BoostProgress *prog, const char *file) {
  atomic_store(&prog->tables_done, 0);
  atomic_store(&prog->tables_total, 0);
  atomic_store(&prog->rows_transferred, 0);
  atomic_store(&prog->bytes_transferred, 0);
  atomic_store(&prog->errors, 0);

  prog->start_time = time(NULL);

  snprintf(prog->progress_file, sizeof(prog->progress_file), "%s", file);

  pthread_mutex_init(&prog->file_lock, NULL);

  return 0;
}

void boost_progress_destroy(BoostProgress *prog) {
  pthread_mutex_destroy(&prog->file_lock);
}

int boost_progress_save(BoostProgress *prog, BoostWorkQueue *queue) {
  pthread_mutex_lock(&prog->file_lock);

  FILE *fp = fopen(prog->progress_file, "w");
  if (fp == NULL) {
    pthread_mutex_unlock(&prog->file_lock);
    return -1;
  }

  long long done   = atomic_load(&prog->tables_done);
  long long total  = atomic_load(&prog->tables_total);
  long long rows   = atomic_load(&prog->rows_transferred);
  long long bytes  = atomic_load(&prog->bytes_transferred);
  long long errors = atomic_load(&prog->errors);

  fprintf(fp, "{\n");
  fprintf(fp, "  \"tables_done\": %lld,\n", done);
  fprintf(fp, "  \"tables_total\": %lld,\n", total);
  fprintf(fp, "  \"rows_transferred\": %lld,\n", rows);
  fprintf(fp, "  \"bytes_transferred\": %lld,\n", bytes);
  fprintf(fp, "  \"errors\": %lld,\n", errors);
  fprintf(fp, "  \"completed\": [\n");

  pthread_mutex_lock(&queue->lock);

  int first = 1;
  for (int i = 0; i < queue->total; i++) {
    if (queue->tables[i].is_synced) {
      if (!first) {
        fprintf(fp, ",\n");
      }
      fprintf(fp, "    \"%s.%s.%s\"",
              queue->tables[i].db_name,
              queue->tables[i].stable_name,
              queue->tables[i].table_name);
      first = 0;
    }
  }

  pthread_mutex_unlock(&queue->lock);

  fprintf(fp, "\n  ]\n");
  fprintf(fp, "}\n");

  fclose(fp);
  pthread_mutex_unlock(&prog->file_lock);

  return 0;
}

int boost_progress_load(BoostProgress *prog, BoostWorkQueue *queue) {
  FILE *fp = fopen(prog->progress_file, "r");
  if (fp == NULL) {
    return -1;
  }

  char line[1024];
  long long val;
  int completed_count = 0;

  while (fgets(line, sizeof(line), fp) != NULL) {
    if (strstr(line, "\"tables_done\"") != NULL) {
      if (sscanf(line, " \"tables_done\": %lld", &val) == 1) {
        atomic_store(&prog->tables_done, val);
      }
    } else if (strstr(line, "\"tables_total\"") != NULL) {
      if (sscanf(line, " \"tables_total\": %lld", &val) == 1) {
        atomic_store(&prog->tables_total, val);
      }
    } else if (strstr(line, "\"rows_transferred\"") != NULL) {
      if (sscanf(line, " \"rows_transferred\": %lld", &val) == 1) {
        atomic_store(&prog->rows_transferred, val);
      }
    } else if (strstr(line, "\"bytes_transferred\"") != NULL) {
      if (sscanf(line, " \"bytes_transferred\": %lld", &val) == 1) {
        atomic_store(&prog->bytes_transferred, val);
      }
    } else if (strstr(line, "\"errors\"") != NULL && strstr(line, "\"completed\"") == NULL) {
      if (sscanf(line, " \"errors\": %lld", &val) == 1) {
        atomic_store(&prog->errors, val);
      }
    } else {
      /* check for completed table entries: "db.stable.table" */
      char *quote_start = strchr(line, '"');
      if (quote_start != NULL) {
        quote_start++;
        char *quote_end = strchr(quote_start, '"');
        if (quote_end != NULL && quote_end > quote_start) {
          size_t len = (size_t)(quote_end - quote_start);
          char entry[1024];
          if (len < sizeof(entry)) {
            memcpy(entry, quote_start, len);
            entry[len] = '\0';

            /* skip JSON structural keys */
            if (strcmp(entry, "completed") == 0) {
              continue;
            }

            /* parse "db.stable.table" */
            char db[BOOST_MAX_DB_NAME];
            char stable[BOOST_MAX_TB_NAME];
            char table[BOOST_MAX_TB_NAME];

            char *dot1 = strchr(entry, '.');
            if (dot1 == NULL) continue;
            char *dot2 = strchr(dot1 + 1, '.');
            if (dot2 == NULL) continue;

            size_t db_len = (size_t)(dot1 - entry);
            size_t stable_len = (size_t)(dot2 - dot1 - 1);
            size_t table_len = strlen(dot2 + 1);

            if (db_len >= BOOST_MAX_DB_NAME || stable_len >= BOOST_MAX_TB_NAME ||
                table_len >= BOOST_MAX_TB_NAME) {
              continue;
            }

            memcpy(db, entry, db_len);
            db[db_len] = '\0';
            memcpy(stable, dot1 + 1, stable_len);
            stable[stable_len] = '\0';
            memcpy(table, dot2 + 1, table_len);
            table[table_len] = '\0';

            /* mark matching entries in queue */
            pthread_mutex_lock(&queue->lock);
            for (int i = 0; i < queue->total; i++) {
              if (strcmp(queue->tables[i].db_name, db) == 0 &&
                  strcmp(queue->tables[i].stable_name, stable) == 0 &&
                  strcmp(queue->tables[i].table_name, table) == 0) {
                queue->tables[i].is_synced = true;
                completed_count++;
                break;
              }
            }
            pthread_mutex_unlock(&queue->lock);
          }
        }
      }
    }
  }

  fclose(fp);
  return 0;
}

void boost_progress_print(BoostProgress *prog) {
  long long done   = atomic_load(&prog->tables_done);
  long long total  = atomic_load(&prog->tables_total);
  long long rows   = atomic_load(&prog->rows_transferred);
  long long bytes  = atomic_load(&prog->bytes_transferred);
  long long errors = atomic_load(&prog->errors);

  time_t now = time(NULL);
  double elapsed = difftime(now, prog->start_time);

  double pct = (total > 0) ? ((double)done / (double)total * 100.0) : 0.0;
  double mb  = (double)bytes / (1024.0 * 1024.0);

  double rows_per_sec = (elapsed > 0) ? ((double)rows / elapsed) : 0.0;
  double mb_per_sec   = (elapsed > 0) ? (mb / elapsed) : 0.0;

  int elapsed_int = (int)elapsed;
  int hours   = elapsed_int / 3600;
  int minutes = (elapsed_int % 3600) / 60;
  int seconds = elapsed_int % 60;

  BOOST_LOG("[Progress] %lld/%lld tables (%.1f%%) | %lld rows | %.1f MB | "
            "%.0f rows/s | %.1f MB/s | elapsed: %02d:%02d:%02d | errors: %lld",
            done, total, pct, rows, mb,
            rows_per_sec, mb_per_sec,
            hours, minutes, seconds, errors);
}
