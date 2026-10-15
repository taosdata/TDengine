/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

static volatile sig_atomic_t stop_requested = 0;
static int last_lost_id = -1;
static int last_lost_active = 0;
static int lost_delete_count = 0;
static int force_delete_count = 0;
static int cancel_fail_pending = 0;

static void request_stop(int signal_number) {
  (void)signal_number;
  stop_requested = 1;
}

static int read_request(int client, char *method, char *path, int *owner_conflict, int *response_lost,
                        int *xnode_id) {
  char request[16384];
  size_t received = 0;
  char *headers_end = NULL;

  while (received < sizeof(request) - 1) {
    ssize_t count = recv(client, request + received, sizeof(request) - 1 - received, 0);
    if (count <= 0) return -1;
    received += (size_t)count;
    request[received] = '\0';
    headers_end = strstr(request, "\r\n\r\n");
    if (headers_end != NULL) break;
  }
  if (headers_end == NULL || sscanf(request, "%15s %255s", method, path) != 2) return -1;

  size_t content_length = 0;
  char *header = strstr(request, "\r\n");
  while (header != NULL && header < headers_end) {
    header += 2;
    if (strncasecmp(header, "Content-Length:", 15) == 0) {
      content_length = strtoul(header + 15, NULL, 10);
      break;
    }
    header = strstr(header, "\r\n");
  }

  size_t body_offset = (size_t)(headers_end + 4 - request);
  size_t body_received = received - body_offset;
  while (body_received < content_length) {
    char overflow[4096];
    ssize_t count = received < sizeof(request) - 1
                        ? recv(client, request + received, sizeof(request) - 1 - received, 0)
                        : recv(client, overflow, sizeof(overflow), 0);
    if (count <= 0) return -1;
    if (received < sizeof(request) - 1) {
      received += (size_t)count;
      request[received] = '\0';
    }
    body_received += (size_t)count;
  }
  *owner_conflict = strcmp(method, "POST") == 0 && strcmp(path, "/xnode") == 0 &&
                    strstr(headers_end + 4, "mock-owner-conflict") != NULL;
  *response_lost = strcmp(method, "POST") == 0 && strcmp(path, "/xnode") == 0 &&
                   (strstr(headers_end + 4, "mock-response-lost") != NULL ||
                    strstr(headers_end + 4, "mock-cancel-unavailable") != NULL);
  *xnode_id = -1;
  if (*response_lost) {
    cancel_fail_pending = strstr(headers_end + 4, "mock-cancel-unavailable") != NULL;
    char *id_field = strstr(headers_end + 4, "\"id\"");
    if (id_field != NULL && (id_field = strchr(id_field, ':')) != NULL) {
      *xnode_id = (int)strtol(id_field + 1, NULL, 10);
    }
  }
  return 0;
}

static void serve_client(int client, int syntax_mode) {
  char method[16];
  char path[256];
  int owner_conflict = 0;
  int response_lost = 0;
  int xnode_id = -1;
  if (read_request(client, method, path, &owner_conflict, &response_lost, &xnode_id) != 0) return;

  if (response_lost) {
    last_lost_id = xnode_id;
    last_lost_active = 1;
    return;
  }
  if (strcmp(method, "DELETE") == 0 && strstr(path, "?force=true") != NULL) {
    force_delete_count++;
  }
  if (strcmp(method, "DELETE") == 0 && strstr(path, "?force=true") != NULL &&
      sscanf(path, "/xnode/%d", &xnode_id) == 1 && xnode_id == last_lost_id) {
    if (cancel_fail_pending) {
      cancel_fail_pending = 0;
      return;
    }
    last_lost_active = 0;
    lost_delete_count++;
  }

  if (syntax_mode && ((strcmp(method, "POST") == 0 &&
                       (strncmp(path, "/xnode/drain/", 13) == 0 || strcmp(path, "/task/check") == 0)) ||
                      (strcmp(method, "DELETE") == 0 && strncmp(path, "/xnode/", 7) == 0 &&
                       strstr(path, "force=true") == NULL))) {
    return;
  }

  const char *body = owner_conflict ? "Xnode is already owned by another cluster" : "{}";
  char state_body[128];
  if (strcmp(method, "GET") == 0 && strcmp(path, "/mock/state") == 0) {
    snprintf(state_body, sizeof(state_body),
             "{\"active\":%s,\"delete_count\":%d,\"force_delete_count\":%d}",
             last_lost_active ? "true" : "false", lost_delete_count, force_delete_count);
    body = state_body;
  }
  if (strcmp(method, "GET") == 0 && strncmp(path, "/xnode/", 7) == 0) {
    body = syntax_mode ? "{\"status\":\"offline\"}" : "{\"status\":\"online\"}";
  }
  char response[256];
  int response_length = snprintf(response, sizeof(response),
                                 "HTTP/1.1 %s\r\nContent-Type: application/json\r\n"
                                 "Content-Length: %zu\r\nConnection: close\r\n\r\n%s",
                                 owner_conflict ? "409 Conflict" : "200 OK", strlen(body), body);
  if (response_length > 0 && (size_t)response_length < sizeof(response)) {
    size_t sent = 0;
    while (sent < (size_t)response_length) {
      ssize_t count = send(client, response + sent, (size_t)response_length - sent, 0);
      if (count <= 0) break;
      sent += (size_t)count;
    }
  }
}

int main(void) {
  const char *socket_path = getenv("XNODED_LISTEN");
  const char *mode = getenv("XNODED_TEST_MOCK_MODE");
  if (socket_path == NULL || mode == NULL || (strcmp(mode, "syntax") != 0 && strcmp(mode, "priv") != 0)) {
    fprintf(stderr, "mock xnoded requires XNODED_LISTEN and XNODED_TEST_MOCK_MODE\n");
    return 1;
  }

  struct sockaddr_un address = {.sun_family = AF_UNIX};
  if (strlen(socket_path) >= sizeof(address.sun_path)) {
    fprintf(stderr, "mock xnoded socket path is too long: %s\n", socket_path);
    return 1;
  }
  strcpy(address.sun_path, socket_path);

  signal(SIGPIPE, SIG_IGN);
  signal(SIGTERM, request_stop);
  signal(SIGINT, request_stop);

  int listener = socket(AF_UNIX, SOCK_STREAM, 0);
  if (listener < 0 || bind(listener, (struct sockaddr *)&address, sizeof(address)) != 0 || listen(listener, 16) != 0) {
    fprintf(stderr, "mock xnoded cannot listen on %s: %s\n", socket_path, strerror(errno));
    if (listener >= 0) close(listener);
    return 1;
  }
  fprintf(stderr, "mock xnoded listening on %s (mode=%s)\n", socket_path, mode);

  while (!stop_requested) {
    struct pollfd descriptor = {.fd = listener, .events = POLLIN};
    int ready = poll(&descriptor, 1, 1000);
    if (ready < 0) {
      if (errno == EINTR) continue;
      break;
    }
    if (descriptor.revents & POLLIN) {
      int client = accept(listener, NULL, NULL);
      if (client >= 0) {
        serve_client(client, strcmp(mode, "syntax") == 0);
        close(client);
      }
    }
  }

  close(listener);
  return 0;
}
