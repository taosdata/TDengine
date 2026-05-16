/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "url_parser.h"

#include "../core/internal.h"        // FIXME:
#include "log.h"
#include "parser.h"

#include <ctype.h>

typedef struct url_str_s               url_str_t;
typedef struct url_strs_s              url_strs_t;

struct url_str_s {
  char                   *str;
  size_t                  cap;
  size_t                  nr;
};

struct url_strs_s {
  char                   **strs;
  size_t                   cap;
  size_t                   nr;
};

static void url_str_release(url_str_t *str)
{
  if (!str) return;
  TOD_SAFE_FREE(str->str);
  str->cap = 0;
  str->nr  = 0;
}

static int url_str_keep(url_str_t *str, size_t cap)
{
  if (cap <= str->cap) return 0;
  cap = (cap + 255) / 256 * 256;
  char *s = (char*)realloc(str->str, cap + 1);
  if (!s) return -1;
  str->str = s;
  str->cap = cap;
  return 0;
}

static int url_str_append(url_str_t *str, const char *s, size_t n)
{
  n = strnlen(s, n);
  if (url_str_keep(str, str->nr + n)) return -1;
  memcpy(str->str + str->nr, s, n);
  str->nr += n;
  str->str[str->nr] = '\0';
  return 0;
}

static int url_str_append_str(url_str_t *str, const char *s)
{
  return url_str_append(str, s, strlen(s));
}

static unsigned char url_decode_char(const char *s)
{
  unsigned char v = 0;
  for (int i=0; i<2; ++i) {
    unsigned char c = s[i];
    if (c >= '0' && c <= '9') {
      v |= (c - '0' +  0) << (4*(1-i));
    } else if (c >= 'a' && c <= 'z') {
      v |= (c - 'a' + 10) << (4*(1-i));
    } else if (c >= 'A' && c <= 'Z') {
      v |= (c - 'A' + 10) << (4*(1-i));
    } else {
      OA_ILE(0);
    }
  }
  return v;
}

static char* url_decode(const char *s, size_t n)
{
  int r = 0;
  n = strnlen(s, n);
  OA_ILE(n > 0);
  url_str_t str = {0};
  for (size_t i=0; i<n; i+=1) {
    char c = s[i];
    if (c != '%') {
      r = url_str_append(&str, &c, 1);
      if (r) break;
      continue;
    }

    c = (char)url_decode_char(s + i + 1);
    r = url_str_append(&str, &c, 1);
    if (r) break;

    i += 2;
  }
  if (r) {
    url_str_release(&str);
    return NULL;
  }
  return str.str;
}

static int url_str_set(url_str_t *str, const char *s, size_t n)
{
  str->nr = 0;
  if (str->str) str->str[0] = '\0';
  return url_str_append(str, s, n);
}

static void url_strs_release(url_strs_t *strs)
{
  if (!strs) return;
  for (size_t i=0; i<strs->nr; ++i) {
    TOD_SAFE_FREE(strs->strs[i]);
  }
  TOD_SAFE_FREE(strs->strs);
  strs->cap = 0;
  strs->nr  = 0;
}

static int url_strs_keep(url_strs_t *strs, size_t cap)
{
  if (cap <= strs->cap) return 0;
  cap = (cap + 255) / 256 * 256;
  char **s = (char**)realloc(strs->strs, cap);
  if (!s) return -1;
  strs->strs = s;
  strs->cap = cap;
  return 0;
}

static int url_strs_append(url_strs_t *strs, url_strs_t *v)
{
  if (url_strs_keep(strs, strs->nr + v->nr)) return -1;
  for (size_t i=0; i<v->nr; ++i) {
    strs->strs[strs->nr + i] = v->strs[i];
  }
  strs->nr += v->nr;
  v->nr = 0;
  return 0;
}

static int url_strs_append_str(url_strs_t *strs, const char *s, size_t n)
{
  if (url_strs_keep(strs, strs->nr + 1)) return -1;
  char *p = strndup(s, n);
  if (!p) return -1;
  strs->strs[strs->nr++] = p;
  return 0;
}

static char* url_strs_join(url_strs_t *strs)
{
  size_t len = 0;
  for (size_t i=0; i<strs->nr; ++i) {
    len += strlen(strs->strs[i]);
  }
  char *s = (char*)malloc(len + 1);
  if (!s) return NULL;
  char *p = s;
  for (size_t i=0; i<strs->nr; ++i) {
    const char *str = strs->strs[i];
    len = strlen(str);
    memcpy(p, str, len);
    p += len;
  }
  *p = '\0';
  return s;
}

static char* url_strs_join_with_head(url_strs_t *strs, const char *h, size_t n)
{
  n = strnlen(h, n);
  size_t len = n;
  for (size_t i=0; i<strs->nr; ++i) {
    len += strlen(strs->strs[i]);
  }
  char *s = (char*)malloc(len + 1);
  if (!s) return NULL;
  char *p = s;
  memcpy(p, h, n);
  p += n;
  for (size_t i=0; i<strs->nr; ++i) {
    const char *str = strs->strs[i];
    len = strlen(str);
    memcpy(p, str, len);
    p += len;
  }
  *p = '\0';
  return s;
}

void url_parser_param_reset(url_parser_param_t *param)
{
  if (!param) return;
  param->ctx.err_msg[0] = '\0';
}

void url_parser_param_release(url_parser_param_t *param)
{
  if (!param) return;
  url_release(&param->url);
  param->ctx.err_msg[0] = '\0';
}

void url_release(url_t *url)
{
  if (!url) return;
  TOD_SAFE_FREE(url->scheme);
  TOD_SAFE_FREE(url->user);
  TOD_SAFE_FREE(url->pass);
  TOD_SAFE_FREE(url->host);
  url->port = 0;
  TOD_SAFE_FREE(url->path);
  TOD_SAFE_FREE(url->query);
  TOD_SAFE_FREE(url->fragment);
}

static int is_unreserved(const char c)
{
  const unsigned char v = (const unsigned char)c;
  if (isalnum(v)) return 1;
  const char *s = "-._~";
  return !!strchr(s, c);
}

// static int is_gen_delims(const char c)
// {
//   const char *s = ":/?#[]@";
//   return !!strchr(s, c);
// }

static int is_sub_delims(const char c)
{
  const char *s = "!$&'()*+,;=";
  return !!strchr(s, c);
}

static int url_encode_scheme(url_t *url, url_str_t *str)
{
  int r = 0;
  r = url_str_append_str(str, url->scheme);
  if (r) return -1;

  r = url_str_append_str(str, ":");
  if (r) return -1;

  if (url->user && url->user[0]) {
    return url_str_append_str(str, "//");
  }

  if (url->host && url->host[0]) {
    return url_str_append_str(str, "//");
  }

  return 0;
}

static int url_str_encode_component(url_str_t *str, const char *s)
{
  int r = 0;
  for (; *s; ++s) {
    const char c = *s;
    if (is_unreserved(c) || is_sub_delims(c)) {
      // TODO: performance
      r = url_str_append(str, &c, 1);
    } else {
      char buf[10]; buf[0] = '\0';
      snprintf(buf, sizeof(buf), "%%%02x", (unsigned char)c);
      r = url_str_append(str, buf, 3);
    }
    if (r) return -1;
  }

  return 0;
}

static int url_encode_user_pass(url_t *url, url_str_t *str)
{
  int r = 0;

  if (!url->user || !*url->user) return 0;
  r = url_str_encode_component(str, url->user);
  if (r) return -1;

  if (url->pass && *url->pass) {
    r = url_str_append_str(str, ":");
    if (r) return -1;
    r = url_str_encode_component(str, url->pass);
    if (r) return -1;
  }

  return url_str_append_str(str, "@");
}

static int url_encode_host_port(url_t *url, url_str_t *str)
{
  int r = 0;
  if (!url->host || !*url->host) return 0;
  const char c = url->host[0];
  if (c == '[' /* ] */ || (c >= '0' && c <= '9')) {
    r = url_str_append_str(str, url->host);
  } else {
    r = url_str_encode_component(str, url->host);
  }
  if (r) return -1;
  if (url->port == 0) return 0;
  char buf[64]; buf[0] = '\0';
  snprintf(buf, sizeof(buf), ":%d", url->port);
  return url_str_append_str(str, buf);
}

static int url_encode_path(url_t *url, url_str_t *str)
{
  if (!url->path || !*url->path) return 0;
  return url_str_append_str(str, url->path);
}

static int url_encode_query(url_t *url, int8_t conn_mode, url_str_t *str)
{
  int r = 0;
  if (!url->query) {
    if (!conn_mode) return 0;
    return url_str_append_str(str, "?conn_mode=1");
  }

  if (!conn_mode) r = url_str_append_str(str, "?");
  else            r = url_str_append_str(str, "?conn_mode=1&");
  if (r) return -1;

  return url_str_append_str(str, url->query);
}

static int url_encode_fragment(url_t *url, url_str_t *str)
{
  int r = 0;
  if (!url->fragment) return 0;
  r = url_str_append_str(str, "#");
  if (r) return -1;
  return url_str_append_str(str, url->fragment);
}

static int url_encode_str(url_t *url, int8_t conn_mode, const char *db, url_str_t *str)
{
  int r = 0;
  r = url_encode_scheme(url, str);
  if (r) return -1;
  r = url_encode_user_pass(url, str);
  if (r) return -1;
  r = url_encode_host_port(url, str);
  if (r) return -1;
  r = url_encode_path(url, str);
  if (r) return -1;

  if (db && *db) {
    // FIXME: better be aware of sql-injection!!!
    size_t len = url->path ? strlen(url->path) : 0;
    if (len == 0 || url->path[len - 1] != '/') {
      const char c = '/';
      r = url_str_append(str, &c, 1);
      if (r) return -1;
    }
    r = url_str_encode_component(str, db);
    if (r) return -1;
  }

  r = url_encode_query(url, conn_mode, str);
  if (r) return -1;
  r = url_encode_fragment(url, str);
  return r ? -1 : 0;
}

static int url_encode_with_db(url_t *url, int8_t conn_mode, const char *db, char **out)
{
  int r = 0;
  url_str_t str = {0};
  r = url_encode_str(url, conn_mode, db, &str);
  if (r) {
    url_str_release(&str);
    return -1;
  }
  *out = str.str;
  return 0;
}

int url_encode(url_t *url, char **out)
{
  return url_encode_with_db(url, 0, NULL, out);
}

int url_encode_with_database(url_t *url, const char *db, char **out)
{
  return url_encode_with_db(url, 0, db, out);
}

int url_parse_and_encode(const conn_cfg_t *cfg, char **out, url_parser_param_t *param)
{
  const char *url = cfg->url;
  const char *uid = cfg->uid;
  const char *pwd = cfg->pwd;
  const char *ip = cfg->ip;
  uint16_t port = cfg->port;
  const char *db = cfg->db;
  int8_t conn_mode = !!cfg->conn_mode;
  // cfg::customproduct: this is local property, no need to propagate to ws


  int r = 0;

  r = url_parser_parse(url, strlen(url), param);
  if (r == 0 && uid && *uid && pwd && *pwd) r = url_set_user_pass(&param->url, uid, strlen(uid), pwd, strlen(pwd));
  if (r == 0 && ip && *ip) r = url_set_host_port(&param->url, ip, port);
  if (r == 0) r = url_encode_with_db(&param->url, conn_mode, db, out);

  return r ? -1 : 0;
}

int url_set_scheme(url_t *url, const char *s, size_t n)
{
  int r = 0;

  char buf[4096]; buf[0] = '\0'; // NOTE: big enough?
  url_parser_param_t param = {0};

  n = strnlen(s, n);
  r = snprintf(buf, sizeof(buf), "%.*s://1.1.1.1", (int)n, s);
  if (r <= 0 || (size_t)r >= sizeof(buf)) return -1;

  r = url_parser_parse(buf, strlen(buf), &param);
  if (r == 0) {
    if (param.url.scheme && *param.url.scheme) {
      TOD_SAFE_FREE(url->scheme);
      url->scheme = param.url.scheme; param.url.scheme = NULL;
    } else {
      r = -1;
    }
  }
  url_parser_param_release(&param);
  return r ? -1 : 0;
}

int url_set_user_pass(url_t *url, const char *u, size_t un, const char *p, size_t pn)
{
  un = strnlen(u, un);
  pn = strnlen(p, pn);
  if (un == 0 && pn > 0) return -1;

  char *user = strndup(u, un);
  if (!user) return -1;

  char *pass = pn ? strndup(p, pn) : NULL;
  if (pn && !pass) {
    TOD_SAFE_FREE(user);
    return -1;
  }

  TOD_SAFE_FREE(url->user);
  TOD_SAFE_FREE(url->pass);

  url->user = user;
  url->pass = pass;

  return 0;
}

int url_set_host_port(url_t *url, const char *host, uint16_t port)
{
  char *s= strdup(host);
  if (!s) return -1;

  TOD_SAFE_FREE(url->host);
  url->host = s;
  url->port = port;

  return 0;
}

#include "url_parser.tab.h"
#include "url_parser.lex.c"

#include "url_parser.lex.h"
#undef yylloc
#undef yylval
#include "url_parser.tab.c"

