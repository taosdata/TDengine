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

#include "internal.h"

#include "charset.h"
#include "tls.h"

#include "log.h"

size_t tls_size(void)
{
  return sizeof(tls_t);
}

void tls_release(tls_t *tls)
{
  mem_release(&tls->intermediate);
  if (tls->mgr) {
    charset_conv_mgr_release(tls->mgr);
    free(tls->mgr);
    tls->mgr = NULL;
  }
  TOD_SAFE_FREE(tls->leakage);
}

mem_t* tls_get_mem_intermediate(void)
{
  tls_t *tls = tls_get();
  if (!tls) return NULL;
  return &tls->intermediate;
}

static void _release_hash_table_node(hash_table_t *hash_table, void *val, void *arg)
{
  (void)hash_table;
  (void)arg;
  if (!val) return;

  charset_conv_t *conv = (charset_conv_t*)val;
  charset_conv_release(conv);
  free(conv);
}

static int _charset_conv_mgr_init(charset_conv_mgr_t *mgr)
{
  mgr->convs = hash_table_new(_release_hash_table_node, NULL);
  if (!mgr->convs) return -1;
  return 0;
}

charset_conv_t* tls_get_charset_conv(const char *fromcode, const char *tocode)
{
  int r = 0;

  tls_t *tls = tls_get();
  if (!tls) return NULL;

  if (tls->mgr == NULL) {
    charset_conv_mgr_t *mgr = (charset_conv_mgr_t*)calloc(1, sizeof(*mgr));
    if (!mgr) return NULL;
    r = _charset_conv_mgr_init(mgr);
    if (r) {
      charset_conv_mgr_release(mgr);
      return NULL;
    }
    tls->mgr = mgr;
  }

  return charset_conv_mgr_get_charset_conv(tls->mgr, fromcode, tocode);
}

// debug leakage only
int tls_leakage_potential(void)
{
  tls_t *tls = tls_get();
  if (!tls) return -1;
  if (!tls->leakage) {
    tls->leakage = malloc(1024 * 1024 /* * 4 */);
    if (!tls->leakage) return -1;
  }
  return 0;
}
