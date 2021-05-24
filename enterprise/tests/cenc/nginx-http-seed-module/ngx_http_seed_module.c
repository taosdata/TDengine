
/*
 * Copyright (C) xywang@taosdata.com
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_http_seed_module.h"


typedef struct ngx_http_seed_loc_conf_s {
    ngx_msec_t  interval;
} ngx_http_seed_loc_conf_t;


static void *ngx_http_seed_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_seed_merge_loc_conf(ngx_conf_t *cf,
    void *parent, void *child);


static char *ngx_http_seed(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void ngx_http_seed_cleanup(void *data);
static ngx_chain_t *ngx_http_seed_append_buf(ngx_http_request_t *r);
static void ngx_http_seed_send_handler(ngx_event_t *ev);
static void ngx_http_seed_write_event_handler(ngx_http_request_t *r);


static ngx_command_t  ngx_http_seed_commands[] = {

    { ngx_string("seed"),
      NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_http_seed,
      0,
      0,
      NULL },

    { ngx_string("interval"),
      NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_msec_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_seed_loc_conf_t, interval),
      NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_seed_module_ctx = {
    NULL,                          /* preconfiguration */
    NULL,                          /* postconfiguration */

    NULL,                          /* create main configuration */
    NULL,                          /* init main configuration */

    NULL,                          /* create server configuration */
    NULL,                          /* merge server configuration */

    ngx_http_seed_create_loc_conf, /* create location configuration */
    ngx_http_seed_merge_loc_conf   /* merge location configuration */
};


ngx_module_t  ngx_http_seed_module = {
    NGX_MODULE_V1,
    &ngx_http_seed_module_ctx,     /* module context */
    ngx_http_seed_commands,        /* module directives */
    NGX_HTTP_MODULE,               /* module type */
    NULL,                          /* init master */
    NULL,                          /* init module */
    NULL,                          /* init process */
    NULL,                          /* init thread */
    NULL,                          /* exit thread */
    NULL,                          /* exit process */
    NULL,                          /* exit master */
    NGX_MODULE_V1_PADDING
};


static void *
ngx_http_seed_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_seed_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_seed_loc_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    conf->interval = NGX_CONF_UNSET_MSEC;

    return conf;
}


static char *
ngx_http_seed_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_seed_loc_conf_t *prev = parent;
    ngx_http_seed_loc_conf_t *conf = child;

    ngx_conf_merge_msec_value(conf->interval, prev->interval, 1000);

    if (conf->interval < 2) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "http seed: directive \"interval\" must >= %d", 2);

        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_seed_handler(ngx_http_request_t *r)
{
    u_char                    *last;
    size_t                     root;
    ngx_int_t                  rc;
    ngx_uint_t                 level;
    ngx_str_t                  path;
    ngx_log_t                 *log;
    ngx_http_seed_ctx_t       *ctx;
    ngx_http_cleanup_t        *cln;
    ngx_http_core_loc_conf_t  *clcf;
    ngx_http_seed_loc_conf_t  *slcf;

    if (!(r->method & (NGX_HTTP_GET|NGX_HTTP_HEAD))) {
        return NGX_HTTP_NOT_ALLOWED;
    }

    if (r->uri.data[r->uri.len - 1] == '/') {
        return NGX_DECLINED;
    }

    rc = ngx_http_discard_request_body(r);

    if (rc != NGX_OK) {
        return rc;
    }

    last = ngx_http_map_uri_to_path(r, &path, &root, 0);
    if (last == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    log = r->connection->log;

    path.len = last - path.data;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, log, 0,
                   "http seed filename: \"%V\"", &path);

    ctx = ngx_http_get_module_ctx(r, ngx_http_seed_module);
    if (ctx == NULL) {
        ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_seed_ctx_t));
        if (ctx == NULL) {
            ngx_log_error(NGX_LOG_ERR, log, 0,
                          "http seed: failed to allocate for ctx");

            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        ngx_http_set_ctx(r, ctx, ngx_http_seed_module);
    }

    ngx_memcpy(ctx->fname, path.data, path.len);

    cln = ngx_http_cleanup_add(r, 0);
    if (cln == NULL) {
        ngx_log_error(NGX_LOG_ERR, log, 0,
                      "http seed: failed to add cleanup");

        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    cln->handler = ngx_http_seed_cleanup;
    cln->data = ctx;

    clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);
    slcf = ngx_http_get_module_loc_conf(r, ngx_http_seed_module);

    ctx->fp = fopen((const char *) path.data, "r");
    if (ctx->fp == NULL) {
        switch (ngx_errno) {

        case 0:
            return NGX_HTTP_INTERNAL_SERVER_ERROR;

        case NGX_ENOENT:
        case NGX_ENOTDIR:
        case NGX_ENAMETOOLONG:

            level = NGX_LOG_ERR;
            rc = NGX_HTTP_NOT_FOUND;
            break;

        case NGX_EACCES:
#if (NGX_HAVE_OPENAT)
        case NGX_EMLINK:
        case NGX_ELOOP:
#endif

            level = NGX_LOG_ERR;
            rc = NGX_HTTP_FORBIDDEN;
            break;

        default:

            level = NGX_LOG_CRIT;
            rc = NGX_HTTP_INTERNAL_SERVER_ERROR;
            break;
        }

        if (rc != NGX_HTTP_NOT_FOUND || clcf->log_not_found) {
            ngx_log_error(level, log, ngx_errno, "\"%s\" failed", path.data);
        }

        return rc;
    }

    r->root_tested = !r->error_page;

    log->action = "sending seed to client";

    r->headers_out.status = NGX_HTTP_OK;
    r->allow_ranges = 0;

    ngx_str_set(&r->headers_out.content_type, "application/octet-stream");

    rc = ngx_http_send_header(r);

    if (rc == NGX_ERROR || rc > NGX_OK || r->header_only) {
        return rc;
    }

    log->action = NULL;

    r->read_event_handler = ngx_http_block_reading;
    r->write_event_handler = ngx_http_seed_write_event_handler;

    ctx->ev.handler = ngx_http_seed_send_handler;
    ctx->ev.log = log;
    ctx->ev.data = (void *) r;

    ngx_add_timer(&ctx->ev, slcf->interval);

    r->keepalive = 0;
    r->main->count++;

    return NGX_DONE;
}


static void
ngx_http_seed_cleanup(void *data)
{
    ngx_http_seed_ctx_t  *ctx;

    ctx = (ngx_http_seed_ctx_t *) data;

    if (ctx->ev.timer_set) {
        ngx_del_timer(&ctx->ev);
    }

    if (ctx->msr) {
        ms3_readmsr(&ctx->msr, NULL, NULL, NULL, 0, 0);
    }

    if (ctx->fp) {
        fclose(ctx->fp);
    }
}


static ngx_chain_t *
ngx_http_seed_append_buf(ngx_http_request_t *r)
{
    ngx_buf_t            *b;
    ngx_chain_t          *out;
    ngx_http_seed_ctx_t  *ctx;

    if (r == NULL || r->connection == NULL || r->connection->destroyed) {
        return NULL;
    }

    ctx = ngx_http_get_module_ctx(r, ngx_http_seed_module);

    out = ngx_chain_get_free_buf(r->pool, &ctx->free);
    if (out == NULL) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "http seed: failed to get chain");

        return NULL;
    }

    b = out->buf;
    if (b->start == NULL) {
        b->start = ngx_pcalloc(r->pool, 512);
        if (b->start == NULL) {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "http seed: failed to get buf");

            return NULL;
        }

        b->end = b->start + 512;
    }

    b->tag = (ngx_buf_tag_t) &ngx_http_seed_module;
    b->temporary = 1;
    b->flush = 1;
    b->pos = b->start;
    b->last = b->pos;

    out->buf = b;

    return out;
}


static void
ngx_http_seed_send_handler(ngx_event_t *ev)
{
    int                        ret;
    int32_t                    reclen, len;
    ngx_connection_t          *c;
    ngx_http_request_t        *r;
    ngx_chain_t               *out, **ll;
    ngx_http_seed_loc_conf_t  *slcf;
    ngx_http_seed_ctx_t       *ctx;

    r = (ngx_http_request_t *) ev->data;
    c = r->connection;

    ctx = ngx_http_get_module_ctx(r, ngx_http_seed_module);
    ret = ms3_readmsr_selection(&ctx->msfp, &ctx->msr,
                                (const char *) ctx->fname,
                                NULL, NULL, 0, NULL, 0);
    if (ret != MS_NOERROR && ret != MS_ENDOFFILE) {
        ngx_log_error(NGX_LOG_ERR, ev->log, 0,
                      "http seed: failed to parse: %s, reason: %s",
                      ctx->fname, ms_errorstr(ret));

        ngx_http_finalize_request(r, NGX_ERROR);
        return;
    }

    if (ret == MS_ENDOFFILE) {
        ngx_log_error(NGX_LOG_NOTICE, ev->log, 0,
                      "http seed: finished to read: %s", ctx->fname);

        ngx_http_finalize_request(r, NGX_DONE);
        return;
    }

    for (ll = &ctx->in; *ll; ll = &(*ll)->next) {
        /* void */
    }

    for ( ;; ) {
        out = ngx_http_seed_append_buf(r);
        if (out == NULL) {
            ngx_http_finalize_request(r, NGX_ERROR);
            return;
        }

        reclen = ctx->msr->reclen;

        if (reclen <= 512) {
            if (fread(out->buf->last, 1, reclen, ctx->fp) != (size_t) reclen) {
                ngx_log_error(NGX_LOG_NOTICE, ev->log, ngx_errno,
                              "http seed: failed to once read: %s",
                              ctx->fname);

                ngx_http_finalize_request(r, NGX_DONE);
                return;
            }

            out->buf->last += reclen;
            *ll = out;

            break;
        } else {
            len = (reclen > 512) ? 512 : reclen;

            if (fread(out->buf->last, 1, len, ctx->fp) != (size_t) len) {
                ngx_log_error(NGX_LOG_NOTICE, ev->log, ngx_errno,
                              "http seed: failed to read: %s", ctx->fname);

                ngx_http_finalize_request(r, NGX_DONE);
                return;
            }

            out->buf->last += len;

            *ll = out;
            ll = &(*ll)->next;

            reclen -= len;
            if (reclen <= 0) {
                break;
            }
        }
    }

    ngx_log_error(NGX_LOG_NOTICE, ev->log, 0,
                  "http seed: read offset: %l", ftell(ctx->fp));

    if (!c->error && !c->timedout
#if (nginx_version >= 1011013)
        && !c->write->delayed
#endif
        && !c->write->active)
    {
        c->write->handler(c->write);
    }

    if (c->destroyed || c->error || c->timedout) {
        return;
    }

    slcf = ngx_http_get_module_loc_conf(r, ngx_http_seed_module);

    ngx_add_timer(ev, slcf->interval);
}


static void
ngx_http_seed_write_event_handler(ngx_http_request_t *r)
{
    ngx_int_t                  rc;
    ngx_connection_t          *c;
    ngx_event_t               *wev;
    ngx_http_core_loc_conf_t  *clcf;
    ngx_chain_t              **ll;
    ngx_http_seed_ctx_t       *ctx;

    c = r->connection;
    if (c->destroyed) {
        return;
    }

    clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);

    wev = c->write;

    if (wev->timedout) {
#if (nginx_version < 1011013)
        if (!wev->delayed) {
#endif
        ngx_log_error(NGX_LOG_ALERT, c->log, NGX_ETIMEDOUT,
                      "http seed: write event timed out");

        c->timedout = 1;
        ngx_http_finalize_request(r, NGX_ERROR);
        return;
#if (nginx_version < 1011013)
        }

        wev->timedout = 0;
        wev->delayed = 0;

        if (!wev->ready) {
            ngx_add_timer(wev, clcf->send_timeout);

            if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
                ngx_http_finalize_request(r, NGX_ERROR);
            }

            return;
        }
#endif
    }

    if (wev->delayed) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, wev->log, 0,
                       "http seed: write event delayed");

        if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
            ngx_http_finalize_request(r, NGX_ERROR);
        }

        return;
    }

    if (wev->timer_set) {
        ngx_del_timer(wev);
    }

    ctx = ngx_http_get_module_ctx(r, ngx_http_seed_module);
    for ( ;; ) {
        if (ctx->out == NULL && ctx->in == NULL) {
            break;
        }

        for (ll = &ctx->out; *ll; ll = &(*ll)->next) {
            /* void */
        }

        *ll = ctx->in;
        ctx->in = NULL;

        rc = ngx_http_output_filter(r, ctx->out);

        ngx_chain_update_chains(r->pool, &ctx->free,
                                &ctx->busy, &ctx->out,
                                (ngx_buf_tag_t) &ngx_http_seed_module);

        if (rc == NGX_ERROR) {
            ngx_log_error(NGX_LOG_ERR, wev->log, 0,
                          "http seed: write event error");

            ngx_http_finalize_request(r, rc);
            return;
        }

        if (r->buffered || r->postponed || (r == r->main && c->buffered)) {
            if (!wev->delayed) {
                ngx_add_timer(wev, clcf->send_timeout);
            }

            if (ngx_handle_write_event(wev, clcf->send_lowat) != NGX_OK) {
                ngx_http_finalize_request(r, NGX_ERROR);
            }

            return;
        }

        if (rc == NGX_AGAIN) {
            if (!wev->timer_set) {
                ngx_add_timer(wev, clcf->send_timeout);
            }

            return;
        }
    }

    if (wev->active) {
        ngx_del_event(wev, NGX_WRITE_EVENT, 0);
    }
}


static char *
ngx_http_seed(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  *clcf;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_seed_handler;

    return NGX_CONF_OK;
}
