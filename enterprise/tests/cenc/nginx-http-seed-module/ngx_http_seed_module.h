
/*
 * Copyright (C) xywang@taosdata.com
 */

#ifndef _NGX_HTTP_SEED_H_INCLUDED_
#define _NGX_HTTP_SEED_H_INCLUDED_


#include <ngx_http.h>
#include "libmseed.h"


typedef struct ngx_http_seed_ctx_s {
    ngx_event_t        ev;

    u_char             fname[NGX_MAX_PATH];

    MS3FileParam      *msfp;
    MS3Record         *msr;
    FILE              *fp;

    ngx_chain_t       *in;
    ngx_chain_t       *out;
    ngx_chain_t       *free;
    ngx_chain_t       *busy;
} ngx_http_seed_ctx_t;


#endif
