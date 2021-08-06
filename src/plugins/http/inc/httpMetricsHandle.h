//
// Created by slzhou on 8/6/21.
//

#ifndef TDENGINE_HTTPMETRICSHANDLE_H
#define TDENGINE_HTTPMETRICSHANDLE_H

#include "http.h"
#include "httpInt.h"
#include "httpUtil.h"
#include "httpResp.h"

void metricsInitHandle(HttpServer* httpServer);

bool metricsProcessRequest(struct HttpContext* httpContext);

#endif  // TDENGINE_HTTPMETRICHANDLE_H
