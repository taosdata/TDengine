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

#include "uv.h"
#include "os.h"
#include "tlog.h"

#include "tudf.h"
#include "tudfInt.h"


static uv_loop_t *loop;

typedef struct SUdfdUvConn {
    uv_stream_t *client;
    char *inputBuf;
    int32_t inputLen;
    int32_t inputCap;
    int32_t inputTotal;
} SUdfdUvConn;

typedef struct SUvUdfWork {
    uv_stream_t *client;
    uv_buf_t input;
    uv_buf_t output;
} SUvUdfWork;

typedef struct SUdf {
    int32_t refCount;

    char name[16];
    int8_t type;

    uv_lib_t lib;
    TUdfFunc normalFunc;
} SUdf;

//TODO: low priority: change name onxxx to xxxCb, and udfc or udfd as prefix
//TODO: add private udf structure.
typedef struct SUdfHandle {
    SUdf *udf;
} SUdfHandle;


void udfdProcessRequest(uv_work_t *req) {
    SUvUdfWork *uvUdf = (SUvUdfWork *) (req->data);
    SUdfRequest *request = NULL;
    decodeRequest(uvUdf->input.base, uvUdf->input.len, &request);

    switch (request->type) {
        case UDF_TASK_SETUP: {
            debugPrint("%s", "process setup request");
            SUdf *udf = taosMemoryMalloc(sizeof(SUdf));
            udf->refCount = 0;
            SUdfSetupRequest *setup = request->subReq;
            strcpy(udf->name, setup->udfName);
            int err = uv_dlopen(setup->path, &udf->lib);
            if (err != 0) {
                debugPrint("can not load library %s. error: %s", setup->path, uv_strerror(err));
                //TODO set error
            }

            char normalFuncName[32] = {0};
            strcpy(normalFuncName, setup->udfName);
	    //TODO error,
	    //TODO find all functions normal, init, destroy, normal, merge, finalize
            uv_dlsym(&udf->lib, normalFuncName, (void **) (&udf->normalFunc));

            SUdfHandle *handle = taosMemoryMalloc(sizeof(SUdfHandle));
            handle->udf = udf;
            udf->refCount++;
            //TODO: allocate private structure and call init function and set it to handle
            SUdfResponse *rsp = taosMemoryMalloc(sizeof(SUdfResponse));
            rsp->seqNum = request->seqNum;
            rsp->type = request->type;
            rsp->code = 0;
            SUdfSetupResponse *subRsp = taosMemoryMalloc(sizeof(SUdfSetupResponse));
            subRsp->udfHandle = (int64_t) (handle);
            rsp->subRsp = subRsp;
            char *buf;
            int32_t len;
            encodeResponse(&buf, &len, rsp);

            uvUdf->output = uv_buf_init(buf, len);

            taosMemoryFree(rsp->subRsp);
            taosMemoryFree(rsp);
            taosMemoryFree(request->subReq);
            taosMemoryFree(request);
            taosMemoryFree(uvUdf->input.base);
            break;
        }

        case UDF_TASK_CALL: {
            debugPrint("%s", "process call request");
            SUdfCallRequest *call = request->subReq;
            SUdfHandle *handle = (SUdfHandle *) (call->udfHandle);
            SUdf *udf = handle->udf;
            char *newState;
            int32_t newStateSize;
            SUdfDataBlock input = {.data = call->input, .size= call->inputBytes};
            SUdfDataBlock output;
	    //TODO: call different functions according to the step 
            udf->normalFunc(call->step, call->state, call->stateBytes, input, &newState, &newStateSize, &output);

            SUdfResponse *rsp = taosMemoryMalloc(sizeof(SUdfResponse));
            rsp->seqNum = request->seqNum;
            rsp->type = request->type;
            rsp->code = 0;
            SUdfCallResponse *subRsp = taosMemoryMalloc(sizeof(SUdfCallResponse));
            subRsp->outputBytes = output.size;
            subRsp->output = output.data;
            subRsp->newStateBytes = newStateSize;
            subRsp->newState = newState;
            rsp->subRsp = subRsp;

            char *buf;
            int32_t len;
            encodeResponse(&buf, &len, rsp);
            uvUdf->output = uv_buf_init(buf, len);

            taosMemoryFree(rsp->subRsp);
            taosMemoryFree(rsp);
            taosMemoryFree(newState);
            taosMemoryFree(output.data);
            taosMemoryFree(request->subReq);
            taosMemoryFree(request);
            taosMemoryFree(uvUdf->input.base);
            break;
        }
        case UDF_TASK_TEARDOWN: {
            debugPrint("%s", "process teardown request");

            SUdfTeardownRequest *teardown = request->subReq;
            SUdfHandle *handle = (SUdfHandle *) (teardown->udfHandle);
            SUdf *udf = handle->udf;
            udf->refCount--;
            if (udf->refCount == 0) {
                uv_dlclose(&udf->lib);
            }
            taosMemoryFree(udf);
	    //TODO: call destroy and free udf private 
            taosMemoryFree(handle);

            SUdfResponse *rsp = taosMemoryMalloc(sizeof(SUdfResponse));
            rsp->seqNum = request->seqNum;
            rsp->type = request->type;
            rsp->code = 0;
            SUdfTeardownResponse *subRsp = taosMemoryMalloc(sizeof(SUdfTeardownResponse));
            rsp->subRsp = subRsp;
            char *buf;
            int32_t len;
            encodeResponse(&buf, &len, rsp);
            uvUdf->output = uv_buf_init(buf, len);

            taosMemoryFree(rsp->subRsp);
            taosMemoryFree(rsp);
            taosMemoryFree(request->subReq);
            taosMemoryFree(request);
            taosMemoryFree(uvUdf->input.base);
            break;
        }
        default: {
            break;
        }

    }

}

void udfdOnWrite(uv_write_t *req, int status) {
    debugPrint("%s", "after writing to pipe");
    if (status < 0) {
        debugPrint("Write error %s", uv_err_name(status));
    }
    SUvUdfWork *work = (SUvUdfWork *) req->data;
    debugPrint("\tlength: %zu", work->output.len);
    taosMemoryFree(work->output.base);
    taosMemoryFree(work);
    taosMemoryFree(req);
}


void udfdSendResponse(uv_work_t *work, int status) {
    debugPrint("%s", "send response");
    SUvUdfWork *udfWork = (SUvUdfWork *) (work->data);

    uv_write_t *write_req = taosMemoryMalloc(sizeof(uv_write_t));
    write_req->data = udfWork;
    uv_write(write_req, udfWork->client, &udfWork->output, 1, udfdOnWrite);

    taosMemoryFree(work);
}

void udfdAllocBuffer(uv_handle_t *handle, size_t suggestedSize, uv_buf_t *buf) {
    debugPrint("%s", "allocate buffer for read");
    SUdfdUvConn *ctx = handle->data;
    int32_t msgHeadSize = sizeof(int32_t) + sizeof(int64_t);
    if (ctx->inputCap == 0) {
        ctx->inputBuf = taosMemoryMalloc(msgHeadSize);
        if (ctx->inputBuf) {
            ctx->inputLen = 0;
            ctx->inputCap = msgHeadSize;
            ctx->inputTotal = -1;

            buf->base = ctx->inputBuf;
            buf->len = ctx->inputCap;
        } else {
            //TODO: log error
            buf->base = NULL;
            buf->len = 0;
        }
    } else {
        ctx->inputCap = ctx->inputTotal > ctx->inputCap ? ctx->inputTotal : ctx->inputCap;
        void *inputBuf = taosMemoryRealloc(ctx->inputBuf, ctx->inputCap);
        if (inputBuf) {
            ctx->inputBuf = inputBuf;
            buf->base = ctx->inputBuf + ctx->inputLen;
            buf->len = ctx->inputCap - ctx->inputLen;
        } else {
            //TODO: log error
            buf->base = NULL;
            buf->len = 0;
        }
    }
    debugPrint("\tinput buf cap - len - total : %d - %d - %d", ctx->inputCap, ctx->inputLen, ctx->inputTotal);

}

bool isUdfdUvMsgComplete(SUdfdUvConn *pipe) {
    if (pipe->inputTotal == -1 && pipe->inputLen >= sizeof(int32_t)) {
        pipe->inputTotal = *(int32_t *) (pipe->inputBuf);
    }
    if (pipe->inputLen == pipe->inputCap && pipe->inputTotal == pipe->inputCap) {
        return true;
    }
    return false;
}

void udfdHandleRequest(SUdfdUvConn *conn) {
    uv_work_t *work = taosMemoryMalloc(sizeof(uv_work_t));
    SUvUdfWork *udfWork = taosMemoryMalloc(sizeof(SUvUdfWork));
    udfWork->client = conn->client;
    udfWork->input = uv_buf_init(conn->inputBuf, conn->inputLen);
    conn->inputBuf = NULL;
    conn->inputLen = 0;
    conn->inputCap = 0;
    conn->inputTotal = -1;
    work->data = udfWork;
    uv_queue_work(loop, work, udfdProcessRequest, udfdSendResponse);
}

void udfdPipeCloseCb(uv_handle_t *pipe) {
    SUdfdUvConn *conn = pipe->data;
    taosMemoryFree(conn->client);
    taosMemoryFree(conn->inputBuf);
    taosMemoryFree(conn);
}

void udfdUvHandleError(SUdfdUvConn *conn) {
    uv_close((uv_handle_t *) conn->client, udfdPipeCloseCb);
}

void udfdPipeRead(uv_stream_t *client, ssize_t nread, const uv_buf_t *buf) {
    debugPrint("%s, nread: %zd", "read from pipe", nread);

    if (nread == 0) return;

    SUdfdUvConn *conn = client->data;

    if (nread > 0) {
        conn->inputLen += nread;
        if (isUdfdUvMsgComplete(conn)) {
            udfdHandleRequest(conn);
        } else {
            //log error or continue;
        }
        return;
    }

    if (nread < 0) {
        debugPrint("Read error %s", uv_err_name(nread));
        if (nread == UV_EOF) {
            //TODO check more when close
        } else {
        }
        udfdUvHandleError(conn);
    }
}

void udfdOnNewConnection(uv_stream_t *server, int status) {
    debugPrint("%s", "on new connection");
    if (status < 0) {
        // TODO
        return;
    }

    uv_pipe_t *client = (uv_pipe_t *) taosMemoryMalloc(sizeof(uv_pipe_t));
    uv_pipe_init(loop, client, 0);
    if (uv_accept(server, (uv_stream_t *) client) == 0) {
        SUdfdUvConn *ctx = taosMemoryMalloc(sizeof(SUdfdUvConn));
        ctx->client = (uv_stream_t *) client;
        ctx->inputBuf = 0;
        ctx->inputLen = 0;
        ctx->inputCap = 0;
        client->data = ctx;
        ctx->client = (uv_stream_t *) client;
        uv_read_start((uv_stream_t *) client, udfdAllocBuffer, udfdPipeRead);
    } else {
        uv_close((uv_handle_t *) client, NULL);
    }
}

void removeListeningPipe(int sig) {
    uv_fs_t req;
    uv_fs_unlink(loop, &req, "udf.sock", NULL);
    exit(0);
}

int main() {
    debugPrint("libuv version: %x", UV_VERSION_HEX);

    loop = uv_default_loop();
    uv_fs_t req;
    uv_fs_unlink(loop, &req, "udf.sock", NULL);

    uv_pipe_t server;
    uv_pipe_init(loop, &server, 0);

    signal(SIGINT, removeListeningPipe);

    int r;
    if ((r = uv_pipe_bind(&server, "udf.sock"))) {
        debugPrint("Bind error %s\n", uv_err_name(r));
        removeListeningPipe(0);
        return 1;
    }
    if ((r = uv_listen((uv_stream_t *) &server, 128, udfdOnNewConnection))) {
        debugPrint("Listen error %s", uv_err_name(r));
        return 2;
    }
    uv_run(loop, UV_RUN_DEFAULT);
    uv_loop_close(loop);
}
