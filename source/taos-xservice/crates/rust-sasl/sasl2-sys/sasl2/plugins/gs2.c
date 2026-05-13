/*
 * Copyright (c) 2010, JANET(UK)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of JANET(UK) nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
/*
 * Copyright (c) 1998-2016 Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The name "Carnegie Mellon University" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For permission or any other legal
 *    details, please contact
 *      Carnegie Mellon University
 *      Center for Technology Transfer and Enterprise Creation
 *      4615 Forbes Avenue
 *      Suite 302
 *      Pittsburgh, PA  15213
 *      (412) 268-7393, fax: (412) 268-7395
 *      innovation@andrew.cmu.edu
 *
 * 4. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by Computing Services
 *     at Carnegie Mellon University (http://www.cmu.edu/computing/)."
 *
 * CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
 * THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE
 * FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
 * OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <config.h>
#include <gssapi/gssapi.h>

#ifndef KRB5_HEIMDAL
#ifdef HAVE_GSSAPI_GSSAPI_EXT_H
#include <gssapi/gssapi_ext.h>
#endif
#endif

#include <fcntl.h>
#include <stdio.h>
#include <sasl.h>
#include <saslutil.h>
#include <saslplug.h>

#include "plugin_common.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <errno.h>
#include <assert.h>
#include "gs2_token.h"

#define GS2_CB_FLAG_MASK    0x0F
#define GS2_CB_FLAG_N       0x00
#define GS2_CB_FLAG_P       0x01
#define GS2_CB_FLAG_Y       0x02
#define GS2_NONSTD_FLAG     0x10

typedef struct context {
    gss_ctx_id_t gss_ctx;
    gss_name_t client_name;
    gss_name_t server_name;
    gss_cred_id_t server_creds;
    gss_cred_id_t client_creds;
    char *out_buf;
    unsigned out_buf_len;
    const sasl_utils_t *utils;
    char *authid;
    char *authzid;
    union {
        sasl_client_plug_t *client;
        sasl_server_plug_t *server;
    } plug;
    gss_OID mechanism;
    int gs2_flags;
    char *cbindingname;
    struct gss_channel_bindings_struct gss_cbindings;
    sasl_secret_t *password;
    unsigned int free_password;
    OM_uint32 lifetime;
} context_t;

static gss_OID_set gs2_mechs = GSS_C_NO_OID_SET;

static int gs2_get_init_creds(context_t *context,
                              sasl_client_params_t *params,
                              sasl_interact_t **prompt_need,
                              sasl_out_params_t *oparams);

static int gs2_verify_initial_message(context_t *text,
                                      sasl_server_params_t *sparams,
                                      const char *in,
                                      unsigned inlen,
                                      gss_buffer_t token);

static int gs2_make_header(context_t *text,
                           sasl_client_params_t *cparams,
                           const char *authzid,
                           char **out,
                           unsigned *outlen);

static int gs2_make_message(context_t *text,
                            sasl_client_params_t *cparams,
                            int initialContextToken,
                            gss_buffer_t token,
                            char **out,
                            unsigned *outlen);

static int gs2_get_mech_attrs(const sasl_utils_t *utils,
                              const gss_OID mech,
                              unsigned int *security_flags,
                              unsigned int *features,
                              const unsigned long **prompts);

static int gs2_indicate_mechs(const sasl_utils_t *utils);

static int gs2_map_sasl_name(const sasl_utils_t *utils,
                             const char *mech,
                             gss_OID *oid);

static int gs2_duplicate_buffer(const sasl_utils_t *utils,
                                const gss_buffer_t src,
                                gss_buffer_t dst);

static int gs2_unescape_authzid(const sasl_utils_t *utils,
                                char **in,
                                unsigned *inlen,
                                char **authzid);

static int gs2_escape_authzid(const sasl_utils_t *utils,
                              const char *in,
                              unsigned inlen,
                              char **authzid);

/* sasl_gs_log: only logs status string returned from gss_display_status() */
#define sasl_gs2_log(x,y,z) sasl_gs2_seterror_(x,y,z,1)
#define sasl_gs2_seterror(x,y,z) sasl_gs2_seterror_(x,y,z,0)

static int
sasl_gs2_seterror_(const sasl_utils_t *utils, OM_uint32 maj, OM_uint32 min,
                   int logonly);

static context_t *
sasl_gs2_new_context(const sasl_utils_t *utils)
{
    context_t *ret;

    ret = utils->malloc(sizeof(context_t));
    if (ret == NULL)
        return NULL;

    memset(ret, 0, sizeof(context_t));
    ret->utils = utils;

    return ret;
}

static int
sasl_gs2_free_context_contents(context_t *text)
{
    OM_uint32 min_stat;

    if (text == NULL)
        return SASL_OK;

    if (text->gss_ctx != GSS_C_NO_CONTEXT) {
        gss_delete_sec_context(&min_stat,&text->gss_ctx,
                               GSS_C_NO_BUFFER);
        text->gss_ctx = GSS_C_NO_CONTEXT;
    }

    if (text->client_name != GSS_C_NO_NAME) {
        gss_release_name(&min_stat,&text->client_name);
        text->client_name = GSS_C_NO_NAME;
    }

    if (text->server_name != GSS_C_NO_NAME) {
        gss_release_name(&min_stat,&text->server_name);
        text->server_name = GSS_C_NO_NAME;
    }

    if (text->server_creds != GSS_C_NO_CREDENTIAL) {
        gss_release_cred(&min_stat, &text->server_creds);
        text->server_creds = GSS_C_NO_CREDENTIAL;
    }

    if (text->client_creds != GSS_C_NO_CREDENTIAL) {
        gss_release_cred(&min_stat, &text->client_creds);
        text->client_creds = GSS_C_NO_CREDENTIAL;
    }

    if (text->authid != NULL) {
        text->utils->free(text->authid);
        text->authid = NULL;
    }

    if (text->authzid != NULL) {
        text->utils->free(text->authzid);
        text->authzid = NULL;
    }

    gss_release_buffer(&min_stat, &text->gss_cbindings.application_data);

    if (text->out_buf != NULL) {
        text->utils->free(text->out_buf);
        text->out_buf = NULL;
    }

    text->out_buf_len = 0;

    if (text->cbindingname != NULL) {
        text->utils->free(text->cbindingname);
        text->cbindingname = NULL;
    }

    if (text->free_password)
        _plug_free_secret(text->utils, &text->password);

    memset(text, 0, sizeof(*text));

    return SASL_OK;
}

static void
gs2_common_mech_dispose(void *conn_context, const sasl_utils_t *utils)
{
    sasl_gs2_free_context_contents((context_t *)(conn_context));
    utils->free(conn_context);
}

static void
gs2_common_mech_free(void *global_context __attribute__((unused)),
                     const sasl_utils_t *utils __attribute__((unused)))
{
    OM_uint32 minor;

    if (gs2_mechs != GSS_C_NO_OID_SET) {
        gss_release_oid_set(&minor, &gs2_mechs);
        gs2_mechs = GSS_C_NO_OID_SET;
    }
}

/*****************************  Server Section  *****************************/

static int
gs2_server_mech_new(void *glob_context,
                    sasl_server_params_t *params,
                    const char *challenge __attribute__((unused)),
                    unsigned challen __attribute__((unused)),
                    void **conn_context)
{
    context_t *text;
    int ret;

    text = sasl_gs2_new_context(params->utils);
    if (text == NULL) {
        MEMERROR(params->utils);
        return SASL_NOMEM;
    }

    text->gss_ctx = GSS_C_NO_CONTEXT;
    text->client_name = GSS_C_NO_NAME;
    text->server_name = GSS_C_NO_NAME;
    text->server_creds = GSS_C_NO_CREDENTIAL;
    text->client_creds = GSS_C_NO_CREDENTIAL;
    text->plug.server = glob_context;

    ret = gs2_map_sasl_name(params->utils, text->plug.server->mech_name,
                            &text->mechanism);
    if (ret != SASL_OK) {
        gs2_common_mech_dispose(text, params->utils);
        return ret;
    }

    *conn_context = text;

    return SASL_OK;
}

static int
gs2_server_mech_step(void *conn_context,
                     sasl_server_params_t *params,
                     const char *clientin,
                     unsigned clientinlen,
                     const char **serverout,
                     unsigned *serveroutlen,
                     sasl_out_params_t *oparams)
{
    context_t *text = (context_t *)conn_context;
    gss_buffer_desc input_token = GSS_C_EMPTY_BUFFER;
    gss_buffer_desc output_token = GSS_C_EMPTY_BUFFER;
    OM_uint32 maj_stat = GSS_S_FAILURE, min_stat = 0;
    gss_buffer_desc name_buf = GSS_C_EMPTY_BUFFER;
    gss_buffer_desc short_name_buf = GSS_C_EMPTY_BUFFER;
    gss_name_t without = GSS_C_NO_NAME;
    gss_OID_set_desc mechs;
    OM_uint32 out_flags = 0;
    int ret = SASL_OK, equal = 0;
    int initialContextToken = (text->gss_ctx == GSS_C_NO_CONTEXT);
    char *p;

    if (serverout == NULL) {
        PARAMERROR(text->utils);
        return SASL_BADPARAM;
    }

    *serverout = NULL;
    *serveroutlen = 0;

    if (initialContextToken) {
        name_buf.length = strlen(params->service) + 1 + strlen(params->serverFQDN);
        name_buf.value = params->utils->malloc(name_buf.length + 1);
        if (name_buf.value == NULL) {
            MEMERROR(text->utils);
            ret = SASL_NOMEM;
            goto cleanup;
        }
        snprintf(name_buf.value, name_buf.length + 1,
                 "%s@%s", params->service, params->serverFQDN);
        maj_stat = gss_import_name(&min_stat,
                                   &name_buf,
                                   GSS_C_NT_HOSTBASED_SERVICE,
                                   &text->server_name);
        params->utils->free(name_buf.value);
        name_buf.value = NULL;

        if (GSS_ERROR(maj_stat))
            goto cleanup;

        assert(text->server_creds == GSS_C_NO_CREDENTIAL);

        mechs.count = 1;
        mechs.elements = (gss_OID)text->mechanism;

        if (params->gss_creds == GSS_C_NO_CREDENTIAL) {
            maj_stat = gss_acquire_cred(&min_stat,
                                        text->server_name,
                                        GSS_C_INDEFINITE,
                                        &mechs,
                                        GSS_C_ACCEPT,
                                        &text->server_creds,
                                        NULL,
                                        &text->lifetime);
            if (GSS_ERROR(maj_stat))
                goto cleanup;
        }

        ret = gs2_verify_initial_message(text,
                                         params,
                                         clientin,
                                         clientinlen,
                                         &input_token);
        if (ret != SASL_OK)
            goto cleanup;
    } else {
        input_token.value = (void *)clientin;
        input_token.length = clientinlen;
    }

    maj_stat = gss_accept_sec_context(&min_stat,
                                      &text->gss_ctx,
                                      (params->gss_creds != GSS_C_NO_CREDENTIAL)
                                        ? (gss_cred_id_t)params->gss_creds
                                        : text->server_creds,
                                      &input_token,
                                      &text->gss_cbindings,
                                      &text->client_name,
                                      NULL,
                                      &output_token,
                                      &out_flags,
                                      &text->lifetime,
                                      &text->client_creds);
    if (GSS_ERROR(maj_stat)) {
        sasl_gs2_log(text->utils, maj_stat, min_stat);
        text->utils->seterror(text->utils->conn, SASL_NOLOG,
                              "GS2 Failure: gss_accept_sec_context");
        ret = (maj_stat == GSS_S_BAD_BINDINGS) ? SASL_BADBINDING : SASL_BADAUTH;
        goto cleanup;
    }

    *serveroutlen = output_token.length;
    if (output_token.value != NULL) {
        ret = _plug_buf_alloc(text->utils, &text->out_buf,
                              &text->out_buf_len, *serveroutlen);
        if (ret != SASL_OK)
            goto cleanup;
        memcpy(text->out_buf, output_token.value, *serveroutlen);
        *serverout = text->out_buf;
    } else {
        /* No output token, send an empty string */
        *serverout = "";
        serveroutlen = 0;
    }

    if (maj_stat == GSS_S_CONTINUE_NEEDED) {
        ret = SASL_CONTINUE;
        goto cleanup;
    }

    assert(maj_stat == GSS_S_COMPLETE);

    if ((out_flags & GSS_C_SEQUENCE_FLAG) == 0)  {
        ret = SASL_BADAUTH;
        goto cleanup;
    }

    maj_stat = gss_display_name(&min_stat, text->client_name,
                                &name_buf, NULL);
    if (GSS_ERROR(maj_stat))
        goto cleanup;

    ret = gs2_duplicate_buffer(params->utils, &name_buf, &short_name_buf);
    if (ret != 0)
        goto cleanup;

    p = (char *)memchr(name_buf.value, '@', name_buf.length);
    if (p != NULL) {
        short_name_buf.length = (p - (char *)name_buf.value);

        maj_stat = gss_import_name(&min_stat,
                                   &short_name_buf,
                                   GSS_C_NT_USER_NAME,
                                   &without);
        if (GSS_ERROR(maj_stat)) {
            goto cleanup;
        }

        maj_stat = gss_compare_name(&min_stat, text->client_name,
                                    without, &equal);
        if (GSS_ERROR(maj_stat)) {
            goto cleanup;
        }

        if (equal)
            ((char *)short_name_buf.value)[short_name_buf.length] = '\0';
    }

    text->authid = (char *)short_name_buf.value;
    short_name_buf.value = NULL;
    short_name_buf.length = 0;

    if (text->authzid != NULL) {
        ret = params->canon_user(params->utils->conn,
                                 text->authzid, 0,
                                 SASL_CU_AUTHZID, oparams);
        if (ret != SASL_OK) {
            goto cleanup;
	}
    }

    ret = params->canon_user(params->utils->conn,
                             text->authid, 0,
                             text->authzid == NULL
                                ? (SASL_CU_AUTHZID | SASL_CU_AUTHID)
                                : SASL_CU_AUTHID,
                             oparams);
    if (ret != SASL_OK) {
        goto cleanup;
    }

    switch (text->gs2_flags & GS2_CB_FLAG_MASK) {
    case GS2_CB_FLAG_N:
        oparams->cbindingdisp = SASL_CB_DISP_NONE;
        break;
    case GS2_CB_FLAG_P:
        oparams->cbindingdisp = SASL_CB_DISP_USED;
        oparams->cbindingname = text->cbindingname;
        break;
    case GS2_CB_FLAG_Y:
        oparams->cbindingdisp = SASL_CB_DISP_WANT;
        break;
    }

    if (text->client_creds != GSS_C_NO_CREDENTIAL)
        oparams->client_creds = &text->client_creds;
    else
        oparams->client_creds = NULL;

    oparams->gss_peer_name = text->client_name;
    oparams->gss_local_name = text->server_name;
    oparams->maxoutbuf = 0xFFFFFF;
    oparams->encode = NULL;
    oparams->decode = NULL;
    oparams->mech_ssf = 0;
    oparams->doneflag = 1;

    ret = SASL_OK;

cleanup:
    if (ret == SASL_OK && maj_stat != GSS_S_COMPLETE) {
        sasl_gs2_seterror(text->utils, maj_stat, min_stat);
        ret = SASL_FAIL;
    }

    if (initialContextToken) {
        gss_release_buffer(&min_stat, &input_token);
    }
    gss_release_buffer(&min_stat, &name_buf);
    gss_release_buffer(&min_stat, &short_name_buf);
    gss_release_buffer(&min_stat, &output_token);
    gss_release_name(&min_stat, &without);

    if (ret < SASL_OK) {
        sasl_gs2_free_context_contents(text);
    }

    return ret;
}

static int
gs2_common_plug_init(const sasl_utils_t *utils,
                     size_t plugsize,
                     int (*plug_alloc)(const sasl_utils_t *,
                                       void *,
                                       const gss_buffer_t,
                                       const gss_OID),
                     void **pluglist,
                     int *plugcount)
{
    OM_uint32 major, minor;
    size_t i, count = 0;
    void *plugs = NULL;

    *pluglist = NULL;
    *plugcount = 0;

    if (gs2_indicate_mechs(utils) != SASL_OK) {
        return SASL_NOMECH;
    }

    plugs = utils->malloc(gs2_mechs->count * plugsize);
    if (plugs == NULL) {
        MEMERROR(utils);
        return SASL_NOMEM;
    }
    memset(plugs, 0, gs2_mechs->count * plugsize);

    for (i = 0; i < gs2_mechs->count; i++) {
        gss_buffer_desc sasl_mech_name = GSS_C_EMPTY_BUFFER;

        major = gss_inquire_saslname_for_mech(&minor,
                                              &gs2_mechs->elements[i],
                                              &sasl_mech_name,
                                              GSS_C_NO_BUFFER,
                                              GSS_C_NO_BUFFER);
        if (GSS_ERROR(major))
            continue;

#define PLUG_AT(index)      (void *)((unsigned char *)plugs + (count * plugsize))

        if (plug_alloc(utils, PLUG_AT(count), &sasl_mech_name,
                       &gs2_mechs->elements[i]) == SASL_OK)
            count++;

        gss_release_buffer(&minor, &sasl_mech_name);
    }

    if (count == 0) {
        utils->free(plugs);
        return SASL_NOMECH;
    }

    *pluglist = plugs;
    *plugcount = count;

    return SASL_OK;
}

static int
gs2_server_plug_alloc(const sasl_utils_t *utils,
                      void *plug,
                      gss_buffer_t sasl_name,
                      gss_OID mech)
{
    int ret;
    sasl_server_plug_t *splug = (sasl_server_plug_t *)plug;
    gss_buffer_desc buf;

    memset(splug, 0, sizeof(*splug));

    ret = gs2_get_mech_attrs(utils, mech,
                             &splug->security_flags,
                             &splug->features,
                             NULL);
    if (ret != SASL_OK)
        return ret;

    ret = gs2_duplicate_buffer(utils, sasl_name, &buf);
    if (ret != SASL_OK)
        return ret;

    splug->mech_name = (char *)buf.value;
    splug->glob_context = plug;
    splug->mech_new = gs2_server_mech_new;
    splug->mech_step = gs2_server_mech_step;
    splug->mech_dispose = gs2_common_mech_dispose;
    splug->mech_free = gs2_common_mech_free;

    return SASL_OK;
}

static sasl_server_plug_t *gs2_server_plugins;
static int gs2_server_plugcount;

int
gs2_server_plug_init(const sasl_utils_t *utils,
                     int maxversion,
                     int *outversion,
                     sasl_server_plug_t **pluglist,
                     int *plugcount)
{
    int ret;

    *pluglist = NULL;
    *plugcount = 0;

    if (maxversion < SASL_SERVER_PLUG_VERSION)
        return SASL_BADVERS;

    *outversion = SASL_SERVER_PLUG_VERSION;

    if (gs2_server_plugins == NULL) {
        ret = gs2_common_plug_init(utils,
                                   sizeof(sasl_server_plug_t),
                                   gs2_server_plug_alloc,
                                   (void **)&gs2_server_plugins,
                                   &gs2_server_plugcount);
        if (ret != SASL_OK)
            return ret;
    }

    *pluglist = gs2_server_plugins;
    *plugcount = gs2_server_plugcount;

    return SASL_OK;
}

/*****************************  Client Section  *****************************/

static int gs2_client_mech_step(void *conn_context,
                                sasl_client_params_t *params,
                                const char *serverin,
                                unsigned serverinlen,
                                sasl_interact_t **prompt_need,
                                const char **clientout,
                                unsigned *clientoutlen,
                                sasl_out_params_t *oparams)
{
    context_t *text = (context_t *)conn_context;
    gss_buffer_desc input_token = GSS_C_EMPTY_BUFFER;
    gss_buffer_desc output_token = GSS_C_EMPTY_BUFFER;
    gss_buffer_desc name_buf = GSS_C_EMPTY_BUFFER;
    OM_uint32 maj_stat = GSS_S_FAILURE, min_stat = 0;
    OM_uint32 req_flags, ret_flags;
    int ret = SASL_FAIL;
    int initialContextToken;

    *clientout = NULL;
    *clientoutlen = 0;

    if (text->gss_ctx == GSS_C_NO_CONTEXT) {
        ret = gs2_get_init_creds(text, params, prompt_need, oparams);
        if (ret != SASL_OK) {
            goto cleanup;
	}

        initialContextToken = 1;
    } else {
        initialContextToken = 0;
    }

    if (text->server_name == GSS_C_NO_NAME) { /* only once */
        if (params->serverFQDN == NULL ||
            strlen(params->serverFQDN) == 0) {
            SETERROR(text->utils, "GS2 Failure: no serverFQDN");
            ret = SASL_FAIL;
            goto cleanup;
        }

        name_buf.length = strlen(params->service) + 1 + strlen(params->serverFQDN);
        name_buf.value = params->utils->malloc(name_buf.length + 1);
        if (name_buf.value == NULL) {
            ret = SASL_NOMEM;
            goto cleanup;
        }
        snprintf(name_buf.value, name_buf.length + 1,
                 "%s@%s", params->service, params->serverFQDN);

        maj_stat = gss_import_name(&min_stat,
                                   &name_buf,
                                   GSS_C_NT_HOSTBASED_SERVICE,
                                   &text->server_name);
        params->utils->free(name_buf.value);
        name_buf.value = NULL;

        if (GSS_ERROR(maj_stat)) {
	    ret = SASL_OK;
            goto cleanup;
	}
    }

    /* From GSSAPI plugin: apparently this is for some IMAP bug workaround */
    if (serverinlen == 0 && text->gss_ctx != GSS_C_NO_CONTEXT) {
        gss_delete_sec_context(&min_stat, &text->gss_ctx, GSS_C_NO_BUFFER);
        text->gss_ctx = GSS_C_NO_CONTEXT;
    }

    input_token.value = (void *)serverin;
    input_token.length = serverinlen;

    if (initialContextToken) {
        if ((text->plug.client->features & SASL_FEAT_GSS_FRAMING) == 0)
            text->gs2_flags |= GS2_NONSTD_FLAG;

        switch (params->cbindingdisp) {
        case SASL_CB_DISP_NONE:
            text->gs2_flags |= GS2_CB_FLAG_N;
            break;
        case SASL_CB_DISP_USED:
            text->gs2_flags |= GS2_CB_FLAG_P;
            break;
        case SASL_CB_DISP_WANT:
            text->gs2_flags |= GS2_CB_FLAG_Y;
            break;
        }

        ret = gs2_make_header(text, params,
                              strcmp(oparams->user, oparams->authid) ?
                                     (char *) oparams->user : NULL,
                              &text->out_buf, &text->out_buf_len);
        if (ret != 0) {
            goto cleanup;
	}
    }

    req_flags = GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG;

    maj_stat = gss_init_sec_context(&min_stat,
                                    (params->gss_creds != GSS_C_NO_CREDENTIAL)
                                        ? (gss_cred_id_t)params->gss_creds
                                        : text->client_creds,
                                    &text->gss_ctx,
                                    text->server_name,
                                    (gss_OID)text->mechanism,
                                    req_flags,
                                    GSS_C_INDEFINITE,
                                    &text->gss_cbindings,
                                    serverinlen ? &input_token : GSS_C_NO_BUFFER,
                                    NULL,
                                    &output_token,
                                    &ret_flags,
                                    &text->lifetime);
    if (GSS_ERROR(maj_stat)) {
	ret = SASL_OK;
        goto cleanup;
    }

    ret = gs2_make_message(text, params, initialContextToken, &output_token,
                           &text->out_buf, &text->out_buf_len);
    if (ret != 0) {
        goto cleanup;
    }

    *clientout = text->out_buf;
    *clientoutlen = text->out_buf_len;

    if (maj_stat == GSS_S_CONTINUE_NEEDED) {
        ret = SASL_CONTINUE;
        goto cleanup;
    }

    if (text->client_name != GSS_C_NO_NAME) {
        gss_release_name(&min_stat, &text->client_name);
    }
    maj_stat = gss_inquire_context(&min_stat,
                                   text->gss_ctx,
                                   &text->client_name,
                                   NULL,
                                   &text->lifetime,
                                   NULL,
                                   &ret_flags, /* flags */
                                   NULL,
                                   NULL);
    if (GSS_ERROR(maj_stat)) {
	ret = SASL_OK;
        goto cleanup;
    }

    if ((ret_flags & req_flags) != req_flags) {
        ret = SASL_BADAUTH;
        goto cleanup;
    }

    maj_stat = gss_display_name(&min_stat,
                                text->client_name,
                                &name_buf,
                                NULL);
    if (GSS_ERROR(maj_stat)) {
	ret = SASL_OK;
        goto cleanup;
    }

    oparams->gss_peer_name = text->server_name;
    oparams->gss_local_name = text->client_name;
    oparams->encode = NULL;
    oparams->decode = NULL;
    oparams->mech_ssf = 0;
    oparams->maxoutbuf = 0xFFFFFF;
    oparams->doneflag = 1;

    ret = SASL_OK;

cleanup:
    if (ret == SASL_OK && maj_stat != GSS_S_COMPLETE) {
        sasl_gs2_seterror(text->utils, maj_stat, min_stat);
        ret = SASL_FAIL;
    }

    gss_release_buffer(&min_stat, &output_token);
    gss_release_buffer(&min_stat, &name_buf);

    if (ret < SASL_OK) {
        sasl_gs2_free_context_contents(text);
    }

    return ret;
}

static int gs2_client_mech_new(void *glob_context,
                               sasl_client_params_t *params,
                               void **conn_context)
{
    context_t *text;
    int ret;

    text = sasl_gs2_new_context(params->utils);
    if (text == NULL) {
        MEMERROR(params->utils);
        return SASL_NOMEM;
    }

    text->gss_ctx = GSS_C_NO_CONTEXT;
    text->client_name = GSS_C_NO_NAME;
    text->server_creds = GSS_C_NO_CREDENTIAL;
    text->client_creds  = GSS_C_NO_CREDENTIAL;
    text->plug.client = glob_context;

    ret = gs2_map_sasl_name(params->utils, text->plug.client->mech_name,
                            &text->mechanism);
    if (ret != SASL_OK) {
        gs2_common_mech_dispose(text, params->utils);
        return ret;
    }

    *conn_context = text;

    return SASL_OK;
}

static int
gs2_client_plug_alloc(const sasl_utils_t *utils,
                      void *plug,
                      gss_buffer_t sasl_name,
                      gss_OID mech)
{
    int ret;
    sasl_client_plug_t *cplug = (sasl_client_plug_t *)plug;
    gss_buffer_desc buf;

    memset(cplug, 0, sizeof(*cplug));

    ret = gs2_get_mech_attrs(utils, mech,
                             &cplug->security_flags,
                             &cplug->features,
                             &cplug->required_prompts);
    if (ret != SASL_OK)
        return ret;

    ret = gs2_duplicate_buffer(utils, sasl_name, &buf);
    if (ret != SASL_OK)
        return ret;

    cplug->mech_name = (char *)buf.value;
    cplug->features |= SASL_FEAT_NEEDSERVERFQDN;
    cplug->glob_context = plug;
    cplug->mech_new = gs2_client_mech_new;
    cplug->mech_step = gs2_client_mech_step;
    cplug->mech_dispose = gs2_common_mech_dispose;
    cplug->mech_free = gs2_common_mech_free;

    return SASL_OK;
}

static sasl_client_plug_t *gs2_client_plugins;
static int gs2_client_plugcount;

int
gs2_client_plug_init(const sasl_utils_t *utils,
                     int maxversion,
                     int *outversion,
                     sasl_client_plug_t **pluglist,
                     int *plugcount)
{
    int ret;

    *pluglist = NULL;
    *plugcount = 0;

    if (maxversion < SASL_CLIENT_PLUG_VERSION)
        return SASL_BADVERS;

    *outversion = SASL_CLIENT_PLUG_VERSION;

    if (gs2_client_plugins == NULL) {
        ret = gs2_common_plug_init(utils,
                                   sizeof(sasl_client_plug_t),
                                   gs2_client_plug_alloc,
                                   (void **)&gs2_client_plugins,
                                   &gs2_client_plugcount);
        if (ret != SASL_OK)
            return ret;
    }

    *pluglist = gs2_client_plugins;
    *plugcount = gs2_client_plugcount;

    return SASL_OK;
}

/*
 * Copy header and application channel bindings to GSS channel bindings
 * structure in context.
 */
static int
gs2_save_cbindings(context_t *text,
                   gss_buffer_t header,
                   const sasl_channel_binding_t *cbinding)
{
    gss_buffer_t gss_cbindings = &text->gss_cbindings.application_data;
    size_t len;
    unsigned char *p;

    assert(gss_cbindings->value == NULL);

    /*
     * The application-data field MUST be set to the gs2-header, excluding
     * the initial [gs2-nonstd-flag ","] part, concatenated with, when a
     * gs2-cb-flag of "p" is used, the application's channel binding data.
     */
    len = header->length;
    if (text->gs2_flags & GS2_NONSTD_FLAG) {
        assert(len > 2);
        len -= 2;
    }
    if ((text->gs2_flags & GS2_CB_FLAG_MASK) == GS2_CB_FLAG_P &&
        cbinding != NULL) {
        len += cbinding->len;
    }

    gss_cbindings->length = len;
    gss_cbindings->value = text->utils->malloc(len);
    if (gss_cbindings->value == NULL)
        return SASL_NOMEM;

    p = (unsigned char *)gss_cbindings->value;
    if (text->gs2_flags & GS2_NONSTD_FLAG) {
        memcpy(p, (unsigned char *)header->value + 2, header->length - 2);
        p += header->length - 2;
    } else {
        memcpy(p, header->value, header->length);
        p += header->length;
    }

    if ((text->gs2_flags & GS2_CB_FLAG_MASK) == GS2_CB_FLAG_P &&
        cbinding != NULL) {
        memcpy(p, cbinding->data, cbinding->len);
    }

    return SASL_OK;
}

#define CHECK_REMAIN(n)     do { if (remain < (n)) return SASL_BADPROT; } while (0)

/*
 * Verify gs2-header, save authzid and channel bindings to context.
 */
static int
gs2_verify_initial_message(context_t *text,
                           sasl_server_params_t *sparams,
                           const char *in,
                           unsigned inlen,
                           gss_buffer_t token)
{
    OM_uint32 major, minor;
    char *p = (char *)in;
    unsigned remain = inlen;
    int ret;
    gss_buffer_desc buf = GSS_C_EMPTY_BUFFER;

    assert(text->cbindingname == NULL);
    assert(text->authzid == NULL);

    token->length = 0;
    token->value = NULL;

    /* minimum header includes CB flag and non-zero GSS token */
    CHECK_REMAIN(4); /* [pny],,. */

    /* non-standard GSS framing flag */
    if (remain > 1 && memcmp(p, "F,", 2) == 0) {
        text->gs2_flags |= GS2_NONSTD_FLAG;
        remain -= 2;
        p += 2;
    }

    /* SASL channel bindings */
    CHECK_REMAIN(1); /* [pny] */
    remain--;
    switch (*p++) {
    case 'p':
        CHECK_REMAIN(1); /* = */
        remain--;
        if (*p++ != '=')
            return SASL_BADPROT;

        ret = gs2_unescape_authzid(text->utils, &p, &remain, &text->cbindingname);
        if (ret != SASL_OK)
            return ret;

        text->gs2_flags |= GS2_CB_FLAG_P;
        break;
    case 'n':
        text->gs2_flags |= GS2_CB_FLAG_N;
        break;
    case 'y':
        text->gs2_flags |= GS2_CB_FLAG_Y;
        break;
    }

    CHECK_REMAIN(1); /* , */
    remain--;
    if (*p++ != ',')
        return SASL_BADPROT;

    /* authorization identity */
    if (remain > 1 && memcmp(p, "a=", 2) == 0) {
        CHECK_REMAIN(2);
        remain -= 2;
        p += 2;

        ret = gs2_unescape_authzid(text->utils, &p, &remain, &text->authzid);
        if (ret != SASL_OK)
            return ret;
    }

    /* end of header */
    CHECK_REMAIN(1); /* , */
    remain--;
    if (*p++ != ',')
        return SASL_BADPROT;

    buf.length = inlen - remain;
    buf.value = (void *)in;

    /* stash channel bindings to pass into gss_accept_sec_context() */
    ret = gs2_save_cbindings(text, &buf, sparams->cbinding);
    if (ret != SASL_OK)
        return ret;

    if (text->gs2_flags & GS2_NONSTD_FLAG) {
        buf.length = remain;
        buf.value = p;
    } else {
        gss_buffer_desc tmp;

        tmp.length = remain;
        tmp.value = p;

        major = gss_encapsulate_token(&tmp, text->mechanism, &buf);
        if (GSS_ERROR(major))
            return SASL_NOMEM;
    }

    token->value = text->utils->malloc(buf.length);
    if (token->value == NULL)
        return SASL_NOMEM;

    token->length = buf.length;
    memcpy(token->value, buf.value, buf.length);

    if ((text->gs2_flags & GS2_NONSTD_FLAG) == 0)
        gss_release_buffer(&minor, &buf);

    return SASL_OK;
}

/*
 * Create gs2-header, save channel bindings to context.
 */
static int
gs2_make_header(context_t *text,
                sasl_client_params_t *cparams,
                const char *authzid,
                char **out,
                unsigned *outlen)
{
    size_t required = 0;
    size_t wire_authzid_len = 0, cbnamelen = 0;
    char *wire_authzid = NULL;
    char *p;
    int ret;
    gss_buffer_desc buf;

    *out = NULL;
    *outlen = 0;

    /* non-standard GSS framing flag */
    if (text->gs2_flags & GS2_NONSTD_FLAG)
        required += 2; /* F, */

    /* SASL channel bindings */
    switch (text->gs2_flags & GS2_CB_FLAG_MASK) {
    case GS2_CB_FLAG_P:
        if (!SASL_CB_PRESENT(cparams))
            return SASL_BADPARAM;
        cbnamelen = strlen(cparams->cbinding->name);
        required += 1 /*=*/ + cbnamelen;
        /* fallthrough */
    case GS2_CB_FLAG_N:
    case GS2_CB_FLAG_Y:
        required += 2; /* [pny], */
        break;
    default:
        return SASL_BADPARAM;
    }

    /* authorization identity */
    if (authzid != NULL) {
        ret = gs2_escape_authzid(text->utils, authzid,
                                 strlen(authzid), &wire_authzid);
        if (ret != SASL_OK)
            return ret;

        wire_authzid_len = strlen(wire_authzid);
        required += 2 /* a= */ + wire_authzid_len;
    }

    required += 1; /* trailing comma */

    ret = _plug_buf_alloc(text->utils, out, outlen, required);
    if (ret != SASL_OK) {
        text->utils->free(wire_authzid);
        return ret;
    }

    *out = text->out_buf;
    *outlen = required;

    p = (char *)text->out_buf;
    if (text->gs2_flags & GS2_NONSTD_FLAG) {
        *p++ = 'F';
        *p++ = ',';
    }
    switch (text->gs2_flags & GS2_CB_FLAG_MASK) {
    case GS2_CB_FLAG_P:
        memcpy(p, "p=", 2);
        memcpy(p + 2, cparams->cbinding->name, cbnamelen);
        p += 2 + cbnamelen;
        break;
    case GS2_CB_FLAG_N:
        *p++ = 'n';
        break;
    case GS2_CB_FLAG_Y:
        *p++ = 'y';
        break;
    }
    *p++ = ',';
    if (wire_authzid != NULL) {
        memcpy(p, "a=", 2);
        memcpy(p + 2, wire_authzid, wire_authzid_len);
        text->utils->free(wire_authzid);
        p += 2 + wire_authzid_len;
    }
    *p++ = ',';

    assert(p == (char *)text->out_buf + required);

    buf.length = required;
    buf.value = *out;

    ret = gs2_save_cbindings(text, &buf, cparams->cbinding);
    if (ret != SASL_OK)
        return ret;

    return SASL_OK;
}

/*
 * Convert a GSS token to a GS2 one
 */
static int
gs2_make_message(context_t *text,
                 sasl_client_params_t *cparams __attribute__((unused)),
                 int initialContextToken,
                 gss_buffer_t token,
                 char **out,
                 unsigned *outlen)
{
    OM_uint32 major, minor;
    int ret;
    unsigned header_len = 0;
    gss_buffer_desc decap_token = GSS_C_EMPTY_BUFFER;

    if (initialContextToken) {
        header_len = *outlen;

        major = gss_decapsulate_token(token, text->mechanism, &decap_token);
        if ((major == GSS_S_DEFECTIVE_TOKEN &&
             (text->plug.client->features & SASL_FEAT_GSS_FRAMING)) ||
            GSS_ERROR(major))
            return SASL_FAIL;

        token = &decap_token;
    }

    ret = _plug_buf_alloc(text->utils, out, outlen,
                          header_len + token->length);
    if (ret != 0)
        return ret;

    memcpy(*out + header_len, token->value, token->length);
    *outlen = header_len + token->length;

    if (initialContextToken)
        gss_release_buffer(&minor, &decap_token);

    return SASL_OK;
}

static const unsigned long gs2_required_prompts[] = {
    SASL_CB_LIST_END
};

/*
 * Map GSS mechanism attributes to SASL ones
 */
static int
gs2_get_mech_attrs(const sasl_utils_t *utils,
                   const gss_OID mech,
                   unsigned int *security_flags,
                   unsigned int *features,
                   const unsigned long **prompts)
{
    OM_uint32 major, minor;
    int present;
    gss_OID_set attrs = GSS_C_NO_OID_SET;

    major = gss_inquire_attrs_for_mech(&minor, mech, &attrs, NULL);
    if (GSS_ERROR(major)) {
        utils->seterror(utils->conn, SASL_NOLOG,
                        "GS2 Failure: gss_inquire_attrs_for_mech");
        return SASL_FAIL;
    }

    *security_flags = SASL_SEC_NOPLAINTEXT | SASL_SEC_NOACTIVE;
    *features = SASL_FEAT_WANT_CLIENT_FIRST | SASL_FEAT_CHANNEL_BINDING;
    if (prompts != NULL)
        *prompts = gs2_required_prompts;

#define MA_PRESENT(a)   (gss_test_oid_set_member(&minor, (gss_OID)(a), \
                                                 attrs, &present) == GSS_S_COMPLETE && \
                         present)

    if (MA_PRESENT(GSS_C_MA_PFS))
        *security_flags |= SASL_SEC_FORWARD_SECRECY;
    if (!MA_PRESENT(GSS_C_MA_AUTH_INIT_ANON))
        *security_flags |= SASL_SEC_NOANONYMOUS;
    if (MA_PRESENT(GSS_C_MA_DELEG_CRED))
        *security_flags |= SASL_SEC_PASS_CREDENTIALS;
    if (MA_PRESENT(GSS_C_MA_AUTH_TARG))
        *security_flags |= SASL_SEC_MUTUAL_AUTH;
    if (MA_PRESENT(GSS_C_MA_AUTH_INIT_INIT) && prompts != NULL)
        *prompts = NULL;
    if (MA_PRESENT(GSS_C_MA_ITOK_FRAMED))
        *features |= SASL_FEAT_GSS_FRAMING;

    gss_release_oid_set(&minor, &attrs);

    return SASL_OK;
}

/*
 * Enumerate GSS mechanisms that can be used for GS2
 */
static int gs2_indicate_mechs(const sasl_utils_t *utils)
{
    OM_uint32 major, minor;
    gss_OID_desc desired_oids[3];
    gss_OID_set_desc desired_attrs;
    gss_OID_desc except_oids[3];
    gss_OID_set_desc except_attrs;

    if (gs2_mechs != GSS_C_NO_OID_SET)
        return SASL_OK;

    desired_oids[0] = *GSS_C_MA_AUTH_INIT;
    desired_oids[1] = *GSS_C_MA_AUTH_TARG;
    desired_oids[2] = *GSS_C_MA_CBINDINGS;
    desired_attrs.count = sizeof(desired_oids)/sizeof(desired_oids[0]);
    desired_attrs.elements = desired_oids;

    except_oids[0] = *GSS_C_MA_MECH_NEGO;
    except_oids[1] = *GSS_C_MA_NOT_MECH;
    except_oids[2] = *GSS_C_MA_DEPRECATED;

    except_attrs.count = sizeof(except_oids)/sizeof(except_oids[0]);
    except_attrs.elements = except_oids;

    major = gss_indicate_mechs_by_attrs(&minor,
                                        &desired_attrs,
                                        &except_attrs,
                                        GSS_C_NO_OID_SET,
                                        &gs2_mechs);
    if (GSS_ERROR(major)) {
        utils->seterror(utils->conn, SASL_NOLOG,
                        "GS2 Failure: gss_indicate_mechs_by_attrs");
        return SASL_FAIL;
    }

    return (gs2_mechs->count > 0) ? SASL_OK : SASL_NOMECH;
}

/*
 * Map SASL mechanism name to OID
 */
static int
gs2_map_sasl_name(const sasl_utils_t *utils,
                  const char *mech,
                  gss_OID *oid)
{
    OM_uint32 major, minor;
    gss_buffer_desc buf;

    buf.length = strlen(mech);
    buf.value = (void *)mech;

    major = gss_inquire_mech_for_saslname(&minor, &buf, oid);
    if (GSS_ERROR(major)) {
        utils->seterror(utils->conn, SASL_NOLOG,
                        "GS2 Failure: gss_inquire_mech_for_saslname");
        return SASL_FAIL;
    }

    return SASL_OK;
}

static int
gs2_duplicate_buffer(const sasl_utils_t *utils,
                     const gss_buffer_t src,
                     gss_buffer_t dst)
{
    dst->value = utils->malloc(src->length + 1);
    if (dst->value == NULL)
        return SASL_NOMEM;

    memcpy(dst->value, src->value, src->length);
    ((char *)dst->value)[src->length] = '\0';
    dst->length = src->length;

    return SASL_OK;
}

static int
gs2_unescape_authzid(const sasl_utils_t *utils,
                     char **endp,
                     unsigned *remain,
                     char **authzid)
{
    char *in = *endp;
    size_t i, len, inlen = *remain;
    char *p;

    *endp = NULL;

    for (i = 0, len = 0; i < inlen; i++) {
        if (in[i] == ',') {
            *endp = &in[i];
            *remain -= i;
            break;
        } else if (in[i] == '=') {
            if (inlen <= i + 2)
                return SASL_BADPROT;
            i += 2;
        }
        len++;
    }

    if (len == 0 || *endp == NULL)
        return SASL_BADPROT;

    p = *authzid = utils->malloc(len + 1);
    if (*authzid == NULL)
        return SASL_NOMEM;

    for (i = 0; i < inlen; i++) {
        if (in[i] == ',')
            break;
        else if (in[i] == '=') {
            if (memcmp(&in[i + 1], "2C", 2) == 0)
                *p++ = ',';
            else if (memcmp(&in[i + 1], "3D", 2) == 0)
                *p++ = '=';
            else {
                utils->free(*authzid);
                *authzid = NULL;
                return SASL_BADPROT;
            }
            i += 2;
        } else
            *p++ = in[i];
    }

    *p = '\0';

    return SASL_OK;
}

static int
gs2_escape_authzid(const sasl_utils_t *utils,
                   const char *in,
                   unsigned inlen,
                   char **authzid)
{
    size_t i;
    char *p;

    p = *authzid = utils->malloc((inlen * 3) + 1);
    if (*authzid == NULL)
        return SASL_NOMEM;

    for (i = 0; i < inlen; i++) {
        if (in[i] == ',') {
            memcpy(p, "=2C", 3);
            p += 3;
        } else if (in[i] == '=') {
            memcpy(p, "=3D", 3);
            p += 3;
        } else {
            *p++ = in[i];
        }
    }

    *p = '\0';

    return SASL_OK;
}

#define GOT_CREDS(text, params) ((text)->client_creds != NULL || (params)->gss_creds != NULL)
#define CRED_ERROR(status)      ((status) == GSS_S_CRED_UNAVAIL || (status) == GSS_S_NO_CRED)

/*
 * Determine the authentication identity from the application supplied
 * GSS credential, the application supplied identity, and the default
 * GSS credential, in that order. Then, acquire credentials.
 */
static int
gs2_get_init_creds(context_t *text,
                   sasl_client_params_t *params,
                   sasl_interact_t **prompt_need,
                   sasl_out_params_t *oparams)
{
    int result = SASL_OK;
    const char *authid = NULL, *userid = NULL;
    int user_result = SASL_OK;
    int auth_result = SASL_OK;
    int pass_result = SASL_OK;
    OM_uint32 maj_stat = GSS_S_COMPLETE, min_stat = 0;
    gss_OID_set_desc mechs;
    gss_buffer_desc cred_authid = GSS_C_EMPTY_BUFFER;
    gss_buffer_desc name_buf = GSS_C_EMPTY_BUFFER;

    mechs.count = 1;
    mechs.elements = (gss_OID)text->mechanism;

    /*
     * Get the authentication identity from the application.
     */
    if (oparams->authid == NULL) {
        auth_result = _plug_get_authid(params->utils, &authid, prompt_need);
        if (auth_result != SASL_OK && auth_result != SASL_INTERACT) {
            result = auth_result;
            goto cleanup;
        }
    }

    /*
     * Get the authorization identity from the application.
     */
    if (oparams->user == NULL) {
        user_result = _plug_get_userid(params->utils, &userid, prompt_need);
        if (user_result != SASL_OK && user_result != SASL_INTERACT) {
            result = user_result;
            goto cleanup;
        }
    }

    /*
     * Canonicalize the authentication and authorization identities before
     * calling GSS_Import_name.
     */
    if (auth_result == SASL_OK && user_result == SASL_OK &&
        oparams->authid == NULL) {
        if (userid == NULL || userid[0] == '\0') {
            result = params->canon_user(params->utils->conn, authid, 0,
                                        SASL_CU_AUTHID | SASL_CU_AUTHZID,
                                        oparams);
        } else {
            result = params->canon_user(params->utils->conn,
                                        authid, 0, SASL_CU_AUTHID, oparams);
            if (result != SASL_OK)
                goto cleanup;

            result = params->canon_user(params->utils->conn,
                                        userid, 0, SASL_CU_AUTHZID, oparams);
            if (result != SASL_OK)
                goto cleanup;
        }

        if (oparams->authid != NULL) {
            name_buf.length = strlen(oparams->authid);
            name_buf.value = (void *)oparams->authid;

            assert(text->client_name == GSS_C_NO_NAME);

            maj_stat = gss_import_name(&min_stat,
                                       &name_buf,
                                       GSS_C_NT_USER_NAME,
                                       &text->client_name);
            if (GSS_ERROR(maj_stat))
                goto cleanup;
        }
    }

    /*
     * If application didn't provide an authid, then use the default
     * credential. If that doesn't work, give up.
     */
    if (!GOT_CREDS(text, params) && oparams->authid == NULL) {
        maj_stat = gss_acquire_cred(&min_stat,
                                    GSS_C_NO_NAME,
                                    GSS_C_INDEFINITE,
                                    &mechs,
                                    GSS_C_INITIATE,
                                    &text->client_creds,
                                    NULL,
                                    &text->lifetime);
        if (GSS_ERROR(maj_stat))
            goto cleanup;

        assert(text->client_name == GSS_C_NO_NAME);

        maj_stat = gss_inquire_cred(&min_stat,
                                    params->gss_creds
                                        ? (gss_cred_id_t)params->gss_creds
                                        : text->client_creds,
                                    &text->client_name,
                                    NULL,
                                    NULL,
                                    NULL);
        if (GSS_ERROR(maj_stat))
            goto cleanup;

        maj_stat = gss_display_name(&min_stat,
                                    text->client_name,
                                    &cred_authid,
                                    NULL);
        if (GSS_ERROR(maj_stat))
            goto cleanup;

        if (userid == NULL || userid[0] == '\0') {
            result = params->canon_user(params->utils->conn,
                                        cred_authid.value, cred_authid.length,
                                        SASL_CU_AUTHID | SASL_CU_AUTHZID,
                                        oparams);
        } else {
            result = params->canon_user(params->utils->conn,
                                        cred_authid.value, cred_authid.length,
                                        SASL_CU_AUTHID, oparams);
            if (result != SASL_OK)
                goto cleanup;

            result = params->canon_user(params->utils->conn,
                                        cred_authid.value, cred_authid.length,
                                        SASL_CU_AUTHZID, oparams);
            if (result != SASL_OK)
                goto cleanup;
        }
    }

    /*
     * Armed with the authentication identity, try to get a credential without
     * a password.
     */
    if (!GOT_CREDS(text, params) && text->client_name != GSS_C_NO_NAME) {
        maj_stat = gss_acquire_cred(&min_stat,
                                    text->client_name,
                                    GSS_C_INDEFINITE,
                                    &mechs,
                                    GSS_C_INITIATE,
                                    &text->client_creds,
                                    NULL,
                                    &text->lifetime);
        if (GSS_ERROR(maj_stat) && !CRED_ERROR(maj_stat))
            goto cleanup;
    }

    /*
     * If that failed, try to get a credential with a password.
     */
    if (!GOT_CREDS(text, params)) {
        if (text->password == NULL) {
            pass_result = _plug_get_password(params->utils, &text->password,
                                             &text->free_password, prompt_need);
            if (pass_result != SASL_OK && pass_result != SASL_INTERACT) {
                result = pass_result;
                goto cleanup;
            }
        }

        if (text->password != NULL) {
            gss_buffer_desc password_buf;

            password_buf.length = text->password->len;
            password_buf.value = text->password->data;

            maj_stat = gss_acquire_cred_with_password(&min_stat,
                                                      text->client_name,
                                                      &password_buf,
                                                      GSS_C_INDEFINITE,
                                                      &mechs,
                                                      GSS_C_INITIATE,
                                                      &text->client_creds,
                                                      NULL,
                                                      &text->lifetime);
            if (GSS_ERROR(maj_stat))
                goto cleanup;
        }
    }

    maj_stat = GSS_S_COMPLETE;

    /* free prompts we got */
    if (prompt_need && *prompt_need) {
        params->utils->free(*prompt_need);
        *prompt_need = NULL;
    }

    /* if there are prompts not filled in */
    if (user_result == SASL_INTERACT || auth_result == SASL_INTERACT ||
        pass_result == SASL_INTERACT) {
        /* make the prompt list */
        result =
            _plug_make_prompts(params->utils, prompt_need,
                               user_result == SASL_INTERACT ?
                               "Please enter your authorization name" : NULL,
                               NULL,
                               auth_result == SASL_INTERACT ?
                               "Please enter your authentication name" : NULL,
                               NULL,
                               pass_result == SASL_INTERACT ?
                               "Please enter your password" : NULL, NULL,
                               NULL, NULL, NULL,
                               NULL,
                               NULL, NULL);
        if (result == SASL_OK)
            result = SASL_INTERACT;
    }

cleanup:
    if (result == SASL_OK && maj_stat != GSS_S_COMPLETE) {
        sasl_gs2_seterror(text->utils, maj_stat, min_stat);
        result = SASL_FAIL;
    }

    gss_release_buffer(&min_stat, &cred_authid);

    return result;
}

static int
sasl_gs2_seterror_(const sasl_utils_t *utils, OM_uint32 maj, OM_uint32 min,
                   int logonly)
{
    OM_uint32 maj_stat, min_stat;
    gss_buffer_desc msg;
    OM_uint32 msg_ctx;
    int ret;
    char *out = NULL;
    unsigned int len, curlen = 0;
    const char prefix[] = "GS2 Error: ";

    len = sizeof(prefix);
    ret = _plug_buf_alloc(utils, &out, &curlen, 256);
    if (ret != SASL_OK)
        return SASL_OK;

    strcpy(out, prefix);

    msg_ctx = 0;
    while (1) {
        maj_stat = gss_display_status(&min_stat, maj,
                                      GSS_C_GSS_CODE, GSS_C_NULL_OID,
                                      &msg_ctx, &msg);

        if (GSS_ERROR(maj_stat)) {
            if (logonly) {
                utils->log(utils->conn, SASL_LOG_FAIL,
                        "GS2 Failure: (could not get major error message)");
            } else {
                utils->seterror(utils->conn, 0,
                                "GS2 Failure "
                                "(could not get major error message)");
            }
            utils->free(out);
            return SASL_OK;
        }

        len += len + msg.length;
        ret = _plug_buf_alloc(utils, &out, &curlen, len);
        if (ret != SASL_OK) {
            utils->free(out);
            return SASL_OK;
        }

        strcat(out, msg.value);

        gss_release_buffer(&min_stat, &msg);

        if (!msg_ctx)
            break;
    }

    /* Now get the minor status */

    len += 2;
    ret = _plug_buf_alloc(utils, &out, &curlen, len);
    if (ret != SASL_OK) {
        utils->free(out);
        return SASL_NOMEM;
    }

    strcat(out, " (");

    msg_ctx = 0;
    while (1) {
        maj_stat = gss_display_status(&min_stat, min,
                                      GSS_C_MECH_CODE, GSS_C_NULL_OID,
                                      &msg_ctx, &msg);

        if (GSS_ERROR(maj_stat)) {
            if (logonly) {
                utils->log(utils->conn, SASL_LOG_FAIL,
                        "GS2 Failure: (could not get minor error message)");
            } else {
                utils->seterror(utils->conn, 0,
                                "GS2 Failure "
                                "(could not get minor error message)");
            }
            utils->free(out);
            return SASL_OK;
        }

        len += len + msg.length;

        ret = _plug_buf_alloc(utils, &out, &curlen, len);
        if (ret != SASL_OK) {
            utils->free(out);
            return SASL_NOMEM;
        }

        strcat(out, msg.value);

        gss_release_buffer(&min_stat, &msg);

        if (!msg_ctx)
            break;
    }

    len += 1;
    ret = _plug_buf_alloc(utils, &out, &curlen, len);
    if (ret != SASL_OK) {
        utils->free(out);
        return SASL_NOMEM;
    }

    strcat(out, ")");

    if (logonly) {
        utils->log(utils->conn, SASL_LOG_FAIL, "%s", out);
    } else {
        utils->seterror(utils->conn, 0, "%s", out);
    }
    utils->free(out);

    return SASL_OK;
}
