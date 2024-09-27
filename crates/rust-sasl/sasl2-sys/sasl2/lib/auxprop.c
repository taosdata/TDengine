/* auxprop.c - auxilliary property support
 * Rob Siemborski
 */
/* 
 * Copyright (c) 1998-2016 Carnegie Mellon University.  All rights reserved.
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
#include <sasl.h>
#include <prop.h>
#include <ctype.h>
#include <stdio.h>
#include "saslint.h"

struct proppool 
{
    struct proppool *next;

    size_t size;          /* Size of Block */
    size_t unused;        /* Space unused in this pool between end
			   * of char** area and beginning of char* area */

    char data[1];         /* Variable Sized */
};

struct propctx  {
    struct propval *values;
    struct propval *prev_val; /* Previous value used by set/setvalues */

    unsigned used_values, allocated_values;

    char *data_end; /* Bottom of string area in current pool */
    char **list_end; /* Top of list area in current pool */

    struct proppool *mem_base;
    struct proppool *mem_cur;
};

typedef struct auxprop_plug_list 
{
    struct auxprop_plug_list *next;
    const sasl_auxprop_plug_t *plug;
} auxprop_plug_list_t;

static auxprop_plug_list_t *auxprop_head = NULL;

static struct proppool *alloc_proppool(size_t size) 
{
    struct proppool *ret;
    /* minus 1 for the one that is already a part of the array
     * in the struct */
    size_t total_size = sizeof(struct proppool) + size - 1;
    ret = sasl_ALLOC(total_size);
    if(!ret) return NULL;

    memset(ret, 0, total_size);

    ret->size = ret->unused = size;

    return ret;
}

/* Resize a proppool.  Invalidates the unused value for this pool */
static struct proppool *resize_proppool(struct proppool *pool, size_t size)
{
    struct proppool *ret;
    
    if(pool->size >= size) return pool;
    ret = sasl_REALLOC(pool, sizeof(struct proppool) + size);
    if(!ret) return NULL;

    ret->size = size;

    return ret;
}

static int prop_init(struct propctx *ctx, unsigned estimate) 
{
    const unsigned VALUES_SIZE = PROP_DEFAULT * sizeof(struct propval);

    ctx->mem_base = alloc_proppool(VALUES_SIZE + estimate);
    if(!ctx->mem_base) return SASL_NOMEM;

    ctx->mem_cur = ctx->mem_base;

    ctx->values = (struct propval *)ctx->mem_base->data;
    ctx->mem_base->unused = ctx->mem_base->size - VALUES_SIZE;
    ctx->allocated_values = PROP_DEFAULT;
    ctx->used_values = 0;

    ctx->data_end = ctx->mem_base->data + ctx->mem_base->size;
    ctx->list_end = (char **)(ctx->mem_base->data + VALUES_SIZE);

    ctx->prev_val = NULL;

    return SASL_OK;
}

/* create a property context
 *  estimate -- an estimate of the storage needed for requests & responses
 *              0 will use module default
 * returns NULL on error
 */
struct propctx *prop_new(unsigned estimate) 
{
    struct propctx *new_ctx;

    if(!estimate) estimate = PROP_DEFAULT * 255;

    new_ctx = sasl_ALLOC(sizeof(struct propctx));
    if(!new_ctx) return NULL;

    if(prop_init(new_ctx, estimate) != SASL_OK) {
	prop_dispose(&new_ctx);
    }

    return new_ctx;
}

/* create new propctx which duplicates the contents of an existing propctx
 * returns -1 on error
 */
int prop_dup(struct propctx *src_ctx, struct propctx **dst_ctx) 
{
    struct proppool *pool;
    struct propctx *retval = NULL;
    unsigned i;
    int result;
    unsigned total_size = 0;
    size_t values_size;
    
    if(!src_ctx || !dst_ctx) return SASL_BADPARAM;

    /* What is the total allocated size of src_ctx? */
    pool = src_ctx->mem_base;
    while(pool) {
	total_size += (unsigned) pool->size;
	pool = pool->next;
    }

    /* allocate the new context */
    retval = prop_new(total_size);
    if(!retval) return SASL_NOMEM;

    retval->used_values = src_ctx->used_values;
    retval->allocated_values = src_ctx->used_values + 1;

    values_size = (retval->allocated_values * sizeof(struct propval));

    retval->mem_base->unused = retval->mem_base->size - values_size;

    retval->list_end = (char **)(retval->mem_base->data + values_size);
    /* data_end should still be OK */

    /* Now dup the values */
    for(i=0; i<src_ctx->used_values; i++) {
	retval->values[i].name = src_ctx->values[i].name;
	result = prop_setvals(retval, retval->values[i].name,
			      src_ctx->values[i].values);
	if(result != SASL_OK)
	    goto fail;
    }

    retval->prev_val = src_ctx->prev_val;

    *dst_ctx = retval;
    return SASL_OK;

    fail:
    if(retval) prop_dispose(&retval);
    return result;
}

/*
 * dispose of property context
 *  ctx      -- is disposed and set to NULL; noop if ctx or *ctx is NULL
 */
void prop_dispose(struct propctx **ctx)
{
    struct proppool *tmp;
    
    if(!ctx || !*ctx) return;

    while((*ctx)->mem_base) {
	tmp = (*ctx)->mem_base;
	(*ctx)->mem_base = tmp->next;
	sasl_FREE(tmp);
    }
    
    sasl_FREE(*ctx);
    *ctx = NULL;

    return;
}

/* Add property names to request
 *  ctx       -- context from prop_new()
 *  names     -- list of property names; must persist until context freed
 *               or requests cleared
 *
 * NOTE: may clear values from context as side-effect
 * returns -1 on error
 */
int prop_request(struct propctx *ctx, const char **names) 
{
    unsigned i, new_values, total_values;

    if(!ctx || !names) return SASL_BADPARAM;

    /* Count how many we need to add */
    for(new_values=0; names[new_values]; new_values++);

    /* Do we need to add ANY? */
    if(!new_values) return SASL_OK;

    /* We always want at least one extra to mark the end of the array */
    total_values = new_values + ctx->used_values + 1;

    /* Do we need to increase the size of our propval table? */
    if(total_values > ctx->allocated_values) {
	unsigned max_in_pool;

	/* Do we need a larger base pool? */
	max_in_pool = (unsigned) (ctx->mem_base->size / sizeof(struct propval));
	
	if(total_values <= max_in_pool) {
	    /* Don't increase the size of the base pool, just use what
	       we need */
	    ctx->allocated_values = total_values;
	    ctx->mem_base->unused =
		ctx->mem_base->size - (sizeof(struct propval)
				       * ctx->allocated_values);
      	} else {
	    /* We need to allocate more! */
	    unsigned new_alloc_length;
	    size_t new_size;

	    new_alloc_length = 2 * ctx->allocated_values;
	    while(total_values > new_alloc_length) {
		new_alloc_length *= 2;
	    }

	    new_size = new_alloc_length * sizeof(struct propval);
	    ctx->mem_base = resize_proppool(ctx->mem_base, new_size);

	    if(!ctx->mem_base) {
		ctx->values = NULL;
		ctx->allocated_values = ctx->used_values = 0;
		return SASL_NOMEM;
	    }

	    /* It worked! Update the structure! */
	    ctx->values = (struct propval *)ctx->mem_base->data;
	    ctx->allocated_values = new_alloc_length;
	    ctx->mem_base->unused = ctx->mem_base->size
		- sizeof(struct propval) * ctx->allocated_values;
	}

	/* Clear out new propvals */
	memset(&(ctx->values[ctx->used_values]), 0,
	       sizeof(struct propval) * (ctx->allocated_values - ctx->used_values));

        /* Finish updating the context -- we've extended the list! */
	/* ctx->list_end = (char **)(ctx->values + ctx->allocated_values); */
	/* xxx test here */
	ctx->list_end = (char **)(ctx->values + total_values);
    }

    /* Now do the copy, or referencing rather */
    for(i=0;i<new_values;i++) {
	unsigned j, flag;

	flag = 0;

	/* Check for dups */
	for(j=0;j<ctx->used_values;j++) {
	    if(!strcmp(ctx->values[j].name, names[i])) {
		flag = 1;
		break;
	    }
	}

	/* We already have it... skip! */
	if(flag) continue;

	ctx->values[ctx->used_values++].name = names[i];
    }

    prop_clear(ctx, 0);

    return SASL_OK;
}

/* return array of struct propval from the context
 *  return value persists until next call to
 *   prop_request, prop_clear or prop_dispose on context
 */
const struct propval *prop_get(struct propctx *ctx) 
{
    if(!ctx) return NULL;
    
    return ctx->values;
}

/* Fill in an array of struct propval based on a list of property names
 *  return value persists until next call to
 *   prop_request, prop_clear or prop_dispose on context
 *  returns -1 on error (no properties ever requested, ctx NULL, etc)
 *  returns number of matching properties which were found (values != NULL)
 *  if a name requested here was never requested by a prop_request, then
 *  the name field of the associated vals entry will be set to NULL
 */
int prop_getnames(struct propctx *ctx, const char **names,
		  struct propval *vals) 
{
    int found_names = 0;
    
    struct propval *cur = vals;
    const char **curname;

    if(!ctx || !names || !vals) return SASL_BADPARAM;
    
    for(curname = names; *curname; curname++) {
	struct propval *val;
	for(val = ctx->values; val->name; val++) {
	    if(!strcmp(*curname,val->name)) { 
		found_names++;
		memcpy(cur, val, sizeof(struct propval));
		goto next;
	    }
	}

	/* If we are here, we didn't find it */
	memset(cur, 0, sizeof(struct propval));
	
	next:
	cur++;
    }

    return found_names;
}


/* clear values and optionally requests from property context
 *  ctx      -- property context
 *  requests -- 0 = don't clear requests, 1 = clear requests
 */
void prop_clear(struct propctx *ctx, int requests) 
{
    struct proppool *new_pool, *tmp;
    unsigned i;

    /* We're going to need a new proppool once we reset things */
    new_pool = alloc_proppool(ctx->mem_base->size +
			      (ctx->used_values+1) * sizeof(struct propval));
    if (new_pool == NULL) {
        _sasl_log(NULL, SASL_LOG_ERR, "failed to allocate memory\n");
        exit(1);
    }

    if(requests) {
	/* We're wiping the whole shebang */
	ctx->used_values = 0;
    } else {
	/* Need to keep around old requets */
	struct propval *new_values = (struct propval *)new_pool->data;
	for(i=0; i<ctx->used_values; i++) {
	    new_values[i].name = ctx->values[i].name;
	}
    }

    while(ctx->mem_base) {
	tmp = ctx->mem_base;
	ctx->mem_base = tmp->next;
	sasl_FREE(tmp);
    }
    
    /* Update allocation-related metadata */
    ctx->allocated_values = ctx->used_values+1;
    new_pool->unused =
	new_pool->size - (ctx->allocated_values * sizeof(struct propval));

    /* Setup pointers for the values array */
    ctx->values = (struct propval *)new_pool->data;
    ctx->prev_val = NULL;

    /* Setup the pools */
    ctx->mem_base = ctx->mem_cur = new_pool;

    /* Reset list_end and data_end for the new memory pool */
    ctx->list_end =
	(char **)((char *)ctx->mem_base->data + ctx->allocated_values * sizeof(struct propval));
    ctx->data_end = (char *)ctx->mem_base->data + ctx->mem_base->size;

    return;
}

/*
 * erase the value of a property
 */
void prop_erase(struct propctx *ctx, const char *name)
{
    struct propval *val;
    int i;

    if(!ctx || !name) return;

    for(val = ctx->values; val->name; val++) {
	if(!strcmp(name,val->name)) {
	    if(!val->values) break;

	    /*
	     * Yes, this is casting away the const, but
	     * we should be okay because the only place this
	     * memory should be is in the proppool's
	     */
	    for(i=0;val->values[i];i++) {
		memset((void *)(val->values[i]),0,strlen(val->values[i]));
		val->values[i] = NULL;
	    }

	    val->values = NULL;
	    val->nvalues = 0;
	    val->valsize = 0;
	    break;
	}
    }
    
    return;
}

/****fetcher interfaces****/

/* format the requested property names into a string
 *  ctx    -- context from prop_new()/prop_request()
 *  sep    -- separator between property names (unused if none requested)
 *  seplen -- length of separator, if < 0 then strlen(sep) will be used
 *  outbuf -- output buffer
 *  outmax -- maximum length of output buffer including NUL terminator
 *  outlen -- set to length of output string excluding NUL terminator
 * returns 0 on success and amount of additional space needed on failure
 */
int prop_format(struct propctx *ctx, const char *sep, int seplen,
		char *outbuf, unsigned outmax, unsigned *outlen) 
{
    unsigned needed, flag = 0;
    struct propval *val;
    
    if (!ctx || !outbuf) return SASL_BADPARAM;

    if (!sep) seplen = 0;    
    if (seplen < 0) seplen = (int) strlen(sep);
/* If seplen is negative now we have overflow.
   But if you have a string longer than 2Gb, you are an idiot anyway */
    if (seplen < 0) return SASL_BADPARAM;

    needed = seplen * (ctx->used_values - 1);
    for(val = ctx->values; val->name; val++) {
	needed += (unsigned) strlen(val->name);
    }
    
    if(!outmax) return (needed + 1); /* Because of unsigned funkiness */
    if(needed > (outmax - 1)) return (needed - (outmax - 1));

    *outbuf = '\0';
    if(outlen) *outlen = needed;

    if(needed == 0) return SASL_OK;

    for(val = ctx->values; val->name; val++) {
	if(seplen && flag) {
	    strncat(outbuf, sep, seplen);
	} else {
	    flag = 1;
	}
	strcat(outbuf, val->name);
    }
    
    return SASL_OK;
}

/* add a property value to the context
 *  ctx    -- context from prop_new()/prop_request()
 *  name   -- name of property to which value will be added
 *            if NULL, add to the same name as previous prop_set/setvals call
 *  value  -- a value for the property; will be copied into context
 *            if NULL, remove existing values
 *  vallen -- length of value, if <= 0 then strlen(value) will be used
 */
int prop_set(struct propctx *ctx, const char *name,
	     const char *value, int vallen)
{
    struct propval *cur;

    if(!ctx) return SASL_BADPARAM;
    if(!name && !ctx->prev_val) return SASL_BADPARAM; 

    if(name) {
	struct propval *val;

	ctx->prev_val = NULL;
	
	for(val = ctx->values; val->name; val++) {
	    if(!strcmp(name,val->name)){
		ctx->prev_val = val;
		break;
	    }
	}

	/* Couldn't find it! */
	if(!ctx->prev_val) return SASL_BADPARAM;
    }

    cur = ctx->prev_val;

    if(name) /* New Entry */ {
	unsigned nvalues = 1; /* 1 for NULL entry */
	const char **old_values = NULL;
	char **tmp, **tmp2;
	size_t size;
	
	if(cur->values) {

	    if(!value) {
		/* If we would be adding a null value, then we are done */
		return SASL_OK;
	    }

	    old_values = cur->values;
	    tmp = (char **)cur->values;
	    while(*tmp) {
		nvalues++;
		tmp++;
	    }

	}

	if(value) {
	    nvalues++; /* for the new value */
	}

	size = nvalues * sizeof(char*);

	if(size > ctx->mem_cur->unused) {
	    size_t needed;

	    for(needed = ctx->mem_cur->size * 2; needed < size; needed *= 2);

	    /* Allocate a new proppool */
	    ctx->mem_cur->next = alloc_proppool(needed);
	    if(!ctx->mem_cur->next) return SASL_NOMEM;

	    ctx->mem_cur = ctx->mem_cur->next;

	    ctx->list_end = (char **)ctx->mem_cur->data;
	    ctx->data_end = ctx->mem_cur->data + needed;
	}

	/* Grab the memory */
	ctx->mem_cur->unused -= size;
	cur->values = (const char **)ctx->list_end;
	cur->values[nvalues - 1] = NULL;

	/* Finish updating the context */
	ctx->list_end = (char **)(cur->values + nvalues);

	/* If we don't have an actual value to fill in, we are done */
	if(!value)
	    return SASL_OK;

	tmp2 = (char **)cur->values;
	if(old_values) {
	    tmp = (char **)old_values;
	    
	    while(*tmp) {
		*tmp2 = *tmp;
		tmp++; tmp2++;
	    }
	}
	    
	/* Now allocate the last entry */
	if(vallen <= 0)
	    size = (size_t)(strlen(value) + 1);
	else
	    size = (size_t)(vallen + 1);

	if(size > ctx->mem_cur->unused) {
	    size_t needed;
	    
	    needed = ctx->mem_cur->size * 2;
	    
	    while(needed < size) {
		needed *= 2;
	    }

	    /* Allocate a new proppool */
	    ctx->mem_cur->next = alloc_proppool(needed);
	    if(!ctx->mem_cur->next) return SASL_NOMEM;

	    ctx->mem_cur = ctx->mem_cur->next;
	    ctx->list_end = (char **)ctx->mem_cur->data;
	    ctx->data_end = ctx->mem_cur->data + needed;
	}

	/* Update the data_end pointer */
	ctx->data_end -= size;
	ctx->mem_cur->unused -= size;

	/* Copy and setup the new value! */
	memcpy(ctx->data_end, value, size-1);
	ctx->data_end[size - 1] = '\0';
	cur->values[nvalues - 2] = ctx->data_end;

	cur->nvalues++;
	cur->valsize += ((unsigned) size - 1);
    } else /* Appending an entry */ {
	char **tmp;
	size_t size;

	/* If we are setting it to be NULL, we are done */
	if(!value) return SASL_OK;

	size = sizeof(char*);

	/* Is it in the current pool, and will it fit in the unused space? */
	if(size > ctx->mem_cur->unused &&
	    (void *)cur->values > (void *)(ctx->mem_cur->data) &&
	    (void *)cur->values < (void *)(ctx->mem_cur->data + ctx->mem_cur->size)) {
	    /* recursively call the not-fast way */
	    return prop_set(ctx, cur->name, value, vallen);
	}

	/* Note the invariant: the previous value list must be
	   at the top of the CURRENT pool at this point */

	/* Grab the memory */
	ctx->mem_cur->unused -= size;
	ctx->list_end++;

	*(ctx->list_end - 1) = NULL;
	tmp = (ctx->list_end - 2);

	/* Now allocate the last entry */
	if(vallen <= 0)
	    size = strlen(value) + 1;
	else
	    size = vallen + 1;

	if(size > ctx->mem_cur->unused) {
	    size_t needed;
	    
	    needed = ctx->mem_cur->size * 2;
	    
	    while(needed < size) {
		needed *= 2;
	    }

	    /* Allocate a new proppool */
	    ctx->mem_cur->next = alloc_proppool(needed);
	    if(!ctx->mem_cur->next) return SASL_NOMEM;

	    ctx->mem_cur = ctx->mem_cur->next;
	    ctx->list_end = (char **)ctx->mem_cur->data;
	    ctx->data_end = ctx->mem_cur->data + needed;
	}

	/* Update the data_end pointer */
	ctx->data_end -= size;
	ctx->mem_cur->unused -= size;

	/* Copy and setup the new value! */
	memcpy(ctx->data_end, value, size-1);
	ctx->data_end[size - 1] = '\0';
	*tmp = ctx->data_end;

	cur->nvalues++;
	cur->valsize += ((unsigned) size - 1);
    }
    
    return SASL_OK;
}


/* set the values for a property
 *  ctx    -- context from prop_new()/prop_request()
 *  name   -- name of property to which value will be added
 *            if NULL, add to the same name as previous prop_set/setvals call
 *  values -- array of values, ending in NULL.  Each value is a NUL terminated
 *            string
 */
int prop_setvals(struct propctx *ctx, const char *name,
		 const char **values)
{
    const char **val = values;
    int result = SASL_OK;

    if(!ctx) return SASL_BADPARAM;

    /* If they want us to add no values, we can do that */
    if(!values) return SASL_OK;
    
    /* Basically, use prop_set to do all our dirty work for us */
    if(name) {
	result = prop_set(ctx, name, *val, 0);
	val++;
    }

    for(;*val;val++) {
	if(result != SASL_OK) return result;
	result = prop_set(ctx, NULL, *val,0);
    }

    return result;
}

/* Request a set of auxiliary properties
 *  conn         connection context
 *  propnames    list of auxiliary property names to request ending with
 *               NULL.  
 *
 * Subsequent calls will add items to the request list.  Call with NULL
 * to clear the request list.
 *
 * errors
 *  SASL_OK       -- success
 *  SASL_BADPARAM -- bad count/conn parameter
 *  SASL_NOMEM    -- out of memory
 */
int sasl_auxprop_request(sasl_conn_t *conn, const char **propnames) 
{
    int result;
    sasl_server_conn_t *sconn;

    if(!conn) return SASL_BADPARAM;
    if(conn->type != SASL_CONN_SERVER)
	PARAMERROR(conn);
    
    sconn = (sasl_server_conn_t *)conn;

    if(!propnames) {
	prop_clear(sconn->sparams->propctx,1);
	return SASL_OK;
    }
    
    result = prop_request(sconn->sparams->propctx, propnames);
    RETURN(conn, result);
}


/* Returns current auxiliary property context.
 * Use functions in prop.h to access content
 *
 *  if authentication hasn't completed, property values may be empty/NULL
 *
 *  properties not recognized by active plug-ins will be left empty/NULL
 *
 *  returns NULL if conn is invalid.
 */
struct propctx *sasl_auxprop_getctx(sasl_conn_t *conn) 
{
    sasl_server_conn_t *sconn;
    
    if(!conn || conn->type != SASL_CONN_SERVER) return NULL;

    sconn = (sasl_server_conn_t *)conn;

    return sconn->sparams->propctx;
}

/* add an auxiliary property plugin */
int sasl_auxprop_add_plugin(const char *plugname,
			    sasl_auxprop_init_t *auxpropfunc)
{
    int result, out_version;
    auxprop_plug_list_t *new_item;
    sasl_auxprop_plug_t *plug;
    
    result = auxpropfunc(sasl_global_utils, SASL_AUXPROP_PLUG_VERSION,
			 &out_version, &plug, plugname);

    /* Check if out_version is too old.
       We only support the current at the moment */
    if (result == SASL_OK && out_version < SASL_AUXPROP_PLUG_VERSION) {
        result = SASL_BADVERS;
    }

    if(result != SASL_OK) {
	_sasl_log(NULL, SASL_LOG_ERR, "auxpropfunc error %s\n",
		  sasl_errstring(result, NULL, NULL));
	return result;
    }

    /* We require that this function is implemented */
    if(!plug->auxprop_lookup) return SASL_BADPROT;

    new_item = sasl_ALLOC(sizeof(auxprop_plug_list_t));
    if(!new_item) return SASL_NOMEM;    

    /* These will load from least-important to most important */
    new_item->plug = plug;
    new_item->next = auxprop_head;
    auxprop_head = new_item;

    return SASL_OK;
}

void _sasl_auxprop_free() 
{
    auxprop_plug_list_t *ptr, *ptr_next;
    
    for(ptr = auxprop_head; ptr; ptr = ptr_next) {
	ptr_next = ptr->next;
	if(ptr->plug->auxprop_free)
	    ptr->plug->auxprop_free(ptr->plug->glob_context,
				    sasl_global_utils);
	sasl_FREE(ptr);
    }

    auxprop_head = NULL;
}

/* Return the updated account status based on the current ("so far") and
   the specific status returned by the latest auxprop call */
static int
_sasl_account_status (int current_status,
                      int specific_status)
{
    switch (specific_status) {
	case SASL_NOVERIFY:
	    specific_status = SASL_OK;
	    /* fall through */
	case SASL_OK:
	    if (current_status == SASL_NOMECH ||
		current_status == SASL_NOUSER) {
		current_status = specific_status;
	    }
	    break;

	case SASL_NOUSER:
	    if (current_status == SASL_NOMECH) {
		current_status = specific_status;
	    }
	    break;

	/* NOTE: The disabled flag sticks, unless we hit an error */
	case SASL_DISABLED:
	    if (current_status == SASL_NOMECH ||
		current_status == SASL_NOUSER ||
		current_status == SASL_OK) {
		current_status = specific_status;
	    }
	    break;

	case SASL_NOMECH:
	    /* ignore */
	    break;

	/* SASL_UNAVAIL overrides everything */
	case SASL_UNAVAIL:
	    current_status = specific_status;
	    break;

	default:
	    current_status = specific_status;
	    break;
    }
    return (current_status);
}

/* Do the callbacks for auxprop lookups */
int _sasl_auxprop_lookup(sasl_server_params_t *sparams,
			  unsigned flags,
			  const char *user, unsigned ulen) 
{
    sasl_getopt_t *getopt;
    int ret, found = 0;
    void *context;
    const char *plist = NULL;
    auxprop_plug_list_t *ptr;
    int result = SASL_NOMECH;

    if(_sasl_getcallback(sparams->utils->conn,
			 SASL_CB_GETOPT,
			 (sasl_callback_ft *)&getopt,
			 &context) == SASL_OK) {
	ret = getopt(context, NULL, "auxprop_plugin", &plist, NULL);
	if(ret != SASL_OK) plist = NULL;
    }

    if(!plist) {
	/* Do lookup in all plugins */

	/* TODO: Ideally, each auxprop plugin should be marked if its failure
	   should be ignored or treated as a fatal error of the whole lookup. */
	for(ptr = auxprop_head; ptr; ptr = ptr->next) {
	    found=1;
	    ret = ptr->plug->auxprop_lookup(ptr->plug->glob_context,
				      sparams, flags, user, ulen);
	    result = _sasl_account_status (result, ret);
	}
    } else {
	char *pluginlist = NULL, *freeptr = NULL, *thisplugin = NULL;

	if(_sasl_strdup(plist, &pluginlist, NULL) != SASL_OK) return SASL_NOMEM;
	thisplugin = freeptr = pluginlist;
	
	/* Do lookup in all *specified* plugins, in order */
	while(*thisplugin) {
	    char *p;
	    int last=0;
	    
	    while(*thisplugin && isspace((int)*thisplugin)) thisplugin++;
	    if(!(*thisplugin)) break;
	    
	    for(p = thisplugin;*p != '\0' && !isspace((int)*p); p++);
	    if(*p == '\0') last = 1;
	    else *p='\0';
	    
	    for(ptr = auxprop_head; ptr; ptr = ptr->next) {
		/* Skip non-matching plugins */
		if(!ptr->plug->name
		   || strcasecmp(ptr->plug->name, thisplugin))
		    continue;
	    
		found=1;
		ret = ptr->plug->auxprop_lookup(ptr->plug->glob_context,
					  sparams, flags, user, ulen);
		result = _sasl_account_status (result, ret);
	    }

	    if(last) break;

	    thisplugin = p+1;
	}

	sasl_FREE(freeptr);
    }

    if(!found) {
	_sasl_log(sparams->utils->conn, SASL_LOG_DEBUG,
		  "could not find auxprop plugin, was searching for '%s'",
		  plist ? plist : "[all]");
    }

    return result;
}

/* Do the callbacks for auxprop stores */
int sasl_auxprop_store(sasl_conn_t *conn,
		       struct propctx *ctx, const char *user)
{
    sasl_getopt_t *getopt;
    int ret;
    void *context;
    const char *plist = NULL;
    auxprop_plug_list_t *ptr;
    sasl_server_params_t *sparams = NULL;
    unsigned userlen = 0;
    int num_constraint_violations = 0;
    int total_plugins = 0;

    if (ctx) {
	if (!conn || !user)
	    return SASL_BADPARAM;

	sparams = ((sasl_server_conn_t *) conn)->sparams;
	userlen = (unsigned) strlen(user);
    }
    
    /* Pickup getopt callback from the connection, if conn is not NULL */
    if(_sasl_getcallback(conn, SASL_CB_GETOPT, (sasl_callback_ft *)&getopt, &context) == SASL_OK) {
	ret = getopt(context, NULL, "auxprop_plugin", &plist, NULL);
	if(ret != SASL_OK) plist = NULL;
    }

    ret = SASL_OK;
    if(!plist) {
	/* Do store in all plugins */
	for(ptr = auxprop_head; ptr && ret == SASL_OK; ptr = ptr->next) {
	    total_plugins++;
	    if (ptr->plug->auxprop_store) {
		ret = ptr->plug->auxprop_store(ptr->plug->glob_context,
					       sparams, ctx, user, userlen);
		if (ret == SASL_CONSTRAINT_VIOLAT) {
		    ret = SASL_OK;
		    num_constraint_violations++;
		}
	    }
	}
    } else {
	char *pluginlist = NULL, *freeptr = NULL, *thisplugin = NULL;

	if(_sasl_strdup(plist, &pluginlist, NULL) != SASL_OK) return SASL_FAIL;
	thisplugin = freeptr = pluginlist;
	
	/* Do store in all *specified* plugins, in order */
	while(*thisplugin) {
	    char *p;
	    int last=0;
	    
	    while(*thisplugin && isspace((int)*thisplugin)) thisplugin++;
	    if(!(*thisplugin)) break;
	    
	    for(p = thisplugin;*p != '\0' && !isspace((int)*p); p++);
	    if(*p == '\0') last = 1;
	    else *p='\0';
	    
	    for(ptr = auxprop_head; ptr && ret == SASL_OK; ptr = ptr->next) {
		/* Skip non-matching plugins */
		if((!ptr->plug->name
		    || strcasecmp(ptr->plug->name, thisplugin)))
		    continue;

		total_plugins++;
		if (ptr->plug->auxprop_store) {
		    ret = ptr->plug->auxprop_store(ptr->plug->glob_context,
						   sparams, ctx, user, userlen);
		    if (ret == SASL_CONSTRAINT_VIOLAT) {
			ret = SASL_OK;
			num_constraint_violations++;
		    }
		}
	    }

	    if(last) break;

	    thisplugin = p+1;
	}

	sasl_FREE(freeptr);
    }

    if(total_plugins == 0) {
	_sasl_log(NULL, SASL_LOG_ERR,
		  "could not find auxprop plugin, was searching for %s",
		  plist ? plist : "[all]");
	return SASL_FAIL;
    } else if (total_plugins == num_constraint_violations) {
	ret = SASL_CONSTRAINT_VIOLAT;
    }

    return ret;
}

/* It would be nice if we can show other information like Author, Company, Year, plugin version */
static void
_sasl_print_mechanism (sasl_auxprop_plug_t *m,
		       sasl_info_callback_stage_t stage,
		       void *rock __attribute__((unused))
)
{
    if (stage == SASL_INFO_LIST_START) {
	printf ("List of auxprop plugins follows\n");
	return;
    } else if (stage == SASL_INFO_LIST_END) {
	return;
    }

    /* Process the mechanism */
    printf ("Plugin \"%s\" ", m->name);

#ifdef NOT_YET
    switch (m->condition) {
	case SASL_OK:
	    printf ("[loaded]");
	    break;

	case SASL_CONTINUE:
	    printf ("[delayed]");
	    break;

	case SASL_NOUSER:
	    printf ("[no users]");
	    break;

	default:
	    printf ("[unknown]");
	    break;
    }
#endif

    printf (", \tAPI version: %d\n", /* m->version */ SASL_AUXPROP_PLUG_VERSION);

    /* TODO - Update for auxprop_export, etc. */
    printf ("\tsupports store: %s\n",
	    (m->auxprop_store != NULL) ? "yes" : "no"
	    );

    /* No features defined yet */
#ifdef NOT_YET
    printf ("\n\tfeatures:");
#endif

    printf ("\n");
}

/* Dump information about available auxprop plugins (separate functions are
   used for canon and server authentication plugins) */
int auxprop_plugin_info (
  const char *c_mech_list,		/* space separated mechanism list or NULL for ALL */
  auxprop_info_callback_t *info_cb,
  void *info_cb_rock
)
{
    auxprop_plug_list_t *m;
    sasl_auxprop_plug_t plug_data;
    char * cur_mech;
    char *mech_list = NULL;
    char * p;

    if (info_cb == NULL) {
	info_cb = _sasl_print_mechanism;
    }

    if (auxprop_head != NULL) {
	info_cb (NULL, SASL_INFO_LIST_START, info_cb_rock);

	if (c_mech_list == NULL) {
	    m = auxprop_head; /* m point to beginning of the list */

	    while (m != NULL) {
                /* TODO: Need to be careful when dealing with auxprop_export, etc. */
		memcpy (&plug_data, m->plug, sizeof(plug_data));

		info_cb (&plug_data, SASL_INFO_LIST_MECH, info_cb_rock);
	    
		m = m->next;
	    }
	} else {
            mech_list = strdup(c_mech_list);

	    cur_mech = mech_list;

	    while (cur_mech != NULL) {
		p = strchr (cur_mech, ' ');
		if (p != NULL) {
		    *p = '\0';
		    p++;
		}

		m = auxprop_head; /* m point to beginning of the list */

		while (m != NULL) {
		    if (strcasecmp (cur_mech, m->plug->name) == 0) {
			memcpy (&plug_data, m->plug, sizeof(plug_data));

			info_cb (&plug_data, SASL_INFO_LIST_MECH, info_cb_rock);
		    }
	    
		    m = m->next;
		}

		cur_mech = p;
	    }

            free (mech_list);
	}

	info_cb (NULL, SASL_INFO_LIST_END, info_cb_rock);

	return (SASL_OK);
    }

    return (SASL_NOTINIT);
}
