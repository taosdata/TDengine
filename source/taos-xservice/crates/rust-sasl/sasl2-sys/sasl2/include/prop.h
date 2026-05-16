/* prop.h -- property request/response management routines
 *
 * Author: Chris Newman
 * Removal of implementation-specific details by: Rob Siemborski
 *
 * This is intended to be used to create a list of properties to request,
 * and _then_ request values for all properties.  Any change to the request
 * list will discard any existing values.  This assumption allows a very
 * efficient and simple memory model.  This was designed for SASL API auxiliary
 * property support, but would be fine for other contexts where this property
 * model is appropriate.
 *
 * The "struct propctx" is allocated by prop_new and is a fixed size structure.
 * If a prop_init() call were added, it would be reasonable to embed a "struct
 * propctx" in another structure.  prop_new also allocates a pool of memory
 * (in the vbase field) which will be used for an array of "struct propval"
 * to list all the requested properties.
 *
 * Properties may be multi-valued.
 */

#ifndef PROP_H
#define PROP_H 1

/* The following ifdef block is the standard way of creating macros
 * which make exporting from a DLL simpler. All files within this DLL
 * are compiled with the LIBSASL_EXPORTS symbol defined on the command
 * line. this symbol should not be defined on any project that uses
 * this DLL. This way any other project whose source files include
 * this file see LIBSASL_API functions as being imported from a DLL,
 * wheras this DLL sees symbols defined with this macro as being
 * exported.  */
/* Under Unix, life is simpler: we just need to mark library functions
 * as extern.  (Technically, we don't even have to do that.) */
# define LIBSASL_API extern

/* Same as above, but used during a variable declaration. */
# define LIBSASL_VAR extern

/* the resulting structure for property values
 */
struct propval {
    const char *name;	 /* name of property; NULL = end of list */
                         /* same pointer used in request will be used here */
    const char **values; /* list of strings, values == NULL if property not
			  * found, *values == NULL if property found with
			  * no values */
    unsigned nvalues;    /* total number of value strings */
    unsigned valsize;	 /* total size in characters of all value strings */
};

/*
 * private internal structure
 */
#define PROP_DEFAULT 4		/* default number of propvals to assume */
struct propctx;

#ifdef __cplusplus
extern "C" {
#endif

/* create a property context
 *  estimate -- an estimate of the storage needed for requests & responses
 *              0 will use module default
 * returns a new property context on success and NULL on any error
 */
LIBSASL_API struct propctx *prop_new(unsigned estimate);

/* create new propctx which duplicates the contents of an existing propctx
 * returns SASL_OK on success
 * possible other return values include: SASL_NOMEM, SASL_BADPARAM
 */
LIBSASL_API int prop_dup(struct propctx *src_ctx, struct propctx **dst_ctx);

/* Add property names to request
 *  ctx       -- context from prop_new()
 *  names     -- list of property names; must persist until context freed
 *               or requests cleared (This extends to other contexts that
 *               are dup'ed from this one, and their children, etc)
 *
 * NOTE: may clear values from context as side-effect
 * returns SASL_OK on success
 * possible other return values include: SASL_NOMEM, SASL_BADPARAM
 */
LIBSASL_API int prop_request(struct propctx *ctx, const char **names);

/* return array of struct propval from the context
 *  return value persists until next call to
 *   prop_request, prop_clear or prop_dispose on context
 *
 *  returns NULL on error
 */
LIBSASL_API const struct propval *prop_get(struct propctx *ctx);

/* Fill in an array of struct propval based on a list of property names
 *  return value persists until next call to
 *   prop_request, prop_clear or prop_dispose on context
 *  returns number of matching properties which were found (values != NULL)
 *  if a name requested here was never requested by a prop_request, then
 *  the name field of the associated vals entry will be set to NULL
 *
 * The vals array MUST be atleast as long as the names array.
 *
 * returns # of matching properties on success
 * possible other return values include: SASL_BADPARAM
 */
LIBSASL_API int prop_getnames(struct propctx *ctx, const char **names,
		  struct propval *vals);

/* clear values and optionally requests from property context
 *  ctx      -- property context
 *  requests -- 0 = don't clear requests, 1 = clear requests
 */
LIBSASL_API void prop_clear(struct propctx *ctx, int requests);

/* erase the value of a property
 */
LIBSASL_API void prop_erase(struct propctx *ctx, const char *name);

/* dispose of property context
 *  ctx      -- is disposed and set to NULL; noop if ctx or *ctx is NULL
 */
LIBSASL_API void prop_dispose(struct propctx **ctx);


/****fetcher interfaces****/

/* format the requested property names into a string
 *  ctx    -- context from prop_new()/prop_request()
 *  sep    -- separator between property names (unused if none requested)
 *  seplen -- length of separator, if < 0 then strlen(sep) will be used
 *  outbuf -- output buffer
 *  outmax -- maximum length of output buffer including NUL terminator
 *  outlen -- set to length of output string excluding NUL terminator
 * returns SASL_OK on success
 * returns SASL_BADPARAM or amount of additional space needed on failure
 */
LIBSASL_API int prop_format(struct propctx *ctx, const char *sep, int seplen,
		char *outbuf, unsigned outmax, unsigned *outlen);

/* add a property value to the context
 *  ctx    -- context from prop_new()/prop_request()
 *  name   -- name of property to which value will be added
 *            if NULL, add to the same name as previous prop_set/setvals call
 *  value  -- a value for the property; will be copied into context
 *            if NULL, remove existing values
 *  vallen -- length of value, if <= 0 then strlen(value) will be used
 * returns SASL_OK on success
 * possible error return values include: SASL_BADPARAM, SASL_NOMEM
 */
LIBSASL_API int prop_set(struct propctx *ctx, const char *name,
	     const char *value, int vallen);

/* set the values for a property
 *  ctx    -- context from prop_new()/prop_request()
 *  name   -- name of property to which value will be added
 *            if NULL, add to the same name as previous prop_set/setvals call
 *  values -- array of values, ending in NULL.  Each value is a NUL terminated
 *            string
 * returns SASL_OK on success
 * possible error return values include: SASL_BADPARAM, SASL_NOMEM
 */
LIBSASL_API int prop_setvals(struct propctx *ctx, const char *name,
		 const char **values);

#ifdef __cplusplus
}
#endif

#endif /* PROP_H */
