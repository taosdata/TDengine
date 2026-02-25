#ifndef _TD_TLS_TTQ_H_
#define _TD_TLS_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#ifdef WITH_TLS

#define SSL_DATA_PENDING(A) ((A)->ssl && SSL_pending((A)->ssl))

#include <openssl/engine.h>
#include <openssl/ssl.h>

int tmqtt__server_certificate_verify(int preverify_ok, X509_STORE_CTX *ctx);
int tmqtt__verify_certificate_hostname(X509 *cert, const char *hostname);

#else

#define SSL_DATA_PENDING(A) 0

#endif /* WITH_TLS */

#ifdef __cplusplus
}
#endif

#endif /*_TD_TLS_TTQ_H_*/
