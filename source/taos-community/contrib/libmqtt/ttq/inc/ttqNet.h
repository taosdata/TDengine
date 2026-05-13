#ifndef _TD_NET_TTQ_H_
#define _TD_NET_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/socket.h>
#include <unistd.h>

#include "tmqttInt.h"
#include "ttq.h"

#define COMPAT_CLOSE(a)    close(a)
#define COMPAT_ECONNRESET  ECONNRESET
#define COMPAT_EINTR       EINTR
#define COMPAT_EWOULDBLOCK EWOULDBLOCK

/* For when not using winsock libraries. */
#ifndef INVALID_SOCKET
#define INVALID_SOCKET -1
#endif
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

/* accessing the MSB and LSB of a uint16_t */
#define TTQ_MSB(A) (uint8_t)((A & 0xFF00) >> 8)
#define TTQ_LSB(A) (uint8_t)(A & 0x00FF)

int  net__init(void);
void net__cleanup(void);
int  net__socket_connect(struct tmqtt *ttq, const char *host, uint16_t port, const char *bind_address, bool blocking);
int  net__socket_close(struct tmqtt *ttq);
int  net__try_connect(const char *host, uint16_t port, ttq_sock_t *sock, const char *bind_address, bool blocking);
int  net__try_connect_step1(struct tmqtt *ttq, const char *host);
int  net__try_connect_step2(struct tmqtt *ttq, uint16_t port, ttq_sock_t *sock);
int  net__socket_connect_step3(struct tmqtt *ttq, const char *host);
int  net__socket_nonblock(ttq_sock_t *sock);
int  net__socketpair(ttq_sock_t *sp1, ttq_sock_t *sp2);
ssize_t net__read(struct tmqtt *ttq, void *buf, size_t count);
ssize_t net__write(struct tmqtt *ttq, const void *buf, size_t count);

#ifdef WITH_TLS
void       net__init_tls(void);
void       net__print_ssl_error(struct tmqtt *ttq);
int        net__socket_apply_tls(struct tmqtt *ttq);
int        net__socket_connect_tls(struct tmqtt *ttq);
int        tmqtt__verify_ocsp_status_cb(SSL *ssl, void *arg);
UI_METHOD *net__get_ui_method(void);

#define ENGINE_FINISH(e) \
  if (e) ENGINE_finish(e)
#define ENGINE_SECRET_MODE     "SECRET_MODE"
#define ENGINE_SECRET_MODE_SHA 0x1000
#define ENGINE_PIN             "PIN"

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_NET_TTQ_H_*/
