#include "ttqNet.h"

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>

#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif

#ifdef WITH_UNIX_SOCKETS
#include <sys/un.h>
#endif

#ifdef WITH_TLS
#include <openssl/conf.h>
#include <openssl/engine.h>
#include <openssl/err.h>
#include <openssl/ui.h>
#include <ttqTls.h>
#endif

#ifdef WITH_BROKER
#include "tmqttBrokerInt.h"
#ifdef WITH_WEBSOCKETS
#include <libwebsockets.h>
#endif
#else
#include "read_handle.h"
#endif

#include "tmqttProto.h"
#include "ttqLogging.h"
#include "ttqMemory.h"
#include "ttqTime.h"
#include "ttqUtil.h"

#ifdef WITH_TLS
int        tls_ex_index_ttq = -1;
UI_METHOD *_ui_method = NULL;

static bool is_tls_initialized = false;

/* Functions taken from OpenSSL s_server/s_client */
static int ui_open(UI *ui) { return UI_method_get_opener(UI_OpenSSL())(ui); }

static int ui_read(UI *ui, UI_STRING *uis) { return UI_method_get_reader(UI_OpenSSL())(ui, uis); }

static int ui_write(UI *ui, UI_STRING *uis) { return UI_method_get_writer(UI_OpenSSL())(ui, uis); }

static int ui_close(UI *ui) { return UI_method_get_closer(UI_OpenSSL())(ui); }

static void setup_ui_method(void) {
  _ui_method = UI_create_method("OpenSSL application user interface");
  UI_method_set_opener(_ui_method, ui_open);
  UI_method_set_reader(_ui_method, ui_read);
  UI_method_set_writer(_ui_method, ui_write);
  UI_method_set_closer(_ui_method, ui_close);
}

static void cleanup_ui_method(void) {
  if (_ui_method) {
    UI_destroy_method(_ui_method);
    _ui_method = NULL;
  }
}

UI_METHOD *net__get_ui_method(void) { return _ui_method; }

#endif

int net__init(void) {
#ifdef WITH_SRV
  ares_library_init(ARES_LIB_INIT_ALL);
#endif

  return TTQ_ERR_SUCCESS;
}

void net__cleanup(void) {
#ifdef WITH_TLS
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  CRYPTO_cleanup_all_ex_data();
  ERR_free_strings();
  ERR_remove_thread_state(NULL);
  EVP_cleanup();

#if !defined(OPENSSL_NO_ENGINE)
  ENGINE_cleanup();
#endif
  is_tls_initialized = false;
#endif

  CONF_modules_unload(1);
  cleanup_ui_method();
#endif

#ifdef WITH_SRV
  ares_library_cleanup();
#endif
}

#ifdef WITH_TLS
void net__init_tls(void) {
  if (is_tls_initialized) return;

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  SSL_load_error_strings();
  SSL_library_init();
  OpenSSL_add_all_algorithms();
#else
  OPENSSL_init_crypto(OPENSSL_INIT_ADD_ALL_CIPHERS | OPENSSL_INIT_ADD_ALL_DIGESTS | OPENSSL_INIT_LOAD_CONFIG, NULL);
#endif
#if !defined(OPENSSL_NO_ENGINE)
  ENGINE_load_builtin_engines();
#endif
  setup_ui_method();
  if (tls_ex_index_ttq == -1) {
    tls_ex_index_ttq = SSL_get_ex_new_index(0, "client context", NULL, NULL, NULL);
  }

  is_tls_initialized = true;
}
#endif

/* Close a socket associated with a context and set it to -1.
 * Returns 1 on failure (context is NULL)
 * Returns 0 on success.
 */
int net__socket_close(struct tmqtt *ttq) {
  int rc = 0;
#ifdef WITH_BROKER
  struct tmqtt *ttq_found;
#endif

#ifdef WITH_TLS
#ifdef WITH_WEBSOCKETS
  if (!ttq->wsi)
#endif
  {
    if (ttq->ssl) {
      if (!SSL_in_init(ttq->ssl)) {
        SSL_shutdown(ttq->ssl);
      }
      SSL_free(ttq->ssl);
      ttq->ssl = NULL;
    }
  }
#endif

#ifdef WITH_WEBSOCKETS
  if (ttq->wsi) {
    if (ttq->state != ttq_cs_disconnecting) {
      tmqtt__set_state(ttq, ttq_cs_disconnect_ws);
    }
    lws_callback_on_writable(ttq->wsi);
  } else
#endif
  {
    if (ttq->sock != INVALID_SOCKET) {
#ifdef WITH_BROKER
      HASH_FIND(hh_sock, db.contexts_by_sock, &ttq->sock, sizeof(ttq->sock), ttq_found);
      if (ttq_found) {
        HASH_DELETE(hh_sock, db.contexts_by_sock, ttq_found);
      }
#endif
      rc = COMPAT_CLOSE(ttq->sock);
      ttq->sock = INVALID_SOCKET;
    }
  }

#ifdef WITH_BROKER
  if (ttq->listener) {
    ttq->listener->client_count--;
    ttq->listener = NULL;
  }
#endif

  return rc;
}

#ifdef FINAL_WITH_TLS_PSK
static unsigned int psk_client_callback(SSL *ssl, const char *hint, char *identity, unsigned int max_identity_len,
                                        unsigned char *psk, unsigned int max_psk_len) {
  struct tmqtt *ttq;
  int           len;

  UNUSED(hint);

  ttq = SSL_get_ex_data(ssl, tls_ex_index_ttq);
  if (!ttq) return 0;

  snprintf(identity, max_identity_len, "%s", ttq->tls_psk_identity);

  len = tmqtt__hex2bin(ttq->tls_psk, psk, (int)max_psk_len);
  if (len < 0) return 0;
  return (unsigned int)len;
}
#endif

#if defined(WITH_BROKER) && defined(__GLIBC__) && defined(WITH_ADNS)
/* Async connect, part 1 (dns lookup) */
int net__try_connect_step1(struct tmqtt *ttq, const char *host) {
  int              s;
  void            *sevp = NULL;
  struct addrinfo *hints;

  if (ttq->adns) {
    gai_cancel(ttq->adns);
    ttq_free((struct addrinfo *)ttq->adns->ar_request);
    ttq_free(ttq->adns);
  }
  ttq->adns = ttq_calloc(1, sizeof(struct gaicb));
  if (!ttq->adns) {
    return TTQ_ERR_NOMEM;
  }

  hints = ttq_calloc(1, sizeof(struct addrinfo));
  if (!hints) {
    ttq_free(ttq->adns);
    ttq->adns = NULL;
    return TTQ_ERR_NOMEM;
  }

  hints->ai_family = AF_UNSPEC;
  hints->ai_socktype = SOCK_STREAM;

  ttq->adns->ar_name = host;
  ttq->adns->ar_request = hints;

  s = getaddrinfo_a(GAI_NOWAIT, &ttq->adns, 1, sevp);
  if (s) {
    errno = s;
    if (ttq->adns) {
      ttq_free((struct addrinfo *)ttq->adns->ar_request);
      ttq_free(ttq->adns);
      ttq->adns = NULL;
    }
    return TTQ_ERR_EAI;
  }

  return TTQ_ERR_SUCCESS;
}

/* Async connect part 2, the connection. */
int net__try_connect_step2(struct tmqtt *ttq, uint16_t port, ttq_sock_t *sock) {
  struct addrinfo *ainfo, *rp;
  int              rc;

  ainfo = ttq->adns->ar_result;

  for (rp = ainfo; rp != NULL; rp = rp->ai_next) {
    *sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (*sock == INVALID_SOCKET) continue;

    if (rp->ai_family == AF_INET) {
      ((struct sockaddr_in *)rp->ai_addr)->sin_port = htons(port);
    } else if (rp->ai_family == AF_INET6) {
      ((struct sockaddr_in6 *)rp->ai_addr)->sin6_port = htons(port);
    } else {
      COMPAT_CLOSE(*sock);
      *sock = INVALID_SOCKET;
      continue;
    }

    /* Set non-blocking */
    if (net__socket_nonblock(sock)) {
      continue;
    }

    rc = connect(*sock, rp->ai_addr, rp->ai_addrlen);
    if (rc == 0 || errno == EINPROGRESS || errno == COMPAT_EWOULDBLOCK) {
      if (rc < 0 && (errno == EINPROGRESS || errno == COMPAT_EWOULDBLOCK)) {
        rc = TTQ_ERR_CONN_PENDING;
      }

      /* Set non-blocking */
      if (net__socket_nonblock(sock)) {
        continue;
      }
      break;
    }

    COMPAT_CLOSE(*sock);
    *sock = INVALID_SOCKET;
  }
  freeaddrinfo(ttq->adns->ar_result);
  ttq->adns->ar_result = NULL;

  ttq_free((struct addrinfo *)ttq->adns->ar_request);
  ttq_free(ttq->adns);
  ttq->adns = NULL;

  if (!rp) {
    return TTQ_ERR_ERRNO;
  }

  return rc;
}

#endif

static int net__try_connect_tcp(const char *host, uint16_t port, ttq_sock_t *sock, const char *bind_address,
                                bool blocking) {
  struct addrinfo  hints;
  struct addrinfo *ainfo, *rp;
  struct addrinfo *ainfo_bind, *rp_bind;
  int              s;
  int              rc = TTQ_ERR_SUCCESS;

  ainfo_bind = NULL;

  *sock = INVALID_SOCKET;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  s = getaddrinfo(host, NULL, &hints, &ainfo);
  if (s) {
    errno = s;
    return TTQ_ERR_EAI;
  }

  if (bind_address) {
    s = getaddrinfo(bind_address, NULL, &hints, &ainfo_bind);
    if (s) {
      freeaddrinfo(ainfo);
      errno = s;
      return TTQ_ERR_EAI;
    }
  }

  for (rp = ainfo; rp != NULL; rp = rp->ai_next) {
    *sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (*sock == INVALID_SOCKET) continue;

    if (rp->ai_family == AF_INET) {
      ((struct sockaddr_in *)rp->ai_addr)->sin_port = htons(port);
    } else if (rp->ai_family == AF_INET6) {
      ((struct sockaddr_in6 *)rp->ai_addr)->sin6_port = htons(port);
    } else {
      COMPAT_CLOSE(*sock);
      *sock = INVALID_SOCKET;
      continue;
    }

    if (bind_address) {
      for (rp_bind = ainfo_bind; rp_bind != NULL; rp_bind = rp_bind->ai_next) {
        if (bind(*sock, rp_bind->ai_addr, rp_bind->ai_addrlen) == 0) {
          break;
        }
      }
      if (!rp_bind) {
        COMPAT_CLOSE(*sock);
        *sock = INVALID_SOCKET;
        continue;
      }
    }

    if (!blocking) {
      /* Set non-blocking */
      if (net__socket_nonblock(sock)) {
        continue;
      }
    }

    rc = connect(*sock, rp->ai_addr, rp->ai_addrlen);
    if (rc == 0 || errno == EINPROGRESS || errno == COMPAT_EWOULDBLOCK) {
      if (rc < 0 && (errno == EINPROGRESS || errno == COMPAT_EWOULDBLOCK)) {
        rc = TTQ_ERR_CONN_PENDING;
      }

      if (blocking) {
        /* Set non-blocking */
        if (net__socket_nonblock(sock)) {
          continue;
        }
      }
      break;
    }

    COMPAT_CLOSE(*sock);
    *sock = INVALID_SOCKET;
  }
  freeaddrinfo(ainfo);
  if (bind_address) {
    freeaddrinfo(ainfo_bind);
  }
  if (!rp) {
    return TTQ_ERR_ERRNO;
  }
  return rc;
}

#ifdef WITH_UNIX_SOCKETS
static int net__try_connect_unix(const char *host, ttq_sock_t *sock) {
  struct sockaddr_un addr;
  int                s;
  int                rc;

  if (host == NULL || strlen(host) == 0 || strlen(host) > sizeof(addr.sun_path) - 1) {
    return TTQ_ERR_INVAL;
  }

  memset(&addr, 0, sizeof(struct sockaddr_un));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, host, sizeof(addr.sun_path) - 1);

  s = socket(AF_UNIX, SOCK_STREAM, 0);
  if (s < 0) {
    return TTQ_ERR_ERRNO;
  }
  rc = net__socket_nonblock(&s);
  if (rc) return rc;

  rc = connect(s, (struct sockaddr *)&addr, sizeof(struct sockaddr_un));
  if (rc < 0) {
    close(s);
    return TTQ_ERR_ERRNO;
  }

  *sock = s;

  return 0;
}
#endif

int net__try_connect(const char *host, uint16_t port, ttq_sock_t *sock, const char *bind_address, bool blocking) {
  if (port == 0) {
#ifdef WITH_UNIX_SOCKETS
    return net__try_connect_unix(host, sock);
#else
    return TTQ_ERR_NOT_SUPPORTED;
#endif
  } else {
    return net__try_connect_tcp(host, port, sock, bind_address, blocking);
  }
}

#ifdef WITH_TLS
void net__print_ssl_error(struct tmqtt *ttq) {
  char          ebuf[256];
  unsigned long e;
  int           num = 0;

  e = ERR_get_error();
  while (e) {
    ttq_log(ttq, TTQ_LOG_ERR, "OpenSSL Error[%d]: %s", num, ERR_error_string(e, ebuf));
    e = ERR_get_error();
    num++;
  }
}

int net__socket_connect_tls(struct tmqtt *ttq) {
  long res;

  ERR_clear_error();
  if (ttq->tls_ocsp_required) {
    /* Note: OCSP is available in all currently supported OpenSSL versions. */
    if ((res = SSL_set_tlsext_status_type(ttq->ssl, TLSEXT_STATUSTYPE_ocsp)) != 1) {
      ttq_log(ttq, TTQ_LOG_ERR, "Could not activate OCSP (error: %ld)", res);
      return TTQ_ERR_OCSP;
    }
    if ((res = SSL_CTX_set_tlsext_status_cb(ttq->ssl_ctx, tmqtt__verify_ocsp_status_cb)) != 1) {
      ttq_log(ttq, TTQ_LOG_ERR, "Could not activate OCSP (error: %ld)", res);
      return TTQ_ERR_OCSP;
    }
    if ((res = SSL_CTX_set_tlsext_status_arg(ttq->ssl_ctx, ttq)) != 1) {
      ttq_log(ttq, TTQ_LOG_ERR, "Could not activate OCSP (error: %ld)", res);
      return TTQ_ERR_OCSP;
    }
  }
  SSL_set_connect_state(ttq->ssl);
  return TTQ_ERR_SUCCESS;
}
#endif

#ifdef WITH_TLS
static int net__tls_load_ca(struct tmqtt *ttq) {
  int ret;

  if (ttq->tls_use_os_certs) {
    SSL_CTX_set_default_verify_paths(ttq->ssl_ctx);
  }
#if OPENSSL_VERSION_NUMBER < 0x30000000L
  if (ttq->tls_cafile || ttq->tls_capath) {
    ret = SSL_CTX_load_verify_locations(ttq->ssl_ctx, ttq->tls_cafile, ttq->tls_capath);
    if (ret == 0) {
#ifdef WITH_BROKER
      if (ttq->tls_cafile && ttq->tls_capath) {
        ttq_log(ttq, TTQ_LOG_ERR,
                "Error: Unable to load CA certificates, check bridge_cafile \"%s\" and bridge_capath \"%s\".",
                ttq->tls_cafile, ttq->tls_capath);
      } else if (ttq->tls_cafile) {
        ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load CA certificates, check bridge_cafile \"%s\".",
                ttq->tls_cafile);
      } else {
        ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load CA certificates, check bridge_capath \"%s\".",
                ttq->tls_capath);
      }
#else
      if (ttq->tls_cafile && ttq->tls_capath) {
        ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load CA certificates, check cafile \"%s\" and capath \"%s\".",
                ttq->tls_cafile, ttq->tls_capath);
      } else if (ttq->tls_cafile) {
        ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load CA certificates, check cafile \"%s\".", ttq->tls_cafile);
      } else {
        ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load CA certificates, check capath \"%s\".", ttq->tls_capath);
      }
#endif
      return TTQ_ERR_TLS;
    }
  }
#else
  if (ttq->tls_cafile) {
    ret = SSL_CTX_load_verify_file(ttq->ssl_ctx, ttq->tls_cafile);
    if (ret == 0) {
#ifdef WITH_BROKER
      ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load CA certificates, check bridge_cafile \"%s\".", ttq->tls_cafile);
#else
      ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load CA certificates, check cafile \"%s\".", ttq->tls_cafile);
#endif
      return TTQ_ERR_TLS;
    }
  }
  if (ttq->tls_capath) {
    ret = SSL_CTX_load_verify_dir(ttq->ssl_ctx, ttq->tls_capath);
    if (ret == 0) {
#ifdef WITH_BROKER
      ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load CA certificates, check bridge_capath \"%s\".", ttq->tls_capath);
#else
      ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load CA certificates, check capath \"%s\".", ttq->tls_capath);
#endif
      return TTQ_ERR_TLS;
    }
  }
#endif
  return TTQ_ERR_SUCCESS;
}

static int net__init_ssl_ctx(struct tmqtt *ttq) {
  int     ret;
  ENGINE *engine = NULL;
  uint8_t tls_alpn_wire[256];
  uint8_t tls_alpn_len;
#if !defined(OPENSSL_NO_ENGINE)
  EVP_PKEY *pkey;
#endif

#ifndef WITH_BROKER
  if (ttq->user_ssl_ctx) {
    ttq->ssl_ctx = ttq->user_ssl_ctx;
    if (!ttq->ssl_ctx_defaults) {
      return TTQ_ERR_SUCCESS;
    } else if (!ttq->tls_cafile && !ttq->tls_capath && !ttq->tls_psk) {
      ttq_log(ttq, TTQ_LOG_ERR,
              "Error: If you use TTQ_OPT_SSL_CTX then TTQ_OPT_SSL_CTX_WITH_DEFAULTS must be true, or at least "
              "one of cafile, capath or psk must be specified.");
      return TTQ_ERR_INVAL;
    }
  }
#endif

  /* Apply default SSL_CTX settings. This is only used if TTQ_OPT_SSL_CTX
   * has not been set, or if both of TTQ_OPT_SSL_CTX and
   * TTQ_OPT_SSL_CTX_WITH_DEFAULTS are set. */
  if (ttq->tls_cafile || ttq->tls_capath || ttq->tls_psk || ttq->tls_use_os_certs) {
    net__init_tls();
    if (!ttq->ssl_ctx) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
      ttq->ssl_ctx = SSL_CTX_new(SSLv23_client_method());
#else
      ttq->ssl_ctx = SSL_CTX_new(TLS_client_method());
#endif

      if (!ttq->ssl_ctx) {
        ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to create TLS context.");
        net__print_ssl_error(ttq);
        return TTQ_ERR_TLS;
      }
    }

#ifdef SSL_OP_NO_TLSv1_3
    if (ttq->tls_psk) {
      SSL_CTX_set_options(ttq->ssl_ctx, SSL_OP_NO_TLSv1_3);
    }
#endif

    if (!ttq->tls_version) {
      SSL_CTX_set_options(ttq->ssl_ctx, SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
#ifdef SSL_OP_NO_TLSv1_3
    } else if (!strcmp(ttq->tls_version, "tlsv1.3")) {
      SSL_CTX_set_options(ttq->ssl_ctx, SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1 | SSL_OP_NO_TLSv1_2);
#endif
    } else if (!strcmp(ttq->tls_version, "tlsv1.2")) {
      SSL_CTX_set_options(ttq->ssl_ctx, SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
    } else if (!strcmp(ttq->tls_version, "tlsv1.1")) {
      SSL_CTX_set_options(ttq->ssl_ctx, SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1);
    } else {
      ttq_log(ttq, TTQ_LOG_ERR, "Error: Protocol %s not supported.", ttq->tls_version);
      return TTQ_ERR_INVAL;
    }

#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    /* Allow use of DHE ciphers */
    SSL_CTX_set_dh_auto(ttq->ssl_ctx, 1);
#endif
    /* Disable compression */
    SSL_CTX_set_options(ttq->ssl_ctx, SSL_OP_NO_COMPRESSION);

    /* Set ALPN */
    if (ttq->tls_alpn) {
      tls_alpn_len = (uint8_t)strnlen(ttq->tls_alpn, 254);
      tls_alpn_wire[0] = tls_alpn_len; /* first byte is length of string */
      memcpy(tls_alpn_wire + 1, ttq->tls_alpn, tls_alpn_len);
      SSL_CTX_set_alpn_protos(ttq->ssl_ctx, tls_alpn_wire, tls_alpn_len + 1U);
    }

#ifdef SSL_MODE_RELEASE_BUFFERS
    /* Use even less memory per SSL connection. */
    SSL_CTX_set_mode(ttq->ssl_ctx, SSL_MODE_RELEASE_BUFFERS);
#endif

#if !defined(OPENSSL_NO_ENGINE)
    if (ttq->tls_engine) {
      engine = ENGINE_by_id(ttq->tls_engine);
      if (!engine) {
        ttq_log(ttq, TTQ_LOG_ERR, "Error loading %s engine\n", ttq->tls_engine);
        return TTQ_ERR_TLS;
      }
      if (!ENGINE_init(engine)) {
        ttq_log(ttq, TTQ_LOG_ERR, "Failed engine initialisation\n");
        ENGINE_free(engine);
        return TTQ_ERR_TLS;
      }
      ENGINE_set_default(engine, ENGINE_METHOD_ALL);
      ENGINE_free(engine); /* release the structural reference from ENGINE_by_id() */
    }
#endif

    if (ttq->tls_ciphers) {
      ret = SSL_CTX_set_cipher_list(ttq->ssl_ctx, ttq->tls_ciphers);
      if (ret == 0) {
        ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to set TLS ciphers. Check cipher list \"%s\".", ttq->tls_ciphers);
#if !defined(OPENSSL_NO_ENGINE)
        ENGINE_FINISH(engine);
#endif
        net__print_ssl_error(ttq);
        return TTQ_ERR_TLS;
      }
    }
    if (ttq->tls_cafile || ttq->tls_capath || ttq->tls_use_os_certs) {
      ret = net__tls_load_ca(ttq);
      if (ret != TTQ_ERR_SUCCESS) {
#if !defined(OPENSSL_NO_ENGINE)
        ENGINE_FINISH(engine);
#endif
        net__print_ssl_error(ttq);
        return TTQ_ERR_TLS;
      }
      if (ttq->tls_cert_reqs == 0) {
        SSL_CTX_set_verify(ttq->ssl_ctx, SSL_VERIFY_NONE, NULL);
      } else {
        SSL_CTX_set_verify(ttq->ssl_ctx, SSL_VERIFY_PEER, tmqtt__server_certificate_verify);
      }

      if (ttq->tls_pw_callback) {
        SSL_CTX_set_default_passwd_cb(ttq->ssl_ctx, ttq->tls_pw_callback);
        SSL_CTX_set_default_passwd_cb_userdata(ttq->ssl_ctx, ttq);
      }

      if (ttq->tls_certfile) {
        ret = SSL_CTX_use_certificate_chain_file(ttq->ssl_ctx, ttq->tls_certfile);
        if (ret != 1) {
#ifdef WITH_BROKER
          ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load client certificate, check bridge_certfile \"%s\".",
                  ttq->tls_certfile);
#else
          ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load client certificate \"%s\".", ttq->tls_certfile);
#endif
#if !defined(OPENSSL_NO_ENGINE)
          ENGINE_FINISH(engine);
#endif
          net__print_ssl_error(ttq);
          return TTQ_ERR_TLS;
        }
      }
      if (ttq->tls_keyfile) {
        if (ttq->tls_keyform == ttq_k_engine) {
#if !defined(OPENSSL_NO_ENGINE)
          UI_METHOD *ui_method = net__get_ui_method();
          if (ttq->tls_engine_kpass_sha1) {
            if (!ENGINE_ctrl_cmd(engine, ENGINE_SECRET_MODE, ENGINE_SECRET_MODE_SHA, NULL, NULL, 0)) {
              ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to set engine secret mode sha1");
              ENGINE_FINISH(engine);
              net__print_ssl_error(ttq);
              return TTQ_ERR_TLS;
            }
            if (!ENGINE_ctrl_cmd(engine, ENGINE_PIN, 0, ttq->tls_engine_kpass_sha1, NULL, 0)) {
              ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to set engine pin");
              ENGINE_FINISH(engine);
              net__print_ssl_error(ttq);
              return TTQ_ERR_TLS;
            }
            ui_method = NULL;
          }
          pkey = ENGINE_load_private_key(engine, ttq->tls_keyfile, ui_method, NULL);
          if (!pkey) {
            ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load engine private key file \"%s\".", ttq->tls_keyfile);
            ENGINE_FINISH(engine);
            net__print_ssl_error(ttq);
            return TTQ_ERR_TLS;
          }
          if (SSL_CTX_use_PrivateKey(ttq->ssl_ctx, pkey) <= 0) {
            ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to use engine private key file \"%s\".", ttq->tls_keyfile);
            ENGINE_FINISH(engine);
            net__print_ssl_error(ttq);
            return TTQ_ERR_TLS;
          }
#endif
        } else {
          ret = SSL_CTX_use_PrivateKey_file(ttq->ssl_ctx, ttq->tls_keyfile, SSL_FILETYPE_PEM);
          if (ret != 1) {
#ifdef WITH_BROKER
            ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load client key file, check bridge_keyfile \"%s\".",
                    ttq->tls_keyfile);
#else
            ttq_log(ttq, TTQ_LOG_ERR, "Error: Unable to load client key file \"%s\".", ttq->tls_keyfile);
#endif
#if !defined(OPENSSL_NO_ENGINE)
            ENGINE_FINISH(engine);
#endif
            net__print_ssl_error(ttq);
            return TTQ_ERR_TLS;
          }
        }
        ret = SSL_CTX_check_private_key(ttq->ssl_ctx);
        if (ret != 1) {
          ttq_log(ttq, TTQ_LOG_ERR, "Error: Client certificate/key are inconsistent.");
#if !defined(OPENSSL_NO_ENGINE)
          ENGINE_FINISH(engine);
#endif
          net__print_ssl_error(ttq);
          return TTQ_ERR_TLS;
        }
      }
#ifdef FINAL_WITH_TLS_PSK
    } else if (ttq->tls_psk) {
      SSL_CTX_set_psk_client_callback(ttq->ssl_ctx, psk_client_callback);
      if (ttq->tls_ciphers == NULL) {
        SSL_CTX_set_cipher_list(ttq->ssl_ctx, "PSK");
      }
#endif
    }
  }

  return TTQ_ERR_SUCCESS;
}
#endif

int net__socket_connect_step3(struct tmqtt *ttq, const char *host) {
#ifdef WITH_TLS
  BIO *bio;

  int rc = net__init_ssl_ctx(ttq);
  if (rc) {
    net__socket_close(ttq);
    return rc;
  }

  if (ttq->ssl_ctx) {
    if (ttq->ssl) {
      SSL_free(ttq->ssl);
    }
    ttq->ssl = SSL_new(ttq->ssl_ctx);
    if (!ttq->ssl) {
      net__socket_close(ttq);
      net__print_ssl_error(ttq);
      return TTQ_ERR_TLS;
    }

    SSL_set_ex_data(ttq->ssl, tls_ex_index_ttq, ttq);
    bio = BIO_new_socket(ttq->sock, BIO_NOCLOSE);
    if (!bio) {
      net__socket_close(ttq);
      net__print_ssl_error(ttq);
      return TTQ_ERR_TLS;
    }
    SSL_set_bio(ttq->ssl, bio, bio);

    /*
     * required for the SNI resolving
     */
    if (SSL_set_tlsext_host_name(ttq->ssl, host) != 1) {
      net__socket_close(ttq);
      return TTQ_ERR_TLS;
    }

    if (net__socket_connect_tls(ttq)) {
      net__socket_close(ttq);
      return TTQ_ERR_TLS;
    }
  }
#else
  UNUSED(ttq);
  UNUSED(host);
#endif
  return TTQ_ERR_SUCCESS;
}

/* Create a socket and connect it to 'ip' on port 'port'.  */
int net__socket_connect(struct tmqtt *ttq, const char *host, uint16_t port, const char *bind_address, bool blocking) {
  int rc, rc2;

  if (!ttq || !host) return TTQ_ERR_INVAL;

  rc = net__try_connect(host, port, &ttq->sock, bind_address, blocking);
  if (rc > 0) return rc;

  if (ttq->tcp_nodelay) {
    int flag = 1;
    if (setsockopt(ttq->sock, IPPROTO_TCP, TCP_NODELAY, (const void *)&flag, sizeof(int)) != 0) {
      ttq_log(ttq, TTQ_LOG_WARNING, "Warning: Unable to set TCP_NODELAY.");
    }
  }

#if defined(WITH_SOCKS) && !defined(WITH_BROKER)
  if (!ttq->socks5_host)
#endif
  {
    rc2 = net__socket_connect_step3(ttq, host);
    if (rc2) return rc2;
  }

  return rc;
}

#ifdef WITH_TLS
static int net__handle_ssl(struct tmqtt *ttq, int ret) {
  int err;

  err = SSL_get_error(ttq->ssl, ret);
  if (err == SSL_ERROR_WANT_READ) {
    ret = -1;
    errno = EAGAIN;
  } else if (err == SSL_ERROR_WANT_WRITE) {
    ret = -1;
#ifdef WITH_BROKER
    mux__add_out(ttq);
#else
    ttq->want_write = true;
#endif
    errno = EAGAIN;
  } else {
    net__print_ssl_error(ttq);
    errno = EPROTO;
  }
  ERR_clear_error();

  return ret;
}
#endif

ssize_t net__read(struct tmqtt *ttq, void *buf, size_t count) {
#ifdef WITH_TLS
  int ret;
#endif

  errno = 0;
#ifdef WITH_TLS
  if (ttq->ssl) {
    ERR_clear_error();
    ret = SSL_read(ttq->ssl, buf, (int)count);
    if (ret <= 0) {
      ret = net__handle_ssl(ttq, ret);
    }
    return (ssize_t)ret;
  } else {
    /* Call normal read/recv */

#endif

    return read(ttq->sock, buf, count);

#ifdef WITH_TLS
  }
#endif
}

ssize_t net__write(struct tmqtt *ttq, const void *buf, size_t count) {
#ifdef WITH_TLS
  int ret;
#endif

  errno = 0;
#ifdef WITH_TLS
  if (ttq->ssl) {
    ERR_clear_error();
    ttq->want_write = false;
    ret = SSL_write(ttq->ssl, buf, (int)count);
    if (ret < 0) {
      ret = net__handle_ssl(ttq, ret);
    }
    return (ssize_t)ret;
  } else {
    /* Call normal write/send */
#endif

    return send(ttq->sock, buf, count, MSG_NOSIGNAL);

#ifdef WITH_TLS
  }
#endif
}

int net__socket_nonblock(ttq_sock_t *sock) {
  int opt;
  /* Set non-blocking */
  opt = fcntl(*sock, F_GETFL, 0);
  if (opt == -1) {
    COMPAT_CLOSE(*sock);
    *sock = INVALID_SOCKET;
    return TTQ_ERR_ERRNO;
  }
  if (fcntl(*sock, F_SETFL, opt | O_NONBLOCK) == -1) {
    /* If either fcntl fails, don't want to allow this client to connect. */
    COMPAT_CLOSE(*sock);
    *sock = INVALID_SOCKET;
    return TTQ_ERR_ERRNO;
  }

  return TTQ_ERR_SUCCESS;
}

#ifndef WITH_BROKER
int net__socketpair(ttq_sock_t *pairR, ttq_sock_t *pairW) {
  int sv[2];

  *pairR = INVALID_SOCKET;
  *pairW = INVALID_SOCKET;

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == -1) {
    return TTQ_ERR_ERRNO;
  }
  if (net__socket_nonblock(&sv[0])) {
    COMPAT_CLOSE(sv[1]);
    return TTQ_ERR_ERRNO;
  }
  if (net__socket_nonblock(&sv[1])) {
    COMPAT_CLOSE(sv[0]);
    return TTQ_ERR_ERRNO;
  }
  *pairR = sv[0];
  *pairW = sv[1];
  return TTQ_ERR_SUCCESS;
}
#endif

#ifndef WITH_BROKER

void *tmqtt_ssl_get(struct tmqtt *ttq) {
#ifdef WITH_TLS
  return ttq->ssl;
#else
  UNUSED(ttq);
  return NULL;
#endif
}

#endif
