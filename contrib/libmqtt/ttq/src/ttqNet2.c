#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif

#ifdef WITH_UNIX_SOCKETS
#include "sys/stat.h"
#include "sys/un.h"
#endif

#ifdef WITH_WRAP
#include <tcpd.h>
#endif

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>

#include "tmqttBrokerInt.h"
#include "tmqttProto.h"
#include "ttqMemory.h"
#include "ttqMisc.h"
#include "ttqNet.h"
#include "ttqSystree.h"
#include "ttqUtil.h"

// Too many file
static ttq_sock_t spare_sock = INVALID_SOCKET;

void ttqNetBrokerInit(void) {
  spare_sock = socket(AF_INET, SOCK_STREAM, 0);
  net__init();
#ifdef WITH_TLS
  net__init_tls();
#endif
}

void ttqNetBrokerCleanup(void) {
  if (spare_sock != INVALID_SOCKET) {
    COMPAT_CLOSE(spare_sock);
    spare_sock = INVALID_SOCKET;
  }
  net__cleanup();
}

static void net__print_error(unsigned int log, const char *format_str) {
  char *buf;

  buf = strerror(errno);
  ttq_log(NULL, log, format_str, buf);
}

struct tmqtt *ttqNetSocketAccept(struct tmqtt__listener_sock *listensock) {
  ttq_sock_t    new_sock = INVALID_SOCKET;
  struct tmqtt *new_context;
#ifdef WITH_TLS
  BIO          *bio;
  int           rc;
  char          ebuf[256];
  unsigned long e;
#endif
#ifdef WITH_WRAP
  struct request_info wrap_req;
  char                address[1024];
#endif

  new_sock = accept(listensock->sock, NULL, 0);
  if (new_sock == INVALID_SOCKET) {
    if (errno == EMFILE || errno == ENFILE) {
      /* Close the spare socket, which means we should be able to accept
       * this connection. Accept it, then close it immediately and create
       * a new spare_sock. This prevents the situation of ever properly
       * running out of sockets.
       * It would be nice to send a "server not available" connack here,
       * but there are lots of reasons why this would be tricky (TLS
       * being the big one). */
      COMPAT_CLOSE(spare_sock);
      new_sock = accept(listensock->sock, NULL, 0);
      if (new_sock != INVALID_SOCKET) {
        COMPAT_CLOSE(new_sock);
      }
      spare_sock = socket(AF_INET, SOCK_STREAM, 0);
      ttq_log(NULL, TTQ_LOG_WARNING,
              "Unable to accept new connection, system socket count has been exceeded. Try increasing \"ulimit "
              "-n\" or equivalent.");
    }
    return NULL;
  }

  G_SOCKET_CONNECTIONS_INC();

  if (net__socket_nonblock(&new_sock)) {
    return NULL;
  }

#ifdef WITH_WRAP
  /* Use tcpd / libwrap to determine whether a connection is allowed. */
  request_init(&wrap_req, RQ_FILE, new_sock, RQ_DAEMON, "tmqtt", 0);
  fromhost(&wrap_req);
  if (!hosts_access(&wrap_req)) {
    /* Access is denied */
    if (db.config->connection_messages == true) {
      if (!ttqNetSocketGetAddress(new_sock, address, 1024, NULL)) {
        ttq_log(NULL, TTQ_LOG_NOTICE, "Client connection from %s denied access by tcpd.", address);
      }
    }
    COMPAT_CLOSE(new_sock);
    return NULL;
  }
#endif

  if (db.config->set_tcp_nodelay) {
    int flag = 1;
    if (setsockopt(new_sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int)) != 0) {
      ttq_log(NULL, TTQ_LOG_WARNING, "Warning: Unable to set TCP_NODELAY.");
    }
  }

  new_context = ttqCxtInit(new_sock);
  if (!new_context) {
    COMPAT_CLOSE(new_sock);
    return NULL;
  }
  new_context->listener = listensock->listener;
  if (!new_context->listener) {
    ttqCxtCleanup(new_context, true);
    return NULL;
  }
  new_context->listener->client_count++;

  if (new_context->listener->max_connections > 0 &&
      new_context->listener->client_count > new_context->listener->max_connections) {
    if (db.config->connection_messages == true) {
      ttq_log(NULL, TTQ_LOG_NOTICE, "Client connection from %s denied: max_connections exceeded.",
              new_context->address);
    }
    ttqCxtCleanup(new_context, true);
    return NULL;
  }

#ifdef WITH_TLS
  /* TLS init */
  if (new_context->listener->ssl_ctx) {
    new_context->ssl = SSL_new(new_context->listener->ssl_ctx);
    if (!new_context->ssl) {
      ttqCxtCleanup(new_context, true);
      return NULL;
    }
    SSL_set_ex_data(new_context->ssl, tls_ex_index_context, new_context);
    SSL_set_ex_data(new_context->ssl, tls_ex_index_listener, new_context->listener);
    new_context->want_write = true;
    bio = BIO_new_socket(new_sock, BIO_NOCLOSE);
    SSL_set_bio(new_context->ssl, bio, bio);
    ERR_clear_error();
    rc = SSL_accept(new_context->ssl);
    if (rc != 1) {
      rc = SSL_get_error(new_context->ssl, rc);
      if (rc == SSL_ERROR_WANT_READ) {
        /* We always want to read. */
      } else if (rc == SSL_ERROR_WANT_WRITE) {
        new_context->want_write = true;
      } else {
        if (db.config->connection_messages == true) {
          e = ERR_get_error();
          while (e) {
            ttq_log(NULL, TTQ_LOG_NOTICE, "Client connection from %s failed: %s.", new_context->address,
                    ERR_error_string(e, ebuf));
            e = ERR_get_error();
          }
        }
        ttqCxtCleanup(new_context, true);
        return NULL;
      }
    }
  }
#endif

  if (db.config->connection_messages == true) {
    ttq_log(NULL, TTQ_LOG_NOTICE, "New connection from %s:%d on port %d.", new_context->address,
            new_context->remote_port, new_context->listener->port);
  }

  return new_context;
}

#ifdef WITH_TLS
static int client_certificate_verify(int preverify_ok, X509_STORE_CTX *ctx) {
  UNUSED(ctx);

  /* Preverify should check expiry, revocation. */
  return preverify_ok;
}
#endif

#ifdef FINAL_WITH_TLS_PSK
static unsigned int psk_server_callback(SSL *ssl, const char *identity, unsigned char *psk, unsigned int max_psk_len) {
  struct tmqtt           *context;
  struct tmqtt__listener *listener;
  char                   *psk_key = NULL;
  int                     len;
  const char             *psk_hint;

  if (!identity) return 0;

  context = SSL_get_ex_data(ssl, tls_ex_index_context);
  if (!context) return 0;

  listener = SSL_get_ex_data(ssl, tls_ex_index_listener);
  if (!listener) return 0;

  psk_hint = listener->psk_hint;

  /* The hex to BN conversion results in the length halving, so we can pass
   * max_psk_len*2 as the max hex key here. */
  psk_key = ttq_calloc(1, (size_t)max_psk_len * 2 + 1);
  if (!psk_key) return 0;

  if (tmqtt_psk_key_get(context, psk_hint, identity, psk_key, (int)max_psk_len * 2) != TTQ_ERR_SUCCESS) {
    ttq_free(psk_key);
    return 0;
  }

  len = tmqtt__hex2bin(psk_key, psk, (int)max_psk_len);
  if (len < 0) {
    ttq_free(psk_key);
    return 0;
  }

  if (listener->use_identity_as_username) {
    if (tmqtt_validate_utf8(identity, (int)strlen(identity))) {
      ttq_free(psk_key);
      return 0;
    }
    context->username = ttq_strdup(identity);
    if (!context->username) {
      ttq_free(psk_key);
      return 0;
    }
  }

  ttq_free(psk_key);
  return (unsigned int)len;
}
#endif

#ifdef WITH_TLS
int ttqNetTlsServerCtx(struct tmqtt__listener *listener) {
  char  buf[256];
  int   rc;
  FILE *dhparamfile;
  DH   *dhparam = NULL;

  if (listener->ssl_ctx) {
    SSL_CTX_free(listener->ssl_ctx);
  }

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  listener->ssl_ctx = SSL_CTX_new(SSLv23_server_method());
#else
  listener->ssl_ctx = SSL_CTX_new(TLS_server_method());
#endif

  if (!listener->ssl_ctx) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to create TLS context.");
    return TTQ_ERR_TLS;
  }

#ifdef SSL_OP_NO_TLSv1_3
  if (db.config->per_listener_settings) {
    if (listener->security_options.psk_file) {
      SSL_CTX_set_options(listener->ssl_ctx, SSL_OP_NO_TLSv1_3);
    }
  } else {
    if (db.config->security_options.psk_file) {
      SSL_CTX_set_options(listener->ssl_ctx, SSL_OP_NO_TLSv1_3);
    }
  }
#endif

  if (listener->tls_version == NULL) {
    SSL_CTX_set_options(listener->ssl_ctx, SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
#ifdef SSL_OP_NO_TLSv1_3
  } else if (!strcmp(listener->tls_version, "tlsv1.3")) {
    SSL_CTX_set_options(listener->ssl_ctx, SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1 | SSL_OP_NO_TLSv1_2);
#endif
  } else if (!strcmp(listener->tls_version, "tlsv1.2")) {
    SSL_CTX_set_options(listener->ssl_ctx, SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
  } else if (!strcmp(listener->tls_version, "tlsv1.1")) {
    SSL_CTX_set_options(listener->ssl_ctx, SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1);
  } else {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Unsupported tls_version \"%s\".", listener->tls_version);
    return TTQ_ERR_TLS;
  }
  /* Use a new key when using temporary/ephemeral DH parameters.
   * This shouldn't be necessary, but we can't guarantee that `dhparam` has
   * been generated using strong primes.
   */
  SSL_CTX_set_options(listener->ssl_ctx, SSL_OP_SINGLE_DH_USE);

#ifdef SSL_OP_NO_COMPRESSION
  /* Disable compression */
  SSL_CTX_set_options(listener->ssl_ctx, SSL_OP_NO_COMPRESSION);
#endif
#ifdef SSL_OP_CIPHER_SERVER_PREFERENCE
  /* Server chooses cipher */
  SSL_CTX_set_options(listener->ssl_ctx, SSL_OP_CIPHER_SERVER_PREFERENCE);
#endif

#ifdef SSL_MODE_RELEASE_BUFFERS
  /* Use even less memory per SSL connection. */
  SSL_CTX_set_mode(listener->ssl_ctx, SSL_MODE_RELEASE_BUFFERS);
#endif

#ifdef WITH_EC
#if OPENSSL_VERSION_NUMBER >= 0x10002000L && OPENSSL_VERSION_NUMBER < 0x10100000L
  SSL_CTX_set_ecdh_auto(listener->ssl_ctx, 1);
#endif
#endif
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
  SSL_CTX_set_dh_auto(listener->ssl_ctx, 1);
#endif

#ifdef SSL_OP_NO_RENEGOTIATION
  SSL_CTX_set_options(listener->ssl_ctx, SSL_OP_NO_RENEGOTIATION);
#endif

  snprintf(buf, 256, "tmqtt-%d", listener->port);
  SSL_CTX_set_session_id_context(listener->ssl_ctx, (unsigned char *)buf, (unsigned int)strlen(buf));

  if (listener->ciphers) {
    rc = SSL_CTX_set_cipher_list(listener->ssl_ctx, listener->ciphers);
    if (rc == 0) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to set TLS ciphers. Check cipher list \"%s\".", listener->ciphers);
      return TTQ_ERR_TLS;
    }
  } else {
    rc = SSL_CTX_set_cipher_list(listener->ssl_ctx, "DEFAULT:!aNULL:!eNULL:!LOW:!EXPORT:!SSLv2:@STRENGTH");
    if (rc == 0) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to set TLS ciphers. Check cipher list \"%s\".", listener->ciphers);
      return TTQ_ERR_TLS;
    }
  }
#if OPENSSL_VERSION_NUMBER >= 0x10101000 && (!defined(LIBRESSL_VERSION_NUMBER) || LIBRESSL_VERSION_NUMBER > 0x3040000FL)
  if (listener->ciphers_tls13) {
    rc = SSL_CTX_set_ciphersuites(listener->ssl_ctx, listener->ciphers_tls13);
    if (rc == 0) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to set TLS 1.3 ciphersuites. Check cipher_tls13 list \"%s\".",
              listener->ciphers_tls13);
      return TTQ_ERR_TLS;
    }
  }
#endif

  if (listener->dhparamfile) {
    dhparamfile = tmqtt__fopen(listener->dhparamfile, "r", true);
    if (!dhparamfile) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error loading dhparamfile \"%s\".", listener->dhparamfile);
      return TTQ_ERR_TLS;
    }
    dhparam = PEM_read_DHparams(dhparamfile, NULL, NULL, NULL);
    fclose(dhparamfile);

    if (dhparam == NULL || SSL_CTX_set_tmp_dh(listener->ssl_ctx, dhparam) != 1) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error loading dhparamfile \"%s\".", listener->dhparamfile);
      net__print_ssl_error(NULL);
      return TTQ_ERR_TLS;
    }
  }
  return TTQ_ERR_SUCCESS;
}
#endif

#ifdef WITH_TLS
static int net__load_crl_file(struct tmqtt__listener *listener) {
  X509_STORE  *store;
  X509_LOOKUP *lookup;
  int          rc;

  store = SSL_CTX_get_cert_store(listener->ssl_ctx);
  if (!store) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to obtain TLS store.");
    net__print_error(TTQ_LOG_ERR, "Error: %s");
    return TTQ_ERR_TLS;
  }
  lookup = X509_STORE_add_lookup(store, X509_LOOKUP_file());
  rc = X509_load_crl_file(lookup, listener->crlfile, X509_FILETYPE_PEM);
  if (rc < 1) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to load certificate revocation file \"%s\". Check crlfile.",
            listener->crlfile);
    net__print_error(TTQ_LOG_ERR, "Error: %s");
    net__print_ssl_error(NULL);
    return TTQ_ERR_TLS;
  }
  X509_STORE_set_flags(store, X509_V_FLAG_CRL_CHECK);

  return TTQ_ERR_SUCCESS;
}
#endif

int ttqNetTlsLoadCertificates(struct tmqtt__listener *listener) {
#ifdef WITH_TLS
  int rc;

  if (listener->require_certificate) {
    SSL_CTX_set_verify(listener->ssl_ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, client_certificate_verify);
  } else {
    SSL_CTX_set_verify(listener->ssl_ctx, SSL_VERIFY_NONE, client_certificate_verify);
  }
  rc = SSL_CTX_use_certificate_chain_file(listener->ssl_ctx, listener->certfile);
  if (rc != 1) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to load server certificate \"%s\". Check certfile.", listener->certfile);
    net__print_ssl_error(NULL);
    return TTQ_ERR_TLS;
  }
  if (listener->tls_engine == NULL || listener->tls_keyform == ttq_k_pem) {
    rc = SSL_CTX_use_PrivateKey_file(listener->ssl_ctx, listener->keyfile, SSL_FILETYPE_PEM);
    if (rc != 1) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to load server key file \"%s\". Check keyfile.", listener->keyfile);
      net__print_ssl_error(NULL);
      return TTQ_ERR_TLS;
    }
  }
  rc = SSL_CTX_check_private_key(listener->ssl_ctx);
  if (rc != 1) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Server certificate/key are inconsistent.");
    net__print_ssl_error(NULL);
    return TTQ_ERR_TLS;
  }
  /* Load CRLs if they exist. */
  if (listener->crlfile) {
    rc = net__load_crl_file(listener);
    if (rc) {
      return rc;
    }
  }
#else
  UNUSED(listener);
#endif
  return TTQ_ERR_SUCCESS;
}

#if defined(WITH_TLS) && !defined(OPENSSL_NO_ENGINE)
static int net__load_engine(struct tmqtt__listener *listener) {
  ENGINE    *engine = NULL;
  UI_METHOD *ui_method;
  EVP_PKEY  *pkey;

  if (!listener->tls_engine) {
    return TTQ_ERR_SUCCESS;
  }

  engine = ENGINE_by_id(listener->tls_engine);
  if (!engine) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error loading %s engine\n", listener->tls_engine);
    net__print_ssl_error(NULL);
    return TTQ_ERR_TLS;
  }
  if (!ENGINE_init(engine)) {
    ttq_log(NULL, TTQ_LOG_ERR, "Failed engine initialisation\n");
    net__print_ssl_error(NULL);
    return TTQ_ERR_TLS;
  }
  ENGINE_set_default(engine, ENGINE_METHOD_ALL);

  if (listener->tls_keyform == ttq_k_engine) {
    ui_method = net__get_ui_method();
    if (listener->tls_engine_kpass_sha1) {
      if (!ENGINE_ctrl_cmd(engine, ENGINE_SECRET_MODE, ENGINE_SECRET_MODE_SHA, NULL, NULL, 0)) {
        ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to set engine secret mode sha");
        net__print_ssl_error(NULL);
        return TTQ_ERR_TLS;
      }
      if (!ENGINE_ctrl_cmd(engine, ENGINE_PIN, 0, listener->tls_engine_kpass_sha1, NULL, 0)) {
        ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to set engine pin");
        net__print_ssl_error(NULL);
        return TTQ_ERR_TLS;
      }
      ui_method = NULL;
    }
    pkey = ENGINE_load_private_key(engine, listener->keyfile, ui_method, NULL);
    if (!pkey) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to load engine private key file \"%s\".", listener->keyfile);
      net__print_ssl_error(NULL);
      return TTQ_ERR_TLS;
    }
    if (SSL_CTX_use_PrivateKey(listener->ssl_ctx, pkey) <= 0) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to use engine private key file \"%s\".", listener->keyfile);
      net__print_ssl_error(NULL);
      return TTQ_ERR_TLS;
    }
  }
  ENGINE_free(engine); /* release the structural reference from ENGINE_by_id() */

  return TTQ_ERR_SUCCESS;
}
#endif

int ttqNetTlsLoadVerify(struct tmqtt__listener *listener) {
#ifdef WITH_TLS
  int rc;

#if OPENSSL_VERSION_NUMBER < 0x30000000L
  if (listener->cafile || listener->capath) {
    rc = SSL_CTX_load_verify_locations(listener->ssl_ctx, listener->cafile, listener->capath);
    if (rc == 0) {
      if (listener->cafile && listener->capath) {
        ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to load CA certificates. Check cafile \"%s\" and capath \"%s\".",
                listener->cafile, listener->capath);
      } else if (listener->cafile) {
        ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to load CA certificates. Check cafile \"%s\".", listener->cafile);
      } else {
        ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to load CA certificates. Check capath \"%s\".", listener->capath);
      }
    }
  }
#else
  if (listener->cafile) {
    rc = SSL_CTX_load_verify_file(listener->ssl_ctx, listener->cafile);
    if (rc == 0) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to load CA certificates. Check cafile \"%s\".", listener->cafile);
      net__print_ssl_error(NULL);
      return TTQ_ERR_TLS;
    }
  }
  if (listener->capath) {
    rc = SSL_CTX_load_verify_dir(listener->ssl_ctx, listener->capath);
    if (rc == 0) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to load CA certificates. Check capath \"%s\".", listener->capath);
      net__print_ssl_error(NULL);
      return TTQ_ERR_TLS;
    }
  }
#endif

#if !defined(OPENSSL_NO_ENGINE)
  if (net__load_engine(listener)) {
    return TTQ_ERR_TLS;
  }
#endif
#endif
  return ttqNetTlsLoadCertificates(listener);
}

static int net__bind_interface(struct tmqtt__listener *listener, struct addrinfo *rp) {
  /*
   * This binds the listener sock to a network interface.
   * The use of SO_BINDTODEVICE requires root access, which we don't have, so instead
   * use getifaddrs to find the interface addresses, and use IP of the
   * matching interface in the later bind().
   */
  struct ifaddrs *ifaddr, *ifa;
  bool            have_interface = false;

  if (getifaddrs(&ifaddr) < 0) {
    net__print_error(TTQ_LOG_ERR, "Error: %s");
    return TTQ_ERR_ERRNO;
  }

  for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL) {
      continue;
    }

    if (!strcasecmp(listener->bind_interface, ifa->ifa_name)) {
      have_interface = true;

      if (ifa->ifa_addr->sa_family == rp->ai_addr->sa_family) {
        if (rp->ai_addr->sa_family == AF_INET) {
          if (listener->host && memcmp(&((struct sockaddr_in *)rp->ai_addr)->sin_addr,
                                       &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr, sizeof(struct in_addr))) {
            ttq_log(NULL, TTQ_LOG_ERR,
                    "Error: Interface address for %s does not match specified listener address (%s).",
                    listener->bind_interface, listener->host);
            return TTQ_ERR_INVAL;
          } else {
            memcpy(&((struct sockaddr_in *)rp->ai_addr)->sin_addr, &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr,
                   sizeof(struct in_addr));

            freeifaddrs(ifaddr);
            return TTQ_ERR_SUCCESS;
          }
        } else if (rp->ai_addr->sa_family == AF_INET6) {
          if (listener->host && memcmp(&((struct sockaddr_in6 *)rp->ai_addr)->sin6_addr,
                                       &((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr, sizeof(struct in6_addr))) {
            ttq_log(NULL, TTQ_LOG_ERR,
                    "Error: Interface address for %s does not match specified listener address (%s).",
                    listener->bind_interface, listener->host);
            return TTQ_ERR_INVAL;
          } else {
            memcpy(&((struct sockaddr_in6 *)rp->ai_addr)->sin6_addr, &((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr,
                   sizeof(struct in6_addr));
            freeifaddrs(ifaddr);
            return TTQ_ERR_SUCCESS;
          }
        }
      }
    }
  }
  freeifaddrs(ifaddr);
  if (have_interface) {
    ttq_log(NULL, TTQ_LOG_WARNING, "Warning: Interface %s does not support %s configuration.", listener->bind_interface,
            rp->ai_addr->sa_family == AF_INET ? "IPv4" : "IPv6");
    return TTQ_ERR_NOT_SUPPORTED;
  } else {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Interface %s does not exist.", listener->bind_interface);
    return TTQ_ERR_NOT_FOUND;
  }
}

static int ttqNetSocketListen_tcp(struct tmqtt__listener *listener) {
  ttq_sock_t       sock = INVALID_SOCKET;
  struct addrinfo  hints;
  struct addrinfo *ainfo, *rp;
  char             service[10];
  int              rc;
  int              ss_opt = 1;
  bool             interface_bound = false;

  if (!listener) return TTQ_ERR_INVAL;

  snprintf(service, 10, "%d", listener->port);
  memset(&hints, 0, sizeof(struct addrinfo));
  if (listener->socket_domain) {
    hints.ai_family = listener->socket_domain;
  } else {
    hints.ai_family = AF_UNSPEC;
  }
  hints.ai_flags = AI_PASSIVE;
  hints.ai_socktype = SOCK_STREAM;

  rc = getaddrinfo(listener->host, service, &hints, &ainfo);
  if (rc) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error creating listener: %s.", gai_strerror(rc));
    return INVALID_SOCKET;
  }

  listener->sock_count = 0;
  listener->socks = NULL;

  for (rp = ainfo; rp; rp = rp->ai_next) {
    if (rp->ai_family == AF_INET) {
      ttq_log(NULL, TTQ_LOG_INFO, "Opening ipv4 listen socket on port %d.",
              ntohs(((struct sockaddr_in *)rp->ai_addr)->sin_port));
    } else if (rp->ai_family == AF_INET6) {
      ttq_log(NULL, TTQ_LOG_INFO, "Opening ipv6 listen socket on port %d.",
              ntohs(((struct sockaddr_in6 *)rp->ai_addr)->sin6_port));
    } else {
      continue;
    }

    sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (sock == INVALID_SOCKET) {
      net__print_error(TTQ_LOG_WARNING, "Warning: %s");
      continue;
    }
    listener->sock_count++;
    listener->socks = ttq_realloc(listener->socks, sizeof(ttq_sock_t) * (size_t)listener->sock_count);
    if (!listener->socks) {
      ttq_log(NULL, TTQ_LOG_ERR, "Error: Out of memory.");
      freeaddrinfo(ainfo);
      COMPAT_CLOSE(sock);
      return TTQ_ERR_NOMEM;
    }
    listener->socks[listener->sock_count - 1] = sock;

    ss_opt = 1;
    /* Unimportant if this fails */
    (void)setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &ss_opt, sizeof(ss_opt));

#ifdef IPV6_V6ONLY
    ss_opt = 1;
    (void)setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY, &ss_opt, sizeof(ss_opt));
#endif

    if (net__socket_nonblock(&sock)) {
      freeaddrinfo(ainfo);
      ttq_free(listener->socks);
      return 1;
    }

    if (listener->bind_interface) {
      /* It might be possible that an interface does not support all relevant sa_families.
       * We should successfully find at least one. */
      rc = net__bind_interface(listener, rp);
      if (rc) {
        COMPAT_CLOSE(sock);
        listener->sock_count--;
        if (rc == TTQ_ERR_NOT_FOUND || rc == TTQ_ERR_INVAL) {
          freeaddrinfo(ainfo);
          return rc;
        } else {
          continue;
        }
      }
      interface_bound = true;
    }

    if (bind(sock, rp->ai_addr, rp->ai_addrlen) == -1) {
#if defined(__linux__)
      if (errno == EACCES) {
        ttq_log(NULL, TTQ_LOG_ERR,
                "If you are trying to bind to a privileged port (<1024), try using setcap and do not start the "
                "broker as root:");
        ttq_log(NULL, TTQ_LOG_ERR, "    sudo setcap 'CAP_NET_BIND_SERVICE=+ep /usr/sbin/tmqtt'");
      }
#endif
      net__print_error(TTQ_LOG_ERR, "Error: %s");
      COMPAT_CLOSE(sock);
      freeaddrinfo(ainfo);
      ttq_free(listener->socks);
      return 1;
    }

    if (listen(sock, 100) == -1) {
      net__print_error(TTQ_LOG_ERR, "Error: %s");
      freeaddrinfo(ainfo);
      COMPAT_CLOSE(sock);
      ttq_free(listener->socks);
      return 1;
    }
  }
  freeaddrinfo(ainfo);

  if (listener->bind_interface && !interface_bound) {
    ttq_free(listener->socks);
    return 1;
  }

  return 0;
}

#ifdef WITH_UNIX_SOCKETS
static int ttqNetSocketListen_unix(struct tmqtt__listener *listener) {
  struct sockaddr_un addr;
  int                sock;
  int                rc;
  mode_t             old_mask;

  if (listener->unix_socket_path == NULL) {
    return TTQ_ERR_INVAL;
  }
  if (strlen(listener->unix_socket_path) > sizeof(addr.sun_path) - 1) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Path to unix socket is too long \"%s\".", listener->unix_socket_path);
    return TTQ_ERR_INVAL;
  }

  unlink(listener->unix_socket_path);
  ttq_log(NULL, TTQ_LOG_INFO, "Opening unix listen socket on path %s.", listener->unix_socket_path);
  memset(&addr, 0, sizeof(struct sockaddr_un));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, listener->unix_socket_path, sizeof(addr.sun_path) - 1);

  sock = socket(AF_UNIX, SOCK_STREAM, 0);
  if (sock == INVALID_SOCKET) {
    net__print_error(TTQ_LOG_ERR, "Error creating unix socket: %s");
    return 1;
  }
  listener->sock_count++;
  listener->socks = ttq_realloc(listener->socks, sizeof(ttq_sock_t) * (size_t)listener->sock_count);
  if (!listener->socks) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Out of memory.");
    COMPAT_CLOSE(sock);
    return TTQ_ERR_NOMEM;
  }
  listener->socks[listener->sock_count - 1] = sock;

  old_mask = umask(0007);
  rc = bind(sock, (struct sockaddr *)&addr, sizeof(struct sockaddr_un));
  umask(old_mask);

  if (rc == -1) {
    net__print_error(TTQ_LOG_ERR, "Error binding unix socket: %s");
    return 1;
  }

  if (listen(sock, 10) == -1) {
    net__print_error(TTQ_LOG_ERR, "Error listening to unix socket: %s");
    return 1;
  }

  if (net__socket_nonblock(&sock)) {
    return 1;
  }

  return 0;
}
#endif

/* Creates a socket and listens on port 'port'.
 * Returns 1 on failure
 * Returns 0 on success.
 */
int ttqNetSocketListen(struct tmqtt__listener *listener) {
  int rc;

  if (!listener) return TTQ_ERR_INVAL;

#ifdef WITH_UNIX_SOCKETS
  if (listener->port == 0 && listener->unix_socket_path != NULL) {
    rc = ttqNetSocketListen_unix(listener);
  } else
#endif
  {
    rc = ttqNetSocketListen_tcp(listener);
  }
  if (rc) return rc;

  /* We need to have at least one working socket. */
  if (listener->sock_count > 0) {
#ifdef WITH_TLS
    if (listener->certfile && listener->keyfile) {
      if (ttqNetTlsServerCtx(listener)) {
        return 1;
      }

      if (ttqNetTlsLoadVerify(listener)) {
        return 1;
      }
    }
#ifdef FINAL_WITH_TLS_PSK
    if (listener->psk_hint) {
      if (tls_ex_index_context == -1) {
        tls_ex_index_context = SSL_get_ex_new_index(0, "client context", NULL, NULL, NULL);
      }
      if (tls_ex_index_listener == -1) {
        tls_ex_index_listener = SSL_get_ex_new_index(0, "listener", NULL, NULL, NULL);
      }

      if (listener->certfile == NULL || listener->keyfile == NULL) {
        if (ttqNetTlsServerCtx(listener)) {
          return 1;
        }
      }
      SSL_CTX_set_psk_server_callback(listener->ssl_ctx, psk_server_callback);
      if (listener->psk_hint) {
        rc = SSL_CTX_use_psk_identity_hint(listener->ssl_ctx, listener->psk_hint);
        if (rc == 0) {
          ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to set TLS PSK hint.");
          net__print_ssl_error(NULL);
          return 1;
        }
      }
    }
#endif /* FINAL_WITH_TLS_PSK */
#endif /* WITH_TLS */
    return 0;
  } else {
    return 1;
  }
}

int ttqNetSocketGetAddress(ttq_sock_t sock, char *buf, size_t len, uint16_t *remote_port) {
  struct sockaddr_storage addr;
  socklen_t               addrlen;

  memset(&addr, 0, sizeof(struct sockaddr_storage));
  addrlen = sizeof(addr);
  if (!getpeername(sock, (struct sockaddr *)&addr, &addrlen)) {
    if (addr.ss_family == AF_INET) {
      if (remote_port) {
        *remote_port = ntohs(((struct sockaddr_in *)&addr)->sin_port);
      }
      if (inet_ntop(AF_INET, &((struct sockaddr_in *)&addr)->sin_addr.s_addr, buf, (socklen_t)len)) {
        return 0;
      }
    } else if (addr.ss_family == AF_INET6) {
      if (remote_port) {
        *remote_port = ntohs(((struct sockaddr_in6 *)&addr)->sin6_port);
      }
      if (inet_ntop(AF_INET6, &((struct sockaddr_in6 *)&addr)->sin6_addr.s6_addr, buf, (socklen_t)len)) {
        return 0;
      }
#ifdef WITH_UNIX_SOCKETS
    } else if (addr.ss_family == AF_UNIX) {
      struct sockaddr_un un;
      addrlen = sizeof(struct sockaddr_un);
      if (!getsockname(sock, (struct sockaddr *)&un, &addrlen)) {
        snprintf(buf, len, "%s", un.sun_path);
      } else {
        snprintf(buf, len, "unix-socket");
      }
      return 0;
#endif
    }
  }
  return 1;
}
