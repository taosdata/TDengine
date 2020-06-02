/*
MIT License

Copyright(c) 2018 Liam Bindle

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files(the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions :

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#include <mqtt.h>

/** 
 * @file 
 * @brief Implements @ref mqtt_pal_sendall and @ref mqtt_pal_recvall and 
 *        any platform-specific helpers you'd like.
 * @cond Doxygen_Suppress
 */


#ifdef MQTT_USE_MBEDTLS
#include <mbedtls/ssl.h>

ssize_t mqtt_pal_sendall(mqtt_pal_socket_handle fd, const void* buf, size_t len, int flags) {
    size_t sent = 0;
    while(sent < len) {
        int rv = mbedtls_ssl_write(fd, buf + sent, len - sent);
        if (rv < 0) {
            if (rv == MBEDTLS_ERR_SSL_WANT_READ ||
                rv == MBEDTLS_ERR_SSL_WANT_WRITE
#if defined(MBEDTLS_ERR_SSL_ASYNC_IN_PROGRESS)
                || rv == MBEDTLS_ERR_SSL_ASYNC_IN_PROGRESS
#endif
#if defined(MBEDTLS_ERR_SSL_CRYPTO_IN_PROGRESS)
                || rv == MBEDTLS_ERR_SSL_CRYPTO_IN_PROGRESS
#endif
                ) {
                /* should call mbedtls_ssl_writer later again */
                break;
            }
            return MQTT_ERROR_SOCKET_ERROR;
        }
        sent += (size_t) rv;
    }
    return sent;
}

ssize_t mqtt_pal_recvall(mqtt_pal_socket_handle fd, void* buf, size_t bufsz, int flags) {
    const void *const start = buf;
    int rv;
    do {
        rv = mbedtls_ssl_read(fd, buf, bufsz);
        if (rv < 0) {
            if (rv == MBEDTLS_ERR_SSL_WANT_READ ||
                rv == MBEDTLS_ERR_SSL_WANT_WRITE
#if defined(MBEDTLS_ERR_SSL_ASYNC_IN_PROGRESS)
                || rv == MBEDTLS_ERR_SSL_ASYNC_IN_PROGRESS
#endif
#if defined(MBEDTLS_ERR_SSL_CRYPTO_IN_PROGRESS)
                || rv == MBEDTLS_ERR_SSL_CRYPTO_IN_PROGRESS
#endif
                ) {
                /* should call mbedtls_ssl_read later again */
                break;
            }
            return MQTT_ERROR_SOCKET_ERROR;
        }
        buf = (char*)buf + rv;
        bufsz -= rv;
    } while (rv > 0);

    return buf - start;
}

#elif defined(MQTT_USE_BIO)
#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

ssize_t mqtt_pal_sendall(mqtt_pal_socket_handle fd, const void* buf, size_t len, int flags) {
    size_t sent = 0;
    while(sent < len) {
        int tmp = BIO_write(fd, buf + sent, len - sent);
        if (tmp > 0) {
            sent += (size_t) tmp;
        } else if (tmp <= 0 && !BIO_should_retry(fd)) {
            return MQTT_ERROR_SOCKET_ERROR;
        }
    }
    
    return sent;
}

ssize_t mqtt_pal_recvall(mqtt_pal_socket_handle fd, void* buf, size_t bufsz, int flags) {
    const void *const start = buf;
    int rv;
    do {
        rv = BIO_read(fd, buf, bufsz);
        if (rv > 0) {
            /* successfully read bytes from the socket */
            buf += rv;
            bufsz -= rv;
        } else if (!BIO_should_retry(fd)) {
            /* an error occurred that wasn't "nothing to read". */
            return MQTT_ERROR_SOCKET_ERROR;
        }
    } while (!BIO_should_read(fd));

    return (ssize_t)(buf - start);
}

#elif defined(__unix__) || defined(__APPLE__)

#include <errno.h>

ssize_t mqtt_pal_sendall(mqtt_pal_socket_handle fd, const void* buf, size_t len, int flags) {
    size_t sent = 0;
    while(sent < len) {
        ssize_t tmp = send(fd, buf + sent, len - sent, flags);
        if (tmp < 1) {
            return MQTT_ERROR_SOCKET_ERROR;
        }
        sent += (size_t) tmp;
    }
    return sent;
}

ssize_t mqtt_pal_recvall(mqtt_pal_socket_handle fd, void* buf, size_t bufsz, int flags) {
    const void *const start = buf;
    ssize_t rv;
    do {
        rv = recv(fd, buf, bufsz, flags);
        if (rv > 0) {
            /* successfully read bytes from the socket */
            buf += rv;
            bufsz -= rv;
        } else if (rv < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            /* an error occurred that wasn't "nothing to read". */
            return MQTT_ERROR_SOCKET_ERROR;
        }
    } while (rv > 0);

    return buf - start;
}

#elif defined(_MSC_VER)

#include <errno.h>

ssize_t mqtt_pal_sendall(mqtt_pal_socket_handle fd, const void* buf, size_t len, int flags) {
    size_t sent = 0;
    while(sent < len) {
        ssize_t tmp = send(fd, (char*)buf + sent, len - sent, flags);
        if (tmp < 1) {
            return MQTT_ERROR_SOCKET_ERROR;
        }
        sent += (size_t) tmp;
    }
    return sent;
}

ssize_t mqtt_pal_recvall(mqtt_pal_socket_handle fd, void* buf, size_t bufsz, int flags) {
    const char *const start = buf;
    ssize_t rv;
    do {
        rv = recv(fd, buf, bufsz, flags);
        if (rv > 0) {
            /* successfully read bytes from the socket */
            buf = (char*)buf + rv;
            bufsz -= rv;
        } else if (rv < 0) {
            int err = WSAGetLastError();
            if (err != WSAEWOULDBLOCK) {
                /* an error occurred that wasn't "nothing to read". */
                return MQTT_ERROR_SOCKET_ERROR;
            }
        }
    } while (rv > 0);

    return (ssize_t)((char*)buf - start);
}

#else

#error No PAL!

#endif

/** @endcond */
