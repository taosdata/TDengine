/*
 * conn_pool.c -- Thread-safe connection pool for TDengine native connections.
 *
 * All connections are pre-created at init time and handed out / returned
 * through boost_pool_get / boost_pool_put.  Access is serialized with a
 * pthread mutex; waiters block on a condition variable with a 5-second
 * timeout so callers never hang indefinitely.
 */

#include "boost.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------------ */
/*  Internal helpers                                                        */
/* ------------------------------------------------------------------------ */

#define CONN_RETRY_MAX   3
#define CONN_RETRY_DELAY 1          /* seconds between retries              */
#define POOL_GET_TIMEOUT 5          /* seconds to wait in boost_pool_get    */

/*
 * Try to establish a single TDengine connection, retrying up to
 * CONN_RETRY_MAX times on failure.
 */
static TAOS *create_connection(const BoostEndpoint *ep)
{
    TAOS *conn = NULL;

    for (int attempt = 1; attempt <= CONN_RETRY_MAX; ++attempt) {
        conn = taos_connect(ep->host, ep->user, ep->pass, NULL, ep->port);
        if (conn) {
            return conn;
        }

        BOOST_ERR("taos_connect failed (attempt %d/%d): %s",
                  attempt, CONN_RETRY_MAX, taos_errstr(NULL));

        if (attempt < CONN_RETRY_MAX) {
            sleep(CONN_RETRY_DELAY);
        }
    }

    return NULL;
}

/* ------------------------------------------------------------------------ */
/*  Public API                                                              */
/* ------------------------------------------------------------------------ */

int boost_pool_init(BoostConnPool *pool, BoostEndpoint *ep, int capacity)
{
    if (!pool || !ep || capacity <= 0) {
        BOOST_ERR("boost_pool_init: invalid arguments");
        return -1;
    }

    memset(pool, 0, sizeof(*pool));

    /* Initialize synchronization primitives. */
    if (pthread_mutex_init(&pool->lock, NULL) != 0) {
        BOOST_ERR("pthread_mutex_init failed: %s", strerror(errno));
        return -1;
    }

    if (pthread_cond_init(&pool->avail, NULL) != 0) {
        BOOST_ERR("pthread_cond_init failed: %s", strerror(errno));
        pthread_mutex_destroy(&pool->lock);
        return -1;
    }

    /* Allocate connection and usage-tracking arrays. */
    pool->conns = (TAOS **)calloc(capacity, sizeof(TAOS *));
    if (!pool->conns) {
        BOOST_ERR("calloc conns failed");
        pthread_cond_destroy(&pool->avail);
        pthread_mutex_destroy(&pool->lock);
        return -1;
    }

    pool->in_use = (bool *)calloc(capacity, sizeof(bool));
    if (!pool->in_use) {
        BOOST_ERR("calloc in_use failed");
        free(pool->conns);
        pthread_cond_destroy(&pool->avail);
        pthread_mutex_destroy(&pool->lock);
        return -1;
    }

    pool->capacity = capacity;
    pool->size     = 0;

    /* Copy endpoint information for potential reconnects. */
    pool->ep = *ep;

    /* Pre-create all connections. */
    for (int i = 0; i < capacity; ++i) {
        pool->conns[i] = create_connection(ep);
        if (!pool->conns[i]) {
            BOOST_ERR("failed to create connection %d/%d", i + 1, capacity);
            /* Tear down connections created so far. */
            for (int j = 0; j < i; ++j) {
                taos_close(pool->conns[j]);
            }
            free(pool->in_use);
            free(pool->conns);
            pthread_cond_destroy(&pool->avail);
            pthread_mutex_destroy(&pool->lock);
            return -1;
        }

        pool->in_use[i] = false;
        pool->size++;
        BOOST_LOG("connection %d/%d created (%s:%u)", i + 1, capacity,
                  ep->host, (unsigned)ep->port);
    }

    BOOST_LOG("connection pool initialized: capacity=%d", capacity);
    return 0;
}

void boost_pool_destroy(BoostConnPool *pool)
{
    if (!pool) {
        return;
    }

    pthread_mutex_lock(&pool->lock);

    for (int i = 0; i < pool->size; ++i) {
        if (pool->conns[i]) {
            taos_close(pool->conns[i]);
            pool->conns[i] = NULL;
        }
    }

    pthread_mutex_unlock(&pool->lock);

    free(pool->conns);
    pool->conns = NULL;

    free(pool->in_use);
    pool->in_use = NULL;

    pool->size     = 0;
    pool->capacity = 0;

    pthread_cond_destroy(&pool->avail);
    pthread_mutex_destroy(&pool->lock);

    BOOST_LOG("connection pool destroyed");
}

TAOS *boost_pool_get(BoostConnPool *pool)
{
    if (!pool) {
        return NULL;
    }

    pthread_mutex_lock(&pool->lock);

    /* Build an absolute deadline for the timed wait. */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += POOL_GET_TIMEOUT;

    /* Spin until a free slot appears or we time out. */
    for (;;) {
        for (int i = 0; i < pool->size; ++i) {
            if (!pool->in_use[i]) {
                pool->in_use[i] = true;
                TAOS *conn = pool->conns[i];
                pthread_mutex_unlock(&pool->lock);
                return conn;
            }
        }

        /* No free connection -- wait for one to be returned. */
        int rc = pthread_cond_timedwait(&pool->avail, &pool->lock, &deadline);
        if (rc == ETIMEDOUT) {
            BOOST_ERR("boost_pool_get: timed out after %d seconds",
                      POOL_GET_TIMEOUT);
            pthread_mutex_unlock(&pool->lock);
            return NULL;
        }
        if (rc != 0 && rc != ETIMEDOUT) {
            BOOST_ERR("pthread_cond_timedwait failed: %s", strerror(rc));
            pthread_mutex_unlock(&pool->lock);
            return NULL;
        }
    }
}

void boost_pool_put(BoostConnPool *pool, TAOS *conn)
{
    if (!pool || !conn) {
        return;
    }

    pthread_mutex_lock(&pool->lock);

    for (int i = 0; i < pool->size; ++i) {
        if (pool->conns[i] == conn) {
            pool->in_use[i] = false;
            pthread_cond_signal(&pool->avail);
            pthread_mutex_unlock(&pool->lock);
            return;
        }
    }

    /* Connection not found -- caller passed a stale or foreign handle. */
    BOOST_ERR("boost_pool_put: unknown connection %p", (void *)conn);
    pthread_mutex_unlock(&pool->lock);
}
