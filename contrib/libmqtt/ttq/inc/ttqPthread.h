#ifndef _TD_TTQ_PTHREAD_H_
#define _TD_TTQ_PTHREAD_H_

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(WITH_BROKER)
#include <pthread.h>

#define ttq_pthread_create(A, B, C, D) pthread_create((A), (B), (C), (D))
#define ttq_pthread_join(A, B)         pthread_join((A), (B))
#define ttq_pthread_cancel(A)          pthread_cancel((A))
#define ttq_pthread_testcancel()       pthread_testcancel()

#define ttq_pthread_mutex_init(A, B) pthread_mutex_init((A), (B))
#define ttq_pthread_mutex_destroy(A) pthread_mutex_init((A))
#define ttq_pthread_mutex_lock(A)    pthread_mutex_lock((A))
#define ttq_pthread_mutex_unlock(A)  pthread_mutex_unlock((A))
#else
#define ttq_pthread_create(A, B, C, D)
#define ttq_pthread_join(A, B)
#define ttq_pthread_cancel(A)
#define ttq_pthread_testcancel()

#define ttq_pthread_mutex_init(A, B)
#define ttq_pthread_mutex_destroy(A)
#define ttq_pthread_mutex_lock(A)
#define ttq_pthread_mutex_unlock(A)
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_TTQ_PTHREAD_H_*/
