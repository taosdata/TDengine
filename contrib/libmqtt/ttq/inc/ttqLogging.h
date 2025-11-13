#ifndef _TD_LOGGING_TTQ_H_
#define _TD_LOGGING_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tmqttInt.h"

#ifndef __GNUC__
#define __attribute__(attrib)
#endif

int ttq_log(struct tmqtt *ttq, unsigned int level, const char *fmt, ...) __attribute__((format(printf, 3, 4)));

#ifdef __cplusplus
}
#endif

#endif /*_TD_LOGGING_TTQ_H_*/
