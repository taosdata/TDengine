#ifndef _TD_ALIAS_TTQ_H_
#define _TD_ALIAS_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "ttq.h"

int  alias__add(struct tmqtt *ttq, const char *topic, uint16_t alias);
int  alias__find(struct tmqtt *ttq, char **topic, uint16_t alias);
void alias__free_all(struct tmqtt *ttq);

#ifdef __cplusplus
}
#endif

#endif /*_TD_ALIAS_TTQ_H_*/
