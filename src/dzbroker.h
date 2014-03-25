
#ifndef DZBROKER_H
#define DZBROKER_H

#include "czmq.h"

//  We'd normally pull these from config data
#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable
#define HEARTBEAT_INTERVAL  2500    //  msecs
#define HEARTBEAT_EXPIRY    HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

typedef struct _dz_broker dz_broker;

dz_broker *dz_broker_new(const char *local, char **remote, int rlen);
void dz_broker_destory(dz_broker **self_p);
const char *dz_broker_get_name(dz_broker *self);
void dz_broker_main_loop_mdp(dz_broker *self);

#endif
