
#ifndef DZSERVICE_H
#define DZSERVICE_H

#include "mdp_common.h"


//  The worker class defines a single worker, idle or active

typedef struct {
    //broker_t *broker;         //  Broker instance
    char *identity;             //  Identity of worker
    char *service;              //  Service name
    zframe_t *address;          //  Address frame to route to
    int64_t expiry;             //  Expires at unless heartbeat
} worker_t;

//worker_t *s_worker_require (broker_t *self, zframe_t *address);
void s_worker_destroy (void *argument);
void s_worker_send (worker_t *self, char *command, char *option, zmsg_t *msg);
void s_worker_waiting (worker_t *self);

#endif

