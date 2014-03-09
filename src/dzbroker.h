
#ifndef DZBROKER_H
#define DZBROKER_H

#include "czmq.h"

#define NBR_CLIENTS 10
#define NBR_WORKERS 5
#define WORKER_READY   "\001"      //  Signals worker is ready

//  We'd normally pull these from config data
#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable
#define HEARTBEAT_INTERVAL  2500    //  msecs
#define HEARTBEAT_EXPIRY    HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

typedef struct _dz_broker dz_broker;

typedef struct{
    int nbr_id;
    const char *bind_addr;
    int verbose;
} bind_info;

dz_broker *dz_broker_new(const char *local, char **remote, int rlen);
void dz_broker_destory(dz_broker **self_p);
void dz_broker_sim_client(dz_broker *self, int client_num, int verbose);
void dz_broker_sim_worker(dz_broker *self, int worker_num, int verbose);
void dz_broker_main_loop(dz_broker *self);
void dz_broker_main_loop_mdp(dz_broker *self);
void *client_task (void *args);
void *client_task_mdp (void *args);
void *worker_task (void *args);
void *worker_task_mdp(void *args);

#endif
