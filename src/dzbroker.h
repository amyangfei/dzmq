
#ifndef DZBROKER_H
#define DZBROKER_H

#include "czmq.h"

#define NBR_CLIENTS 10
#define NBR_WORKERS 5
#define WORKER_READY   "\001"      //  Signals worker is ready

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
void *client_task (void *args);
void *client_task_mdp (void *args);
void *worker_task (void *args);

#endif
