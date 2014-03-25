
#ifndef DZTESTER_H
#define DZTESTER_H

#include "czmq.h"
#include "dzbroker.h"

#define NBR_CLIENTS 10
#define NBR_WORKERS 5

typedef struct{
    int nbr_id;
    const char *bind_addr;
    int verbose;
} bind_info;

void dz_broker_sim_client(dz_broker *self, int client_num, int verbose);
void dz_broker_sim_worker(dz_broker *self, int worker_num, int verbose);

#endif
