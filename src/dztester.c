#include "mdp_worker.h"
#include "mdp_client.h"
#include "dztester.h"
#include "dzlog.h"
#include "dzutil.h"

static void *client_task_mdp (void *args);
static void *worker_task_mdp(void *args);

void dz_broker_sim_client(dz_broker *self, int client_num, int verbose) {
    for (int i = 0; i < client_num; i++) {
        const char *addr = dz_broker_get_name(self);
        bind_info binfo = {i, addr, verbose};
        // TODO: a strange segmentation fault bug
        /*bind_info binfo = {i, self->name, verbose};*/
        zthread_new(client_task_mdp, &binfo);
        /*LOG_PRINT(LOG_DEBUG, "binfo:%d, %s, %d", binfo.nbr_id, binfo.bind_addr, binfo.verbose);*/
    }
}

void dz_broker_sim_worker(dz_broker *self, int worker_num, int verbose) {
    for (int i = 0; i < worker_num; i++) {
        const char *addr = dz_broker_get_name(self);
        bind_info binfo = {i, addr, verbose};
        // TODO: a strange segmentation fault bug
        /*bind_info binfo = {i, self->name, verbose};*/
        zthread_new(worker_task_mdp, &binfo);
    }
}

static void *client_task_mdp (void *args) {
    static int total = 0;
    static int success  = 0;
    static int fail = 0;
    static int lost = 0;
    bind_info *binfo = (bind_info *)args;
    int client_id = binfo->nbr_id;
    const char *bind_addr = binfo->bind_addr;
    int verbose = binfo->verbose;

    zctx_t *ctx = zctx_new();
    char endpoint[256];
    sprintf(endpoint, "ipc://%s-localfe.ipc", bind_addr);
    mdp_client_t *mdp_client = mdp_client_new(endpoint, verbose);

    while (true) {
        millisecond_sleep(0, 500);
        int burst = randof (15);
        while (burst--) {
            zmsg_t *request = zmsg_new();
            char task_id [5];
            sprintf (task_id, "%04X", randof (0x10000));
            zmsg_pushstr(request, task_id);

            //  Send request with random hex ID
            LOG_PRINT(LOG_DEBUG, "client-%d new request %s", client_id, task_id);
            mdp_client_send(mdp_client, "echo", &request);
            __sync_fetch_and_add(&total, 1);

            zmsg_t *reply = mdp_client_timeout_recv(mdp_client, NULL, NULL, client_id, task_id);
            if (reply == NULL) {
                __sync_fetch_and_add(&lost, 1);
                printf("total:%d, success:%d, fail:%d\n", total, success, fail);
                break;
            } else {
                zmsg_log_dump(reply, "client recv back:):):):):)");
                zframe_t *data = zmsg_first(reply);
                if (strcmp(task_id, zframe_strdup(data)) == 0) {
                    __sync_fetch_and_add(&success, 1);
                    printf("total:%d, success:%d, fail:%d, lost:%d\n", total, success, fail, lost);
                    zmsg_log_dump(reply, "client recv back success");
                } else {
                    __sync_fetch_and_add(&fail, 1);
                    printf("%s------%s\n", task_id, zframe_strdup(data));
                    printf("total:%d, success:%d, fail:%d, lost:%d\n", total, success, fail, lost);
                    zmsg_log_dump(reply, "client recv back fail");
                }
            }
            zmsg_destroy(&reply);
        }
    }
    zctx_destroy (&ctx);
    return NULL;
}

static void *worker_task_mdp(void *args) {
    bind_info *binfo = (bind_info *)args;
    int worker_id = binfo->nbr_id;
    const char *bind_addr = binfo->bind_addr;
    int verbose = binfo->verbose;

    zctx_t *ctx = zctx_new ();
    char endpoint[256];
    sprintf(endpoint, "ipc://%s-localbe.ipc", bind_addr);
    mdp_worker_t *mdp_worker = mdp_worker_new(endpoint, "echo", verbose);

    //  Process messages as they arrive
    while (true) {
        zframe_t *reply_to;
        zmsg_t *request = mdp_worker_recv (mdp_worker, &reply_to);
        if (request == NULL)
            break;              //  Worker was interrupted
        zmsg_log_dump(request, "WORKER RECV");

        millisecond_sleep(0, randof(3) * 100);
        //  Echo message
        mdp_worker_send (mdp_worker, &request, reply_to);
        LOG_PRINT(LOG_DEBUG, "worker-%d done and send back", worker_id);
        zframe_destroy (&reply_to);
    }
    mdp_worker_destroy(&mdp_worker);
    zctx_destroy (&ctx);
    return NULL;
}

