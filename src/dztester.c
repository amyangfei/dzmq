#include "mdp_worker.h"
#include "mdp_client.h"
#include "dztester.h"
#include "dzlog.h"
#include "dzutil.h"

static void *client_task_mdp (void *args);
static void *worker_task_mdp(void *args);
static void *client_task_sync_test (void *args);
static void *client_task_async_test (void *args);

static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

static void delete_bindinfo(bind_info *binfo) {
    free(binfo->bind_addr);
    free(binfo);
}

void dz_broker_sim_client(dz_broker *self, int client_num, int verbose) {
    for (int i = 0; i < client_num; i++) {
        const char *addr = dz_broker_get_name(self);
        /*bind_info binfo = {i, addr, verbose};*/

        bind_info *binfo = (bind_info *) zmalloc(sizeof(bind_info));
        binfo->nbr_id = i;
        binfo->bind_addr = strdup(addr);
        binfo->verbose = verbose;

        // TODO: a strange segmentation fault bug
        // done
        /*zthread_new(client_task_mdp, &binfo);*/
        zthread_new(client_task_sync_test, binfo);
        /*zthread_new(client_task_async_test, &binfo);*/
    }
}

void dz_broker_sim_worker(dz_broker *self, int worker_num, int verbose) {
    for (int i = 0; i < worker_num; i++) {
        const char *addr = dz_broker_get_name(self);
        bind_info *binfo = (bind_info *) zmalloc(sizeof(bind_info));
        binfo->nbr_id = i;
        binfo->bind_addr = strdup(addr);
        binfo->verbose = verbose;
        zthread_new(worker_task_mdp, binfo);
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
        int burst = randof (REQ_PER_SECOND);
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
                zframe_t *data = zmsg_first(reply);
                if (strcmp(task_id, zframe_strdup(data)) == 0) {
                    __sync_fetch_and_add(&success, 1);
                    printf("total:%d, success:%d, fail:%d, lost:%d\n", total, success, fail, lost);
                    /*zmsg_log_dump(reply, "client recv back success");*/
                } else {
                    __sync_fetch_and_add(&fail, 1);
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

        /*millisecond_sleep(0, randof(3) * 100);*/

        mdp_worker_send (mdp_worker, &request, reply_to);
        LOG_PRINT(LOG_DEBUG, "worker-%d done and send back", worker_id);
        zframe_destroy (&reply_to);
    }
    mdp_worker_destroy(&mdp_worker);
    zctx_destroy (&ctx);
    return NULL;
}

static void *client_task_sync_test (void *args) {
    enum _reply_type
    {
        r_success=1, r_fail, r_lost
    };
    static const char *reply_type_str[] = {
        "r_unknown", "r_success", "r_fail", "r_lost",
    };
    static int internal_millisecond = 800;
    static int data_size = 8;
    static long long total_latency = 0;
    static int total = 0;
    static int success  = 0;
    static int fail = 0;
    static int lost = 0;
    bind_info *binfo = (bind_info *)args;
    int client_id = binfo->nbr_id;
    const char *bind_addr = binfo->bind_addr;
    int verbose = binfo->verbose;
    char *work_load = (char *)malloc((data_size + 1) * sizeof(char));
    for (int i = 0; i < data_size; ++i) {
        work_load[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    work_load[data_size] = 0;

    zctx_t *ctx = zctx_new();
    char endpoint[256];
    sprintf(endpoint, "ipc://%s-localfe.ipc", bind_addr);
    mdp_client_t *mdp_client = mdp_client_new(endpoint, verbose);

    while (true) {
        sleep(1);

        int burst = REQ_PER_SECOND;
        while (burst--) {
            zmsg_t *request = zmsg_new();
            char task_id [5];
            sprintf (task_id, "%04X", randof (0x10000));
            zmsg_pushstr(request, work_load);
            zmsg_pushstr(request, task_id);

            LOG_PRINT(LOG_DEBUG, "client-%d new request %s", client_id, task_id);
            mdp_client_send(mdp_client, "echo", &request);
            __sync_fetch_and_add(&total, 1);

            // latency statistics
            struct timeval stop, start;
            gettimeofday(&start, NULL);

            zmsg_t *reply = mdp_client_timeout_recv(mdp_client, NULL, NULL, client_id, task_id);

            gettimeofday(&stop, NULL);
            long long interval = (stop.tv_sec - start.tv_sec) * 1e6 + stop.tv_usec - start.tv_usec;
            /*total_latency += (stop.tv_sec - start.tv_sec) * 1e6 + stop.tv_usec - start.tv_usec;*/
            /*__sync_fetch_and_add(&total_latency, (stop.tv_sec - start.tv_sec) * 1e6 + stop.tv_usec - start.tv_usec);*/

            int this_reply_type = 0;
            if (reply == NULL) {
                __sync_fetch_and_add(&lost, 1);
                /*printf("total:%d, success:%d, fail:%d\n", total, success, fail);*/
                this_reply_type = r_lost;
                break;
            } else {
                zframe_t *data = zmsg_first(reply);
                if (strcmp(task_id, zframe_strdup(data)) == 0) {
                    __sync_fetch_and_add(&success, 1);
                    this_reply_type = r_success;
                    /*printf("total:%d, success:%d, fail:%d, lost:%d, average latency:%lld\n", total, success, fail, lost, total_latency/total);*/
                    /*zmsg_log_dump(reply, "client recv back success");*/
                } else {
                    __sync_fetch_and_add(&fail, 1);
                    this_reply_type = r_fail;
                    /*printf("total:%d, success:%d, fail:%d, lost:%d\n", total, success, fail, lost);*/
                    /*zmsg_log_dump(reply, "client recv back fail");*/
                }
            }
            LOG_PRINT(LOG_INFO, "type:%s:interval:%d", reply_type_str[this_reply_type], (int)interval);
            zmsg_destroy(&reply);
        }
    }
    zctx_destroy (&ctx);
    return NULL;
}

static void *client_task_async_test (void *args) {
    enum _reply_type
    {
        r_success=1, r_fail, r_lost
    };
    static const char *reply_type_str[] = {
        "r_unknown", "r_success", "r_fail", "r_lost",
    };
    static int data_size = 8;
    static int total = 0;
    static int success  = 0;
    static int fail = 0;
    static int lost = 0;
    bind_info *binfo = (bind_info *)args;
    int client_id = binfo->nbr_id;
    const char *bind_addr = binfo->bind_addr;
    int verbose = binfo->verbose;
    char *work_load = (char *)malloc((data_size + 1) * sizeof(char));
    for (int i = 0; i < data_size; ++i) {
        work_load[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    work_load[data_size] = 0;

    zctx_t *ctx = zctx_new();
    char endpoint[256];
    sprintf(endpoint, "ipc://%s-localfe.ipc", bind_addr);
    mdp_client_t *mdp_client = mdp_client_new(endpoint, verbose);

    zhash_t *sent_sets = zhash_new();

    while (true) {
        sleep(1);

        for (int i = 0; i < REQ_GROUP; ++i) {
            int burst = REQ_PER_SECOND / REQ_GROUP, burst2 = REQ_PER_SECOND / REQ_GROUP;
            long long interval = 0;
            struct timeval stop, start;
            while (burst--) {
                zmsg_t *request = zmsg_new();
                char task_id [5];
                sprintf (task_id, "%04X", randof (0x10000));
                zmsg_pushstr(request, work_load);
                zmsg_pushstr(request, task_id);

                LOG_PRINT(LOG_DEBUG, "client-%d new request %s", client_id, task_id);
                mdp_client_send(mdp_client, "echo", &request);
                __sync_fetch_and_add(&total, 1);
                int *sent_value = (int *)zhash_lookup(sent_sets, task_id);
                if (sent_value == NULL) {
                    sent_value = (int *)zmalloc(sizeof(int *));
                    *sent_value = 1;
                    zhash_insert(sent_sets, task_id, sent_value);
                } else {
                    (*sent_value)++;
                }
            }
            while (burst2--) {
                // latency statistics
                /*struct timeval stop, start;*/
                /*gettimeofday(&start, NULL);*/

                gettimeofday(&start, NULL);

                zmsg_t *reply = mdp_client_timeout_async_recv(mdp_client, NULL, NULL, client_id, burst2 == REQ_PER_SECOND-1 ? true : false);

                gettimeofday(&stop, NULL);
                interval += ( (stop.tv_sec - start.tv_sec) * 1e6 + stop.tv_usec - start.tv_usec );

                int this_reply_type = 0;
                if (reply == NULL) {
                    __sync_fetch_and_add(&lost, 1);
                    this_reply_type = r_lost;
                } else {
                    zframe_t *data = zmsg_first(reply);
                    int *sent_value = (int *)zhash_lookup(sent_sets, zframe_strdup(data));
                    if (sent_value != NULL) {
                        __sync_fetch_and_add(&success, 1);
                        this_reply_type = r_success;
                        if (*sent_value == 1) {
                            zhash_delete(sent_sets, zframe_strdup(data));
                        } else {
                            (*sent_value)--;
                        }
                    } else {
                        __sync_fetch_and_add(&fail, 1);
                        this_reply_type = r_fail;
                    }
                }
                zmsg_destroy(&reply);
            }
            LOG_PRINT(LOG_INFO, "type:%s:interval:%d", "r_success", (int)interval);
        }
    }
    zctx_destroy (&ctx);
    return NULL;
}

