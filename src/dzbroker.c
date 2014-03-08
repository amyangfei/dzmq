
#include "dzbroker.h"
#include "mdp_client.h"
#include "dzlog.h"
#include "dzcommon.h"
#include "dzutil.h"

struct _dz_broker {
    zctx_t *ctx;
    const char *name;
    char **remote;
    int rlen;
    void *localfe;
    void *localbe;
    void *cloudfe;
    void *cloudbe;
    void *statefe;
    void *statebe;
    int local_capacity;
    int cloud_capacity;
    zlist_t *workers;
};

dz_broker *
dz_broker_new(const char *local, char **remote, int rlen) {
    dz_broker *self = (dz_broker *) zmalloc(sizeof(dz_broker));
    self->name = local;
    self->remote = (char **)malloc(sizeof(char *) * rlen);
    memcpy(self->remote, remote, rlen * sizeof(char *));
    self->rlen = rlen;
    self->ctx = zctx_new();

    self->localfe = zsocket_new(self->ctx, ZMQ_ROUTER);
    zsocket_bind(self->localfe, "ipc://%s-localfe.ipc", local);

    self->localbe = zsocket_new(self->ctx, ZMQ_ROUTER);
    zsocket_bind(self->localbe, "ipc://%s-localbe.ipc", local);

    self->cloudfe = zsocket_new(self->ctx, ZMQ_ROUTER);
    zsocket_set_identity(self->cloudfe, local);
    zsocket_bind(self->cloudfe, "ipc://%s-cloud.ipc", local);

    self->cloudbe = zsocket_new(self->ctx, ZMQ_ROUTER);
    for (int i = 0; i < rlen; i++) {
        char *peer = remote[i];
        LOG_PRINT(LOG_DEBUG, "I: connecting to cloud frontend at '%s'\n", peer);
        zsocket_connect(self->cloudbe, "ipc://%s-cloud.ipc", peer);
    }

    self->statebe = zsocket_new(self->ctx, ZMQ_PUB);
    zsocket_bind(self->statebe, "ipc://%s-state.ipc", local);

    self->statefe = zsocket_new(self->ctx, ZMQ_SUB);
    zsocket_set_subscribe(self->statefe, "");
    for (int i = 0; i < rlen; i++) {
        char *peer = remote[i];
        LOG_PRINT(LOG_DEBUG, "I: connecting to state backend at '%s'\n", peer);
        zsocket_connect(self->statefe, "ipc://%s-state.ipc", peer);
    }

    self->local_capacity = 0;
    self->cloud_capacity = 0;
    self->workers = zlist_new();

    return self;
}

void
dz_broker_destory(dz_broker **self_p) {
    assert(self_p);
    if (*self_p) {
        dz_broker *self = *self_p;
        while (zlist_size(self->workers)) {
            zframe_t *frame = (zframe_t *)zlist_pop(self->workers);
            zframe_destroy(&frame);
        }
        zlist_destroy(&self->workers);
        zctx_destroy(&self->ctx);
        free(self->remote);
        free(self);
        *self_p = NULL;
    }
}

void
dz_broker_sim_client(dz_broker *self, int client_num, int verbose) {
    for (int i = 0; i < client_num; i++) {
        const char *addr = self->name;
        bind_info binfo = {i, addr, verbose};
        // TODO: a strange segmentation fault bug
        /*bind_info binfo = {i, self->name, verbose};*/
        zthread_new(client_task_mdp, &binfo);
        /*LOG_PRINT(LOG_DEBUG, "binfo:%d, %s, %d", binfo.nbr_id, binfo.bind_addr, binfo.verbose);*/
    }
}

void
dz_broker_sim_worker(dz_broker *self, int worker_num, int verbose) {
    for (int i = 0; i < worker_num; i++) {
        const char *addr = self->name;
        bind_info binfo = {i, addr, verbose};
        // TODO: a strange segmentation fault bug
        /*bind_info binfo = {i, self->name, verbose};*/
        zthread_new(worker_task, &binfo);
    }
}

void dz_broker_main_loop(dz_broker *self) {
    while (true) {
        zmq_pollitem_t primary [] = {
            { self->localbe, 0, ZMQ_POLLIN, 0 },
            { self->cloudbe, 0, ZMQ_POLLIN, 0 },
            { self->statefe, 0, ZMQ_POLLIN, 0 }
        };
        //  If we have no workers ready, wait indefinitely
        int rc = zmq_poll (primary, 3,
            self->local_capacity? 1000 * ZMQ_POLL_MSEC: -1);
        if (rc == -1)
            break;              //  Interrupted

        //  Track if capacity changes during this iteration
        int previous = self->local_capacity;
        zmsg_t *msg = NULL;     //  Reply from local worker

        if (primary [0].revents & ZMQ_POLLIN) {
            msg = zmsg_recv(self->localbe);
            if (!msg)
                break;          //  Interrupted
            zframe_t *identity = zmsg_unwrap (msg);
            zlist_append(self->workers, identity);
            self->local_capacity++;

            //  If it's READY, don't route the message any further
            zframe_t *frame = zmsg_first (msg);
            if (memcmp (zframe_data (frame), WORKER_READY, 1) == 0) {
                LOG_PRINT(LOG_DEBUG, "worker-identity-%d READY", identity);
                zmsg_destroy (&msg);
            }
        }
        //  Or handle reply from peer broker
        else
        if (primary [1].revents & ZMQ_POLLIN) {
            msg = zmsg_recv(self->cloudbe);
            if (!msg)
                break;          //  Interrupted
            //  We don't use peer broker identity for anything
            zframe_t *identity = zmsg_unwrap (msg);
            zframe_destroy (&identity);
        }
        //  Route reply to cloud if it's addressed to a broker
        for (int i = 0; msg && i < self->rlen; i++) {
            char *data = (char *) zframe_data (zmsg_first (msg));
            size_t size = zframe_size (zmsg_first (msg));
            if (size == strlen(self->remote[i]) &&  memcmp (data, self->remote[i], size) == 0) {
                zmsg_send (&msg, self->cloudfe);
            }
        }
        //  Route reply to client if we still need to
        if (msg)
            zmsg_send (&msg, self->localfe);

        //  .split handle state messages
        //  If we have input messages on our statefe sockets, we
        //  can process these immediately:

        if (primary[2].revents & ZMQ_POLLIN) {
            char *peer = zstr_recv(self->statefe);
            char *status = zstr_recv(self->statefe);
            self->cloud_capacity = atoi(status);
            free(peer);
            free(status);
        }

        //  Now route as many clients requests as we can handle. If we have
        //  local capacity, we poll both localfe and cloudfe. If we have cloud
        //  capacity only, we poll just localfe. We route any request locally
        //  if we can, else we route to the cloud.

        while (self->local_capacity + self->cloud_capacity) {
            zmq_pollitem_t secondary [] = {
                { self->localfe, 0, ZMQ_POLLIN, 0 },
                { self->cloudfe, 0, ZMQ_POLLIN, 0 }
            };
            if(self->local_capacity)
                rc = zmq_poll(secondary, 2, 0);
            else
                rc = zmq_poll(secondary, 1, 0);
            assert (rc >= 0);

            if (secondary [0].revents & ZMQ_POLLIN)
                msg = zmsg_recv(self->localfe);
            else if (secondary [1].revents & ZMQ_POLLIN) {
                msg = zmsg_recv(self->cloudfe);
                LOG_PRINT(LOG_DEBUG, "GET msg from broker peer");
            } else
                break;      //  No work, go back to primary

            if (self->local_capacity) {
                zframe_t *frame = (zframe_t *)zlist_pop(self->workers);
                zmsg_wrap (msg, frame);
                zmsg_send (&msg, self->localbe);
                self->local_capacity--;
            }
            else {
                //  Route to random broker peer
                int peer = randof(self->rlen);
                zmsg_pushmem (msg, self->remote[peer], strlen(self->remote[peer]));

                zmsg_log_dump(msg, "Route msg");

                zmsg_send (&msg, self->cloudbe);
                LOG_PRINT(LOG_DEBUG, "Route to random broker peer %s", self->remote[peer]);
            }
        }
        //  We broadcast capacity messages to other peers; to reduce chatter,
        //  we do this only if our capacity changed.

        if (self->local_capacity != previous) {
            //  We stick our own identity onto the envelope
            zstr_sendm(self->statebe, self->name);
            //  Broadcast new capacity
            zstr_sendf(self->statebe, "%d", self->local_capacity);
        }
    }
}

/*static void **/
void *
client_task (void *args)
{
    bind_info *binfo = (bind_info *)args;
    int client_id = binfo->nbr_id;
    const char *bind_addr = binfo->bind_addr;

    zctx_t *ctx = zctx_new ();
    void *client = zsocket_new (ctx, ZMQ_REQ);
    zsocket_connect (client, "ipc://%s-localfe.ipc", bind_addr);

    while (true) {
        sleep (randof (5));
        int burst = randof (15);
        while (burst--) {
            char task_id [5];
            sprintf (task_id, "%04X", randof (0x10000));

            //  Send request with random hex ID
            LOG_PRINT(LOG_INFO, "client-%d send %s", client_id, task_id);
            zstr_send (client, task_id);

            //  Wait max ten seconds for a reply, then complain
            zmq_pollitem_t pollset [1] = { { client, 0, ZMQ_POLLIN, 0 } };
            int rc = zmq_poll (pollset, 1, 10 * 1000 * ZMQ_POLL_MSEC);
            if (rc == -1)
                break;          //  Interrupted

            if (pollset [0].revents & ZMQ_POLLIN) {
                char *reply = zstr_recv (client);
                if (!reply) {
                    LOG_PRINT(LOG_ERROR, "client-%d interrupted", client_id);
                    break;              //  Interrupted
                }
                //  Worker is supposed to answer us with our task id
                assert (streq (reply, task_id));
                LOG_PRINT(LOG_INFO, "client-%d work done and recv %s", client_id, reply);
                free (reply);
            }
            else {
                LOG_PRINT(LOG_ERROR, "client-%d E: CLIENT EXIT - lost task %s", client_id, task_id);
                return NULL;
            }
        }
    }
    zctx_destroy (&ctx);
    return NULL;
}

void *
client_task_mdp (void *args)
{
    bind_info *binfo = (bind_info *)args;
    int client_id = binfo->nbr_id;
    const char *bind_addr = binfo->bind_addr;
    int verbose = binfo->verbose;

    zctx_t *ctx = zctx_new();
    char endpoint[256];
    sprintf(endpoint, "ipc://%s-localfe.ipc", bind_addr);
    mdp_client_t *mdp_client = mdp_client_new(endpoint, verbose);

    while (true) {
        sleep (randof (5));
        int burst = randof (15);
        while (burst--) {
            zmsg_t *request = zmsg_new();
            char task_id [5];
            sprintf (task_id, "%04X", randof (0x10000));
            zmsg_pushstr(request, task_id);

            //  Send request with random hex ID
            LOG_PRINT(LOG_INFO, "client-%d send %s", client_id, task_id);
            mdp_client_send(mdp_client, "echo", &request);

            /**
            //  Wait max ten seconds for a reply, then complain
            zmq_pollitem_t pollset [1] = { { mdp_client, 0, ZMQ_POLLIN, 0 } };
            int rc = zmq_poll (pollset, 1, 1 * 1000 * ZMQ_POLL_MSEC);
            if (rc == -1) {
                LOG_PRINT(LOG_ERROR, "client-%d -recv Interrupted - lost task %s", client_id, task_id);
                break;          //  Interrupted
            }

            if (pollset [0].revents & ZMQ_POLLIN) {
                zmsg_t *reply = mdp_client_recv(mdp_client, NULL, NULL);
                if (!reply) {
                    LOG_PRINT(LOG_ERROR, "client-%d interrupted", client_id);
                    break;              //  Interrupted
                }
                //  Worker is supposed to answer us with our task id
                assert (streq (zmsg_popstr(reply), task_id));
                LOG_PRINT(LOG_INFO, "client-%d work done and recv %s", client_id, reply);
                zmsg_destroy(&reply);
            }
            else {
                LOG_PRINT(LOG_ERROR, "client-%d E: CLIENT EXIT - lost task %s", client_id, task_id);
                return NULL;
            }
            */
            zmsg_t *reply = mdp_client_timeout_recv(mdp_client, NULL, NULL, client_id, task_id);
            zmsg_destroy(&reply);
        }
    }
    zctx_destroy (&ctx);
    return NULL;
}

void *
worker_task (void *args)
{
    bind_info *binfo = (bind_info *)args;
    int worker_id = binfo->nbr_id;
    const char *bind_addr = binfo->bind_addr;

    zctx_t *ctx = zctx_new ();
    void *worker = zsocket_new (ctx, ZMQ_REQ);
    zsocket_connect (worker, "ipc://%s-localbe.ipc", bind_addr);

    //  Tell broker we're ready for work
    zframe_t *frame = zframe_new (WORKER_READY, 1);
    zframe_send (&frame, worker, 0);

    //  Process messages as they arrive
    while (true) {
        zmsg_t *msg = zmsg_recv (worker);
        if (!msg)
            break;              //  Interrupted

        //  Workers are busy for 0/1 seconds
        sleep (randof (2));
        /*zmsg_t *debug_msg = zmsg_dup(msg);*/
        zmsg_send (&msg, worker);
        /**
        int msglen = zmsg_size(debug_msg);
        zframe_t **mfs = (zframe_t **)zmalloc(sizeof(zframe_t *) * msglen);
        for (int i = 0; i < msglen; i++){
            mfs[i] = zmsg_pop(debug_msg);
            LOG_PRINT(LOG_DEBUG, "worker-%d done and frame %d %s", worker_id, i, zframe_data(mfs[i]));
        }
        free (mfs);
        zmsg_destroy(&debug_msg);
        */
        LOG_PRINT(LOG_INFO, "worker-%d done and send back", worker_id);
    }
    zctx_destroy (&ctx);
    return NULL;
}

