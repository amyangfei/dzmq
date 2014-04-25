
#include "dzbroker.h"
#include "dzservice.h"
#include "mdp_client.h"
#include "mdp_worker.h"
#include "dzlog.h"
#include "dzcommon.h"
#include "dzutil.h"

struct _dz_broker {
    zctx_t *ctx;            // Context
    const char *name;       // Broker name
    char **remote;          // Remote brokers' name
    int rlen;               // number of remote brokers
    void *localfe;
    void *localbe;
    void *cloudfe;
    void *cloudbe;
    void *statefe;
    void *statebe;
    int local_capacity;
    int cloud_capacity;
    zlist_t *workers;
    zhash_t *services;      // Hash of known services
    zhash_t *workers_hash;  // Hash of known workers
    zlist_t *waiting;       // List of waiting workers
    uint64_t heartbeat_at;  // When to send HEARTBEAT
};

static void
    s_broker_worker_msg (dz_broker *self, zmsg_t *msg, bool from_local);
static void
    s_broker_client_msg (dz_broker *self, zmsg_t *msg, bool from_local);
static void
    s_broker_purge (dz_broker *self);


dz_broker *dz_broker_new(const char *local, char **remote, int rlen) {
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
    /*zsocket_bind(self->cloudfe, "ipc://%s-cloud.ipc", local);*/
    char node_cloudfe[MAX_LINE] = "";
    snprintf(node_cloudfe, MAX_LINE, "%s-cloudfe", local);
    char *cloudfe_endpoint = (char *)zhash_lookup(settings.nodes, node_cloudfe);
    zsocket_bind(self->cloudfe, "tcp://%s", cloudfe_endpoint);

    self->cloudbe = zsocket_new(self->ctx, ZMQ_ROUTER);
    zsocket_set_identity(self->cloudbe, local);
    for (int i = 0; i < rlen; i++) {
        char *peer = remote[i];
        snprintf(node_cloudfe, MAX_LINE, "%s-cloudfe", peer);
        cloudfe_endpoint = (char *)zhash_lookup(settings.nodes, node_cloudfe);
        LOG_PRINT(LOG_INFO, "I: connecting to cloud frontend at '%s %s'\n", peer, cloudfe_endpoint);
        /*zsocket_connect(self->cloudbe, "ipc://%s-cloud.ipc", peer);*/
        zsocket_connect(self->cloudbe, "tcp://%s", cloudfe_endpoint);
    }

    self->statebe = zsocket_new(self->ctx, ZMQ_PUB);
    char node_statebe[MAX_LINE] = "";
    snprintf(node_statebe, MAX_LINE, "%s-statebe", local);
    char *statebe_endpoint = (char *)zhash_lookup(settings.nodes, node_statebe);
    /*zsocket_bind(self->statebe, "ipc://%s-state.ipc", local);*/
    zsocket_bind(self->statebe, "tcp://%s", statebe_endpoint);

    self->statefe = zsocket_new(self->ctx, ZMQ_SUB);
    zsocket_set_subscribe(self->statefe, "");
    for (int i = 0; i < rlen; i++) {
        char *peer = remote[i];
        snprintf(node_statebe, MAX_LINE, "%s-statebe", peer);
        statebe_endpoint = (char *)zhash_lookup(settings.nodes, node_statebe);
        LOG_PRINT(LOG_INFO, "I: connecting to state backend at '%s', %s\n", peer, statebe_endpoint);
        /*zsocket_connect(self->statefe, "ipc://%s-state.ipc", peer);*/
        zsocket_connect(self->statefe, "tcp://%s", statebe_endpoint);
    }

    self->local_capacity = 0;
    self->cloud_capacity = 0;
    self->workers = zlist_new();

    self->services = zhash_new();
    self->workers_hash = zhash_new();
    self->waiting = zlist_new();
    self->heartbeat_at = zclock_time() + HEARTBEAT_INTERVAL;

    return self;
}

void dz_broker_destory(dz_broker **self_p) {
    assert(self_p);
    if (*self_p) {
        dz_broker *self = *self_p;
        while (zlist_size(self->workers)) {
            zframe_t *frame = (zframe_t *)zlist_pop(self->workers);
            zframe_destroy(&frame);
        }
        zlist_destroy(&self->workers);
        zctx_destroy(&self->ctx);
        zhash_destroy(&self->services);
        zhash_destroy(&self->workers_hash);
        zlist_destroy(&self->waiting);
        free(self->remote);
        free(self);
        *self_p = NULL;
    }
}

const char *dz_broker_get_name(dz_broker *self) {
    return self->name;
}

//  Here is the implementation of the methods that work on a worker.
//  Lazy constructor that locates a worker by identity, or creates a new
//  worker if there is no worker already with that identity.

//  Delete worker by dz_broker.
void s_broker_worker_delete (dz_broker *self, worker_t *worker) {
    /*s_worker_delete(worker, disconnect);*/
    self->local_capacity--;
    zlist_remove (self->workers, worker->identity);
    zlist_remove (self->waiting, worker);
    //  This implicitly calls s_worker_destroy
    zhash_delete (self->workers_hash, worker->identity);
    // TODO: notify other brokers about local_capacity change
}

static worker_t *s_worker_require (dz_broker *self, zframe_t *address)
{
    assert (address);

    //  self->workers is keyed off worker identity
    char *identity = zframe_strhex (address);
    worker_t *worker =
        (worker_t *) zhash_lookup (self->workers_hash, identity);

    if (worker == NULL) {
        worker = (worker_t *) zmalloc (sizeof (worker_t));
        /*worker->broker = self;*/
        worker->identity = identity;
        worker->address = zframe_dup (address);
        zhash_insert (self->workers_hash, identity, worker);
        zhash_freefn (self->workers_hash, identity, s_worker_destroy);
        LOG_PRINT(LOG_DEBUG, "Registering new worker: %s", identity);
    }
    else
        free (identity);
    return worker;
}

void s_broker_worker_msg(dz_broker *self, zmsg_t *msg, bool from_local) {
    // At least: identity, empty, header, command
    // or identity, empty, header, command [peer_name] service data
    assert(zmsg_size(msg) >= 4);
    zframe_t *sender = zmsg_pop (msg);
    zframe_t *empty  = zmsg_pop (msg);
    zframe_t *header = zmsg_pop (msg);
    assert( zframe_streq(header, MDPW_WORKER) == true );

    zframe_t *command = zmsg_pop(msg);

    /*char *identity = zframe_strhex(sender);*/
    /*int worker_ready = (zhash_lookup (self->workers_hash, identity) != NULL);*/
    /*free(identity);*/
    worker_t *worker = s_worker_require(self, sender);

    if (zframe_streq(command, MDPW_READY)) {
        if (from_local) {
            self->local_capacity++;
            zlist_append(self->workers, sender);
        }

        //  Attach worker to service and mark as idle
        zframe_t *service_frame = zmsg_pop (msg);
        zlist_append (self->waiting, worker);
        worker->expiry = zclock_time () + HEARTBEAT_EXPIRY;

        if (worker->service) {
            free(worker->service);
            // worker->service = NULL;
        }
        worker->service = zframe_strdup(service_frame);

        zframe_destroy (&service_frame);
        char *identity = zframe_strhex(sender);
        LOG_PRINT(LOG_INFO, "worker-%s ready and broker worker_t created", identity);
        free(identity);
        zmsg_destroy(&msg);
    }  else if (zframe_streq(command, MDPW_REPORT_LOCAL)) {
        // handle self client's request, and send back to local client
        zmsg_log_dump(msg, "REPORT TO LOCAL");
        zframe_t *client = zmsg_unwrap (msg);
        /*zmsg_pushstr (msg, worker->service);*/
        zmsg_pushstr (msg, MDPC_REPORT);
        zmsg_pushstr (msg, MDPC_CLIENT);
        zmsg_wrap (msg, client);

        if (from_local) {
            self->local_capacity++;
            zlist_append(self->workers, sender);
        }

        //  Route reply to client if we still need to
        if (msg) {
            zmsg_send (&msg, self->localfe);
        }
    } else if (zframe_streq(command, MDPW_REPORT_CLOUD)) {
        if (from_local) {
            // handle peer cient's request, and send back to cloud
            self->local_capacity++;
            zlist_append(self->workers, sender);

            zframe_t *client = zmsg_unwrap (msg);
            zframe_t *peer_name = zmsg_pop(msg);

            char *data = (char *) zframe_data (peer_name);
            //  Route reply to cloud if it's addressed to a broker
            for (int i = 0; msg && i < self->rlen; i++) {
                size_t size = zframe_size (peer_name);
                if (size == strlen(self->remote[i]) &&  memcmp (data, self->remote[i], size) == 0) {
                    zmsg_pushstr (msg, MDPW_REPORT_CLOUD);
                    zmsg_pushstr (msg, MDPW_WORKER);
                    zmsg_wrap (msg, client);
                    zmsg_push(msg, peer_name);

                    zmsg_send (&msg, self->cloudfe);
                }
            }

        }
        else {
            // msg: service data

            zmsg_pushstr (msg, MDPC_REPORT);
            zmsg_pushstr (msg, MDPC_CLIENT);
            zmsg_wrap (msg, sender);

            zmsg_log_dump(msg, "WORK DONE BY PEER");
            zmsg_send (&msg, self->localfe);
        }

    } else if (zframe_streq(command, MDPW_HEARTBEAT)) {
        worker->expiry = zclock_time() + HEARTBEAT_EXPIRY;
    } else if (zframe_streq(command, MDPW_DISCONNECT)) {
        s_broker_worker_delete(self, worker);
    } else {
        LOG_PRINT(LOG_ERROR, "invalid worker message");
    }
    zframe_destroy(&command);
    zmsg_destroy(&msg);
}

static void s_broker_client_msg(dz_broker *self, zmsg_t *msg, bool from_local) {
    // At least: identity, empty, header, service_name, body
    assert (zmsg_size (msg) >= 5);

    zframe_t *sender = zmsg_pop (msg);

    // TODO: check whether service available
    if (self->local_capacity) {
        if (from_local) {
            zframe_t *empty  = zmsg_pop (msg);
            zframe_t *header = zmsg_pop (msg);
            zframe_destroy(&empty);
            zframe_destroy(&header);
            zframe_t *service = zmsg_pop (msg);

            zmsg_wrap(msg, sender);
            zmsg_push(msg, service);
            zmsg_pushstr(msg, MDPW_REQUEST);
        } else {
            // send other peer's client request
            zframe_t *header = zmsg_pop(msg);
            assert(zframe_streq(header, MDPC_CLIENT));
            zframe_destroy (&header);

            zframe_t *command = zmsg_pop(msg);
            assert(zframe_streq(command, MDPC_REPOST));
            zframe_destroy (&command);

            zmsg_pushstr(msg, MDPW_REPOST);
        }
        zmsg_pushstr(msg, MDPW_WORKER);
        zframe_t *frame = (zframe_t *)zlist_pop(self->workers);
        zmsg_wrap (msg, frame);

        zmsg_send (&msg, self->localbe);
        self->local_capacity--;

    } else {
        zframe_t *empty  = zmsg_pop (msg);
        zframe_t *header = zmsg_pop (msg);
        zframe_t *service = zmsg_pop (msg);
        zframe_destroy(&empty);
        zframe_destroy(&header);

        //  Route to random broker peer
        int peer = randof(self->rlen);

        zmsg_wrap(msg, sender);
        zmsg_pushstr(msg, self->name);
        /*zmsg_pushmem (msg, self->remote[peer], strlen(self->remote[peer]));*/
        /*zmsg_pushmem (msg, self->name, strlen(self->name));*/
        zmsg_push(msg, service);
        zmsg_pushstr(msg, MDPC_REPOST);
        zmsg_pushstr(msg, MDPC_CLIENT);

        zmsg_pushmem (msg, self->remote[peer], strlen(self->remote[peer]));
        zmsg_log_dump(msg, "Route msg");

        zmsg_send (&msg, self->cloudbe);
        LOG_PRINT(LOG_DEBUG, "Route to random broker peer %s", self->remote[peer]);
    }

    /**
    //  Send a NAK message back to the client.
    zmsg_push (msg, zframe_dup (service_frame));
    zmsg_pushstr (msg, MDPC_NAK);
    zmsg_pushstr (msg, MDPC_CLIENT);
    zmsg_wrap (msg, zframe_dup (sender));
    zmsg_send (&msg, self->socket);
    */
}

void dz_broker_main_loop_mdp(dz_broker *self) {
    while (true) {
        zmq_pollitem_t primary [] = {
            { self->localbe, 0, ZMQ_POLLIN, 0 },
            { self->cloudbe, 0, ZMQ_POLLIN, 0 },
            { self->statefe, 0, ZMQ_POLLIN, 0 }
        };
        //  If we have no workers ready, wait indefinitely
        int rc = zmq_poll (primary, 3,
            self->local_capacity? 1000 * ZMQ_POLL_MSEC: -1);
        /*int rc = zmq_poll (primary, 3, self->local_capacity? 0 * ZMQ_POLL_MSEC: -1);*/

        if (rc == -1)
            break;              //  Interrupted

        //  Track if capacity changes during this iteration
        int previous = self->local_capacity;
        zmsg_t *msg = NULL;     //  Reply from local worker

        if (primary [0].revents & ZMQ_POLLIN) {
            msg = zmsg_recv(self->localbe);
            if (!msg)
                break;          //  Interrupted
            s_broker_worker_msg(self, msg, true);
        }
        // Or handle reply from peer broker, in fact this msg only goes to localfe?
        else
        if (primary [1].revents & ZMQ_POLLIN) {
            msg = zmsg_recv(self->cloudbe);
            if (!msg)
                break;          //  Interrupted
            zframe_t *remote_name = zmsg_pop(msg);
            zframe_destroy(&remote_name);
            s_broker_worker_msg(self, msg, false);
        }

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

            bool from_local = true;
            if (secondary [0].revents & ZMQ_POLLIN) {
                msg = zmsg_recv(self->localfe);
            }
            else if (secondary [1].revents & ZMQ_POLLIN) {
                msg = zmsg_recv(self->cloudfe);
                from_local = false;
                LOG_PRINT(LOG_DEBUG, "GET msg from broker peer");
            } else {
                break;      //  No work, go back to primary
            }
            s_broker_client_msg(self, msg, from_local);
        }
        //  We broadcast capacity messages to other peers; to reduce chatter,
        //  we do this only if our capacity changed.

        printf("dzbroker-%s local_capacity = %d\n", self->name, self->local_capacity);
        if (self->local_capacity != previous) {
            //  We stick our own identity onto the envelope
            zstr_sendm(self->statebe, self->name);
            //  Broadcast new capacity
            zstr_sendf(self->statebe, "%d", self->local_capacity);
        }
    }
}

