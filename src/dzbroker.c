
#include "dzbroker.h"
#include "dzservice.h"
#include "mdp_client.h"
#include "mdp_worker.h"
#include "dzlog.h"
#include "dzcommon.h"
#include "dzutil.h"

#ifdef _HAVE_TIMERFD
#include <sys/timerfd.h>
#include <unistd.h>
#endif
#include <time.h>

struct _dz_broker {
    zctx_t *ctx;            // Context
    const char *name;       // Broker name

    int peers_num;          // active peers' number
    char **peers_avail;     // available peers' name
    int peers_avail_num;    // available peers' number
    int peers_avail_cap;    // available peers' capacity
    zhash_t *peers_info;    // active peers' information

    void *localfe;
    void *localbe;
    void *cloudfe;
    void *cloudbe;
    void *statefe;
    void *statebe;
#ifdef _HAVE_TIMERFD
    int timer_fd;
#else
    void *timer_sock;
#endif
    int local_capacity;
    int cloud_capacity;
    zlist_t *workers;
    zhash_t *services;      // Hash of known services
    zhash_t *workers_hash;  // Hash of known workers
    zlist_t *waiting;       // List of waiting workers
    uint64_t heartbeat_at;  // When to send HEARTBEAT
};

static void s_broker_worker_msg (dz_broker *self, zmsg_t *msg, bool from_local);
static void s_broker_client_msg (dz_broker *self, zmsg_t *msg, bool from_local);
static void s_broker_purge (dz_broker *self);
static void status_timer_task(void *args, zctx_t *ctx, void *pipe);
static void add_active_peer (dz_broker *self, const char *peer_name);
#ifndef _HAVE_TIMERFD
static void timer_thread(void *args, zctx_t *ctx, void *pipe);
#endif


dz_broker *dz_broker_new(const char *local, char **remote, int rlen) {
    dz_broker *self = (dz_broker *) zmalloc(sizeof(dz_broker));
    self->name = local;
    self->peers_num = rlen;
    self->peers_avail = (char **) malloc (rlen * sizeof(char *));
    self->peers_avail_num = 0;
    self->peers_avail_cap = rlen;
    self->peers_info = zhash_new();
    self->ctx = zctx_new();

    self->localfe = zsocket_new(self->ctx, ZMQ_ROUTER);
    zsocket_bind(self->localfe, "ipc://%s-localfe.ipc", local);

    self->localbe = zsocket_new(self->ctx, ZMQ_ROUTER);
    zsocket_bind(self->localbe, "ipc://%s-localbe.ipc", local);

    self->cloudfe = zsocket_new(self->ctx, ZMQ_ROUTER);
    zsocket_set_identity(self->cloudfe, local);
    char node_cloudfe[MAX_LINE] = "";
    snprintf(node_cloudfe, MAX_LINE, "%s-cloudfe", local);
    char *cloudfe_endpoint = (char *)zhash_lookup(settings.nodes, node_cloudfe);
    zsocket_bind(self->cloudfe, "tcp://%s", cloudfe_endpoint);

    self->cloudbe = zsocket_new(self->ctx, ZMQ_ROUTER);
    zsocket_set_identity(self->cloudbe, local);
    for (int i = 0; i < rlen; i++) {
        broker_info *p_info = (broker_info *) zmalloc(sizeof(broker_info));
        p_info->name = strdup(remote[i]);
        p_info->capacity = 0;
        zhash_insert(self->peers_info, remote[i], p_info);
        snprintf(node_cloudfe, MAX_LINE, "%s-cloudfe", remote[i]);
        cloudfe_endpoint = (char *)zhash_lookup(settings.nodes, node_cloudfe);
        LOG_PRINT(LOG_INFO, "I: connecting to cloud frontend at '%s %s'\n", remote[i], cloudfe_endpoint);
        zsocket_connect(self->cloudbe, "tcp://%s", cloudfe_endpoint);
    }

    self->statebe = zsocket_new(self->ctx, ZMQ_PUB);
    char node_statebe[MAX_LINE] = "";
    snprintf(node_statebe, MAX_LINE, "%s-statebe", local);
    char *statebe_endpoint = (char *)zhash_lookup(settings.nodes, node_statebe);
    zsocket_bind(self->statebe, "tcp://%s", statebe_endpoint);

    self->statefe = zsocket_new(self->ctx, ZMQ_SUB);
    zsocket_set_subscribe(self->statefe, "");
    for (int i = 0; i < rlen; i++) {
        snprintf(node_statebe, MAX_LINE, "%s-statebe", remote[i]);
        statebe_endpoint = (char *)zhash_lookup(settings.nodes, node_statebe);
        LOG_PRINT(LOG_INFO, "I: connecting to state backend at '%s', %s\n", remote[i], statebe_endpoint);
        zsocket_connect(self->statefe, "tcp://%s", statebe_endpoint);
    }

#ifdef _HAVE_TIMERFD
    self->timer_fd = timerfd_create(CLOCK_REALTIME, 0);
#endif

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
        for (int i= 0; i < self->peers_avail_num; ++i) {
            free(self->peers_avail[i]);
        }
        free(self->peers_avail);
        zhash_destroy(&self->peers_info);
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
    NOTUSED(empty);
    NOTUSED(header);

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

            /*char *data = (char *) zframe_data (peer_name);*/
            /*size_t size = zframe_size (peer_name);*/
            char *data = zframe_strdup(peer_name);
            //  Route reply to cloud if it's addressed to a broker
            // TODO: validation
            if (zhash_lookup(self->peers_info, data) != NULL) {
                zmsg_pushstr (msg, MDPW_REPORT_CLOUD);
                zmsg_pushstr (msg, MDPW_WORKER);
                zmsg_wrap (msg, client);
                zmsg_push(msg, peer_name);

                zmsg_send (&msg, self->cloudfe);
            } else {
                LOG_PRINT(LOG_ERROR, "wrong message frame to peer:%s", data);
            }
            free(data);
            /**
            for (int i = 0; msg && i < self->rlen; i++) {
                size_t size = zframe_size (peer_name);
                if (size == strlen(self->remote[i]) &&  memcmp (data, self->remote[i], size) == 0) {
                    zmsg_pushstr (msg, MDPW_REPORT_CLOUD);
                    zmsg_pushstr (msg, MDPW_WORKER);
                    zmsg_wrap (msg, client);
                    zmsg_push(msg, peer_name);

                    zmsg_send (&msg, self->cloudfe);
                }
            } */
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

        zmsg_wrap(msg, sender);
        zmsg_pushstr(msg, self->name);
        zmsg_push(msg, service);
        zmsg_pushstr(msg, MDPC_REPOST);
        zmsg_pushstr(msg, MDPC_CLIENT);

        //  Route to random broker peer
        // TODO: validation
        /**
        int peer = randof(self->rlen);
        zmsg_pushmem (msg, self->remote[peer], strlen(self->remote[peer]));
        zmsg_log_dump(msg, "Route msg");
        */
        int peer = randof(self->peers_avail_num);
        zmsg_pushmem (msg, self->peers_avail[peer], strlen(self->peers_avail[peer]));
        zmsg_log_dump(msg, "Route msg");

        zmsg_send (&msg, self->cloudbe);
        /*LOG_PRINT(LOG_DEBUG, "Route to random broker peer %s", self->remote[peer]);*/
        LOG_PRINT(LOG_DEBUG, "Route to random broker peer %s", self->peers_avail[peer]);
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

        /*printf("dzbroker-%s local_capacity = %d\n", self->name, self->local_capacity);*/
        /**
        if (self->local_capacity != previous) {
            //  We stick our own identity onto the envelope
            zstr_sendm(self->statebe, self->name);
            //  Broadcast new capacity
            zstr_sendf(self->statebe, "%d", self->local_capacity);
        }
        */
    }
}

static void status_timer_task(void *args, zctx_t *ctx, void *pipe) {
    NOTUSED(args);
    NOTUSED(ctx);
    while (true) {
        sleep(BROADCAST_INTERVAL);
        zstr_send(pipe, "status_timer timeout");
    }
}

static void add_active_peer (dz_broker *self, const char *peer_name) {
    if (self->peers_avail_num == self->peers_avail_cap) {
        char **new_peers_avail =
            (char **) realloc(self->peers_avail, 2 * self->peers_avail_cap * sizeof(char *));
        self->peers_avail = new_peers_avail;
        self->peers_avail_cap *= 2;
    }
    self->peers_avail[self->peers_avail_num++] = strdup(peer_name);
    LOG_PRINT(LOG_DEBUG, "broker-%s, peer-%s", self->name, peer_name);
}

static void remove_active_peer (dz_broker *self, const char *peer_name) {
    size_t size = strlen(peer_name);
    for (int i = 0; i < self->peers_avail_num; ++i) {
        if (strlen(self->peers_avail[i]) == size
                && memcmp(self->peers_avail[i], peer_name, size) == 0)
        {
            if (i == self->peers_avail_num - 1) {
                free(self->peers_avail[i]);
                self->peers_avail_num--;
            } else {
                free(self->peers_avail[i]);
                int copy_len = strlen(self->peers_avail[self->peers_avail_num-1]);
                self->peers_avail[i] = (char *) malloc(copy_len + 1);
                strncpy(self->peers_avail[i], self->peers_avail[self->peers_avail_num-1], copy_len);
                free(self->peers_avail[self->peers_avail_num--]);
            }
            LOG_PRINT(LOG_DEBUG, "broker-%s, peer-%s", self->name, peer_name);
            break;
        }
    }
}

void dz_broker_main_loop_mdp2(dz_broker *self) {
#ifdef _HAVE_TIMERFD
    struct itimerspec status_timerspec;
    struct timespec now;

    if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
        LOG_PRINT(LOG_ERROR, "clock_gettime error");
    }

    status_timerspec.it_value.tv_sec = now.tv_sec + BROADCAST_INTERVAL;
    status_timerspec.it_value.tv_nsec = now.tv_nsec;
    status_timerspec.it_interval.tv_sec = BROADCAST_INTERVAL;
    status_timerspec.it_interval.tv_nsec = 0;

    if (timerfd_settime(self->timer_fd, TFD_TIMER_ABSTIME, &status_timerspec, NULL) == -1) {
        LOG_PRINT(LOG_ERROR, "timerfd_settime error");
    }
#else
    self->timer_sock = zthread_fork(self->ctx, status_timer_task, NULL);
#endif
    while (true) {
        zmq_pollitem_t poller_items [] = {
            { self->localbe, 0, ZMQ_POLLIN, 0 },
            { self->cloudbe, 0, ZMQ_POLLIN, 0 },
            { self->statefe, 0, ZMQ_POLLIN, 0 },
#ifdef _HAVE_TIMERFD
            { 0, self->timer_fd, ZMQ_POLLIN, 0 },
#else
            { self->timer_sock, 0, ZMQ_POLLIN, 0 },
#endif
            { self->localfe, 0, ZMQ_POLLIN, 0 },
            { self->cloudfe, 0, ZMQ_POLLIN, 0 }
        };
        int poller_num = 4;
        if (self->local_capacity > 0) {
            poller_num += 2;
        } else if (self->cloud_capacity > 0) {
            poller_num += 1;
        }
        int rc = zmq_poll(poller_items, poller_num, -1);
        if (rc == -1)
            break;              //  Interrupted

        zmsg_t *msg = NULL;

        if (poller_items[0].revents & ZMQ_POLLIN) {
            msg = zmsg_recv(self->localbe);
            if (!msg)
                break;          //  Interrupted
            s_broker_worker_msg(self, msg, true);
        }
        // Or handle reply from peer broker, in fact this msg only goes to localfe?
        else if (poller_items[1].revents & ZMQ_POLLIN) {
            msg = zmsg_recv(self->cloudbe);
            if (!msg)
                break;          //  Interrupted
            zframe_t *remote_name = zmsg_pop(msg);
            zframe_destroy(&remote_name);
            s_broker_worker_msg(self, msg, false);
        }

        //  If we have input messages on our statefe sockets, we can process these immediately:
        //  TODO: fix state handle for multi-connecting peers
        else if (poller_items[2].revents & ZMQ_POLLIN) {
            char *peer = zstr_recv(self->statefe);
            char *status = zstr_recv(self->statefe);
            broker_info *p_info = (broker_info *) zhash_lookup(self->peers_info, peer);
            LOG_PRINT(LOG_DEBUG, "------->");
            if (p_info == NULL) {
                LOG_PRINT(LOG_ERROR, "receive status from wrong peer %s", peer);
            } else {
                int old_capacity = p_info->capacity;
                p_info->capacity = atoi(status);
                if (old_capacity != p_info->capacity) {
                    zhash_update(self->peers_info, peer, p_info);
                    self->cloud_capacity += (p_info->capacity - old_capacity);
                }
                if (old_capacity == 0 && p_info->capacity > 0) {
                    add_active_peer(self, peer);
                }
                if (old_capacity > 0 && p_info->capacity == 0) {
                    remove_active_peer(self, peer);
                }
                LOG_PRINT(LOG_DEBUG, "old-%d, new-%d", old_capacity, p_info->capacity);
            }
            free(peer);
            free(status);
        }

        else if (poller_items[3].revents & ZMQ_POLLIN) {
#ifdef _HAVE_TIMERFD
            uint64_t exp;
            ssize_t s = read(self->timer_fd, &exp, sizeof(uint64_t));
            if (s != sizeof(uint64_t)) {
                LOG_PRINT(LOG_ERROR, "timerfd read error");
            }
            LOG_PRINT(LOG_DEBUG, "use timerfd status timeout, broker-%s, capacity-%d", self->name, self->cloud_capacity);
#else
            char *status_timeout_str = zstr_recv(self->timer_sock);
            NOTUSED(status_timeout_str);
            LOG_PRINT(LOG_DEBUG, "not use timerfd status timeout, broker-%s, capacity-%d", self->name, self->cloud_capacity);
#endif
            zstr_sendm(self->statebe, self->name);
            zstr_sendf(self->statebe, "%d", self->local_capacity);
        }

        else if (poller_items[4].revents & ZMQ_POLLIN) {
            msg = zmsg_recv(self->localfe);
            s_broker_client_msg(self, msg, true);
        }

        else if (poller_items[5].revents & ZMQ_POLLIN) {
            msg = zmsg_recv(self->cloudfe);
            s_broker_client_msg(self, msg, false);
            LOG_PRINT(LOG_DEBUG, "GET msg from broker peer");
        }
        // printf("dzbroker-%s local_capacity = %d\n", self->name, self->local_capacity);
    }
}

