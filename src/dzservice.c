#include "dzservice.h"

//  Here is the implementation of the methods that work on a worker.
//  Lazy constructor that locates a worker by identity, or creates a new
//  worker if there is no worker already with that identity.


//  Worker destructor is called automatically whenever the worker is
//  removed from broker->workers.

void
s_worker_destroy (void *argument)
{
    worker_t *self = (worker_t *) argument;
    zframe_destroy (&self->address);
    free (self->identity);
    free (self);
}

//  The send method formats and sends a command to a worker. The caller may
//  also provide a command option, and a message payload.

void
s_worker_send (worker_t *self, char *command, char *option, zmsg_t *msg)
{
    msg = msg? zmsg_dup (msg): zmsg_new ();

    //  Stack protocol envelope to start of message
    if (option)
        zmsg_pushstr (msg, option);
    zmsg_pushstr (msg, command);
    zmsg_pushstr (msg, MDPW_WORKER);

    //  Stack routing envelope to start of message
    zmsg_wrap (msg, zframe_dup (self->address));

    /**
    if (self->broker->verbose) {
        zclock_log ("I: sending %s to worker",
            mdpw_commands [(int) *command]);
        zmsg_dump (msg);
    }
    zmsg_send (&msg, self->broker->socket);
    */
}


