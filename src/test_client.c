//
//  Majordomo Protocol client example
//

#include "mdp_client.h"

int main (int argc, char *argv [])
{
    int verbose = (argc > 1 && streq (argv [1], "-v"));
    mdp_client_t *session = mdp_client_new ("tcp://localhost:5555", verbose);

    int count;
    for (count = 0; count < 100; count++) {
        zmsg_t *request = zmsg_new ();
        zmsg_pushstr (request, "Hello world");
        /*char *service_name = NULL;*/
        /*service_name = strdup("echo");*/
        /*mdp_client_send (session, service_name, &request);*/
        /*free(service_name);*/
        mdp_client_send (session, "echo", &request);
        zmsg_t *reply = mdp_client_recv (session, NULL, NULL);
        if (reply)
            zmsg_destroy (&reply);
        else
            break;              //  Interrupted by Ctrl-C
    }
    printf ("%d replies received\n", count);
    mdp_client_destroy (&session);
    return 0;
}
