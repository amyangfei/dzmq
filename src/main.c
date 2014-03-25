#include <unistd.h>
#include <execinfo.h>
#include <string.h>
#include "dzcommon.h"
#include "dzlog.h"
#include "dzutil.h"
#include "dzbroker.h"

extern struct setting settings;

static void settings_init(const char *broker_name, const bool log);
static void nodes_info_init(const char *config_file);
static int test_foreach (const char *key, void *item, void *arg);
static void nodes_endpoint_destory(void *argument);

static void settings_init(const char *broker_name, const bool log) {
    char temp[32] = "";
    sprintf(temp, "./log/%s.log", broker_name);
    strcpy(settings.log_name, temp);
    settings.log = log;
    settings.nodes = zhash_new();
    nodes_info_init("./config/nodes.example.conf");
}

static void nodes_info_init(const char *config_file) {
    FILE *fp = fopen(config_file, "r");
    char node[MAX_LINE] = "";
    char buf[MAX_LINE] = "";
    int r, line = 0;
    if (fp == NULL) {
        printf("Error opening file");
        exit(EXIT_FAILURE);
    } else {
        r = fscanf(fp, "%s %s\n", node, buf);
        while (r != EOF) {
            line++;
            if (r == 2) {
                char *endpoint = (char *)malloc(sizeof(char)*strlen(buf) + 1);
                strncpy(endpoint, buf, strlen(buf) + 1);
                zhash_insert(settings.nodes, node, endpoint);
                zhash_freefn(settings.nodes, node, nodes_endpoint_destory);
            } else {
                printf("Error, line %d in wrong format\n", line);
            }
            r = fscanf(fp, "%s %s\n", node, buf);
        }
        fclose(fp);
    }
    /*zhash_foreach(settings.nodes, test_foreach, NULL);*/
}

static void nodes_endpoint_destory(void *argument) {
    char *endpoint = (char *) argument;
    free(endpoint);
    endpoint = NULL;
}

static int test_foreach (const char *key, void *item, void *arg) {
    char temp[MAX_LINE];
    char port[6] = "";
    sscanf((char *)item, "%[^:]:%s", temp, port);
    printf("%s: %s, port=%s\n", key, item, port);
    return 0;
}

void segfault_sigaction(int signal, siginfo_t *si, void *arg) {
    void *array[20];
    size_t size;

    // get void*'s for all entries on the stack
    size = backtrace(array, 20);

    // print out all the frames to stderr
    fprintf(stderr, "Error: signal %d:\n", signal);
    backtrace_symbols_fd(array, size, STDERR_FILENO);

    exit(0);
}


void test_broker(int argc, char **argv) {
    int verbose = 0;
    if (argc < 2) {
        printf ("syntax: main me {you}...\n");
        return;
    }
    const char *local = argv[1];
    int rlen = argc - 2;
    char **remote = (char **)malloc(rlen * sizeof(char *));
    /*memcpy(remote, argv+2*sizeof(char *), rlen * sizeof(char *));*/
    memcpy(remote, argv+2, rlen * sizeof(char *));

    dz_broker *broker = dz_broker_new(local, remote, rlen);
    dz_broker_sim_worker(broker, NBR_WORKERS, verbose);
    dz_broker_sim_client(broker, NBR_CLIENTS, verbose);
    dz_broker_main_loop_mdp(broker);

    free(remote);
    dz_broker_destory(&broker);
}

int main(int argc, char **argv) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sigemptyset(&sa.sa_mask);
    sa.sa_sigaction = segfault_sigaction;
    sa.sa_flags   = SA_SIGINFO;
    sigaction(SIGSEGV, &sa, NULL);

    _init_path = getcwd(NULL, 0);

    int c;
    const char *optstr = "l:h";
    while (-1 != (c = getopt(argc, argv, optstr)) ) {
        switch(c) {
            case 'l':
                settings.log = true;
                strcpy(settings.log_name, optarg);
                break;
            case 'h':
                printf("Useage .\n");
                exit(1);
            default:
                fprintf(stderr, "Illegal argument\n");
                return 1;
        }
    }

    if (argc < 2) {
        printf ("syntax: main me {you}...\n");
        return 0;
    }
    const char *broker_name = argv[1];
    settings_init(broker_name, true);
    if (settings.log) {
        const char *log_path = "./log";
        if (is_dir(log_path) != 1) {
            if (mk_dir(log_path) != 1) {
                LOG_PRINT(LOG_ERROR, "log_path[%s] Create Failed!", log_path);
                return -1;
            }
        }
        log_init();
    }

    srandom ((unsigned) time (NULL));
    test_broker(argc, argv);
    LOG_PRINT(LOG_INFO, "main return %s", _init_path);
}
