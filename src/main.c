#include <unistd.h>
#include "dzcommon.h"
#include "dzlog.h"
#include "dzutil.h"

extern struct setting settings;

static void settings_init();

static void settings_init() {
    strcpy(settings.log_name, "./log/test.log");
    settings.log = true;
}

int main(int argc, char **argv) {
    _init_path = getcwd(NULL, 0);
    settings_init();

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
    LOG_PRINT(LOG_INFO, "main return %s", _init_path);
}
