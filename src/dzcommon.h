
#ifndef DZCOMMON_H
#define DZCOMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "czmq.h"

#define NOTUSED(V) ((void) V)
#define MAX_LINE 1024

struct setting{
    char root_path[512];
    char log_name[512];
    int backlog;
    bool log;
    zhash_t *nodes;
} settings;


char *_init_path;

#define LOG_FATAL 0                        /* System is unusable */
#define LOG_ALERT 1                        /* Action must be taken immediately */
#define LOG_CRIT 2                       /* Critical conditions */
#define LOG_ERROR 3                        /* Error conditions */
#define LOG_WARNING 4                      /* Warning conditions */
#define LOG_NOTICE 5                      /* Normal, but significant */
#define LOG_INFO 6                      /* Information */
#define LOG_DEBUG 7                       /* DEBUG message */


#ifdef _DEBUG
  #define LOG_PRINT(level, fmt, ...)            \
    do { \
        int log_id = log_open(settings.log_name, "a"); \
        log_printf0(log_id, level, "%s:%d %s() "fmt,   \
        __FILE__, __LINE__, __FUNCTION__, \
        ##__VA_ARGS__); \
        log_close(log_id); \
    }while(0)
#else
  #define LOG_PRINT(level, fmt, ...)            \
    do { \
        int log_id = log_open(settings.log_name, "a"); \
        log_printf0(log_id, level, fmt, ##__VA_ARGS__) ; \
        log_close(log_id); \
    }while(0)
#endif

#endif
