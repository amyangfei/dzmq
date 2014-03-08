#ifndef DZUTIL_H
#define DZUTIL_H

#include "dzcommon.h"
#include "czmq.h"

int is_dir(const char *path);
int mk_dir(const char *path);
int mk_dirs(const char *dir);
void zmsg_log_dump(zmsg_t *msg, const char *prefix);

#endif
