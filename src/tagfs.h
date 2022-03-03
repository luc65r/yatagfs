#pragma once

#include <inttypes.h>
#include <sqlite3.h>

extern struct tagfs {
    char *datadir;
    int datadirfd;
    sqlite3 *db;
} tagfs;

/* missing in carray.h */
SQLITE_API int sqlite3_carray_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);

int tagfs_has_file_tags(char *path, char **tags, size_t ntags);
int64_t tagfs_get_tag(const char *name);
int64_t tagfs_get_file(const char *name);
int tagfs_create_file(const char *path);
int tagfs_add_tags_to_file(const char *path, char **tags, size_t ntags);
int tagfs_create_tag(char *name);
