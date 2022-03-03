#define _GNU_SOURCE

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FUSE_USE_VERSION 35
#include <fuse.h>
#include <fuse_lowlevel.h>

#include <sqlite3.h>
#include "carray.h"

#include "ops.h"
#include "sql_queries.h"
#include "tagfs.h"

enum {
    KEY_VERSION,
    KEY_HELP,
};

#define TAG_OPT(t, p, v) { t, offsetof(struct tagfs, p), v }
static const struct fuse_opt tagfs_opts[] = {
    FUSE_OPT_KEY("-V", KEY_VERSION),
    FUSE_OPT_KEY("--version", KEY_VERSION),
    FUSE_OPT_KEY("-h", KEY_HELP),
    FUSE_OPT_KEY("--help", KEY_HELP),
    FUSE_OPT_END
};

static void tagfs_usage(struct fuse_args *args) {
    printf("usage: %s datadir mountpoint [options]\n"
           "\n"
           "    -h   --help      print help\n"
           "    -V   --version   print version\n"
           "    -o opt,[opt...]  mount options\n"
           "\n"
           "FUSE options:\n",
           args->argv[0]);
    fuse_lib_help(args);
}

static int tagfs_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs) {
    (void)data;

    switch (key) {
    case FUSE_OPT_KEY_NONOPT:
        if (!tagfs.datadir) {
            tagfs.datadir = strdup(arg);
            return 0;
        }
        return 1;

    case KEY_VERSION:
        printf("YATAGFS version 0.1.0\n"
               "FUSE library version %s\n",
               fuse_pkgversion());
        fuse_lowlevel_version();
        exit(0);

    case KEY_HELP:
        tagfs_usage(outargs);
        exit(1);
    }

    return 1;
}

int main(int argc, char **argv) {
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    int rc = fuse_opt_parse(&args, NULL, tagfs_opts, tagfs_opt_proc);
    if (rc < 0)
        return 1;

    if (!tagfs.datadir) {
        tagfs_usage(&args);
        return 1;
    }

    struct stat stbuf;
    rc = stat(tagfs.datadir, &stbuf);
    if (!rc) {
        if ((stbuf.st_mode & S_IFMT) == S_IFDIR) {
            rc = open(tagfs.datadir, O_DIRECTORY);
            if (rc < 0) {
                perror("open");
                return 1;
            }
            tagfs.datadirfd = rc;
        } else {
            fprintf(stderr, "%s is not a directory\n", tagfs.datadir);
            return 1;
        }
    } else {
        if (errno == ENOENT) {
            rc = mkdir(tagfs.datadir, 0755);
            if (rc < 0) {
                perror("mkdir");
                return 1;
            }
            tagfs.datadirfd = rc;
        } else {
            perror("stat");
            return 1;
        }
    }

    char *path = realpath(tagfs.datadir, NULL);
    if (!path) {
        perror("realpath");
        return 1;
    }
    size_t dirpathlen = strlen(path);
    char filename[] = "/.yatagfs.db";
    size_t filenamelen = sizeof filename;
    path = realloc(path, dirpathlen + filenamelen);
    assert(path != NULL);
    memcpy(path + dirpathlen, filename, filenamelen);

    rc = sqlite3_open(path, &tagfs.db);
    free(path);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't open SQLite database: %s\n",
                tagfs.db ? sqlite3_errmsg(tagfs.db) : sqlite3_errstr(rc));
        rc = 1;
        goto err;
    }

    char *errormsg;
    rc = sqlite3_carray_init(tagfs.db, &errormsg, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't load SQLite carray extension: %s\n", errormsg);
        sqlite3_free(errormsg);
        rc = 1;
        goto err;
    }

    rc = sqlite3_exec(tagfs.db, tagfs_sql_set_recursive_triggers, NULL, NULL, &errormsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't set recursive_triggers pragma: %s\n", errormsg);
        sqlite3_free(errormsg);
        rc = 1;
        goto err;
    }

    rc = sqlite3_exec(tagfs.db, tagfs_sql_create_tables, NULL, NULL, &errormsg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't create tables: %s\n", errormsg);
        sqlite3_free(errormsg);
        rc = 1;
        goto err;
    }

    rc = fuse_main(args.argc, args.argv, &tagfs_ops, NULL);

err:
    sqlite3_close(tagfs.db);
    fuse_opt_free_args(&args);

    return rc;
}
