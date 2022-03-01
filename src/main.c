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

#include "sql_queries.h"

static struct tagfs {
    char *datadir;
    int datadirfd;
    sqlite3 *db;
} tagfs;

static char **tagfs_separate_path(char *path) {
    size_t slash_count = 0;
    for (char *s = path; *s != '\0'; s++)
        if (*s == '/')
            slash_count++;

    char **parts = malloc(sizeof *parts * (slash_count + 1));
    assert(parts != NULL);

    assert(path[0] == '/');
    path[0] = '\0';
    path++;

    int i = 0;
    for (;;) {
        if (*path == '\0')
            break;

        char *next = strchr(path, '/');
        parts[i++] = path;
        if (next == NULL)
            break;

        *next = '\0';
        path = next + 1;
    }
    parts[i] = NULL;

    return parts;
}

static int tagfs_has_file_tags(char *path, char **tags, size_t ntags) {
    int res, rc;
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(tagfs.db, tagfs_sql_has_file_tags, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
        return -1;
    }
    assert(stmt != NULL);

    rc = sqlite3_bind_text(stmt, 1, path, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_bind_text: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

    rc = sqlite3_carray_bind(stmt, 2, tags, ntags, CARRAY_TEXT, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_carray_bind: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

    rc = sqlite3_bind_int64(stmt, 3, (int64_t)ntags);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_bind_int64: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

    rc = sqlite3_step(stmt);
    switch (rc) {
    case SQLITE_DONE:
        res = 0;
        break;
    case SQLITE_ROW:
        res = 1;
        break;
    default:
        fuse_log(FUSE_LOG_ERR, "sqlite3_step: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

end:
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_finalize: %s\n", sqlite3_errmsg(tagfs.db));
        /* it should be fine to still return `res` */
    }

    return res;
}

static int64_t tagfs_get_id(const char *sql_query, const char *name) {
    int64_t id;
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(tagfs.db, sql_query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
        return -1;
    }
    assert(stmt != NULL);

    rc = sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_bind_text: %s\n", sqlite3_errmsg(tagfs.db));
        id = -1;
        goto end;
    }

    rc = sqlite3_step(stmt);
    switch (rc) {
    case SQLITE_DONE:
        id = 0;
        break;
    case SQLITE_ROW:
        id = sqlite3_column_int64(stmt, 0);
        break;
    default:
        fuse_log(FUSE_LOG_ERR, "sqlite3_step: %s\n", sqlite3_errmsg(tagfs.db));
        id = -1;
        goto end;
    }

end:
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_finalize: %s\n", sqlite3_errmsg(tagfs.db));
        /* it should be fine to still return `id` */
    }

    return id;
}

static int64_t tagfs_get_tag(const char *name) {
    return tagfs_get_id(tagfs_sql_get_tag, name);
}

static int64_t tagfs_get_file(const char *name) {
    return tagfs_get_id(tagfs_sql_get_file, name);
}

static int tagfs_getattr(const char *_path, struct stat *stat, struct fuse_file_info *fi) {
    int res;
    (void)fi;
    char *path = strdup(_path);
    memset(stat, 0, sizeof *stat);

    stat->st_uid = getuid();
    stat->st_gid = getgid();

    char **parts = tagfs_separate_path(path);
    assert(parts != NULL);

    size_t nparts = 0;
    for (char **p = parts; *p != NULL; p++)
        nparts++;

    if (nparts == 0) {
        assert(strcmp(_path, "/") == 0);
        stat->st_mode = S_IFDIR | 0755;
        stat->st_nlink = 2;
        res = 0;
        goto end;
    }

    int64_t fid = tagfs_get_file(parts[nparts - 1]);
    if (fid < 0) {
        res = -EIO;
        goto end;
    }
    if (fid) {
        int rc = tagfs_has_file_tags(parts[nparts - 1], parts, nparts - 1);
        if (rc < 0) {
            res = -EIO;
            goto end;
        }
        if (rc) {
            rc = fstatat(tagfs.datadirfd, parts[nparts - 1], stat, 0);
            if (rc < 0) {
                res = -errno;
                goto end;
            }
            res = 0;
        } else {
            res = -ENOENT;
        }
    } else {
        for (size_t i = 0; i < nparts; i++) {
            int64_t tid = tagfs_get_tag(parts[i]);
            if (tid < 0) {
                res = -EIO;
                goto end;
            }
            if (!tid) {
                res = -ENOENT;
                goto end;
            }
        }

        stat->st_mode = S_IFDIR | 0755;
        stat->st_nlink = 2;
        res = 0;
    }

end:
    free(path);
    return res;
}

static const struct fuse_operations tagfs_ops = {
    .getattr = tagfs_getattr,
};

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

    struct stat sb;
    rc = stat(tagfs.datadir, &sb);
    if (!rc) {
        if ((sb.st_mode & S_IFMT) == S_IFDIR) {
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
