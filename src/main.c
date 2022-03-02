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

static int tagfs_getattr(const char *_path, struct stat *stbuf, struct fuse_file_info *fi) {
    int res;
    (void)fi;
    char *path = strdup(_path);
    memset(stbuf, 0, sizeof *stbuf);

    stbuf->st_uid = getuid();
    stbuf->st_gid = getgid();

    char **parts = tagfs_separate_path(path);
    assert(parts != NULL);

    size_t nparts = 0;
    for (char **p = parts; *p != NULL; p++)
        nparts++;

    if (nparts == 0) {
        assert(strcmp(_path, "/") == 0);
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
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
            rc = fstatat(tagfs.datadirfd, parts[nparts - 1], stbuf, 0);
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

        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        res = 0;
    }

end:
    free(path);
    return res;
}

static int tagfs_create_tag(char *name) {
    int res, rc;
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(tagfs.db, tagfs_sql_insert_tag, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
        return -1;
    }
    assert(stmt != NULL);

    rc = sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_bind_text: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

    rc = sqlite3_step(stmt);
    switch (rc) {
    case SQLITE_DONE:
        res = 1;
        break;
    case SQLITE_CONSTRAINT:
        res = 0;
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

static int tagfs_mkdir(const char *_path, mode_t mode) {
    (void)mode;
    int res, rc;

    char *path = strdup(_path);
    assert(path != NULL);

    char **parts = tagfs_separate_path(path);
    assert(parts != NULL);

    size_t nparts = 0;
    for (char **p = parts; *p != NULL; p++)
        nparts++;

    if (nparts == 0) {
        assert(strcmp(_path, "/") == 0);
        res = -EEXIST;
        goto end;
    }

    for (size_t i = 0; i < nparts - 1; i++) {
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

    int64_t fid = tagfs_get_file(parts[nparts - 1]);
    if (fid < 0) {
        res = -EIO;
        goto end;
    }
    if (fid) {
        res = -EEXIST;
        goto end;
    }

    rc = tagfs_create_tag(parts[nparts - 1]);
    switch (rc) {
    case 1:
        res = 0;
        break;
    case 0:
        res = -EEXIST;
        goto end;
    default:
        res = -EIO;
        goto end;
    }

end:
    free(path);
    return res;
}

static int tagfs_readdir(const char *_path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
    (void)offset;
    (void)fi;
    (void)flags;

    int res, rc;
    struct stat st = {0};

    char *path = strdup(_path);
    assert(path != NULL);

    char **parts = tagfs_separate_path(path);
    assert(parts != NULL);

    size_t nparts = 0;
    for (char **p = parts; *p != NULL; p++)
        nparts++;

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

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(tagfs.db, tagfs_sql_get_tags_not_in, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
        res = -EIO;
        goto end;
    }
    assert(stmt != NULL);

    rc = sqlite3_carray_bind(stmt, 1, parts, nparts, CARRAY_TEXT, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_carray_bind: %s\n", sqlite3_errmsg(tagfs.db));
        res = -EIO;
        goto end;
    }

    st.st_mode = S_IFDIR | 0755;
    st.st_nlink = 2;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *dir = (const char *)sqlite3_column_text(stmt, 1);
        assert(dir != NULL);
        filler(buf, dir, &st, 0, 0);
    }

    if (rc != SQLITE_DONE) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_step: %s\n", sqlite3_errmsg(tagfs.db));
        res = -EIO;
        goto end;
    }

    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_finalize: %s\n", sqlite3_errmsg(tagfs.db));
        /* I guess we can continue */
    }
    stmt = NULL;

    rc = sqlite3_prepare_v2(tagfs.db, tagfs_sql_get_files_in_tags, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
        res = -EIO;
        goto end;
    }
    assert(stmt != NULL);

    rc = sqlite3_carray_bind(stmt, 1, parts, nparts, CARRAY_TEXT, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_carray_bind: %s\n", sqlite3_errmsg(tagfs.db));
        res = -EIO;
        goto end;
    }

    rc = sqlite3_bind_int64(stmt, 2, (int64_t)nparts);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_bind_int64: %s\n", sqlite3_errmsg(tagfs.db));
        res = -EIO;
        goto end;
    }

    st.st_mode = 0644;
    st.st_nlink = 1;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *file = (const char *)sqlite3_column_text(stmt, 1);
        assert(file != NULL);
        filler(buf, file, &st, 0, 0);        
    }

    if (rc != SQLITE_DONE) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_step: %s\n", sqlite3_errmsg(tagfs.db));
        res = -EIO;
        goto end;
    }
    res = 0;

end:
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        fuse_log(FUSE_LOG_ERR, "sqlite3_finalize: %s\n", sqlite3_errmsg(tagfs.db));
        /* it should be fine to still return `res` */
    }
    free(path);
    return res;
}

static int tagfs_open(const char *_path, struct fuse_file_info *fi) {
    int res, rc;
    char *path = strdup(_path);
    assert(path != NULL);

    char **parts = tagfs_separate_path(path);
    assert(parts != NULL);

    size_t nparts = 0;
    for (char **p = parts; *p != NULL; p++)
        nparts++;

    if (nparts == 0) {
        assert(strcmp(_path, "/") == 0);
        res = -EISDIR;
        goto end;
    }

    rc = tagfs_has_file_tags(parts[nparts - 1], parts, nparts - 1);
    if (rc < 0) {
        res = -EIO;
        goto end;
    }
    if (!rc) {
        res = -ENOENT;
        goto end;
    }

    rc = openat(tagfs.datadirfd, parts[nparts - 1], 0);
    if (rc < 0) {
        fuse_log(FUSE_LOG_ERR, "openat: %s\n", strerror(errno));
        res = -EIO;
        goto end;
    }

    fi->fh = rc;
    fi->direct_io = 1;
    res = 0;

end:
    free(path);
    return res;
}

static const struct fuse_operations tagfs_ops = {
    .getattr = tagfs_getattr,
    .mkdir = tagfs_mkdir,
    .open = tagfs_open,
    .readdir = tagfs_readdir,
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
    SQLITE_API int sqlite3_carray_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi);
    rc = sqlite3_carray_init(tagfs.db, &errormsg, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't load SQLite carray extension: %s\n", errormsg);
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
