#define _GNU_SOURCE

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <sqlite3.h>
#include "carray.h"

#include "log.h"
#include "ops.h"
#include "sql_queries.h"
#include "tagfs.h"
#include "utils.h"

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
        log_err("sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
        res = -EIO;
        goto end;
    }
    assert(stmt != NULL);

    rc = sqlite3_carray_bind(stmt, 1, parts, nparts, CARRAY_TEXT, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_carray_bind: %s\n", sqlite3_errmsg(tagfs.db));
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
        log_err("sqlite3_step: %s\n", sqlite3_errmsg(tagfs.db));
        res = -EIO;
        goto end;
    }

    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_finalize: %s\n", sqlite3_errmsg(tagfs.db));
        /* I guess we can continue */
    }
    stmt = NULL;

    if (nparts > 0) {
        rc = sqlite3_prepare_v2(tagfs.db, tagfs_sql_get_files_in_tags, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            log_err("sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
            res = -EIO;
            goto end;
        }
        assert(stmt != NULL);

        rc = sqlite3_carray_bind(stmt, 1, parts, nparts, CARRAY_TEXT, SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            log_err("sqlite3_carray_bind: %s\n", sqlite3_errmsg(tagfs.db));
            res = -EIO;
            goto end;
        }

        rc = sqlite3_bind_int64(stmt, 2, (int64_t)nparts);
        if (rc != SQLITE_OK) {
            log_err("sqlite3_bind_int64: %s\n", sqlite3_errmsg(tagfs.db));
            res = -EIO;
            goto end;
        }
    } else {
        rc = sqlite3_prepare_v2(tagfs.db, tagfs_sql_get_files, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            log_err("sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
            res = -EIO;
            goto end;
        }
        assert(stmt != NULL);
    }        

    st.st_mode = 0644;
    st.st_nlink = 1;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *file = (const char *)sqlite3_column_text(stmt, 1);
        assert(file != NULL);
        filler(buf, file, &st, 0, 0);        
    }

    if (rc != SQLITE_DONE) {
        log_err("sqlite3_step: %s\n", sqlite3_errmsg(tagfs.db));
        res = -EIO;
        goto end;
    }
    res = 0;

end:
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_finalize: %s\n", sqlite3_errmsg(tagfs.db));
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
        log_err("openat: %s\n", strerror(errno));
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

static int tagfs_create(const char *_path, mode_t mode, struct fuse_file_info *fi) {
    int res, rc;

    char *path = strdup(_path);
    assert(path != NULL);

    char **parts = tagfs_separate_path(path);
    assert(parts != NULL);

    size_t nparts = 0;
    for (char **p = parts; *p != NULL; p++)
        nparts++;

    char *filename = parts[nparts - 1];

    if (nparts == 0) {
        assert(strcmp(_path, "/") == 0);
        res = -EISDIR;
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

    int64_t tid = tagfs_get_tag(filename);
    if (tid < 0) {
        res = -EIO;
        goto end;
    }
    if (tid) {
        res = -EEXIST;
        goto end;
    }

    rc = tagfs_create_file(filename);
    if (rc < 0) {
        res = -EIO;
        goto end;
    }

    rc = tagfs_add_tags_to_file(filename, parts, nparts - 1);
    if (rc < 0) {
        res = -EIO;
        goto end;
    }

    rc = openat(tagfs.datadirfd, filename, O_CREAT | O_TRUNC, mode);
    if (rc < 0) {
        log_err("openat: %s\n", strerror(errno));
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

static int tagfs_flush(const char *path, struct fuse_file_info *fi) {
    (void)path;

    int fd = dup(fi->fh);
    if (fd < 0) {
        log_err("dup: %s\n", strerror(errno));
        return -errno;
    }

    if (close(fd) < 0) {
        log_err("close: %s\n", strerror(errno));
        return -errno;
    }

    return 0;
}

static int tagfs_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
    (void)path;

    if (datasync ? fdatasync(fi->fh) : fsync(fi->fh)) {
        log_err("f(data)sync: %s\n", strerror(errno));
        return -errno;
    }

    return 0;
}

static int tagfs_release(const char *path, struct fuse_file_info *fi) {
    (void)path;

    if (close(fi->fh) < 0) {
        log_err("close: %s\n", strerror(errno));
        return -errno;
    }

    return 0;
}

static int tagfs_read(const char *path, char *buf, size_t size,
                      off_t offset, struct fuse_file_info *fi) {
    (void)path;

    ssize_t r = pread(fi->fh, buf, size, offset);
    if (r < 0) {
        log_err("pread: %s\n", strerror(errno));
        return -errno;
    }

    return r;
}

const struct fuse_operations tagfs_ops = {
    .create = tagfs_create,
    .flush = tagfs_flush,
    .fsync = tagfs_fsync,
    .getattr = tagfs_getattr,
    .mkdir = tagfs_mkdir,
    .open = tagfs_open,
    .read = tagfs_read,
    .readdir = tagfs_readdir,
    .release = tagfs_release,
};
