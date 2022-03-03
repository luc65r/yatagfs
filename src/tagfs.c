#include <assert.h>
#include <stdlib.h>

#define FUSE_USE_VERSION 35
#include <fuse.h>

#include <sqlite3.h>
#include "carray.h"

#include "log.h"
#include "sql_queries.h"
#include "tagfs.h"

struct tagfs tagfs;

int tagfs_has_file_tags(char *path, char **tags, size_t ntags) {
    int res, rc;
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(tagfs.db, tagfs_sql_has_file_tags, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
        return -1;
    }
    assert(stmt != NULL);

    rc = sqlite3_bind_text(stmt, 1, path, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_bind_text: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

    rc = sqlite3_carray_bind(stmt, 2, tags, ntags, CARRAY_TEXT, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_carray_bind: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

    rc = sqlite3_bind_int64(stmt, 3, (int64_t)ntags);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_bind_int64: %s\n", sqlite3_errmsg(tagfs.db));
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
        log_err("sqlite3_step: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

end:
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_finalize: %s\n", sqlite3_errmsg(tagfs.db));
        /* it should be fine to still return `res` */
    }

    return res;
}

static int64_t tagfs_get_id(const char *sql_query, const char *name) {
    int64_t id;
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(tagfs.db, sql_query, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
        return -1;
    }
    assert(stmt != NULL);

    rc = sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_bind_text: %s\n", sqlite3_errmsg(tagfs.db));
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
        log_err("sqlite3_step: %s\n", sqlite3_errmsg(tagfs.db));
        id = -1;
        goto end;
    }

end:
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_finalize: %s\n", sqlite3_errmsg(tagfs.db));
        /* it should be fine to still return `id` */
    }

    return id;
}

int64_t tagfs_get_tag(const char *name) {
    return tagfs_get_id(tagfs_sql_get_tag, name);
}

int64_t tagfs_get_file(const char *name) {
    return tagfs_get_id(tagfs_sql_get_file, name);
}

int tagfs_add_tags_to_file(const char *path, char **tags, size_t ntags) {
    int res, rc;
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(tagfs.db, tagfs_sql_add_tags_to_file, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
        return -1;
    }
    assert(stmt != NULL);

    rc = sqlite3_bind_text(stmt, 1, path, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_bind_text: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

    rc = sqlite3_carray_bind(stmt, 2, tags, ntags, CARRAY_TEXT, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_carray_bind: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        log_err("sqlite3_step: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }
    res = 0;

end:
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_finalize: %s\n", sqlite3_errmsg(tagfs.db));
        /* it should be fine to still return `res` */
    }

    return res;
}

int tagfs_create_file(const char *path) {
    int res, rc;
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(tagfs.db, tagfs_sql_create_file, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
        return -1;
    }
    assert(stmt != NULL);

    rc = sqlite3_bind_text(stmt, 1, path, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_bind_text: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        log_err("sqlite3_step: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }
    res = 0;

end:
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_finalize: %s\n", sqlite3_errmsg(tagfs.db));
        /* it should be fine to still return `res` */
    }

    return res;
}

int tagfs_create_tag(char *name) {
    int res, rc;
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(tagfs.db, tagfs_sql_insert_tag, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_prepare_v2: %s\n", sqlite3_errmsg(tagfs.db));
        return -1;
    }
    assert(stmt != NULL);

    rc = sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_bind_text: %s\n", sqlite3_errmsg(tagfs.db));
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
        log_err("sqlite3_step: %s\n", sqlite3_errmsg(tagfs.db));
        res = -1;
        goto end;
    }

end:
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
        log_err("sqlite3_finalize: %s\n", sqlite3_errmsg(tagfs.db));
        /* it should be fine to still return `res` */
    }

    return res;
}
