#include <assert.h>
#include <stdarg.h>
#include <stdio.h>

#include "log.h"

static const char *strings[] = {
    [FUSE_LOG_EMERG] = "EMERG",
    [FUSE_LOG_ALERT] = "ALERT",
    [FUSE_LOG_CRIT] = "CRIT",
    [FUSE_LOG_ERR] = "ERR",
    [FUSE_LOG_WARNING] = "WARNING",
    [FUSE_LOG_NOTICE] = "NOTICE",
    [FUSE_LOG_INFO] = "INFO",
    [FUSE_LOG_DEBUG] = "DEBUG",
};

static const char *colors[] = {
    [FUSE_LOG_EMERG] = "35",
    [FUSE_LOG_ALERT] = "35",
    [FUSE_LOG_CRIT] = "35",
    [FUSE_LOG_ERR] = "31",
    [FUSE_LOG_WARNING] = "33",
    [FUSE_LOG_NOTICE] = "93",
    [FUSE_LOG_INFO] = "32",
    [FUSE_LOG_DEBUG] = "36",
};

static void vlog(enum fuse_log_level level, const char *restrict file,
                     int line, const char *restrict fmt, va_list ap) {
    static_assert(sizeof strings / sizeof *strings == 8);
    static_assert(sizeof colors / sizeof *colors == 8);

    fprintf(stderr, "\033[%sm%-7s\033[m \033[90m%s:%d\033[m ",
            colors[level], strings[level], file, line);
    vfprintf(stderr, fmt, ap);
}

void log_log(enum fuse_log_level level, const char *restrict file,
             int line, const char *restrict fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vlog(level, file, line, fmt, args);
    va_end(args);
}

void log_fuse(enum fuse_log_level level, const char *fmt, va_list ap) {
    vlog(level, NULL, -1, fmt, ap);
}
