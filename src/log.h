#pragma once

#include <stdarg.h>

#define FUSE_USE_VERSION 35
#include <fuse.h>

#define log_err(...) log_log(FUSE_LOG_ERR, __FILE__, __LINE__, __VA_ARGS__)
#define log_warn(...) log_log(FUSE_LOG_WARNING, __FILE__, __LINE__, __VA_ARGS__)
#define log_notice(...) log_log(FUSE_LOG_NOTICE, __FILE__, __LINE__, __VA_ARGS__)
#define log_info(...) log_log(FUSE_LOG_INFO, __FILE__, __LINE__, __VA_ARGS__)
#define log_debug(...) log_log(FUSE_LOG_DEBUG, __FILE__, __LINE__, __VA_ARGS__)

#define log_fatal(...) do { log_err(__VA_ARGS__); exit(1); } while (0)

void log_log(enum fuse_log_level level, const char *restrict file, int line, const char *restrict fmt, ...)
    __attribute__((format(printf, 4, 5)));

void log_fuse(enum fuse_log_level level, const char *fmt, va_list ap);
