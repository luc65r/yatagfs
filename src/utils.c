#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

char **tagfs_separate_path(char *path) {
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
