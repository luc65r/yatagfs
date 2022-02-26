#!/bin/sh

source="$1"
header="$2"

shift; shift

cat << EOF > "$source"
#include "$header"

EOF

cat << EOF > "$header"
#pragma once

EOF

for s in "$@"; do
    f="${s##*/}"
    id="tagfs_sql_${f%.sql}"

    printf "const char *$id =\n" >> "$source"
    sed 's/.*/"&\\n"/' "$s" >> "$source"
    printf ';\n\n' >> "$source"

    printf "extern const char *$id;\n\n" >> "$header"
done
