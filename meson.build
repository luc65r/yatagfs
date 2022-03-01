project('yatagfs', 'c', default_options : [
  'c_std=gnu11',
  'warning_level=2',
])

cc = meson.get_compiler('c')

fuse_dep = dependency('fuse3')
sqlite_dep = dependency('sqlite3', version : '>= 3.34')

srcs = []

subdir('src')
subdir('vendor')

executable('yatagfs', srcs, dependencies : [
  fuse_dep,
  sqlite_dep,
], include_directories : include_directories(
  'vendor',
), link_with : [
  sqlite_carray_lib,
])
