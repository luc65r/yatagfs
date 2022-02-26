project('yatagfs', 'c', default_options : [
  'c_std=gnu11',
  'warning_level=2',
])

cc = meson.get_compiler('c')

fuse_dep = dependency('fuse3')
sqlite_dep = dependency('sqlite3')

srcs = []

subdir('src')

executable('yatagfs', srcs, dependencies : [
  fuse_dep,
  sqlite_dep,
])
