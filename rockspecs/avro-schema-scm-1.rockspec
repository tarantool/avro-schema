package = 'avro-schema'
version = 'scm-1'
source  = {
    url    = 'git://github.com/tarantool/avro-schema.git',
    branch = 'master',
}
description = {
    summary  = "Apache Avro schema tools for Tarantool",
    homepage = 'https://github.com/tarantool/avro-schema',
    license  = 'BSD'
}
dependencies = {
    'tarantool',
    'lua >= 5.1',
}
build = {
    type = 'cmake',
    variables = {
        TARANTOOL_DIR="$(TARANTOOL_DIR)";
        CMAKE_BUILD_TYPE="RelWithDebInfo",
        TARANTOOL_INSTALL_LIBDIR="$(LIBDIR)",
        TARANTOOL_INSTALL_LUADIR="$(LUADIR)",
    },
}
-- vim: syntax=lua
