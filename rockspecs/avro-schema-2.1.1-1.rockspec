package = 'avro-schema'
version = '2.1.1-1'
source  = {
    url = 'git://github.com/tarantool/avro-schema.git',
    tag = '2.1.1',
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
external_dependencies = {
    TARANTOOL = {
        header = 'tarantool/module.h';
    };
}
build = {
    type = 'cmake';
    variables = {
        TARANTOOL_DIR="$(TARANTOOL_DIR)";
        CMAKE_BUILD_TYPE="RelWithDebInfo";
        TARANTOOL_INSTALL_LIBDIR="$(LIBDIR)";
        TARANTOOL_INSTALL_LUADIR="$(LUADIR)";
    };
}
-- vim: syntax=lua
