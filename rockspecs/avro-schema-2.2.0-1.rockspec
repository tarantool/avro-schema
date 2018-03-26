package = 'avro-schema'
version = '2.2.0-1'
source  = {
    url = 'git://github.com/tarantool/avro-schema.git',
    tag = '2.2.0',
}
description = {
    summary  = "Apache Avro schema tools for Tarantool",
    homepage = 'https://github.com/tarantool/avro-schema',
    license  = 'BSD'
}
dependencies = {
    'lua >= 5.1'
}
build = {
    type = 'cmake';
    variables = {
        CMAKE_BUILD_TYPE="RelWithDebInfo";
        TARANTOOL_INSTALL_LIBDIR="$(LIBDIR)";
        TARANTOOL_INSTALL_LUADIR="$(LUADIR)";
    };
}
-- vim: syntax=lua
