package = 'avro'
version = 'scm-1'

source  = {
    url    = 'git://github.com/tarantool/tarantool-avro.git';
    branch = 'master';
}

description = {
    summary  = "A set of Tarantool module templates";
    detailed = [[
    A ready to use module templates. Clone and modify to create
    new modules.
    ]];
    homepage = 'https://github.com/tarantool/tarantool-avro.git';
    license  = 'BSD';
    maintainer = "NickZ <mejedi@gmail.com>";
}

dependencies = {
    'lua >= 5.1';
}

external_dependencies = {
    TARANTOOL = {
        header = 'tarantool/module.h'
    };
}

build = {
    type = 'cmake',

    modules = {
        avro = {
            sources = 'avro/avro.cpp',
            incdirs = {
                "${TARANTOOL_INCDIR}"
            }
        }
    }
}
-- vim: syntax=lua ts=4 sts=4 sw=4 et
