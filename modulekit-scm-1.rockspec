package = 'modulekit'
version = 'scm-1'

source  = {
    url    = 'git://github.com/tarantool/modulekit.git';
    branch = 'master';
}

description = {
    summary  = "A set of Tarantool module templates";
    detailed = [[
    A ready to use module templates. Clone and modify to create
    new modules.
    ]];
    homepage = 'https://github.com/tarantool/modulekit.git';
    license  = 'BSD';
    maintainer = "Roman Tsisyk <roman@tarantool.org>";
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
    type = 'builtin',

    modules = {
        ['modulekit'] = 'modulekit/init.lua';
        ['modulekit.functions'] = 'modulekit/functions.lua';
        ['modulekit.cfunctions'] = {
            incdirs = {
                '$(TARANTOOL_INCDIR)';
            };
            sources = 'modulekit/cfunctions.c';
        }
    }
}
-- vim: syntax=lua ts=4 sts=4 sw=4 et
