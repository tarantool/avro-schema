#!/usr/bin/env tarantool

-- Fix tests in out of source build
local dirs = { "..", ".",  os.getenv("BINARY_DIR") }
package.path = ""
package.cpath = ""
for _, dir in pairs(dirs) do
    package.path = package.path .. dir .. "/?/init.lua;" .. dir .. "/?.lua;"
    package.cpath = package.cpath .. dir .. "/?.so;" .. dir .. "/?.dylib;"
end

local kit = require('modulekit')

require('tap').test('kit', function(test)
    test:plan(3)
    test:is(kit.test(1), 11, "Lua function in init.lua")
    test:is(kit.functions.func(10, 15), 25, "Lua function in submodule")
    test:is(kit.cfunctions.func(10, 15), 25, "Lua/C function in submodule")
end)
