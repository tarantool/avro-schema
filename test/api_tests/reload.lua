local tap = require('tap')
local test = tap.test('reload test')

test:plan(1)

test:test('reload test', function(test)
    test:plan(1)
    package.loaded['avro_schema'] = nil
    package.loaded['avro_schema.il'] = nil
    local ok = pcall(require, 'avro_schema')
    test:ok(ok, 'Successfully reloaded')
end)

os.exit(test:check() and 0 or 1)
