local msgpack = require('msgpack')  
local schema  = require('avro_schema')
local runtime = require('avro_schema.runtime')
local tap     = require('tap')

local test = tap.test('buf-grow')
test:plan(1)

local log = {}
local buf_grow = runtime.buf_grow

-- hook buf_grow, must happen *before* compile
runtime.buf_grow = function(r, min_capacity)
    local cur_capacity = tonumber(r.ot_capacity)
    if not log[1] then log[1] = cur_capacity end
    table.insert(log, min_capacity - cur_capacity)
    buf_grow(r, min_capacity)
    r.ot_capacity = min_capacity -- normally it grows X1.5
end

local _, s = schema.create({
    type = 'array', items = {
        name = 'FooBar', type = 'record', fields = {
            {name = 'A', type = 'long'},
            {name = 'B', type = 'long'},
            {name = 'C', type = 'long'},
            {name = 'D', type = {
                type = 'array', items = 'long'
            }}
        }
    }
})

if not _ then error(s) end

local _, m = schema.compile(s)
if not _ then error(m) end


local item = msgpack.encode({
    A = 1, B = 2, C = 3, D = { 0, -1, -2, -3 }
})

m.flatten_msgpack('\220\0\20' .. string.rep(item, 20))

-- Ensure that during the run buffer size is properly checked and increased.
-- Initial buffer capacity is 128.
-- The capcity is increased by +5 to accomodate for:
--   array_header(FooBar) long(A), long(B), long(C), array_header(D)
-- and then by +4 to accomodate nested array content.
test:is_deeply(log, {128, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4, 5, 4}, '#1')

test:check()
os.exit(test.planned == test.total and test.failed == 0 and 0 or -1)
