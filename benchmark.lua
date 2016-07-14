local avro = require('avro_schema')

local ok, person = avro.create({
    type = 'record',
    name = 'Person',
    namespace = 'Person',
    fields = {
        { name = 'FirstName', type = 'string' },
        { name = 'LastName',  type = 'string' },
        { name = 'Class',     type = 'string' },
        { name = 'Age',       type = 'long'   },
        { 
            name = 'Sex',
            type = {
                type = 'enum',
                name = 'Sex',
                symbols = { 'FEMALE', 'MALE' }
            }
        },
        {
            name = 'Stats',
            type = {
                type = 'record',
                name = 'Stats',
                fields = {
                    { name = 'Strength',     type = 'long' },
                    { name = 'Perception',   type = 'long' },
                    { name = 'Endurance',    type = 'long' },
                    { name = 'Charisma',     type = 'long' },
                    { name = 'Intelligence', type = 'long' },
                    { name = 'Agility',      type = 'long' },
                    { name = 'Luck',         type = 'long' }
                }
            }
        },
        {
            name = 'Journal',
            type = {
                type  = 'array',
                items = 'string'
            }
        }
    }
})

if not ok then error(person) end

local ok, person_c = avro.compile(person)
if not ok then error(person_c) end

local flatten = person_c.flatten_msgpack
local unflatten = person_c.unflatten_msgpack

local data  = require('msgpack').encode({
    FirstName = 'John',
    LastName  = 'Doe',
    Class     = 'TechWizard',
    Age       = 17,
    Sex       = 'MALE',
    Stats     = {
        Strength     = 3,
        Perception   = 5,
        Endurance    = 1,
        Charisma     = 4,
        Intelligence = 9,
        Agility      = 3,
        Luck         = 6
    },
    Journal   = {
        'You are standing at the end of a road before a small brick building.',
        'Around you is a forest.',
        'A small stream plows out of the building and down a gully.',
        'You enter the forest.',
        'You are in a valley in the forest besides a stream tumling along a rocky end.',
        'You feel thirsty!'
    }
})

print('benchmark started...')

local clock       = require('clock')

local n = 10000000
local t = clock.bench(function()
    for i = 1,n do
        flatten(data)
    end
end)[1]
print(string.format('  flatten: %d RPS', math.floor(n/t)))

local _, data = flatten(data)
local t = clock.bench(function()
    for i = 1,n do
        unflatten(data)
    end
end)[1]
print(string.format('unflatten: %d RPS', math.floor(n/t)))
