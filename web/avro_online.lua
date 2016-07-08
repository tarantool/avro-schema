local avro = require('avro_schema')
local json =  require('json')

local Flattener = {
    compile = function(self, data_model)
        local ok, schema = avro.create(data_model)
        if not ok then
            return false, schema
        end
        return avro.compile({schema})
    end,

    flatten = function(self, schema, data)
        local ok, compiled = self:compile(schema)
        if type(compiled) == 'string' then
            return compiled
        end
        local ok, tuple = compiled.flatten(data)
        if not ok then
            return string.gsub(tuple, '\"', '\'')
        end
        return tuple
    end
}

-- export compile, flatten functions and listen 3301
function compile(request, schema)
    local data = nil
    local ok, err = pcall(function()
        data = json.decode(schema)
    end)
    if not ok then
        return {'Invalid JSON'}
    end

    local ok, compiled = Flattener:compile(data)
    if not ok then
        return string.gsub(compiled, '\"', '\'')
    end
    return 'Schema OK'
end
function validate(request, schema, data)
    local json_data = nil
    local json_schema = nil
    local ok, err = pcall(function()
        json_schema = json.decode(schema)
        json_data = json.decode(data)
    end)
    if not ok then
        return {'Invalid JSON'}
    end
    return Flattener:flatten(json_schema, json_data)
end
box.cfg{listen="3301"}
box.schema.user.grant('guest', 'read,write,execute', 'universe')
