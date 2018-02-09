-- This file implements fingerprinting mechanism for Avro schema.
-- It was necessary to implement our json encoder, because of some special
-- rules for avro fingerptint generation and Parsing Canonical Form generation.

local json = require "json"
-- Tarantool specific module
local digest = require "digest"

local avro_json

local function raise_error(message, ...)
    error(string.format("avro-fingerprint: "..message, ...))
end

local function is_primitive_type(xtype)
    local ptypes = {"string", "number", "boolean"}
    for _,t in ipairs(ptypes) do
        if xtype == t then return true end
    end
    return false
end

local function avro_json_array(data)
    local res = {}
    for _,item in ipairs(data) do
        table.insert(res,avro_json(item))
    end
    return string.format("[%s]", table.concat(res, ","))
end

local function avro_json_object(data)
    local res = {}
    local necessary_order = {"name", "type", "fields", "symbols", "items", "values", "size"}
    for _,name in ipairs(necessary_order) do
        local item = data[name]
        if item ~= nil then
            local inner = avro_json(item)
            inner = string.format([[%s:%s]], json.encode(name), inner)
            table.insert(res, inner)
        end
    end
    return string.format("{%s}", table.concat(res, ","))
end

-- Takes normalized avro schema and produces normalized schema representation
-- encoded in json format.
avro_json = function (data)
    local xtype = type(data)
    if is_primitive_type(xtype) then
        return json.encode(data)
    end
    if xtype ~= "table" then
        raise_error("data type is not supported: %s", xtype)
    end
    -- array
    if #data > 0 then
        return avro_json_array(data)
    end
    -- object (dict)
    return avro_json_object(data)
end

local function get_fingerprint(schema, algo, size)
    if digest[algo] == nil or type(digest[algo]) ~= "function" then
        raise_error("The hash function %s is not supported", algo)
    end
    local fp = digest[algo](avro_json(schema))
    return fp:sub(1, size)
end

return {
    avro_json = avro_json,
    get_fingerprint = get_fingerprint,
}
