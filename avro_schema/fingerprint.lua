-- This file implements fingerprinting mechanism for Avro schema.
-- It was necessary to implement our json encoder, because of some special
-- rules for avro fingerptint generation and Parsing Canonical Form generation.

local json = require "json"
local frontend = require "avro_schema.frontend"
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

local function avro_json_array(data, extra_fields)
    local res = {}
    for _,item in ipairs(data) do
        table.insert(res,avro_json(item, extra_fields))
    end
    return string.format("[%s]", table.concat(res, ","))
end

local function avro_json_object(data, extra_fields)
    local res = {}
    local necessary_order = {"name", "type", "fields", "symbols", "items", "values", "size"}
    --
    -- There are a cases in which it is necessary to extend a schema.
    -- The source below provides method to add those attrs in sustainable way.
    --
    for _, val in ipairs(extra_fields) do
        table.insert(necessary_order, val)
    end

    for _,name in ipairs(necessary_order) do
        local item = data[name]
        if item ~= nil then
            local inner = avro_json(item, extra_fields)
            inner = string.format([[%s:%s]], json.encode(name), inner)
            table.insert(res, inner)
        end
    end
    return string.format("{%s}", table.concat(res, ","))
end

-- Takes normalized avro schema and produces normalized schema representation
-- encoded in json format.
avro_json = function (data, extra_fields)
    extra_fields = extra_fields or {}
    -- should be sorted for consistency
    table.sort(extra_fields)
    local xtype = type(data)
    if is_primitive_type(xtype) then
        return json.encode(data)
    end
    if xtype ~= "table" then
        raise_error("data type is not supported: %s", xtype)
    end
    -- array
    if #data > 0 then
        return avro_json_array(data, extra_fields)
    end
    -- object (dict)
    return avro_json_object(data, extra_fields)
end

local function get_fingerprint(schema, algo, size, options)
    if digest[algo] == nil or type(digest[algo]) ~= "function" then
        raise_error("The hash function %s is not supported", algo)
    end
    -- We have to call export first to replace type definitions on type
    -- references (all except the first).
    schema = frontend.export_helper(schema)
    local fp = digest[algo](avro_json(schema, options.preserve_in_fingerprint))
    return fp:sub(1, size)
end

return {
    avro_json = avro_json,
    get_fingerprint = get_fingerprint,
}
