local function table_contains(t, xval)
    for k, val in ipairs(t) do
        if type(k) == "number" and val == xval then
            return true
        end
    end
    return false
end

-- This function copies fields from one table to the other deeply.
-- In case `fields` param passed, only fields are copied.
--
-- From is an array of field names.
local function copy_fields(from, to, fields)
    if fields then
        for _,field in ipairs(fields) do
            if from[field] ~= nil then
                to[field] = table.deepcopy(from[field])
            end
        end
    else
        for k,v in pairs(from) do
            to[k] = v
        end
    end
end

local function table_find_value(xtable, xvalue)
    for _, value in pairs(xtable) do
        if value == xvalue then
            return true
        end
    end
    return false
end

-- Analog for `copy_fields` function, except this one copies all fields
-- except those mentioned in `fields_not_to_copy`.
local function copy_fields_except(from, to, fields_not_to_copy)
    for key, value in pairs(from) do
        if not table_find_value(fields_not_to_copy, key) then
            to[key] = table.deepcopy(value)
        end
    end
end

return {
    table_contains = table_contains,
    copy_fields = copy_fields,
    copy_fields_except = copy_fields_except
}
