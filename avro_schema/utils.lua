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

return {
    table_contains = table_contains,
    copy_fields = copy_fields
}
