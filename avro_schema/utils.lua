local function table_contains(t, xval)
    for k, val in ipairs(t) do
        if type(k) == "number" and val == xval then
            return true
        end
    end
    return false
end

-- This function copies fields from one table to the other.
--
-- The precise behavior may be controlled with help of `opts` arg.
-- opts: precise copy settings
--     fields: table of field keys (names) which should be copied;
--             if `nil` then all fields copied
--     exclude: table of field keys (names) which should not be copied;
--              works if `fields` are not passed
--     deep: whether to copy fields deeply or not
local function copy_fields(from, to, opts)
    opts = opts or {}
    assert(opts.fields == nil or opts.exclude == nil,
        "`exclude` attr should not be set if `fields` attr is set")
    local deep = opts.deep or true
    local fields_exclude = opts.exclude or {}
    -- convert exclude fields to dict to decrease asimptotic exec time
    local fields_exclude_dict = {}
    for _, name in pairs(fields_exclude) do
        fields_exclude_dict[name] = true
    end
    local fields_copy = opts.fields
    if fields_copy == nil then
        fields_copy = {}
        for k, _ in pairs(from) do
            if fields_exclude_dict[k] == nil then
                table.insert(fields_copy, k)
            end
        end
    end
    for _, name in pairs(fields_copy) do
        local field = from[name]
        if deep then
            to[name] = table.deepcopy(field)
        else
            to[name] = field
        end
    end
end

return {
    table_contains = table_contains,
    copy_fields = copy_fields
}
