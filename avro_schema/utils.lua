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

--- Check if given table has only one specific key.
local function has_only(t, key)
    local fst_key = next(t)
    local snd_key = next(t, fst_key)
    return fst_key == key and snd_key == nil
end

local function map(func, tab)
    local res = {}
    for k, v in pairs(tab) do
        res[k] = func(k, v)
    end
    return res
end

-- Fast stack implementation.
-- This stack can store several value in each frame.
-- instead of storing tables for each frame, this stack maintains
-- several arrays and stores different keys to different arrays.
--
-- E.g. init_fstack({'schema', 'data'}) would create a table like this:
-- {
--      schema = {}, -- array which stores schema values
--      set_schema = function(schema),
--      get_schema = function(),
--      data = {},   -- array which stores data values
--      set_data = function(data),
--      get_data = function(),
--      len = 0,     -- current size of the array
--      push = function(schema, data),
--      pop  = function() -> { schema = schema[i], data = data[i] }
--      get  = function(i) -> { schema = schema[i], data = data[i] }
--      remove_last = function(),
--      clear = function(),
-- }
local function init_fstack(keys)
    assert(table_contains(keys, 'push') == false)
    assert(table_contains(keys, 'pop') == false)
    assert(table_contains(keys, 'remove_last') == false)
    assert(table_contains(keys, 'get') == false)
    assert(table_contains(keys, 'clear') == false)
    local fstack_str = [[
        DECLARE_VARS
        local stack = {
            len = 0,
            MOVE_VARS_TO_MODULE
        }
        stack.push = function(PUSH_VARS)
            stack.len = stack.len + 1
            PUSH_SET_VALS
        end
        stack.pop = function()
            local len = stack.len
            assert(len > 0)
            stack.len = len - 1
            return POP_RETURN_STMT
        end
        -- remove_last does not actually remove frames due to performance
        -- reasons (~10% RPS).
        stack.remove_last = function()
            stack.len = stack.len - 1
            assert(stack.len >= 0)
        end
        stack.get = function(pos)
            if pos <= 0 or pos > stack.len then
                error(("Attempt to get element %d " ..
                    "from stack of size %d"):format(pos, stack.len))
            end
            return GET_RETURN_STMT
        end
        stack.clear = function()
            stack.len = 0
            CLEAR_CLEAR_TABLES
        end
        SETTERS_GETTERS
        return stack
        ]]
    fstack_str = fstack_str:gsub('DECLARE_VARS',
        table.concat(
            map(function(i, key)
                return ('local %s = {}'):format(key, i)
            end, keys),
            '\n')
        )
    fstack_str = fstack_str:gsub('MOVE_VARS_TO_MODULE',
        table.concat(
            map(function(_, key)
                return ('%s = %s'):format(key, key)
            end, keys),
            ',\n')
        )
    fstack_str = fstack_str:gsub('PUSH_VARS',
        table.concat(
            map(function(i, _) return 'v' .. i end, keys),
            ',')
        )
    fstack_str = fstack_str:gsub('PUSH_SET_VALS',
        table.concat(
            map(function(i, key)
                return ('%s[stack.len] = v%d'):format(key, i)
            end, keys),
            '\n')
        )
    fstack_str = fstack_str:gsub('POP_RETURN_STMT',
        table.concat(
            map(function(_, key)
                return ('%s[len]'):format(key)
            end, keys),
            ',')
        )
    -- remove_last does not actually remove frames due to performance
    -- reasons (~10% RPS).
    fstack_str = fstack_str:gsub('GET_RETURN_STMT',
        table.concat(
            map(function(_, key)
                return ('%s[pos]'):format(key)
            end, keys),
            ',')
        )
    fstack_str = fstack_str:gsub('CLEAR_CLEAR_TABLES',
        table.concat(
            map(function(_, key)
                return ('table.clear(%s)'):format(key)
            end, keys),
            '\n')
        )
    -- Gen setters/getters.
    -- Setters/getters are a bit slower than raw access.
    local setters_getters = {}
    for _, key in ipairs(keys) do
        local setter_str = ([[
            stack.set_%s = function(val)
                assert(stack.len > 0)
                stack.%s[stack.len] = val
            end
            ]]):format(key, key)
        table.insert(setters_getters, setter_str)
        local getter_str = ([[
            stack.get_%s = function(val)
                assert(stack.len > 0)
                return stack.%s[stack.len]
            end
            ]]):format(key, key)
        table.insert(setters_getters, getter_str)
    end
    setters_getters = table.concat(setters_getters, '\n')
    fstack_str = fstack_str:gsub('SETTERS_GETTERS', setters_getters)
    local stack = loadstring(fstack_str, 'avro.utils.fstack')()
    return stack
end

return {
    table_contains = table_contains,
    copy_fields = copy_fields,
    has_only = has_only,
    init_fstack = init_fstack,
}
