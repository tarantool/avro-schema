-- The frontend.
-- Loads schema and generates IR.
--
-- # About schemas
--
-- Internally, a schema is encoded precisely as defined by Avro spec.
-- So create_schema simply creates a private copy so no one changes
-- data from under us.  Makes it possible to validate and sanitize
-- schema definition exactly once.
--
-- Create also normalizes schema definition (did you know that
-- both "int" and { "type": "int" } are the same thing?)
--
-- For convenience, any reference to an earlier-defined type by name
-- is replaced with an object itself (loops!).
--
--
-- # About IR
--
-- Compiler generates code from the IR, which is more generic than
-- a bare schema. IR defines a 'from' schema, a 'to' schema and a mapping.
--
-- IR is a graph of nodes mirroring a schema structure.
-- Simple terminal nodes (string):
--
--   NUL, BOOL, INT, LONG, FLT, DBL, BIN, STR,
--   INT2LONG, INT2FLT, INT2DBL, LONG2FLT, LONG2DBL,
--   FLT2DBL, STR2BIN, BIN2STR
--
-- Complex nodes (table):
--
--   { type = 'FIXED',  size   = <n>  }
--   { type = 'ARRAY',  nested = <ir> }
--   { type = 'MAP',    nested = <ir> }
--
--   { type = 'ENUM',       from = <schema>, to = <schema>, i2o = ? }
--
--   { type = 'RECORD', nested = {
--           type = '__RECORD__',
--           from = <schema>, to = <schema>, i2o = ?, o2i = ?, ... }}
--
--   { type = 'UNION', nested = {
--           type = '__UNION__',
--           from = <schema>, to = <schema>, i2o = ?, ... }}
--
-- Legend: n      — size of fixed
--         ir     - ir of the array item / map value
--         schema - source or destination schema (for this particular node)
--         i2o    - input-index-to-output-index mapping
--         o2i    - i2o backwards
--         ...    - ir of record fields / union branches (source schema order)
--
-- Note 1: RECORD/__RECORD__ are for the compiler's convenience
--         (distinguishing container and contents)
--
-- Note 2: In a UNION, is_union(from) or is_union(to) holds
--         (Avro allows mapping a union to non-union and vice versa)

local debug = require('debug')
local ffi = require('ffi')
local null = ffi.cast('void *', 0)
local format, find, gsub = string.format, string.find, string.gsub
local sub, lower = string.sub, string.lower
local insert, remove, concat = table.insert, table.remove, table.concat
local floor = math.floor
local clear = require('table.clear')
local next, type = next, type

local function deepcopy(v)
    if type(v) == 'table' then
        res = {}
        for k, v in pairs(v) do
            res[k] = deepcopy(v)
        end
        return res
    else
        return v
    end
end

-- primitive types
local primitive_type = {
    null  = 'NUL', boolean = 'BOOL', int   = 'INT', long   = 'LONG',
    float = 'FLT', double  = 'DBL',  bytes = 'BIN', string = 'STR', any = 'XXX'
}

-- type promotions
local promotions = {
    int    = { long   = 'INT2LONG', float  = 'INT2FLT', double = 'INT2DBL' },
    long   = { float  = 'LONG2FLT', double = 'LONG2DBL' },
    float  = { double = 'FLT2DBL' },
    string = { bytes  = 'STR2BIN' },
    bytes  = { string = 'BIN2STR' }
}

-- check if name is a valid Avro identifier
local function validname(name)
    return gsub(name, '[_A-Za-z][_0-9A-Za-z]*', '-') == '-'
end

-- like validname(), but with support for dot-separated components
local function validfullname(name)
    return gsub(gsub(name, '[_A-Za-z][_0-9A-Za-z]*', '-'), '-%.', '') == '-'
end

-- add namespace to the name
local function fullname(name, ns)
    if find(name, '%.') or not ns then
        return name
    end
    return format('%s.%s', ns, name)
end

-- type tags used in unions
local function type_tag(t)
    return (type(t) == 'string' and t) or t.name or t.type
end

local copy_schema_error
local copy_schema_location_info

-- handle @name attribute of a named type
local function checkname(schema, ns, scope)
    local xname = schema.name
    if not xname then
        copy_schema_error('Must have a "name"') 
    end
    xname = tostring(xname)
    if find(xname, '%.') then
        ns = gsub(xname, '%.[^.]*$', '')
    else
        local xns = schema.namespace
        if xns then
            ns = tostring(xns)
            xname = format('%s.%s', xns, xname)
        end
    end
    if not validfullname(xname) then
        copy_schema_error('Bad type name: %s', xname)
    end
    if primitive_type[gsub(xname, '.*%.', '')] then
        copy_schema_error('Redefining primitive type name: %s', xname)
    end
    xname = fullname(xname, ns)
    if scope[xname] then
        copy_schema_error('Type name already defined: %s', xname)
    end
    return xname, ns
end

-- handle @aliases attribute of a named type
local function checkaliases(schema, ns, scope)
    local xaliases = schema.aliases
    if not xaliases then
        return
    end
    if type(xaliases) ~= 'table' then
        copy_schema_error('Property "aliases" must be a list')
    end
    if #xaliases == 0 then
        return
    end
    local aliases = {}
    for aliasno, alias in ipairs(xaliases) do
        alias = tostring(alias)
        if not validfullname(alias) then
            copy_schema_error('Bad type name: %s', alias)
        end
        alias = fullname(alias, ns)
        if scope[alias] then
            copy_schema_error('Alias type name already defined: %s', alias)
        end
        aliases[aliasno] = alias
        scope[alias] = true
    end
    return aliases
end

-- it makes sense to cache certain derived data
-- keyed by schema node,
--   <union>  -> tagstr_to_branch_no_map
--   <record> -> field_name_to_field_no_map (aliases included)
--   <enum>   -> symbolstr_to_symbol_no_map
local dcache = setmetatable({}, { __mode = 'k' })

local copy_field_default

-- create a private copy and sanitize recursively;
-- [ns]       current ns (or nil)
-- [scope]    a dictionary of named types (ocasionally used for unnamed too)
-- [open_rec] a set consisting of the current record + parent records;
--            it is used to reject records containing themselves
copy_schema = function(schema, ns, scope, open_rec)
    local res, ptr -- we depend on these being locals #5 and #6
    if type(schema) == 'table' then
        if scope[schema] then
            -- this check is necessary for unnamed complex types (union, array, map)
            copy_schema_error('Infinite loop detected in the data')
        end
        if #schema > 0 then
            local tagmap = {}
            scope[schema] = 1
            res = {}
            for branchno, xbranch in ipairs(schema) do
                ptr = branchno
                local branch = copy_schema(xbranch, ns, scope)
                local bxtype, bxname
                if type(branch) == 'table' and not branch.type then
                    copy_schema_error('Union may not immediately contain other unions')
                end
                local bxid = type_tag(branch)
                if tagmap[bxid] then
                    copy_schema_error('Union contains %s twice', bxid)
                end
                res[branchno] = branch
                tagmap[bxid] = branchno
            end
            scope[schema] = nil
            dcache[res] = tagmap
            return res
        else
            if not next(schema) then
                copy_schema_error('Union type must have at least one branch')
            end
            local xtype = schema.type
            if not xtype then
                copy_schema_error('Must have a "type"')
            end
            xtype = tostring(xtype)
            if primitive_type[xtype] then
                return xtype
            elseif xtype == 'record' then
                res = { type = 'record' }
                local name, ns = checkname(schema, ns, scope)
                scope[name] = res
                res.name = name
                res.aliases = checkaliases(schema, ns, scope)
                open_rec = open_rec or {}
                open_rec[res] = 1
                local xfields = schema.fields
                if not xfields then
                    copy_schema_error('Record type must have "fields"')
                end
                if type(xfields) ~= 'table' then
                    copy_schema_error('Record "fields" must be a list')
                end
                if #xfields == 0 then
                    copy_schema_error('Record type must have at least one field')
                end
                res.fields = {}
                local fieldmap = {}
                for fieldno, xfield in ipairs(xfields) do
                    ptr = fieldno
                    local field = {}
                    res.fields[fieldno] = field
                    if type(xfield) ~= 'table' then
                        copy_schema_error('Record field must be a list')
                    end
                    local xname = xfield.name
                    if not xname then
                        copy_schema_error('Record field must have a "name"')
                    end
                    xname = tostring(xname)
                    if not validname(xname) then
                        copy_schema_error('Bad record field name: %s', xname)
                    end
                    if fieldmap[xname] then
                        copy_schema_error('Record contains field %s twice', xname)
                    end
                    fieldmap[xname] = fieldno
                    field.name = xname
                    local xtype = xfield.type
                    if not xtype then
                        copy_schema_error('Record field must have a "type"')
                    end
                    field.type = copy_schema(xtype, ns, scope, open_rec)
                    if open_rec[field.type] then
                        local path, n = {}
                        for i = 1, 1000000 do
                            local _, res = debug.getlocal(i, 5)
                            if res == field.type then
                                n = i
                                break
                            end
                        end
                        for i = n, 1, -1 do
                            local _, res = debug.getlocal(i, 5)
                            local _, ptr = debug.getlocal(i, 6)
                            insert(path, res.fields[ptr].name)
                        end
                        error(format('Record %s contains itself via %s',
                                     field.type.name,
                                     concat(path, '/')), 0)
                    end
                    local xdefault = xfield.default
                    if xdefault ~= nil then
                        local ok, res = copy_field_default(field.type, xdefault)
                        if not ok then
                            copy_schema_error('Default value not valid (%s)', res)
                        end
                        field.default = res
                    end
                    local xaliases = xfield.aliases
                    if xaliases then
                        if type(xaliases) ~= 'table' then
                            copy_schema_error('Property "aliases" must be a list')
                        end
                        local aliases = {}
                        for aliasno, alias in ipairs(xaliases) do
                            alias = tostring(alias)
                            if not validname(alias) then
                                copy_schema_error('Bad field alias name: %s', alias)
                            end
                            if fieldmap[alias] then
                                copy_schema_error('Alias field name already defined: %s', alias)
                            end
                            fieldmap[alias] = fieldno
                            aliases[aliasno] = alias
                        end
                        if #aliases ~= 0 then
                            field.aliases = aliases
                        end
                    end
                    field.hidden = not not xfield.hidden or nil -- extension
                end
                dcache[res] = fieldmap
                open_rec[res] = nil
                return res
            elseif xtype == 'enum' then
                res = { type = 'enum', symbols = {} }
                local name, ns = checkname(schema, ns, scope)
                scope[name] = res
                res.name = name
                res.aliases = checkaliases(schema, ns, scope)
                local xsymbols = schema.symbols
                if not xsymbols then
                    copy_schema_error('Enum type must have "symbols"')
                end
                if type(xsymbols) ~= 'table' then
                    copy_schema_error('Enum "symbols" must be a list')
                end
                if #xsymbols == 0 then
                    copy_schema_error('Enum type must contain at least one symbol')
                end
                local symbolmap = {}
                for symbolno, symbol in ipairs(xsymbols) do
                    symbol = tostring(symbol)
                    if not validname(symbol) then
                        copy_schema_error('Bad enum symbol name: %s', symbol)
                    end
                    if symbolmap[symbol] then
                        copy_schema_error('Enum contains symbol %s twice', symbol)
                    end
                    symbolmap[symbol] = symbolno
                    res.symbols[symbolno] = symbol
                end
                dcache[res] = symbolmap
                return res
            elseif xtype == 'array' then
                res = { type = 'array' }
                scope[schema] = true
                local xitems = schema.items
                if not xitems then
                    copy_schema_error('Array type must have "items"')
                end
                res.items = copy_schema(xitems, ns, scope)
                scope[schema] = nil
                return res
            elseif xtype == 'map' then
                res = { type = 'map' }
                scope[schema] = true
                local xvalues = schema.values
                if not xvalues then
                    copy_schema_error('Map type must have "values"')
                end
                res.values = copy_schema(xvalues, ns, scope)
                scope[schema] = nil
                return res
            elseif xtype == 'fixed' then
                res = { type = 'fixed' }
                local name, ns = checkname(schema, ns, scope)
                scope[name] = res
                res.name = name
                res.aliases = checkaliases(schema, ns, scope)
                local xsize = schema.size
                if xsize==nil then
                    copy_schema_error('Fixed type must have "size"')
                end
                if type(xsize) ~= 'number' or xsize < 1 or math.floor(xsize) ~= xsize then
                    copy_schema_error('Bad fixed type size: %s', xsize)
                end
                res.size = xsize
                return res
            else
                copy_schema_error('Unknown Avro type: %s', xtype)
            end
        end
    else
        local typeid = tostring(schema)
        if primitive_type[typeid] then
            return typeid
        end
        typeid = fullname(typeid, ns)
        schema = scope[typeid]
        if schema and schema ~= true then -- ignore alias names
            return schema
        end
        copy_schema_error('Unknown Avro type: %s', typeid)
    end
end

-- find 1+ consequetive func frames
local function find_frames(func)
    local top
    for i = 2, 1000000 do
        local info = debug.getinfo(i)
        if not info then
            return 1, 0
        end
        if info.func == func then
            top = i
            break
        end
    end
    for i = top, 1000000 do
        local info = debug.getinfo(i)
        if not info or info.func ~= func then
            return top - 1, i - 2
        end
    end
end

-- extract copy_schema() current location
copy_schema_location_info = function()
    local top, bottom = find_frames(copy_schema)
    local res = {}
    for i = bottom, top, -1 do
        local _, node = debug.getlocal(i, 5)
        local _, ptr  = debug.getlocal(i, 6)
        if type(node) == 'table' then
            if node.type == nil then -- union
                insert(res, '<union>')
                if i <= top + 1 then
                    local _, next_node = debug.getlocal(i - 1, 6)
                    if i == top or (i == top + 1 and
                                    not (next_node and next_node.name)) then
                        insert(res, format('<branch-%d>', ptr))
                    end
                end
            elseif node.type == 'record' then
                if not node.name then
                    insert(res, '<record>')
                else
                    insert(res, node.name)
                    if node.fields and ptr then
                        if node.fields[ptr].name then
                            insert(res, node.fields[ptr].name)
                        else
                            insert(res, format('<field-%d>', ptr))
                        end
                    end
                end
            elseif node.name then
                insert(res, node.name)
            else
                insert(res, format('<%s>', node.type))
            end
        end
    end
    return #res ~= 0 and concat(res, '/') or nil
end

-- report error condition while in copy_schema()
copy_schema_error = function(fmt, ...)
    local msg = format(fmt, ...)
    local li  = copy_schema_location_info()
    if li then
        error(format('%s: %s', li, msg), 0)
    else
        error(msg, 0)
    end
end

-- validate schema definition (creates a copy)
local function create_schema(schema)
    return copy_schema(schema, nil, {})
end

-- get a mapping from a (string) type tag -> union branch id
local function get_union_tag_map(union)
    local res = dcache[union]
    if not res then
        res = {}
        for bi, b in ipairs(union) do
            res[type_tag(b)] = bi
        end
        dcache[union] = res
    end
    return res
end

-- get a mapping from a field name -> field id (incl. aliases)
local function get_record_field_map(record)
    local res = dcache[record]
    if not res then
        res = {}
        for fi, f in ipairs(record.fields) do
            res[f.name] = fi
            if f.aliases then
                for _, a in ipairs(f.aliases) do
                    res[a] = fi
                end
            end
        end
        dcache[record] = res
    end
    return res
end

-- get a mapping from a symbol name -> symbol id
local function get_enum_symbol_map(enum)
    local res = dcache[enum]
    if not res then
        res = {}
        for si, s in ipairs(enum.symbols) do
            res[s] = si
        end
        dcache[enum] = res
    end
    return res
end

-- from.type == to.type and from.name == to.name (considering aliases)
local function complex_types_may_match(from, to, imatch)
    if from.type ~= to.type then return false end
    if from.name == to.name then return true end
    if imatch then
        local tmp = from; from = to; to = tmp
    end
    local aliases = to.aliases
    if not aliases then return false end
    local alias_set = dcache[aliases]
    if alias_set then return alias_set[from.name] end
    local alias_set = {}
    for _, name in ipairs(aliases) do
        alias_set[name] = true
    end
    dcache[aliases] = alias_set
    return alias_set[from.name]
end

local copy_data

-- validate data against a schema; return a copy
copy_data = function(schema, data, visited)
    -- error handler peeks into ptr using debug.getlocal();
    local ptr
    local schematype = type(schema) == 'string' and schema or schema.type
    -- primitives
    -- Note: sometimes we don't check the type explicitly, but instead
    -- rely on an operation to fail on a wrong type. Done with integer
    -- and fp types, also with tables.
    -- Due to this technique, a error message is often misleading,
    -- e.x. "attempt to perform arithmetic on a string value". Unless
    -- a message starts with '@', we replace it (see copy_data_eh).
    if     schematype == 'null' then
        if data ~= null then
            error()
        end
        return null
    elseif schematype == 'boolean' then
        if type(data) ~= 'boolean' then
            error()
        end
        return data
    elseif schematype == 'int' then
        if data < -2147483648 or data > 2147483647 or floor(tonumber(data)) ~= data then
            error()
        end
        return data
    elseif schematype == 'long' then
        local n = tonumber(data)
        -- note: if it's not a number or cdata(numbertype),
        --       the expression below will raise
        -- note: boundaries were carefully picked to avoid
        --       rounding errors, they are INT64_MIN and INT64_MAX+1,
        --       respectively (both 2**k)
        if n < -9223372036854775808 or n >= 9223372036854775808 or
           floor(n) ~= n then
            -- due to rounding errors, INT64_MAX-1023..INT64_MAX
            -- fails the range check above, check explicitly for this
            -- case; in number > cdata(uint64_t) expression, number
            -- is implicitly coerced to uint64_t
            if n ~= 9223372036854775808 or data > 9223372036854775807ULL then
                error()
            end
        end
        return data
    elseif schematype == 'double' or schema == 'float' then
        return 0 + tonumber(data)
    elseif schematype == 'bytes' or schematype == 'string' then
        if type(data) ~= 'string' then
            error()
        end
        return data
    elseif schematype == 'enum' then
        if not get_enum_symbol_map(schema)[data] then
            error()
        end
        return data
    elseif schematype == 'fixed' then
        if type(data) ~= 'string' or #data ~= schema.size then
            error()
        end
        return data
    else
        if visited[data] then
            error('@Infinite loop detected in the data', 0)
        end
        local res = {}
        visited[data] = true
        -- record, enum, array, map, fixed
        if     schematype == 'record' then
            local fieldmap = get_record_field_map(schema)
            for k,v in pairs(data) do
                ptr = k
                local field = schema.fields[fieldmap[k]]
                if not field or field.name ~= k then
                    error('@Unknown field', 0)
                end
                res[k] = copy_data(field.type, v, visited)
            end
            ptr = nil
            for _,field in ipairs(schema.fields) do
                if     data[field.name] then
                elseif field.default ~= nil then
                    res[field.name] = deepcopy(field.default)
                else
                    error(format('@Field %s missing', field.name), 0)
                end
            end
        elseif schematype == 'array'  then
            for i, v in ipairs(data) do
                ptr = i
                res[i] = copy_data(schema.items, v, visited)
            end
        elseif schematype == 'map'    then
            for k, v in pairs(data) do
                ptr = k
                if type(k) ~= 'string' then
                    error('@Non-string map key', 0)
                end
                res[k] = copy_data(schema.values, v, visited)
            end
        elseif not schematype then -- union
            local tagmap = get_union_tag_map(schema)
            if data == null then
                if not tagmap['null'] then
                    error('@Unexpected type in union: null', 0)
                end
                res = null
            else
                local k, v = next(data)
                local bpos = tagmap[k]
                ptr = k
                if not bpos then
                    error('@Unexpected key in union', 0)
                end
                res[k] = copy_data(schema[bpos], v, visited)
                ptr = next(data, k)
                if ptr then
                    error('@Unexpected key in union', 0)
                end
            end
        elseif schematype == 'any' then
            if type(data) == 'table' then
                for k, v in pairs(data) do
                    ptr = k
                    if type(k) == 'table' then
                        error('@Invalid key', 0)
                    end
                    res[k] = copy_data('any', v, visited)
                end
            else
                res = data
            end
        else
            assert(false)
        end
        visited[data] = nil
        return res
    end
end

-- extract from the call stack a path to the fragment that failed
-- validation; enhance error message 
local function copy_data_eh(err)
    local top, bottom = find_frames(copy_data)
    local path = {}
    for i = bottom, top, -1 do
        local _, ptr = debug.getlocal(i, 4)
        insert(path, (ptr ~= nil and tostring(ptr)) or nil)
    end
    if type(err) == 'string' and sub(err, 1, 1) == '@' then
        err = sub(err, 2)
    else
        local _, schema = debug.getlocal(top, 1)
        local _, data   = debug.getlocal(top, 2)
        err = format('Not a %s: %s', (
            type(schema) == 'table' and (
                schema.name or schema.type or 'union')) or schema, data)
    end
    if #path == 0 then
        return err
    else
        return format('%s: %s', concat(path, '/'), err)
    end
end

local function validate_data(schema, data)
    return xpcall(copy_data, copy_data_eh, schema, data, {})
end

copy_field_default = function(fieldtype, default)
    if type(fieldtype) == 'table' and not fieldtype.type then
        -- "Default values for union fields correspond to the first 
        --  schema in the union." - the spec
        local ok, res = validate_data(fieldtype[1], default)
        if not ok or res == null then
            return ok, res
        else
            return true, { [type_tag(fieldtype[1])] = res }
        end
    else
        return validate_data(fieldtype, default)
    end
end

local function create_records_field_mapping(from, to)
    local i2o, o2i, field_map = {}, {}, get_record_field_map(to)
    for field_pos, field in ipairs(from.fields) do
        local to_pos = field_map[field.name]
        if to_pos then i2o[field_pos] = to_pos; o2i[to_pos] = field_pos end
    end
    return i2o, o2i
end

local build_ir_error
local build_ir

-- build IR recursively, mapping schemas from -> to
-- [mem]      handling loops
-- [imatch]   normally if from.name ~= to.name, to.aliases are considered;
--            in imatch mode we consider from.aliases instead
build_ir = function(from, to, mem, imatch)
    local ptrfrom, ptrto
    local from_union = type(from) == 'table' and not from.type
    local to_union   = type(to)   == 'table' and not to.type
    if     from_union or to_union then
        local i2o = {}
        local ir = { type = '__UNION__', from = from, to = to, i2o = i2o }
        from = from_union and from or {from}
        to = to_union and to or {to}
        local have_common = false
        local err
        for i, branch in ipairs(from) do
            for o, to_branch in ipairs(to) do
                if type(branch) == 'string' then
                    if branch == to_branch then
                        ir[i] = primitive_type[branch]
                        i2o[i] = o
                        have_common = true; break
                    elseif promotions[branch] and
                           promotions[branch][to_branch] then
                        ir[i] = promotions[branch][to_branch]
                        i2o[i] = o
                        have_common = true; break
                    end
                elseif complex_types_may_match(branch, to_branch, imatch) then
                    ir[i], err = build_ir(branch, to_branch, mem, imatch)
                    if not err then
                        i2o[i] = o; have_common = true; break
                    end
                end
            end
        end
        if not have_common then
            return nil, (err or build_ir_error(nil, 'No common types'))
        end
        return { type = 'UNION', nested = ir }
    elseif type(from) == 'string' then
        if from == to then
            return primitive_type[from]
        elseif promotions[from] and promotions[from][to] then
            return promotions[from][to]
        else
            return nil, build_ir_error(1, 'Types incompatible: %s and %s', from,
                                       type(to) == 'string' and to or to.name or to.type)
        end
    elseif not complex_types_may_match(from, to, imatch) then
        return nil, build_ir_error(1, 'Types incompatible: %s and %s',
                                   from.name or from.type,
                                   to.name or to.type or to)
    elseif from.type == 'array' then
        local bc, err = build_ir(from.items, to.items, mem, imatch)
        if not bc then
            return nil, err
        end
        return { type = 'ARRAY', nested = bc }
    elseif from.type == 'map'   then
        local bc, err = build_ir(from.values, to.values, mem, imatch)
        if not bc then
            return nil, err
        end
        return { type = 'MAP', nested = bc }
    elseif from.type == 'fixed' then
        if from.size ~= to.size then
            return nil, build_ir_error(nil, 'Size mismatch: %d vs %d',
                                       from.size, to.size)
        end
        return { type = 'FIXED', size = from.size }
    elseif from.type == 'record' then
        local res = mem[to]
        if res then return res end
        local i2o, o2i
        if imatch then
            o2i, i2o = create_records_field_mapping(to, from)
        else
            i2o, o2i = create_records_field_mapping(from, to)
        end
        local ir = {
            type = '__RECORD__', from = from, to = to, i2o = i2o, o2i = o2i
        }
        res = { type = 'RECORD', nested = ir }
        mem[to] = res -- NB: clean on error!
        for i, field in ipairs(from.fields) do
            local o = i2o[i]
            if o then
                local to_field = to.fields[o]
                ptrfrom = i; ptrto = o
                ir[i], err = build_ir(field.type, to_field.type, mem, imatch)
                if err then
                    mem[to] = nil
                    return nil, err
                end
                if field.default and not to_field.default then
                    mem[to] = nil
                    return nil, build_ir_error(nil, [[
Default value defined in source schema but missing in target schema]])
                end
            else
                ir[i] = build_ir(field.type, field.type, mem) -- never fails
            end
        end
        for o, field in ipairs(to.fields) do
            if field.default == nil and not o2i[o] then
                mem[to] = nil; ptrfrom = nil; ptrto = nil
                return nil, build_ir_error(nil, [[
Field %s is missing in source schema, and no default value was provided]],
                                           field.name)
            end
        end
        return res
    elseif from.type == 'enum' then
        local res = mem[to]
        if res then return res end
        local symmap      = get_enum_symbol_map(to)
        local i2o         = {}
        local have_common = nil
        for symbol_val, symbol in ipairs(from.symbols) do
            local to_val = symmap[symbol]
            i2o[symbol_val] = to_val
            have_common = have_common or to_val
        end
        if not have_common then
            return nil, build_ir_error(nil, 'No common symbols')
        end
        res = { type = 'ENUM', from = from, to = to, i2o = i2o }
        mem[to] = res
        return res
    else
        assert(false)
    end
end

build_ir_error = function(offset, fmt, ...)
    local msg = format(fmt, ...)
    local top, bottom = find_frames(build_ir)
    local res = {}
    top = top + (offset or 0)
    for i = bottom, top, -1 do
        local _, from    = debug.getlocal(i, 1)
        local _, to      = debug.getlocal(i, 2)
        local _, ptrfrom = debug.getlocal(i, 5)
        local _, ptrto   = debug.getlocal(i, 6)
        assert(type(from) == 'table')
        assert(type(to) == 'table')
        if not from.type then
            insert(res, '<union>')
        elseif not from.name then
            insert(res, format('<%s>', from.type))
        elseif from.name ~= to.name then
            insert(res, format('(%s aka %s)', from.name, to.name))
        else
            insert(res,from.name)
        end
        if ptrfrom and ptrto and from.type == 'record' and to.type == 'record' then
            local fromfield = from.fields[ptrfrom].name
            local tofield   = to.fields[ptrto].name
            if fromfield == tofield then
                insert(res, fromfield)
            else
                insert(res, format('(%s aka %s)', fromfield, tofield))
            end
        end
    end
    if #res == 0 then
        return msg
    else
        return format('%s: %s', concat(res, '/'), msg)
    end
end

local function create_ir(from, to, imatch)
    return build_ir(from, to, {}, imatch)
end

return {
    create_schema         = create_schema,
    validate_data         = validate_data,
    create_ir             = create_ir,
    get_enum_symbol_map   = get_enum_symbol_map,
    get_union_tag_map     = get_union_tag_map
}
