-- The frontend.
-- Loads schema and generates IR.

local debug = require('debug')
local ffi = require('ffi')
local null = ffi.cast('void *', 0)
local format, find, gsub = string.format, string.find, string.gsub
local sub, lower = string.sub, string.lower
local insert, remove, concat = table.insert, table.remove, table.concat
local floor = math.floor
local clear = require('table.clear')
local bnot  = bit.bnot

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

-- primitive types in Avro;
-- weird-looking strings are the IR bytecode
local primitive_type = {
    null  = 'NUL', boolean = 'BOOL', int   = 'INT', long   = 'LONG',
    float = 'FLT', double  = 'DBL',  bytes = 'BIN', string = 'STR', any = 'XXX'
}

-- permitted type promotions, more IR bytecode featured
local promotions = {
    int    = { long   = 'INT2LONG', float  = 'INT2FLT', double = 'INT2DBL' },
    long   = { float  = 'LONG2FLT', double = 'LONG2DBL' },
    float  = { double = 'FLT2DBL' },
    string = { bytes  = 'BIN2STR' },
    bytes  = { string = 'STR2BIN' }
}

-- more IR:
-- { 'FIXED',  N   }
-- { 'ARRAY',  ... }
-- { 'MAP',    ... }
-- { 'UNION',  bc,  from_tags,  id_map, to_tags }
-- { 'RECORD', bc,  from_names, id_map, to_names, defaults, is_hidden, from_name, to_name }
-- { 'ENUM',   nil, from_syms,  id_map, to_syms,  nil,      nil,       from_name, to_name }

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
        copy_schema_error('Property "aliases" must be a table')
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

-- sometimes if a type doesn't contain records
-- a faster algorithm is applicable (false positives are ok)
local function type_may_contain_records(t)
    if     type(t) == 'string' then
        return false
    elseif t.type == 'array' then
        return type_may_contain_records(t.items)
    elseif t.type == 'map' then
        return type_may_contain_records(t.values)
    elseif not t.type then
        -- union, up to 1 non-trivial branch checked (bb)
        local bb
        for _, b in ipairs(t) do
            if     type(b) == 'string' then
                -- definitely no records here
            elseif bb then -- second non-trivial branch
                return true
            else
                bb = b
            end
        end
        if bb then
            return type_may_contain_records(bb)
        else
            return false
        end
    else
        return false
    end
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
-- [defaults] array of cat-ed <context_info, field, default> triples;
--            Note: defaults are installed after checking default values
-- [open_rec] a set consisting of the current record + parent records;
--            it is used to reject records containing themselves
copy_schema = function(schema, ns, scope, defaults, open_rec)
    local res, ptr -- we depend on these being locals #6 and #7
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
                local branch = copy_schema(xbranch, ns, scope, defaults)
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
                    copy_schema_error('Record "fields" must be a table')
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
                        copy_schema_error('Record field must be a table')
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
                    field.type = copy_schema(xtype, ns, scope, defaults, open_rec)
                    if open_rec[field.type] then
                        local path, n = {}
                        for i = 1, 1000000 do
                            local _, res = debug.getlocal(i, 6)
                            if res == field.type then
                                n = i
                                break
                            end
                        end
                        for i = n, 1, -1 do
                            local _, res = debug.getlocal(i, 6)
                            local _, ptr = debug.getlocal(i, 7)
                            insert(path, res.fields[ptr].name)
                        end
                        error(format('Record %s contains itself via %s',
                                     field.type.name,
                                     concat(path, '/')), 0)
                    end
                    local xdefault = xfield.default
                    if xdefault ~= nil then
                        if type_may_contain_records(field.type) then
                            -- copy it later - may depend on parts we didn't build yet
                            insert(defaults, copy_schema_location_info() or '?')
                        else
                            -- can safely copy it now; it's faster
                            local ok, res = copy_field_default(field.type, xdefault)
                            if not ok then
                                copy_schema_error('Default value not valid (%s)', res)
                            end
                            insert(defaults, '')
                            xdefault = res
                        end
                        insert(defaults, field)
                        insert(defaults, xdefault)
                    end
                    local xaliases = xfield.aliases
                    if xaliases then
                        if type(xaliases) ~= 'table' then
                            copy_schema_error('Property "aliases" must be a table')
                        end
                        local aliases = {}
                        for aliasno, alias in ipairs(xaliases) do
                            alias = tostring(alias)
                            if not validname(alias) then
                                copy_schema_error('Bad field alias name: %s', alias)
                            end
                            if fieldmap[alias] then
                                copy_schema_error('Alias name already defined: %s', alias)
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
                    copy_schema_error('Enum "symbols" must be a table')
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
                res.items = copy_schema(xitems, ns, scope, defaults)
                scope[schema] = nil
                return res
            elseif xtype == 'map' then
                res = { type = 'map' }
                scope[schema] = true
                local xvalues = schema.values
                if not xvalues then
                    copy_schema_error('Map type must have "values"')
                end
                res.values = copy_schema(xvalues, ns, scope, defaults)
                scope[schema] = nil
                return res
            elseif xtype == 'fixed' then
                res = { type = 'fixed' }
                local name, ns = checkname(schema, ns, scope)
                scope[name] = res
                res.name = name
                res.aliases = checkaliases(schema, ns, scope)
                local xsize = schema.size
                if not xsize then
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
        local _, node = debug.getlocal(i, 6)
        local _, ptr  = debug.getlocal(i, 7)
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
    local d = {}
    local root = copy_schema(schema, nil, {}, d)
    for i = 1, #d, 3 do
        local context, field, default = d[i], d[i + 1], d[i + 2]
        if context == '' then
            -- already cloned
            field.default = default
        else
            local ok, res = copy_field_default(field.type, default)
            if not ok then
                error(format('%s: Default value not valid (%s)', context, res), 0)
            end
            field.default = res
        end
    end
    return root
end

-- create a mapping from a (string) type tag -> union branch id 
local function create_union_tag_map(union)
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

local ucache = setmetatable({}, { __mode = 'k' })

local function create_union_tag_list(union)
    local res = ucache[union]
    if not res then
        res = {}
        for bi, b in ipairs(union) do
            res[bi] = type_tag(b)
        end
        ucache[union] = res
    end
    return res
end

-- create a mapping from a field name -> field id (incl. aliases) 
local function create_record_field_map(record)
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

local function create_record_field_names_list(record)
    local res = dcache[record.fields]
    if not res then
        res = {}
        for fi, f in ipairs(record.fields) do
            res[fi] = f.name
        end
        dcache[record.fields] = res
    end
    return res
end

-- create a mapping from a symbol name -> symbol id
local function create_enum_symbol_map(enum)
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

local function create_aliases_set(aliases)
    local res = dcache[aliases]
    if not res then
        res = {}
        for i = 1, #aliases do
            res[aliases[i]] = 1
        end
        dcache[aliases] = res
    end
    return res
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
        if data < -0x80000000 or data > 0x7fffffff or floor(tonumber(data)) ~= data then
            error()
        end
        return data
    elseif schematype == 'long' then
        if data > 0x7fffffffffffffffLL or (
                floor(tonumber(data)) ~= data and tonumber(data) == data) then
            error()
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
        if not create_enum_symbol_map(schema)[data] then
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
            local fieldmap = create_record_field_map(schema)
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
                elseif field.default then
                    res[field.name] = deepcopy(field.default)
                else
                    error(format('@Field %s mising', field.name), 0)
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
            local tagmap = create_union_tag_map(schema)
            if data == null then
                if not tagmap['null'] then
                    error('@Unexpected type in union', 0)
                end
                res = null
            else
                local iter = pairs(data)
                local k, v = iter(data)
                local bpos = tagmap[k]
                ptr = k
                if not bpos then
                    error('@Unexpected type in union', 0)
                end
                res[k] = copy_data(schema[bpos], v, visited)
                ptr = iter(data, k)
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
    -- return xpcall(copy_data, copy_data_eh, schema, data, {})
    -- LuaJit issue (?)
    local ok, res = pcall(copy_data, schema, data, {})
    return ok, res
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
        if not from_union then
            from = { from }
        end
        if not to_union then
            to = { to }
        end
        local mm, bc     = {}, {}
        local havecommon = false
        local err
        for fbi, fb in ipairs(from) do
            for tbi, tb in ipairs(to) do
                if type(fb) == 'string' then
                    if fb == 'any' then
                        err = build_ir_error(1, 'NYI: any')
                        break
                    end
                    if     fb == tb then
                        bc[fbi] = primitive_type[fb]
                        mm[fbi] = tbi
                        break
                    elseif promotions[fb] and promotions[fb][tb] then
                        bc[fbi] = promotions[fb][tb]
                        mm[fbi] = tbi
                        break
                    end
                elseif type(tb) ~= 'table' or fb.type ~= tb.type then
                    -- mismatch
                elseif from.name ~= to.name and imatch and (
                       not from.aliases or
                       not create_aliases_set(from.aliases)[to.name]) then
                    -- mismatch
                elseif from.name ~= to.name and not imatch and (
                       not to.aliases or
                       not create_aliases_set(to.aliases)[from.name]) then
                    -- mismatch
                else
                    bc[fbi], err = build_ir(fb, tb, mem, imatch)
                    if not err then
                        mm[fbi] = tbi
                        break
                    end
                end
            end
            havecommon = havecommon or mm[fbi]
        end
        if not havecommon then
            return nil, (err or build_ir_error(nil, 'No common types'))
        end
        return {
            'UNION',
            bc,
            from_union and create_union_tag_list(from) or nil,
            mm,
            to_union and create_union_tag_list(to) or nil
        }
    elseif type(from) == 'string' then
        if from == 'any' then
            return nil, build_ir_error(1, 'NYI: any')
        end
        if from == to then
            return primitive_type[from]
        elseif promotions[from] and promotions[from][to] then
            return promotions[from][to]
        else
            return nil, build_ir_error(1, 'Types incompatible: %s and %s', from,
                                       type(to) == 'string' and to or to.name or to.type)
        end
    elseif type(to) ~= 'table' or from.type ~= to.type then
        return nil, build_ir_error(1, 'Types incompatible: %s and %s',
                                   from.name or from.type,
                                   type(to) == 'string' and to or to.name or to.type)
    elseif from.type == 'array' then
        local bc, err = build_ir(from.items, to.items, mem, imatch)
        if not bc then
            return nil, err
        end
        return { 'ARRAY', bc }
    elseif from.type == 'map'   then
        local bc, err = build_ir(from.values, to.values, mem, imatch)
        if not bc then
            return nil, err
        end
        return { 'MAP', bc }
    elseif from.name ~= to.name and imatch and (
           not from.aliases or
           not create_aliases_set(from.aliases)[to.name]) then
        return nil, build_ir_error(1, 'Types incompatible: %s and %s',
                                   from.name, to.name)
    elseif from.name ~= to.name and not imatch and (
           not to.aliases or
           not create_aliases_set(to.aliases)[from.name]) then
        return nil, build_ir_error(1, 'Types incompatible: %s and %s',
                                   from.name, to.name)
    elseif from.type == 'fixed' then
        if from.size ~= to.size then
            return nil, build_ir_error(nil, 'Size mismatch: %d vs %d',
                                       from.size, to.size)
        end
        return { 'FIXED', from.size }
    else -- record or enum
        -- About mem and IR
        -- (1) *named* schema elements can participate in loops;
        -- (2) mem to the resque! keyed by <source-element, target-element>;
        -- (3) a *named* source schema element can ever be successfully mapped
        --     to a single target schema element;
        -- (4) however, fields in the source schema missing from the target
        --     schema still have their IR built, build_ir() is
        --     called with from == to;
        -- (5) the same IR may be used to generate *both* transformation and
        --     validation programs;
        -- (6) if IR_A was built for <from, from> and later IR_B was built
        --     for <from, to>, every ref to IR_A is to be replaced with IR_B;
        --
        --     [[ This unification helps when chaining IRs: IR1-IR2-IR3.
        --        Assume we started with N types. Without unification every
        --        subsequent step creates up to N new types. ]]
        --
        -- (7) however, build_ir() is ocasionally called with types that fail
        --     to match, often not fatal (think unions);
        -- (8) if it fails, we leave IR_A intact.
        local k, k2 = format('%p.%p', from, to)
        local res, err = mem[k]
        if res then
            return res
        end
        k2 = format('%p.%p', from, from)
        res = mem[k2]
        if not res then
            res = {}
            mem[k2] = res
        end
        mem[k] = res
        if from.type == 'record' then
            local mm, bc = {}, {}
            local hidden
            local defaults
            if imatch then
                local fieldmap = create_record_field_map(from)
                for fi, f in ipairs(to.fields) do
                    local mi = fieldmap[f.name]
                    if mi then
                        mm[mi] = fi
                    end
                end
            else
                local fieldmap = create_record_field_map(to)
                for fi, f in ipairs(from.fields) do
                    mm[fi] = fieldmap[f.name]
                end
            end
            for fi, f in ipairs(from.fields) do
                local mi = mm[fi]
                if mi then
                    local tf = to.fields[mi]
                    ptrfrom = fi
                    ptrto = mi
                    bc[fi], err = build_ir(f.type, tf.type, mem, imatch)
                    if err then
                        goto done
                    end
                    if f.default and not tf.default then
                        -- The spec says nothing about this case.
                        -- Converting defaults into the target schema
                        -- makes sense as well, but the implementation
                        -- was way too complex.
                        -- Caveat: works funny if defaults are different.
                        err = build_ir_error(nil, 'Default value defined in source schema but missing in target schema')
                        goto done
                    end
                else
                    if f.default then
                        -- a subtle difference:
                        -- mm[fi] == nil    a field is missing in target schema
                        -- mm[fi] == false  a field is missing in target schema
                        --                  the source schema defines default
                        -- affect validation
                        mm[fi] = false
                    end
                    bc[fi] = build_ir(f.type, f.type, mem) -- never fails
                end
            end
            for fi, f in ipairs(to.fields) do
                if f.default ~= nil then
                    defaults           = defaults or {}
                    defaults[bnot(fi)] = f.type
                    defaults[fi]       = f.default
                elseif not bc[fi] then
                    ptrfrom = nil
                    ptrto = nil
                    err = build_ir_error(nil, 'Field %s is missing in source schema, and no default value was provided',
                                         f.name)
                    goto done
                end
                if f.hidden then
                    hidden = hidden or {}
                    hidden[fi] = true
                end
            end
            clear(res)
            res[1] = 'RECORD'
            res[2] = bc
            res[3] = create_record_field_names_list(from)
            res[4] = mm
            res[5] = create_record_field_names_list(to)
            res[6] = defaults
            res[7] = hidden
            res[8] = from.name
            res[9] = to.name
        else -- enum
            local symmap     = create_enum_symbol_map(to)
            local mm         = {}
            local havecommon = nil
            for si, s in ipairs(from.symbols) do
                local mi = symmap[s]
                mm[si] = mi
                havecommon = havecommon or mi
            end
            if not havecommon then
                err = build_ir_error(nil, 'No common symbols')
            else
                clear(res)
                res[1] = 'ENUM'
                res[3] = from.symbols
                res[4] = mm
                res[5] = to.symbols
                res[8] = from.name
                res[9] = to.name
            end
        end
::done::
        if err then 
            if #res == 0 then -- there was no IR_A before we came
                mem[k2] = nil
            end
            mem[k] = nil
            return nil, err
        end
        return res
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
        if     not from.type then
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

-- Fix IR inplace, removing duplicate equivalent IR instances,
-- e.g. if there were multiple int arrays mapped to double arrays,
--      we are going to have multiple {'ARRAY', 'INT2DOUBLE'} blocks
local function dedup_ir(ir, cache)
    local k
    if     type(ir) == 'string' or ir[1] == 'ENUM' then
        return ir
    elseif ir[1] == 'RECORD' then
        -- records are already reasonably unique; dedup fields once
        if not cache[ir] then
            cache[ir] = true
            local bc = ir[2]
            for i = 1, #bc do
                bc[i] = dedup_ir(bc[i], cache)
            end
        end
        return ir
    elseif ir[1] == 'FIXED'  then
        k = ir[2]
    elseif ir[1] == 'ARRAY'  then
        ir[2] = dedup_ir(ir[2], cache)
        k = format('A.%p', ir[2])
    elseif ir[1] == 'MAP'    then
        ir[2] = dedup_ir(ir[2], cache)
        k = format('M.%p', ir[2])
    elseif ir[1] == 'UNION'  then
        local bc        = ir[2]
        local mm        = ir[4]
        local fromunion = not not ir[3]
        local tounion   = not not ir[5]
        local kc  = { (fromunion and 'U' or 'u') .. (tounion and 'U' or 'u') }
        for i = 1, #bc do
            local bir = bc[i]
            bc[i] = dedup_ir(bir, cache)
            if type(bir) ~= 'table' or bir[1] ~= 'FIXED' then
                bir = bc[i]
            end
            kc[i + 1] = format('%s%p', mm[i] and '' or '-', bir)
        end
        -- Does the key capture all the relevant information about the UNION?
        -- Well, apparently it does. The following info is captured:
        --  (1) whether the source is a union or not;
        --  (2) whether the dest is a union or not;
        --  (3) source/dest type for every mapped branch;
        --  (4) does the branch produce output or if it is validate only.
        k = concat(kc, '.')
    end
    -- return existing instance if present; update cache 
    local res = cache[k]
    if res then
        return res
    else
        cache[k] = ir
        return ir
    end
end

local function create_ir(from, to, imatch)
    local ir, err = build_ir(from, to, {}, imatch)
    if ir then
        return dedup_ir(ir, {})
    else
        return nil, err
    end
end

return {
    validname             = validname,
    fullname              = fullname,
    validfullname         = validfullname,
    checkname             = checkname,
    checkaliases          = checkaliases,
    copy_field_default    = copy_field_default,

    -- semipublic
    create_schema         = create_schema,
    validate_data         = validate_data,
    create_ir             = create_ir
}
