--
-- This module generates `il` code for a given schema.
--
-- Terminology:
-- * `ipv` register which stores current position in an output
--   buffer.
-- * `ipo` offset from the current position in ibuf to the next
--   piece of data to be transormed.
-- * `code` Lua array, which contains a sequence of opcodes.
-- * mode: c - check, x - transform, n - get next
--  * CHECK - ensure an object at [$ipv + ipo] matches
--    the IR (a shallow check, ex: in an array ignore contents)
--  * TRANSFORM - convert object at [$ipv + ipo] according to the IR,
--    ensure obuf has enough capacity, render results at [$0], and update $0.
--  * GET NEXT - compute position of an object following [$ipv + ipo]
--    and store it back to $ipv.
--
-- For more information read
-- https://github.com/tarantool/avro-schema/wiki/Developer-Guide
--

local json = require('json')
json.cfg{encode_use_tostring = true}

local front      = require('avro_schema.frontend')
local insert     = table.insert
local find, gsub = string.find, string.gsub
local abs        = math.abs

local get_union_tag_map   = front.get_union_tag_map
local get_enum_symbol_map = front.get_enum_symbol_map
local weak_keys           = { __mode = 'k' }

-----------------------------------------------------------------------
-- list utils

-- extend({1, 2, 3}, 4, 5, 6) -> {1, 2, 3, 4, 5, 6}
local function extend(array, ...)
    local base = #array
    for i = 1, select('#',...) do
        array[base + i] = select(i, ...)
    end
end

-- append({1, 2, 3}, {4, 5, 6}) -> {1, 2, 3, 4, 5, 6}
local function append(array1, array2)
    local base = #array1
    for i = 1, #array2 do
        array1[base + i] = array2[i]
    end
end

-----------------------------------------------------------------------
-- schema utils

local function is_record(s)
    return s.type == 'record'
end

local function is_union(s)
    return type(s) == 'table' and s.type == nil
end

local function is_record_or_union(s)
    return type(s) == 'table' and (s.type or 'record') == 'record'
end

-- Those tables contain a size of a field by its type.
-- Negative size means that the field is complex and may contain
-- a data inside of it. However this field requires `|size|`
-- places in a msgpack array.

local default_type_size = {
    -- complex
    array      = -1, enum  = 1,
    fixed      = 1, map    = -1,
    -- scalars
    boolean    = 1, bytes  = 1,
    double     = 1, float  = 1,
    int        = 1, long   = 1,
    null       = 1, string = 1,
    -- not allowed
    any = 0 -- TODO disallow `any` on build ir stage
}

local default_nullable_type_size = {
    -- complex
    array      = -1, enum   = 1,
    fixed      = 1, map    = -1,
    -- scalars
    boolean    = 1, bytes  = 1,
    double     = 1, float  = 1,
    int        = 1, long   = 1,
    null       = 1, string = 1,
    -- not allowed
    any = 0 -- TODO disallow `any` on build ir stage
}

local schema_width
local schema_width_cache = setmetatable({}, weak_keys)
--
-- How many places in an array should be reserved for the structure.
-- In case of positive result, the same obuf size is required.
-- In case of negative result:
-- * abs(size) places in an msgpack array are required
-- * number of required places in obuf depends on the data
--
-- Used only for flatten.
--
schema_width = function(s)
    if type(s) == "string" then
        if default_type_size[s] then
            return default_type_size[s]
        else
            error("Invalid type " .. s)
        end
    end
    assert(type(s) == "table")
    local s_type = s.type
    if default_type_size[s_type] then
        -- default type encapsulated into a table
        if s.nullable then
            return default_nullable_type_size[s_type]
        end
        return default_type_size[s_type]
    end

    local res = schema_width_cache[s]
    if res then return res end
    if s_type == 'record' then
        -- Extra space in obuf for an array in case of
        -- nullable record.
        if s.nullable then return -1 end
        local width, vlo = 0, false
        for _, field in ipairs(s.fields) do
            local field_width = schema_width(field.type)
            width = width + abs(field_width)
            vlo = vlo or field_width < 0
        end
        res = vlo and -width or width
    elseif s_type == nil then -- union
        -- Union takes two places in an array.
        res = 2
        for _, branch in ipairs(s) do
            if is_record(branch) or schema_width(branch) ~= 1 then
                res = -2; break
            end
        end
    else
        error("Invalid type " .. tostring(s_type))
    end

    schema_width_cache[s] = res
    return res
end

--
-- The function calculates how many places in the array internals of the
-- record take.
--
local function record_internal_width(s)
    if s.type ~= "record" then
        return schema_width(s)
    end
    local width = 0
    local vlo = false
    for _, field in ipairs(s.fields) do
        local field_width = schema_width(field.type)
        width = width + abs(field_width)
        vlo = vlo or field_width < 0
    end
    width = vlo and -width or width
    return width
end

-----------------------------------------------------------------------
-- value utils

-- assume value was validated
local function split_union_value(schema, value)
    local m = get_union_tag_map(schema)
    if type(value) == 'table' then
        local k, v = next(value)
        return m[k], v
    else -- null
        return m['null'], value
    end
end

-- convenience table for append_put_value
-- map: type -> opcode
local schema2ilfunc = {
    null = 'putnulc', boolean = 'putboolc', int = 'putintc',
    long = 'putlongc', float = 'putfloatc', double = 'putdoublec',
    bytes = 'putbinc', string = 'putstrc'
}

--
-- Emit code that writes value at $0 (obuf) and updates obuf
-- position.
--
local function append_put_value(il, flat, code, schema, value)
    local n = #code
    code[n + 1] = il.checkobuf(1)
    local ilfunc = schema2ilfunc[schema.nullable and schema.type or schema]
    if value == nil then
        -- Set default value for nullable types.
        assert(schema.nullable or is_union(schema))
        code[n + 2] = il.putnulc(0)
    elseif ilfunc then
        code[n + 2] = il[ilfunc](0, value)
    elseif schema.type == 'fixed' then
        code[n + 2] = il.putstrc(0, value)
    elseif schema.type == 'enum' then
        code[n + 2] = flat and
                      il.putintc(0, get_enum_symbol_map(schema)[value] - 1) or
                      il.putstrc(0, value)
    elseif schema.type == 'array' and next(value) == nil then
        code[n + 2] = il.putarrayc(0, 0)
    elseif schema.type == 'map' and next(value) == nil then
        code[n + 2] = il.putmapc(0, 0)
    else
        code[n + 2] = assert(false, 'NYI: default value too complex')
    end
    code[n + 3] = il.move(0, 0, 1)
end

-----------------------------------------------------------------------
-- ir utils

-- RECORD/UNION ir consists of two objects, ex. (record):
--   RECORD ---(nested)---> __RECORD__
-- Basically, RECORD represents a container (a MsgPack array
-- of a sufficient length), while __RECORD__ represents a content.
local function unwrap_ir(ir)
    local ir_type = ir and ir.type
    if ir_type == 'RECORD' or ir_type == 'UNION' then
        return ir.nested
    else
        return ir
    end
end

-- this method is used when nullable record is stored directly inside of array
-- or union, because without unwrapping record data would be flattened inside of
-- two levels of arrays (one for RECORD and one for nullability), while
-- one level of arrays is enough to restore the data properly.
local function unwrap_nullable_record(ir, is_flatten)
    if ir.type == 'RECORD' then
        if (is_flatten and ir.nested.to.nullable) or
                (not is_flatten and ir.nested.from.nullable) then
            return ir.nested
        end
    end
    return ir
end

-----------------------------------------------------------------------
-- basic codegen

local function append_objforeach(il, code, ipv, ipo)
    local loop_var = il.id()
    local loop_body = { il.objforeach(loop_var, ipv, ipo) }
    extend(code, il.beginvar(loop_var), loop_body, il.endvar(loop_var))
    return loop_var, loop_body
end

-- Convenience table for do_append_code.
local ir2ilfuncs = {
    NUL      = { is = 'isnul',    put = 'putnulc' },
    BOOL     = { is = 'isbool',   put = 'putbool' },
    INT      = { is = 'isint',    put = 'putint' },
    LONG     = { is = 'islong',   put = 'putlong' },
    FLT      = { is = 'isfloat',  put = 'putfloat' },
    DBL      = { is = 'isdouble', put = 'putdouble' },
    BIN      = { is = 'isbin',    put = 'putbin' },
    STR      = { is = 'isstr',    put = 'putstr' },
    INT2LONG = { is = 'isint',    put = 'putint2long' },
    INT2FLT  = { is = 'isint',    put = 'putint2flt' },
    INT2DBL  = { is = 'isint',    put = 'putint2dbl' },
    LONG2FLT = { is = 'islong',   put = 'putlong2flt'},
    LONG2DBL = { is = 'islong',   put = 'putlong2dbl' },
    FLT2DBL  = { is = 'isfloat',  put = 'putflt2dbl' },
    BIN2STR  = { is = 'isbin',    put = 'putbin2str' },
    STR2BIN  = { is = 'isstr',    put = 'putstr2bin' }
}

--
-- This function is helper for nullable type code emitting.
-- It checks if the argument is nullable, then:
-- 1. if nullable: store it to the output buffer
-- 2. if non-nullable: generate code for non-nullable version of the type
--
-- It is implemented by emitting the first part straight in this function
-- and returning non-null branch, so that it can be extended with a basic
-- procedure (for non-null type).
--
local function do_append_nullable_type(il, mode, code, ipv, ipo)
    local null_branch = { il.ibranch(1) }
    local non_null_branch = { il.ibranch(0) }
    insert(code, {
           il.ifnul(ipv, ipo),
           null_branch,
           non_null_branch})
    -- Emit type specific code directly to the non_null_branch.
    code = non_null_branch
    if find(mode, 'x') then
        extend(null_branch,
               il.checkobuf(1),
               il.putnulc(0),
               il.move(0, 0, 1))
    end
    if find(mode, 'n') then
        insert(null_branch, il.move(ipv, ipv, ipo + 1))
    end
    return code
end

-- See new_codegen() below for il:append_code() / __FUNC__ / __CALL__ info.
local function do_append_code(il, mode, code, ir, ipv, ipo, is_flatten)
    if ir.nullable then
        code = do_append_nullable_type(il, mode, code, ipv, ipo)
    end
    local ir_type = ir.type
    if not ir_type then
        -- extract basic type
        ir = ir[1] or ir
        assert(ir)
        local ilfuncs = ir2ilfuncs[ir]
        -- "ANY: not supported" is reported here
        assert(ilfuncs, ir)
        if find(mode, 'c') then insert(code, il[ilfuncs.is] (ipv, ipo)) end
        if find(mode, 'x') then
            if ir ~= 'NUL' then
                extend(code,
                       il.checkobuf(1),
                       il[ilfuncs.put] (0, ipv, ipo),
                       il.move(0, 0, 1))
            else
                extend(code,
                       il.checkobuf(2),
                       il[ilfuncs.put] (0, ipv, ipo),
                       il.move(0, 0, 1))
            end
        end
        if find(mode, 'n') then insert(code, il.move(ipv, ipv, ipo + 1)) end
    elseif ir_type == 'FIXED' then
        if find(mode, 'c') then
            extend(code, il.isbin(ipv, ipo), il.lenis(ipv, ipo, ir.size))
        end
        if find(mode, 'x') then
            extend(code,
                   il.checkobuf(1), il.putbin(0, ipv, ipo), il.move(0, 0, 1))
        end
        if find(mode, 'n') then insert(code, il.move(ipv, ipv, ipo + 1)) end
    elseif ir_type == 'ARRAY' then
        if find(mode, 'c') then insert(code, il.isarray(ipv, ipo)) end
        if find(mode, 'x') then
            extend(code,
                   il.checkobuf(1),
                   il.putarray(0, ipv, ipo),
                   il.move(0, 0, 1))
            local loop_var, loop_body = append_objforeach(il, code,
                ipv, ipo)
            il:append_code('cxn', loop_body,
                unwrap_nullable_record(ir.nested, is_flatten), loop_var, 0)
        end
        if find(mode, 'n') then
            insert(code, il.skip(ipv, ipv, ipo))
        end
    elseif ir_type == 'MAP' then
        if find(mode, 'c') then insert(code, il.ismap(ipv, ipo)) end
        if find(mode, 'x') then
            extend(code, il.checkobuf(1),
                   il.putmap(0, ipv, ipo), il.move(0, 0, 1))
            local loop_var, loop_body = append_objforeach(il, code, ipv, ipo)
            extend(loop_body, il.isstr(loop_var, 0), il.checkobuf(1),
                   il.putstr(0, loop_var, 0), il.move(0, 0, 1))
            il:append_code('cxn', loop_body, ir.nested, loop_var, 1)
        end
        if find(mode, 'n') then insert(code, il.skip(ipv, ipv, ipo)) end
    elseif ir_type == '__CALL__' then
        insert(code, il.callfunc(find(mode, 'n') and ipv, ipv, ipo,
               ir.func[1].name))
    elseif ir_type == '__FUNC__' then
        return il:append_code(mode, code, ir.nested, ipv, ipo)
    else
        assert(false, 'Unhandled type: '..ir_type)
    end
end

-- To convert IR object into code, one calls il:append_code().
-- This is a recursive process; i.e. a nested call to
-- append_code is made for each IR object's child.
--
-- Technically, il:append_code() is a *chain* of appenders.
-- If an appender doesn't handle an IR object, it passes it further
-- down the chain. Do_append_code() /above/ is meant to be the
-- last appender in a chain.
--
-- The first appender, provided by new_codegen() itself,
-- decides whether to emit a code inline or to make it
-- into a separate function. In the later case, synthesized
-- __FUNC__ / __CALL__ IR objects are passed down the chain
-- to allow for customization.
local function new_codegen(il, funcs, next_appender, root_ir, root_func, is_flatten)
    local open_records = {}
    local cache = { [root_ir] = {
        type = '__CALL__', func = root_func, nested = root_ir }
    }
    local appender
    appender = function(il, mode, code, ir, ipv, ipo)
        if ir.type == '__RECORD__' and find(mode, 'x') then
            local call = cache[ir]
            if not call and (open_records[ir] or #ir.from.fields > 15) then
                -- make a func: recursion detected / very complex record
                local func_id = il.id()
                local func = { il.declfunc(func_id, 1) }
                insert(funcs, func)
                call = { type = '__CALL__', func = func, nested = ir }
                cache[ir] = call
                local prev_open_records = open_records
                open_records = {}
                next_appender(il, 'cxn', func,
                              { type = '__FUNC__', nested = ir }, 1, 0)
                open_records = prev_open_records
            end
            if call and call.func ~= code then
                -- the function to be called isn't the one generated right now
                if call.func[1].name < 3 then
                    -- change name to indicate that a function is not
                    -- only an entry point, but also called by other funcs
                    call.func[1].name = il.id()
                end
                return next_appender(il, mode, code, call, ipv, ipo, is_flatten)
            end
            open_records[ir] = true
            next_appender(il, mode, code, ir, ipv, ipo, is_flatten)
            open_records[ir] = nil
        else
            return next_appender(il, mode, code, ir, ipv, ipo, is_flatten)
        end
    end
    return setmetatable({ append_code = appender }, { __index = il })
end

-----------------------------------------------------------------------
--                             FLATTEN                               --

-- prepare a mapping table for PUTENUMS2I (string->integer)
local enums2i_tab_cache = setmetatable({}, weak_keys)
local function make_enums2i_tab(ir)
    local tab = enums2i_tab_cache[ir]
    if tab then return tab end
    tab = {}
    local isymbols, i2o = ir.from.symbols, ir.i2o
    for i, symbol in ipairs(isymbols) do
        tab[symbol] = (i2o[i] or 0) - 1
    end
    enums2i_tab_cache[ir] = tab
    return tab
end

-- emit code that fills-in field defaults starting at $0 and
-- updates $0 accordingly
local append_put_field_values
append_put_field_values = function(il, flat, code, field_type, field_val)
    if is_record(field_type) and field_val ~= nil then
        if field_type.nullable and flat then
            -- Warning! Number of places in obuf should be calculated
            -- recursively for this type.
            table.insert(code, il.checkobuf(1))
            -- TODO: record_internal_width is designed for
            -- `flatten` and in case it has nested record or
            -- union would return wrong value for `unflatten`.
            table.insert(code, il.putarrayc(0,
                abs(record_internal_width(field_type))))
            table.insert(code, il.move(0, 0, 1))
        end
        if not flat then
            table.insert(code, il.checkobuf(1))
            table.insert(code, il.putmapc(0, #field_type.fields))
            table.insert(code, il.move(0, 0, 1))
        end
        for _, field in ipairs(field_type.fields) do
            if not flat then
                table.insert(code, il.checkobuf(1))
                table.insert(code, il.putstrc(0, field.name))
                table.insert(code, il.move(0, 0, 1))
            end
            append_put_field_values(il, flat, code,
                field.type, field_val[field.name])
        end
    elseif is_union(field_type) and field_val ~= nil then
        local branch_no, branch_v = split_union_value(field_type, field_val)
        if flat then
            extend(code, il.checkobuf(1), il.putintc(0, branch_no - 1),
                il.move(0, 0, 1))
        else
            extend(code, il.checkobuf(2), il.putmapc(0, 1),
                il.putstrc(1, next(field_val)), il.move(0, 0, 2))
        end
        append_put_value(il, flat, code, field_type[branch_no], branch_v)
    else
        append_put_value(il, flat, code, field_type, field_val)
    end
end

-- x mode for records (flatten)
local function do_append_convert_record_flatten(il, code, ir, ipv, ipo)
    local i2o, o2i = ir.i2o, ir.o2i
    local from_fields, to_fields = ir.from.fields, ir.to.fields
    -- reserve a range of ids for all field variables at once
    local v_base = il.id(#from_fields) - 1
    -- we append instructions before and after the loop when convenient;
    -- for these reasons we initially accumulate the loop and subsequent
    -- instructions in code_section2, and append it to code once done
    local code_section2 = {}
    -- emit parsing loop
    local loop_var, loop_body = append_objforeach(il, code_section2, ipv, ipo)
    local strswitch = { il.strswitch(loop_var, 0) }
    insert(loop_body, strswitch)
    -- create a branch in strswitch for each field (source schema)
    for i, field in ipairs(from_fields) do
        -- declare field var before the loop
        insert(code, il.beginvar(v_base + i))
        local branch = {
            il.sbranch(field.name),
            il.isnotset(v_base + i),
            il.move(v_base + i, loop_var, 1)
        }
        strswitch[i + 1] = branch
        -- Check that fields which are only in input schema were in passed
        -- data. It should be performed here, because only fields from output
        -- schema are checked in the loop below.
        if not i2o[i] then -- missing from target schema
            il:append_code('cn', branch, ir[i], loop_var, 1)
            if type(field.default) == 'nil' and not field.nullable then
                -- mandatory field, add a check
                insert(code_section2,
                       il.isset(v_base + i, ipv, ipo, field.name))
            end
            insert(code_section2, il.endvar(v_base + i))
        end
    end
    -- iterate fields in target schema, track current offset into a tuple
    local offset = 0
    for o, field in ipairs(to_fields) do
        local i = o2i[o]
        local next_offset
        -- compute next_offset
        if offset then
            local width = schema_width(field.type)
            if i and width < 0 then -- variable length, activate append mode
                insert(code_section2, il.move(0, 0, offset))
            else
                next_offset = offset + abs(width)
            end
        end
        local field_ir = unwrap_ir(ir[i])
        local field_default = field.default
        if next_offset then -- at fixed offset, patch
            -- Process all fields of fixed size until the first field of
            -- variable size.
            -- Those fields have a known offset and can be transformed
            -- (written to obuf) right inside of the field-loop.

            if i then
                local branch = strswitch[i + 1]
                insert(branch, il.move(0, 0, offset))
                il:append_code('cxn', branch, field_ir, loop_var, 1)
                insert(branch, il.move(0, 0, -next_offset))
            end

            if field.type.nullable or type(field_default) ~= 'nil' then
                insert(code, il.move(0, 0, offset))
                append_put_field_values(il, true, code, field.type, field.default)
                -- Reverse append_put_field_values side-effect.
                insert(code, il.move(0, 0, -next_offset))
            else
                insert(code_section2,
                        il.isset(v_base + i, ipv, ipo, from_fields[i].name))
            end
        elseif i then -- and not next_offset: v/offset or v/length, append
            -- Process fields goes after the first occurrence of a
            -- field with a variable size.
            -- Those fields are only found inside of the field-loop,
            -- the transform is performed after the field-loop (when
            -- its position in the obuf is known).

            local branch = strswitch[i + 1]
            il:append_code('cn', branch, field_ir, loop_var, 1)
            if field.type.nullable or type(field_default) ~= 'nil' then
                local t_branch = { il.ibranch(1) }
                il:append_code('x', t_branch, field_ir, v_base + i, 0)
                local f_branch = { il.ibranch(0) }
                append_put_field_values(il, true, f_branch, field.type, field.default)
                insert(code_section2,
                       { il.ifset(v_base + i), f_branch, t_branch })
            else
                -- check if the field was found in the input
                insert(code_section2,
                       il.isset(v_base + i, ipv, ipo, from_fields[i].name))
                -- process the field
                il:append_code('x', code_section2, field_ir, v_base + i, 0)
            end
        else -- not next_offset and not i: v/offset (defaults only), append
            append_put_field_values(il, true, code_section2,
                                    field.type, field.default)
        end
        offset = next_offset

        -- kill variable (if any)
        if i then insert(code_section2, il.endvar(v_base + i)) end
    end
    -- sync offset (unless already synced)
    if offset then insert(code_section2, il.move(0, 0, offset)) end
    append(code, code_section2)
end

-- all modes for a union (flatten)
-- xgap is a secret knock to create a gap between a
-- UNION discriminator and a branch (for XUPDATE)
local function do_append_union_flatten(il, mode, code, ir,
                                       ipv, ipo, xgap)
    xgap = xgap or 1
    local i2o, from = ir.i2o, ir.from
    if not is_union(from) then -- non-union mapped to a union
        if find(mode, 'x') then
            extend(code, il.checkobuf(xgap),
                   il.putintc(0, i2o[1] - 1), il.move(0, 0, xgap))
        end
        return il:append_code(mode, code, ir[1], ipv, ipo)
    end
    -- a union mapped to either a union or a non-union
    local accepts_null = get_union_tag_map(from) ['null']
    local num_branches = #from
    local check, skip
    if accepts_null then
        check, skip = (num_branches > 1 and 'isnulormap' or 'isnul'), 'pskip'
    else
        check, skip = 'ismap', 'skip'
    end
    if find(mode, 'c') then insert(code, il[check](ipv, ipo)) end
    if find(mode, 'x') then
        local to_union = is_union(ir.to)
        local strswitch, null_branch = { il.strswitch(ipv, ipo + 1) }
        -- emit control structure depending on whether null is accepted and
        -- the number of branches
        if not accepts_null then
            extend(code,
                   il.lenis(ipv, ipo, 1), il.isstr(ipv, ipo + 1), strswitch)
        elseif num_branches > 1 then
            null_branch = { il.ibranch(1) }
            insert(code, {
                   il.ifnul(ipv, ipo), null_branch,
                   {
                       il.ibranch(0),
                       il.lenis(ipv, ipo, 1), il.isstr(ipv, ipo + 1), strswitch
                   }})
        else
            null_branch = code
        end
        -- emit code for each union branch
        for i, branch_schema in ipairs(from) do
            local o = i2o[i]
            local dest, x_or_cx, val_ipo, err_ipo = null_branch, 'x', ipo, ipo
            if branch_schema ~= 'null' then
                dest = { il.sbranch(branch_schema.name or branch_schema.type or
                                    branch_schema) }
                insert(strswitch, dest)
                x_or_cx = 'cx'
                err_ipo = ipo + 1 -- associate error message with a key
                val_ipo = ipo + 2 -- value embedded in map
            end
            if o then
                if to_union then
                    extend(dest, il.checkobuf(xgap),
                           il.putintc(0, o - 1), il.move(0, 0, xgap))
                    il:append_code(x_or_cx, dest,
                        unwrap_nullable_record(ir[i], true), ipv, val_ipo)
                else -- target is not a union (maybe a record, hence unwrap)
                    il:append_code(x_or_cx, dest, unwrap_ir(ir[i]),
                                   ipv, val_ipo)
                end
            else -- branch doesn't exist in target schema
                insert(dest, il.errvaluev(ipv, err_ipo))
            end
        end
    end
    if find(mode, 'n') then insert(code, il[skip](ipv, ipv, ipo)) end
end

-- main flatten codegen func
-- xgap is a secret knock to create a gap between a
-- UNION discriminator and a branch (for XUPDATE)
local function do_append_flatten(il, mode, code, ir, ipv, ipo, xgap)
    local  ir_type = ir.type
    if     ir_type == 'ENUM' then
        if ir.to.nullable then
            code = do_append_nullable_type(il, mode, code, ipv, ipo)
        end
        if find(mode, 'c') then insert(code, il.isstr(ipv, ipo)) end
        if find(mode, 'x') then
            extend(code,
                   il.checkobuf(1),
                   il.putenums2i(0, ipv, ipo, make_enums2i_tab(ir)),
                   il.move(0, 0, 1))
        end
        if find(mode, 'n') then insert(code, il.move(ipv, ipv, ipo + 1)) end
    elseif ir_type == 'RECORD' or ir_type == 'UNION' then
        local to = ir.nested.to
        if find(mode, 'x') and is_record_or_union(to) then
            extend(code,
                   il.checkobuf(1),
                   il.putarrayc(0, abs(schema_width(to))),
                   il.move(0, 0, 1))
        end
        il:append_code(mode, code, ir.nested, ipv, ipo)
    elseif ir_type == '__RECORD__' then
        if ir.to.nullable then
            -- If null is passed, then just a null is encoded.
            code = do_append_nullable_type(il, mode, code, ipv, ipo)
        end
        if find(mode, 'c') then insert(code, il.ismap(ipv, ipo)) end
        if find(mode, 'x') then
            -- If nullable record is passed to the input the record would be
            -- encoded as a subarray.
            if ir.to.nullable then
                local record_width = abs(record_internal_width(ir.to))
                extend(code,
                    il.checkobuf(1),
                    il.putarrayc(0, record_width),
                    il.move(0, 0, 1))

            end
            do_append_convert_record_flatten(il, code, ir, ipv, ipo)
        end
        if find(mode, 'n') then
            insert(code, il.pskip(ipv, ipv, ipo))
        end
    elseif ir_type == '__UNION__' then
        return do_append_union_flatten(il, mode, code, ir,
                                       ipv, ipo, xgap)
    elseif ir_type == 'ARRAY' or ir_type == 'MAP' then
        return do_append_code(il, mode, code, ir, ipv, ipo, true)
    else -- defer to basic codegen
        return do_append_code(il, mode, code, ir, ipv, ipo, true)
    end
end

-----------------------------------------------------------------------
--                            UNFLATTEN                              --

-- prepare a mapping table for PUTENUMI2S (integer->string)
local enumi2s_tab_cache = setmetatable({}, weak_keys)
local function make_enumi2s_tab(ir)
    local tab = enumi2s_tab_cache[ir]
    if tab then return tab end
    tab = {}
    local n, i2o, symbols = #ir.from.symbols, ir.i2o, ir.to.symbols
    for i = 1, n do
        local o = i2o[i]
        tab[i] = o and symbols[o] or ''
    end
    enumi2s_tab_cache[ir] = tab
    return tab
end

local function do_append_record_unflatten(il, mode, code, ir, ipv, ipo)
    assert(find(mode, 'n'))
    local to, i2o, o2i = ir.to, ir.i2o, ir.o2i
    local to_fields = to.fields
    local x, putmapc = find(mode, 'x')
    if x then
        putmapc = il.putmapc(0, 0)
        extend(code, il.checkobuf(1), putmapc, il.move(0, 0, 1))
    end
    insert(code, il.move(ipv, ipv, ipo))
    for i, field_ir in ipairs(ir) do
        local o = i2o[i]
        local field = to_fields[o]
        if x and o and not field.hidden then
            putmapc.ci = putmapc.ci + 1
            extend(code, il.checkobuf(1),
                    il.putstrc(0, field.name), il.move(0, 0, 1))
            il:append_code('cxn', code, unwrap_ir(field_ir), ipv, 0)
        else
            il:append_code('cn', code, unwrap_ir(field_ir), ipv, 0)
        end
    end
    for o, field in ipairs(to_fields) do
        if x and not field.hidden and not o2i[o] then
            putmapc.ci = putmapc.ci + 1
            extend(code, il.checkobuf(1),
                    il.putstrc(0, field.name), il.move(0, 0, 1))
            append_put_field_values(il, false, code, field.type, field.default)
        end
    end
end

local function do_append_union_unflatten(il, mode, code, ir, ipv, ipo)
    local x = find(mode, 'x')
    assert(x or find(mode, 'n'))
    local i2o, from, to = ir.i2o, ir.from, ir.to
    local to_union = is_union(to)
    if not is_union(from) then -- non-union mapped to a union
        local target = to[i2o[1]]
        if find(mode, 'x') and target ~= 'null' then
            extend(code, il.checkobuf(2), il.putmapc(0, 1),
                    il.putstrc(1, target.name or target.type or target),
                    il.move(0, 0, 2))
        end
        return il:append_code(mode, code, unwrap_ir(ir[1]), ipv, ipo)
    end
    if find(mode, 'c') then insert(code, il.isint(ipv, ipo)) end
    if x and to_union then -- extract common code
        extend(code, il.checkobuf(2), il.putmapc(0, 1))
    end
    local intswitch = { il.intswitch(ipv, ipo) }
    insert(code, intswitch)
    for i = 1,#from do
        local code_branch = { il.ibranch(i - 1) }
        insert(intswitch, code_branch)
        local o = i2o[i]
        if not o then
            code_branch[2] = il.errvaluev(ipv, ipo)
        else
            if x and to_union then
                local schema = to[o]
                if schema ~= 'null' then
                    extend(code_branch,
                           il.putstrc(1, schema.name or schema.type or schema),
                           il.move(0, 0, 2))
                end
            end
            il:append_code(mode, code_branch,
                unwrap_nullable_record(ir[i], false), ipv, ipo + 1)
        end
    end
end

-- main unflatten codegen func
local function do_append_unflatten(il, mode, code, ir, ipv, ipo)
    local  ir_type = ir.type
    if     ir_type == 'ENUM' then
        if ir.from.nullable then
            code = do_append_nullable_type(il, mode, code, ipv, ipo)
        end
        if find(mode, 'c') then insert(code, il.isint(ipv, ipo)) end
        if find(mode, 'x') then
            extend(code,
                   il.checkobuf(1),
                   il.putenumi2s(0, ipv, ipo, make_enumi2s_tab(ir)),
                   il.move(0, 0, 1))
        end
        if find(mode, 'n') then
            local offset = 1
            if ir.from.nullable then
                offset = 2
            end
            insert(code, il.move(ipv, ipv, ipo + offset))
        end
    elseif ir_type == 'RECORD' or ir_type == 'UNION' then
        local from = ir.nested.from
        if is_record_or_union(from) then
            if find(mode, 'c') then
                extend(code,
                       il.isarray(ipv, ipo),
                       il.lenis(ipv, ipo, abs(schema_width(from))))
            end
            ipo = ipo + 1
        end
        il:append_code(mode, code, ir.nested, ipv, ipo)
    elseif ir_type == '__RECORD__' then
        if ir.from.nullable then
            code = do_append_nullable_type(il, mode, code, ipv, ipo)
            if find(mode, 'c') then
                extend(code, il.isarray(ipv, ipo))
            end
            extend(code, il.move(ipv, ipv, 1))
        end
        return do_append_record_unflatten(il, mode, code, ir, ipv, ipo)
    elseif ir_type == '__UNION__' then
        return do_append_union_unflatten(il, mode, code, ir, ipv, ipo)
    elseif ir_type == 'ARRAY' or ir_type == 'MAP' then
        do_append_code(il, mode, code, ir, ipv, ipo)
    else -- defer to basic codegen
        return do_append_code(il, mode, code, ir, ipv, ipo)
    end
end

-----------------------------------------------------------------------

local sf2ilfuncs = {
    boolean = { is = 'isbool',   put = 'putboolc',   v = false },
    int =     { is = 'isint',    put = 'putintc',    v = 0 },
    long =    { is = 'islong',   put = 'putlongc',   v = 0 },
    float =   { is = 'isfloat',  put = 'putfloatc',  v = 0 },
    double =  { is = 'isdouble', put = 'putdoublec', v = 0 },
    string =  { is = 'isstr',    put = 'putstrc',    v = '' },
    bytes =   { is = 'isbin',    put = 'putbinc',    v = '' }
}

local function emit_code(il, ir, service_fields, alpha_nullable_record_xflatten)
    ir = unwrap_ir(ir)
    local from, to = ir.from, ir.to
    local funcs = {
        { il.declfunc(1, 1) },
        { il.declfunc(2, 1) },
        { il.declfunc(3, 1) }
    }

    local f_codegen = new_codegen(il, funcs, do_append_flatten,   ir, funcs[1], true)
    local u_codegen = new_codegen(il, funcs, do_append_unflatten, ir, funcs[2])

    f_codegen:append_code('cxn', funcs[1], ir, 1, 0)
    u_codegen:append_code('cxn', funcs[2], ir, 1, 0)

    local update_cell = 0

    local function do_append_convert_record_xflatten(il, code, ir, ipv, ipo)
        local i2o, o2i = ir.i2o, ir.o2i
        local from_fields, to_fields = ir.from.fields, ir.to.fields
        -- reserve a range of ids for all field variables at once
        local v_base = il.id(#from_fields) - 1
        -- we append instructions before and after the loop when convenient;
        -- for these reasons we initially accumulate the loop and subsequent
        -- instructions in code_section2, and append it to code once done
        local code_section2 = {}
        -- emit parsing loop
        local loop_var, loop_body = append_objforeach(il, code_section2, ipv, ipo)
        local strswitch = { il.strswitch(loop_var, 0) }
        insert(loop_body, strswitch)
        -- create a branch in strswitch for each field (source schema)
        for i, field in ipairs(from_fields) do
            -- declare field var; before the loop
            insert(code, il.beginvar(v_base + i))
            local branch = {
                il.sbranch(field.name),
                il.isnotset(v_base + i),
                il.move(v_base + i, loop_var, 1)
            }
            strswitch[i + 1] = branch
            if not i2o[i] then -- missing from target schema
                f_codegen:append_code('cn', branch, ir[i], loop_var, 1)
            end
            insert(code_section2, il.endvar(v_base + i))
        end
        -- iterate fields in target schema order
        for o, field in ipairs(to_fields) do
            local i = o2i[o]
            local field_ir = unwrap_ir(ir[i])
            if i then
                local branch = strswitch[i + 1]
                il:append_code('cxn', branch, field_ir, loop_var, 1)
            else
                update_cell = update_cell + schema_width(field.type)
            end
        end
        append(code, code_section2)
    end

    local function do_append_xflatten(il, mode, code, ir, ipv, ipo)
        assert(find(mode, 'cx'), mode)
        local ir_type = ir.type
        if ir_type == '__FUNC__' then
            local prev_update_cell = update_cell
            update_cell = 0
            il:append_code(mode, code, ir.nested, ipv, ipo)
            update_cell = prev_update_cell
        elseif ir_type == '__CALL__' then
            insert(code, il.callfunc(find(mode, 'n') and ipv, ipv, ipo,
                   ir.func[1].name, update_cell))
            update_cell = update_cell + schema_width(ir.nested.to)
        elseif ir_type == '__RECORD__' then
            insert(code, il.ismap(ipv, ipo))
            if ir.to.nullable then
                -- would be deleted after #85
                if not alpha_nullable_record_xflatten then
                    local err_msg = "xflatten for nullable record is on developement stage. Use alpha_nullable_record_xflatten option if you understand what you do."
                    insert(code, il.error(err_msg))
                    update_cell = update_cell + 1
                    return
                end
                extend(code,
                        il.checkobuf(3),
                        il.putarrayc(0, 3),
                        il.putstrc(1, '='),
                        il.putintkc(2, update_cell),
                        il.move(0, 0, 3))
                f_codegen:append_code('cxn', code, ir, ipv, ipo)
                update_cell = update_cell + 1
                return
            else
                do_append_convert_record_xflatten(il, code, ir, ipv, ipo,
                    f_codegen)
                if find(mode, 'n') then
                    insert(code, il.skip(ipv, ipv, ipo))
                end
            end
        elseif ir_type == '__UNION__' and is_record_or_union(ir.to) then
            if is_union(ir.to) then
                extend(code, il.checkobuf(6),
                       il.putarrayc(0, 3),
                       il.putstrc(1, '='), il.putintkc(2, update_cell),
                       il.putarrayc(4, 3),
                       il.putstrc(5, '='), il.putintkc(6, update_cell + 1),
                       il.move(0, 0, 3))
                update_cell = update_cell + 2
                il = f_codegen
            end
            return do_append_flatten(il, mode, code, ir, ipv, ipo, 4)
        else
            extend(code, il.checkobuf(3), il.putarrayc(0, 3),
                   il.putstrc(1, '='), il.putintkc(2, update_cell),
                   il.move(0, 0, 3))
            update_cell = update_cell + 1
            return do_append_flatten(f_codegen, mode, code, ir, ipv, ipo)
        end
    end

    local x_codegen = new_codegen(il, funcs, do_append_xflatten, ir, funcs[3])
    x_codegen:append_code('cxn', funcs[3], ir, 1, 0)

    -- augment code (see comments)

    -- flatten: output array header + fill in defaults
    local flatten, _flatten, pos = {
        il.declfunc(1, 1), il.checkobuf(1 + 2*#service_fields),
        il.putarrayc(0, #service_fields + (to and abs(schema_width(to)) or 1))
    }, funcs[1], 1
    for i, ft in ipairs(service_fields) do
        local m = sf2ilfuncs[ft]
        insert(flatten, il[m.put](pos, m.v)); pos = pos + 1
        if m.v == '' then
            insert(flatten, il.putdummyc(pos)); pos = pos + 1
        end
    end
    insert(flatten, il.move(0, 0, pos))

    funcs[1] = flatten
    if _flatten[1].name == 1 then -- not called recursively?
        _flatten[1] = il.move(0, 0, 0) -- kill function header
        append(flatten, _flatten)
    else
        insert(flatten, il.callfunc(1, 1, 0, _flatten[1].name))
        insert(funcs, _flatten)
    end

    -- unflatten: check array header + validate defaults
    local uflatten, _uflatten = {
        il.declfunc(2, 1), il.isarray(1, 0),
        il.lenis(1, 0, #service_fields + (from and abs(schema_width(from)) or 1)),
    }, funcs[2]
    for i, ft in ipairs(service_fields) do
        insert(uflatten, il[sf2ilfuncs[ft].is](1, i))
    end
    insert(uflatten, il.move(1, 1, 1 + #service_fields))

    funcs[2] = uflatten
    if _uflatten[1].name == 2 then -- not called recursively?
        _uflatten[1] = il.move(0, 0, 0) -- kill function header
        append(uflatten, _uflatten)
    else
        insert(uflatten, il.callfunc(1, 1, 0, _uflatten[1].name))
        insert(funcs, _uflatten)
    end

    -- xflatten: skip output cell #0 (array header)
    insert(funcs[3], 2, il.move(0, 0, 1))

    return funcs,
           from and abs(schema_width(from)) or 1,
           to and abs(schema_width(to)) or 1
end

-----------------------------------------------------------------------
return {
    emit_code = emit_code
}
