local insert = table.insert
local bnot = bit.bnot

-- Count the particular IR block uses.
-- We need it to decide whether to inline the particular block. 
-- A block could be used to either generate a conversion code (counters_c)
-- or to generate validation code (counters_v).
-- The later is done when a field is missing in target schema.
local function count_refs(ir, counters_c, counters_v)
    if type(ir) == 'string' then
        return
    end
    local count = counters_c[ir]
    if count then
        counters_c[ir] = count + 1
        return
    end
    counters_c[ir] = 1
    if      ir[1] == 'ARRAY' or ir[1] == 'MAP' then
        count_refs(ir[2], counters_c, counters_v)
    elseif ir[1] == 'UNION' or ir[1] == 'RECORD' then
        local bc = ir[2]
        local mm = ir[4]
        for i = 1, #bc do
            if mm[i] then
                count_refs(bc[i], counters_c, counters_v)
            elseif ir[1] ~= 'UNION' then
                -- in union it is a runtime error
                count_refs(bc[i], counters_v, counters_v)
            end
        end
    end
end

-----------------------------------------------------------------------
local function ir_type(ir)
    return type(ir) == 'table' and ir[1] or ir
end

local function ir_record_bc(ir)
    assert(ir_type(ir) == 'RECORD')
    return ir[2]
end

local function ir_record_inames(ir)
    assert(ir_type(ir) == 'RECORD')
    return ir[3]
end

local function ir_record_i2o(ir)
    assert(ir_type(ir) == 'RECORD')
    return ir[4]
end

local function ir_record_onames(ir)
    assert(ir_type(ir) == 'RECORD')
    return ir[5]
end

-- ir_record_odefault(ir, o) -> schema?, val?
local function ir_record_odefault(ir, o)
    assert(ir_type(ir) == 'RECORD')
    local d = ir[6]
    if d then
        return d[bnot(o)], d[o]
    end
end

local function ir_record_ohidden(ir, o)
    assert(ir_type(ir) == 'RECORD')
    return ir[7] and ir[7][o]
end

local function ir_record_ioptional(ir, i)
    assert(ir_type(ir) == 'RECORD')
    return ir[4][i] == false
end

local ir_record_o2i_cache = setmetatable({}, { __mode = 'k' })

local function ir_record_o2i(ir)
    assert(ir_type(ir) == 'RECORD')
    local o2i = ir_record_o2i_cache[ir]
    if o2i then
        return o2i
    end
    o2i = {}
    local inames, i2o = ir[3], ir[4] 
    for i = 1, #inames do
        local o = i2o[i]
        if o then
            o2i[o] = i
        end
    end
    ir_record_o2i_cache[ir] = o2i
    return o2i
end

local function ir_fixed_size(ir)
    assert(ir_type(ir) == 'FIXED')
    return ir[2]
end

-----------------------------------------------------------------------
local schema2lirfunc = {
    null = 'putnul', boolean = 'putboolc', int = 'putintc',
    long = 'putlongc', float = 'putfloatc', double = 'putdoublec',
    bytes = 'putbinc', string = 'putstrc'
}

local function prepare_default(lir, schema, val)
    local lirfunc = schema2lirfunc[schema]
    if lirfunc then
        return lir[lirfunc], val
    else
        assert(false, 'NYI: complex default')
    end
end

local function prepare_flat_default(lir, schema, val)
    return prepare_default(lir, schema, val) -- XXX
end

local prepare_flat_defaults_vec_helper
prepare_flat_defaults_vec_helper = function(lir, schema, val, res, curcell)
    if     type(schema) == 'table' and schema.type == 'record' then
        local fields = schema.fields
        for i = 1, #fields do
            local field = fields[i]
            curcell = prepare_flat_defaults_vec_helper(
                lir, field.type, val[field.name], res, curcell)
        end
        return curcell
    elseif type(schema) == 'table' and not schema.type then
        assert(false, 'NYI: union')
    else
        res[curcell * 2 - 1], res[curcell * 2] = prepare_flat_default(
            lir, schema, val)
        return curcell + 1
    end
end
local function prepare_flat_defaults_vec(lir, schema, val)
    res = {}
    return prepare_flat_defaults_vec_helper(lir, schema, val, res, 1) - 1, res
end

-----------------------------------------------------------------------
local ir2lirfuncs = {
    NUL      = { 'isnul',    'putnul' },
    BOOL     = { 'isbool',   'putbool' },
    INT      = { 'isint',    'putint' },
    LONG     = { 'islong',   'putlong' },
    FLT      = { 'isfloat',  'putfloat' },
    DBL      = { 'isdouble', 'putdouble' },
    BIN      = { 'isbin',    'putbin' },
    STR      = { 'isstr',    'putstr' },
    INT2LONG = { 'isint',    'putint2long' },
    INT2FLT  = { 'isint',    'putint2flt' },
    INT2DBL  = { 'isint',    'putint2dbl' },
    LONG2FLT = { 'islong',   'putlong2flt'},
    LONG2DBL = { 'islong',   'putlong2dbl' },
    FLT2DBL  = { 'isfloat',  'putflt2dbl' },
    BIN2STR  = { 'isbin',    'putbin2str' },
    STR2BIN  = { 'isstr',    'putstr2bin' }
}

local emit_patch
emit_patch = function(lir, ir, ripv, ipv, ipo, opo)
    local irt = ir_type(ir)
    local lirfuncs = ir2lirfuncs[irt]
    if lirfuncs then
        local isfunc, putfunc = unpack(lirfuncs)
        return {
            lir[isfunc]  (ipv, ipo),
            lir[putfunc] (opo, ipv, ipo),
            lir.move(ripv, ipv, ipo + 1)
        }
    elseif irt == 'FIXED' then
        return {
            lir.isbin(ipv, ipo),
            lir.lenis(ipv, ipo, ir_fixed_size(ir)),
            lir.putbin(opo, ipv, ipo),
            lir.move(ripv, ipv, ipo + 1)
        }
    elseif irt == 'ENUM' then
        assert(false, 'NYI: enum')
    else
        assert(false, 'VLO') -- VLO, can't patch
    end
end

local emit_check
emit_check = function(lir, ir, ripv, ipv, ipo)
    local irt = ir_type(ir)
    local lirfuncs = ir2lirfuncs[irt]
    if lirfuncs then
        local isfunc = unpack(lirfuncs)
        return {
            lir[isfunc]  (ipv, ipo),
            lir.move(ripv, ipv, ipo + 1)
        }
    elseif irt == 'FIXED' then
        return {
            lir.isbin(ipv, ipo),
            lir.lenis(ipv, ipo, ir_fixed_size(ir)),
            lir.move(ripv, ipv, ipo + 1)
        }
    elseif irt == 'ARRAY' then
        return {
            lir.isarray(ipv, ipo),
            lir.skip(ripv, ipv, ipo)
        }
    elseif irt == 'MAP' then
        return {
            lir.ismap(ipv, ipo),
            lir.skip(ripv, ipv, ipo)
        }
    elseif irt == 'UNION' then
        assert(false, 'NYI: union')
    elseif irt == 'RECORD' then
        assert(false, 'NYI: record')
    elseif irt == 'ENUM' then
        assert(false, 'NYI: enum')
    else
        assert(false)
    end
end

local emit_validate
emit_validate = function(lir, ir, ripv, ipv, ipo)
    return emit_check(lir, ir, ripv, ipv, ipo) -- XXX
end

local function objforeach(lir, ripv, ipv, ipo, handler)
    if ripv == ipv then
        local ptr = lir.new_var()
        return {
            lir.beginvar(ptr),
            lir.move(ptr, ipv, ipo),
            {
                lir.objforeach(ripv, ptr, 0),
                handler(ripv)
            },
            lir.endvar(ptr)
        }
    elseif not ripv then
        local lipv = lir.new_var()
        return {
            lir.beginvar(lipv),
            {
                lir.objforeach(lipv, ipv, ipo),
                handler(lipv)
            },
            lir.endvar(lipv)
        }
    else
        return {
            lir.objforeach(ripv, ipv, ipo),
            handler(ripv)
        }
    end
end

local emit_convert
emit_convert = function(lir, ir, ripv, ipv, ipo)
    local irt = ir_type(ir)
    local lirfuncs = ir2lirfuncs[irt]
    if lirfuncs then
        local isfunc, putfunc = unpack(lirfuncs)
        return {
            lir[isfunc]  (ipv, ipo),
            lir.checkobuf(0),
            lir[putfunc] (0, ipv, ipo),
            lir.move(0, 0, 1),
            lir.move(ripv, ipv, ipo + 1)
        }
    elseif irt == 'FIXED' then
        return {
            { lir.isbin(ipv, ipo), lir.lenis(ipv, ipo, ir_fixed_size(ir)) },
            lir.checkobuf(0),
            lir.putbin(0, ipv, ipo),
            lir.move(0, 0, 1),
            lir.move(ripv, ipv, ipo + 1)
        }
    elseif irt == 'ARRAY' then
        return {
            lir.isarray(ipv, ipo),
            lir.checkobuf(0),
            lir.putarray(0, ipv, ipo),
            lir.move(0, 0, 1),
            objforeach(lir, ripv, ipv, ipo, function(xipv)
                return emit_convert(lir, ir[2], xipv, xipv, 0)
            end)
        }
    elseif irt == 'MAP' then
        return {
            lir.ismap(ipv, ipo),
            lir.checkobuf(0),
            lir.putmap(0, ipv, ipo),
            lir.move(0, 0, 1),
            objforeach(lir, ripv, ipv, ipo, function(xipv)
                return {
                    lir.isstr(xipv, 0),
                    lir.checkobuf(0),
                    lir.putstr(0, xipv, 0),
                    lir.move(0, 0, 1),
                    emit_convert(lir, ir[2], xipv, xipv, 1)
                }
            end)
        }
    elseif irt == 'UNION' then
        assert(false, 'NYI: union')
    elseif irt == 'RECORD' then
        assert(false, 'NYI: record')
    elseif irt == 'ENUM' then
        assert(false, 'NYI: enum')
    else
        assert(false)
    end
end

local emit_convert_unchecked
emit_convert_unchecked = function(lir, ir, ripv, ipv, ipo)
    local res = emit_convert(lir, ir, ripv, ipv, ipo)
    -- get rid of checks - emit_convert must cooperate
    res[1] = lir.nop()
    return res
end

-----------------------------------------------------------------------
-- emit_rec_flatten(lir, ir, ripv, ipv, ipo) -> code
local emit_rec_flatten_pass1
local emit_rec_flatten_pass2
local emit_rec_flatten_pass3
local function emit_rec_flatten(lir, ir, ripv, ipv, ipo)
    assert(ir_type(ir) == 'RECORD')
    local var_block, aux_block, defaults = {}, {}, {}
    local context = {
        lir = lir,
        defaults = defaults,   -- [celli * 2 - 1] lir_put* func,
                               -- [celli * 2]     argument
        var_block = var_block, -- variable declarations
        aux_block = aux_block, -- certain field checks 
        vlocell = nil          -- first cell with a VLO
    }
    -- a shadow tree (keyed by o)
    -- used for inter-pass sharing
    -- after 1st pass contains 'direct write' cell indices,
    -- after 2nd - stores input vars
    -- (either directly or in [0] of a nested table)
    local tree = {}
    emit_rec_flatten_pass1(context, ir, tree, 1)
    local init_block = {}
    for i = 1, context.vlocell - 1 do
        local lirfunc = defaults[i * 2 - 1]
        if lirfunc then
            insert(init_block, lirfunc(i, defaults[i * 2]))
        end
    end
    local parser_block = emit_rec_flatten_pass2(context, ir, tree,
                                                ripv, ipv, ipo)
    local generator_block = emit_rec_flatten_pass3(context, ir, tree, 1)
    local vlocell = context.vlocell
    local maxcell = context.maxcell
    if vlocell == maxcell then -- update $0
        insert(generator_block, lir.move(0, 0, vlocell))
    end
    return {
        var_block,
        lir.checkobuf(vlocell - 1),
        lir.putarrayc(0, maxcell - 1),
        init_block,
        parser_block,
        aux_block,
        generator_block
    }
end

-- A subset of cells in the resulting flattened record
-- are at fixed offsets. Compute offsets allowing the parser
-- to store a value immediately instead of postponing until
-- the generation phase (important for optional fields -
-- eliminates one IF). Stop once we hit a VLO, like array.
--
-- Also compute default values to store in cells beforehand.
-- Computes context.vlocell - the index of the first cell hosting
-- a VLO.
emit_rec_flatten_pass1 = function(context, ir, tree, curcell)
    local o2i, onames = ir_record_o2i(ir), ir_record_onames(ir)
    local bc = ir_record_bc(ir)
    local defaults = context.defaults
    for o = 1, #onames do
        local ds, dv = ir_record_odefault(ir, o)
        local dcells
        if ds then
            -- default value attached; it may expand into
            -- several cells (e.g. a nested record); it is
            -- also possible that there is no associated IR
            -- so we are going to increase curcell based on
            -- defaults alone (that's why we have dcells);
            -- finally if there is an IR it may define
            -- a different default value, overriding defaults
            -- we're preparing now
            local ddata
            dcells, ddata = prepare_flat_defaults_vec(context.lir, ds, dv)
            for i = 1, dcells do
                defaults[ (curcell + i)*2 - 3 ] = ddata[ i * 2 - 1 ]
                defaults[ (curcell + i)*2 - 2 ] = ddata[ i * 2 ]
            end
        end
        local fieldir = bc[o2i[o]]
        if not fieldir then
            assert(dcells)
            curcell = curcell + dcells
        else
            local fieldirt
::restart::
            fieldirt = ir_type(fieldir)
            if type(fieldir) ~= 'table' or fieldirt == 'FIXED' or fieldirt == 'ENUM' then
            tree[o] = curcell
                curcell = curcell + 1
            elseif fieldirt == 'RECORD' then
                local childtree = {}
                tree[o] = childtree
                if emit_rec_flatten_pass1(context, fieldir,
                                          childtree, curcell) then
                    return true
                end
                curcell = context.vlocell
            elseif fieldirt == 'UNION' then
                assert(false, 'NYI')
                if ir_union_osimple(fieldir) then
                    -- union in source schema mapped to a simple type in target
                    fieldir = nil -- XXX
                    goto restart
                else
                    -- consider it VLO for simplicity (XXX simple optional fields?)
                    context.vlocell = curcell
                    return true
                end
            else
                -- ARRAY or MAP, it's a VLO
                context.vlocell = curcell
                return true
            end
        end
    end
    context.vlocell = curcell
    return false
end

-- Emit a parser code, uses offsets computed during pass1.
-- Allocates a variable to store position of each input field value.
-- Puts variable names in tree, replacing offsets (which are no longer needed).
emit_rec_flatten_pass2 = function(context, ir, tree, ripv, ipv, ipo)
    local lir = context.lir
    return {
        lir.ismap(ipv, ipo),
        objforeach(lir, ripv, ipv, ipo, function(xipv)
            local inames, bc = ir_record_inames(ir), ir_record_bc(ir)
            local i2o = ir_record_i2o(ir)
            local var_block = context.var_block
            local aux_block = context.aux_block
            local switch = { lir.strswitch(xipv, 0) }
            for i = 1, #inames do
                local fieldir = bc[i]
                local fieldirt = ir_type(fieldir)
                local o = i2o[i]
                local fieldvar = lir.new_var()
                local targetcell = tree[o]
                insert(var_block, lir.beginvar(fieldvar))
                local branch = {
                    lir.sbranch(inames[i]),
                    lir.isnotset(fieldvar),
                    lir.move(fieldvar, xipv, 1)
                }
                switch[i + 1] = branch
                -- we aren't going to see this var during pass3
                if not o and not ir_record_ioptional(ir, i) then
                    insert(aux_block, {
                        lir.isset(fieldvar),
                        lir.endvar(fieldvar)
                })
                end
                if fieldirt == 'RECORD' then
                    if o then
                        if not tree[o] then
                            tree[o] = {}
                        end
                        tree[o][0] = fieldvar
                    end
                    insert(branch, emit_rec_flatten_pass2(context, fieldir, tree[o],
                                                          xipv, fieldvar, 0))
                elseif fieldirt == 'UNION' then
                    assert(false, 'NYI')
                else
                    tree[o] = fieldvar
                    if targetcell then
                        insert(branch, emit_patch(lir, fieldir, xipv, fieldvar, 0,
                                                  targetcell))
                    elseif o then
                        insert(branch, emit_check(lir, fieldir, xipv, fieldvar, 0))
                    else
                        insert(branch, emit_validate(lir, fieldir, xipv, fieldvar, 0))
                    end
                end
            end
            return { lir.isstr(xipv, 0), switch }
        end)
    }
end

-- Emit code generating the (flattened) output record.
-- Note: a subset of cells until the first VLO are already
-- filled at this point (defaults and/or values stored by the parser),
-- however $0 wasn't incremented yet.
-- Computes context.maxcell - total number of cells, plus 1.
emit_rec_flatten_pass3 = function(context, ir, tree, curcell)
    local o2i, onames = ir_record_o2i(ir), ir_record_onames(ir)
    local bc = ir_record_bc(ir)
    local defaults = context.defaults
    local vlocell = context.vlocell
    local lir = context.lir
    local code = {}
    for o = 1, #onames do
        local ds, dv = ir_record_odefault(ir, o)
        local dcells
        local fieldvar = tree[o]
        if type(fieldvar) == 'table' then
            fieldvar = fieldvar[0]
        end
        local tbranch, fbranch
        if not fieldvar then
            fbranch = code
        elseif not ds then -- it's mandatory
            tbranch = {}
            insert(code, {
                lir.isset(fieldvar),
                tbranch,
                lir.endvar(fieldvar)
            })
        else
            tbranch = { lir.ibranch(1) }
            fbranch = { lir.ibranch(0) }
            insert(code, { lir.ifset(fieldvar), tbranch, fbranch })
            insert(code, lir.endvar(fieldvar))
        end
        -- fbranch - if field was missing from the input
        if ds then
            local ddata, didcross
            dcells, ddata = prepare_flat_defaults_vec(context.lir, ds, dv)

            local dsplit -- a spliting point (before/after vlocell)
            if     curcell > vlocell then
                dsplit = 0
            elseif curcell + dcells <= vlocell then
                dsplit = dcells
            else
                dsplit = vlocell - curcell
                didcross = true
            end

            -- before vlocell; patch (unless the same value already stored)
            for i = 1, dsplit do
                local dcurcell = curcell + i - 1
                local lirfunc, arg = ddata[ i * 2 - 1 ], ddata[ i * 2 ]

                if lirfunc ~= defaults[ dcurcell * 2 - 1 ] or
                       arg ~= defaults[ dcurcell * 2 ] then

                    insert(fbranch, lirfunc(dcurcell - 1, arg))
                end
            end

            if dsplit ~= dcells then -- after vlocell; append
                if didcross then -- update $0
                    insert(fbranch, lir.move(0, 0, vlocell))
                end
                insert(fbranch, lir.checkobuf(dcells - dsplit - 1))
                for i = dsplit + 1, dcells do
                    local lirfunc, arg = ddata[ i * 2 - 1 ], ddata[ i * 2 ]
                    insert(fbranch, lirfunc(i - dsplit - 1, arg))
                end
                insert(fbranch, lir.move(0, 0, dcells - dsplit))
            end
        end
        -- tbranch - if field was present in the input
        local fieldir = bc[o2i[o]]
        if not fieldir then
            curcell = curcell + dcells
        else
            local fieldirt
            fieldirt = ir_type(fieldir)
            if fieldirt == 'RECORD' then
                insert(tbranch, emit_rec_flatten_pass3(context, fieldir,
                                                       tree[o], curcell))
                curcell = context.maxcell
            elseif fieldrt == 'UNION' then
                assert(false, 'NYI: union')
            elseif curcell >= vlocell then -- append
                if curcell == vlocell then -- update $0
                    insert(tbranch, lir.move(0, 0, vlocell))
                end
                insert(tbranch, emit_convert_unchecked(lir, fieldir,
                                                       nil, fieldvar, 0))
                curcell = curcell + 1
            else
                curcell = curcell + 1
            end
        end
    end
    context.maxcell = curcell
    return code
end

-----------------------------------------------------------------------
local emit_rec_unflatten_pass1
local emit_rec_unflatten_pass2
local emit_rec_unflatten_pass3
local function emit_rec_unflatten(lir, ir, ripv, ipv, ipo)
    assert(ir_type(ir) == 'RECORD')
    if not ripv then
        ripv = lir.new_var()
        return {
            lir.beginvar(ripv),
            emit_rec_unflatten(lir, ir, ripv, ipv, ipo),
            lir.endvar(ripv)
        }
    end
    local context = {
        lir = lir,
        ipv = ripv,
        fieldv = {},
        fieldo = {},
        lastref = {}
    }
    local tree = {}
    local parser_block = emit_rec_unflatten_pass1(context, ir, tree, 1, false)
    return {
        lir.isarray(ipv, ipo),
        lir.lenis(ipv, ipo, context.maxcell - 1),
        lir.move(ripv, ipv, ipo + 1),
        parser_block,
        emit_rec_unflatten_pass2(context, ir, tree, 1) and
            emit_rec_unflatten_pass3(context, ir, tree, 1)
    }
end

emit_rec_unflatten_pass1 = function(context, ir, tree, curcell, hidden)
    local bc, inames = ir_record_bc(ir), ir_record_inames(ir)
    local i2o = ir_record_i2o(ir)
    local lir, ipv = context.lir, context.ipv
    local fieldv, fieldo = context.fieldv, context.fieldo
    local code = {}
    for i = 1, #inames do
        local fieldir = bc[i]
        local fieldirt = ir_type(fieldir)
        if fieldirt == 'RECORD' then
            local childtree = {}
            tree[i] = childtree
            insert(code, emit_rec_unflatten_pass1(context, fieldir,
                                                  childtree, curcell,
                                                  hidden or ir_record_ohidden(ir, i2o[i])))
            curcell = context.maxcell
        elseif fieldrt == 'UNION' then
            assert(false, 'NYI: union')
        elseif i2o[i] and not ir_record_ohidden(ir, i2o[i]) and not hidden then
            tree[i] = curcell
            local lastcell = context.lastcell
            if lastcell then
                fieldv[curcell] = fieldv[lastcell]
                fieldo[curcell] = curcell - lastcell
            else
                local fieldvar = lir.new_var()
                insert(code, lir.beginvar(fieldvar))
                insert(code, lir.move(fieldvar, ipv))
                fieldv[curcell] = fieldvar
                fieldo[curcell] = 0
                context.lastcell = curcell
            end
            insert(code, emit_check(context.lir, fieldir,
                                    ipv, ipv, 0))
            curcell = curcell + 1
            if not ir2lirfuncs[fieldirt] then
                context.lastcell = nil -- VLO
            end
        else
            insert(code, emit_validate(context.lir, fieldir,
                                       ipv, ipv, 0))
            curcell = curcell + 1
            if not ir2lirfuncs[fieldirt] then
                context.lastcell = nil -- VLO
            end
        end
    end
    context.maxcell = curcell
    return code
end

emit_rec_unflatten_pass2 = function(context, ir, tree, curfield)
    local bc, onames = ir_record_bc(ir), ir_record_onames(ir)
    local o2i = ir_record_o2i(ir)
    local fieldv = context.fieldv
    local lastref = context.lastref
    for o = 1, #onames do
        local i = o2i[o]
        if ir_record_ohidden(ir, o) or not i then
            curfield = curfield + 1
        else
            local fieldir = bc[i]
            local fieldirt = ir_type(fieldir)
            if fieldirt == 'RECORD' then
                curfield = emit_rec_unflatten_pass2(context, fieldir, tree[i],
                                                    curfield)
            elseif fieldirt == 'UNION' then
                assert(false, 'NYI: union')
            else
                local curcell = tree[i]
                local fieldvar = fieldv[curcell]
                lastref[fieldvar] = curfield
                curfield = curfield + 1
            end
        end
    end
    return curfield
end

emit_rec_unflatten_pass3 = function(context, ir, tree, curfield)
    local bc, onames = ir_record_bc(ir), ir_record_onames(ir)
    local o2i = ir_record_o2i(ir)
    local lir, fieldv, fieldo = context.lir, context.fieldv, context.fieldo
    local lastref = context.lastref
    local maplen = 0
    local code = { lir.checkobuf(opo), lir.nop(), lir.move(0, 0, 1) }
    for o = 1, #onames do
        local i = o2i[o]
        if ir_record_ohidden(ir, o) then
            curfield = curfield + 1
        elseif not i then
            -- put defaults
            local schema, val = ir_record_odefault(ir, o)
            local lirfunc, arg = prepare_default(lir, schema, val)
            insert(code, {
                lir.checkobuf(1),
                lir.putstrc(0, onames[o]),
                lirfunc(1, arg),
                lir.move(0, 0, 2)
            })
            maplen = maplen + 1
            curfield = curfield + 1
        else
            local fieldir = bc[i]
            local fieldirt = ir_type(fieldir)
            insert(code, {
                lir.checkobuf(0),
                lir.putstrc(0, onames[o]),
                lir.move(0, 0, 1)
            })
            if fieldirt == 'RECORD' then
                insert(code, emit_rec_unflatten_pass3(context, fieldir, tree[i],
                                                      curfield))
                curfield = context.maxfield
            elseif fieldirt == 'UNION' then
                assert(false, 'NYI: union')
            else
                local curcell = tree[i]
                local fieldvar = fieldv[curcell]
                insert(code, emit_convert_unchecked(lir, fieldir, nil,
                                                    fieldvar,
                                                    fieldo[curcell]))
                if lastref[fieldvar] == curfield then
                    insert(code, lir.endvar(fieldvar))
                end
                curfield = curfield + 1
            end
            maplen = maplen + 1
        end
    end
    context.maxfield = curfield
    code[2] = lir.putmapc(0, maplen)
    return code
end

-----------------------------------------------------------------------
local emit_rec_xflatten_pass1
local emit_rec_xflatten_pass2

local function emit_rec_xflatten(lir, ir, internal, ipv)
    assert(ir_type(ir) == 'RECORD')
    local headpos, counter = lir.new_var(), lir.new_var()
    local var_block = {}
    local context = {
        lir = lir,
        internal = internal,
        var_block = var_block,
        counter = counter
    }
    local tree = {}
    emit_rec_xflatten_pass1(context, ir, tree, 1)
    return {
        lir.beginvar(headpos),
        lir.beginvar(counter),
        var_block,
        lir.move(headpos, 0, 0),
        lir.move(counter, nil, 0),
        lir.checkobuf(0),
        lir.putarrayc(0, 0),
        lir.move(0, 0, 1),
        emit_rec_xflatten_pass2(context, ir, tree, ipv, ipv, 0),
        lir.setlen(headpos, 0, counter)
    }
end

emit_rec_xflatten_pass1 = function(context, ir, tree, curcell)
    local o2i, onames = ir_record_o2i(ir), ir_record_onames(ir)
    local bc = ir_record_bc(ir)
    local internal = context.internal
    for o = 1, #onames do
        local fieldir = bc[o2i[o]]
        if not fieldir then
            local ds, dv = ir_record_odefault(ir, o)
            curcell = curcell + prepare_flat_defaults_vec(context.lir, ds, dv)
        else
            local fieldirt
            tree[o] = curcell + internal
            fieldirt = ir_type(fieldir)
            if     fieldirt == 'RECORD' then
                local ctree = {}
                tree[o] = ctree
                curcell = emit_rec_xflatten_pass1(context, fieldir,
                                                  ctree, curcell)
            elseif fieldirt == 'UNION' then
                assert(false, 'NYI: union')
            else
                curcell = curcell + 1
            end
        end
    end
    return curcell
end

emit_rec_xflatten_pass2 = function(context, ir, tree, ripv, ipv, ipo)
    local lir = context.lir
    return {
        lir.ismap(ipv, ipo),
        objforeach(lir, ripv, ipv, ipo, function(xipv)
            local inames, bc = ir_record_inames(ir), ir_record_bc(ir)
            local i2o = ir_record_i2o(ir)
            local var_block = context.var_block
            counter = context.counter
            local switch = { lir.strswitch(xipv, 0) }
            for i = 1, #inames do
                local fieldir = bc[i]
                local fieldirt = ir_type(fieldir)
                local o = i2o[i]
                local fieldvar = lir.new_var()
                local targetcell = tree[o]
                insert(var_block, lir.beginvar(fieldvar))
                local branch = {
                    lir.sbranch(inames[i]),
                    lir.isnotset(fieldvar),
                    lir.move(fieldvar, xipv, 1)
                }
                switch[i + 1] = branch
                if fieldirt == 'RECORD' then
                    insert(branch, emit_rec_xflatten_pass2(context, fieldir,
                                                           tree and tree[o],
                                                           xipv, fieldvar, 0))
                elseif fieldirt == 'UNION' then
                    assert(false, 'NYI')
                else
                    if o then
                        insert(branch, {
                            lir.checkobuf(2),
                            lir.putarrayc(0, 3),
                            lir.putstrc(1, '='),
                            lir.putintc(2, tree[o]),
                            lir.move(counter, counter, 1),
                            lir.move(0, 0, 3),
                            emit_convert(lir, fieldir, xipv, fieldvar, 0)
                        })
                    else
                        insert(branch, emit_validate(lir, fieldir, xipv, fieldvar, 0))
                    end
                end
            end
            return { lir.isstr(xipv, 0), switch }
        end)
    }
end

-----------------------------------------------------------------------
local function emit_code(lir, ir)
    -- reserve f001, f002, f003
    lir.new_func(); lir.new_func(); lir.new_func()
    -- reserve v001, v002, v003
    lir.new_var(); lir.new_var(); lir.new_var()
    return {
        { lir.cvtfunc(1, 1), emit_rec_flatten(lir, ir, 1, 1, 0) },
        { lir.cvtfunc(2, 2), emit_rec_unflatten(lir, ir, 2, 2, 0) },
        { lir.cvtfunc(3, 3), emit_rec_xflatten(lir, ir, 0, 3) }
    }
end

-----------------------------------------------------------------------
return {
    emit_code          = emit_code, 
    emit_rec_flatten   = emit_rec_flatten,
    emit_rec_unflatten = emit_rec_unflatten,
    emit_rec_xflatten  = emit_rec_xflatten
}
