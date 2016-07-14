local ffi            = require('ffi')
local bit            = require('bit')
local digest         = require('digest')
local rt             = require('avro_schema.runtime')
local ffi_new        = ffi.new
local ffi_string     = ffi.string
local ffi_sizeof     = ffi.sizeof
local format, rep    = string.format, string.rep
local byte, max      = string.byte, math.max
local insert, remove = table.insert, table.remove
local concat         = table.concat
local band, rshift   = bit.band, bit.rshift

local rt_C           = ffi.load(rt.C_path)

local opcode = ffi_new('struct schema_il_Opcode')

local random_bytes   = digest.base64_decode([[
X1CntcveBDc4inHKyWfyXw5iCjg1f3TmeX88pZ2Galb9tXpTt1uBO0pZrkU5NW1/4Ki9g8fAwElq
B3dRsBscsg==]])
    
local sched_variables_helper
sched_variables_helper = function(block, n, varmap, reusequeue)
    for i = 1, #block do
        local o = block[i]
        if type(o) == 'table' then
            n = sched_variables_helper(o, n, varmap, reusequeue)
        elseif o.op == opcode.BEGINVAR then
            local var = remove(reusequeue)
            if not var then
                var = n + 1
                n = var
            end
            varmap[o.ipv] = var
        elseif o.op == opcode.ENDVAR then
            insert(reusequeue, varmap[o.ipv])
        end
    end
    return n
end
local function sched_func_variables(func)
    local varmap = {}
    return sched_variables_helper(func, 0, varmap, {}), varmap
end

-- Due to the quirks of the tracing JIT-compiler, it's
-- beneficial to get rid of nested loops and to transform the code
-- into a state-machine wrapped in the single top-level for loop.
-- Also, for loop is far better than any other kind of a loop.
-- Anyway, this transformation requires goto-s and in Lua goto
-- is very restricted; basically one can't enter a block/scope
-- via goto. For this reason we get rid of nested blocks if we
-- are going to enter it via goto (we call it "peeling").

-- The function adds 'peel' anotations to IL blocks. Also
-- added are 'break_jit_trace' annotations.
local peel_annotate
peel_annotate = function(block, k)
    if block.peel ~= nil then return end -- tree already processed
    local peel = false
    for i = 2, #block do
        local o = block[i]
        if type(o) == 'table' then
            local head = o[1]
            if head.op >= opcode.IFSET and head.op <= opcode.STRSWITCH then
                -- too many conditionals in a row
                if k >= 2 then o.break_jit_trace = true; k = 0; peel = true end
                local nk = 0
                for j = 2, #o do
                    local branch = o[j]
                    local bp, bk = peel_annotate(branch, k+1)
                    peel = peel or bp
                    if bk > nk then nk = bk end
                end
                k = nk
            elseif head.op == opcode.OBJFOREACH then
                k = 0
                for j = 2, #o do
                    if type(o[j]) == 'table' then
                        peel_annotate(o, 0)
                        o.peel = true
                        peel = true
                        break
                    end
                end
                if not o.peel and head.step == 0 then
                    o.peel = true
                    peel = true
                end
            end
        end
    end
    block.peel = peel
    return peel, k
end

-- variable [ + offset]
local function varref(ipv, ipo, map)
    local class = 'v'
    local override = map[ipv]
    if override then
        class = 'x'
        ipv = override
    end
    if ipo == 0 then
        return class..ipv
    else
        return format('%s%d+%d', class, ipv, ipo)
    end
end

local emit_instruction_tab = {
    ----------------------- T
    [opcode.PUTARRAYC  ] = 11,
    [opcode.PUTMAPC    ] = 12,
    [opcode.PUTFLOATC  ] =  6,
    [opcode.PUTDOUBLEC ] =  7,
    [opcode.PUTSTRC    ] = 18,
    [opcode.PUTBINC    ] = 19,
    [opcode.PUTXC      ] = 20,
    ------------------------- T, tofield, fromfield
    [opcode.PUTINT     ] = {  4, 'ival',  'ival' },
    [opcode.PUTLONG    ] = {  4, 'ival',  'ival' },
    [opcode.PUTFLOAT   ] = {  6, 'dval',  'dval' },
    [opcode.PUTDOUBLE  ] = {  7, 'dval',  'dval' },
    [opcode.PUTSTR     ] = {  8, 'uval',  'uval' },
    [opcode.PUTBIN     ] = {  9, 'uval',  'uval' },
    [opcode.PUTARRAY   ] = { 11, 'xlen',  'xlen' },
    [opcode.PUTMAP     ] = { 12, 'xlen',  'xlen' },
    [opcode.PUTINT2LONG] = {  4, 'ival',  'ival' },
    [opcode.PUTINT2FLT ] = {  6, 'dval',  'ival' },
    [opcode.PUTINT2DBL ] = {  7, 'dval',  'ival' },
    [opcode.PUTLONG2FLT] = {  6, 'dval',  'ival' },
    [opcode.PUTLONG2DBL] = {  7, 'dval',  'ival' },
    [opcode.PUTFLT2DBL ] = {  7, 'dval',  'dval' },
    [opcode.PUTSTR2BIN ] = {  9, 'uval',  'uval' },
    [opcode.PUTBIN2STR ] = {  8, 'uval',  'uval' },
    ----------------------- T
    [opcode.ISBOOL     ] = '\0\0\1\1\0\0\0\0\0\0\0\0\0',
    [opcode.ISNULORMAP ] = '\0\1\0\0\0\0\0\0\0\0\0\0\1',
    ----------------------- T
    [opcode.ISNUL      ] =  1,
    [opcode.ISLONG     ] =  4,
    [opcode.ISSTR      ] =  8,
    [opcode.ISBIN      ] =  9,
    [opcode.ISARRAY    ] = 11,
    [opcode.ISMAP      ] = 12
}

local emit_compute_hash_func_tab = {
    [0x01] = 't = r.b1[-r.v[%s].xoff+%d]',
    [0x02] = 't = r.b1[-r.v[%s].xoff+%d]+r.b1[-r.v[%s].xoff+%d]',
    [0x03] = 't = r.b1[-r.v[%s].xoff+%d]+r.b1[-r.v[%s].xoff+%d]+r.b1[-r.v[%s].xoff+%d]',
    [0x04] = 't = r.v[%s].xlen',
    [0x05] = 't = r.v[%s].xlen+r.b1[%d-r.v[%s].xoff]',
    [0x06] = 't = r.v[%s].xlen+r.b1[%d-r.v[%s].xoff]+r.b1[%d-r.v[%s].xoff]',
    [0x07] = 't = r.v[%s].xlen+r.b1[%d-r.v[%s].xoff]+r.b1[%d-r.v[%s].xoff]+r.b1[%d-r.v[%s].xoff]',
    [0x08] = 't = 0',
    [0x09] = 't = r.b1[-r.v[%s].xoff+%d]',
    [0x0a] = 't = bor(lshift(r.b1[-r.v[%s].xoff+%d], 8), r.b1[-r.v[%s].xoff+%d])',
    [0x0b] = 't = bor(lshift(r.b1[-r.v[%s].xoff+%d], 16), bor(lshift(r.b1[-r.v[%s].xoff+%d], 8), r.b1[-r.v[%s].xoff+%d]))',
    [0x0c] = 't = r.v[%s].xlen',
    [0x0d] = 't = bor(lshift(r.v[%s].xlen, 8), r.b1[%d-r.v[%s].xoff])',
    [0x0e] = 't = bor(lshift(r.v[%s].xlen, 16), bor(lshift(r.b1[%d-r.v[%s].xoff], 8), r.b1[%d-r.v[%s].xoff]))',
    [0x0f] = 't = bor(bor(lshift(r.v[%s].xlen, 24), lshift(r.b1[%d-r.v[%s].xoff], 16)), bor(lshift(r.b1[%d-r.v[%s].xoff], 8), r.b1[%d-r.v[%s].xoff]))'
}

local function emit_compute_hash_func(func, pos, res)
    if func == 0 then
        assert(false)
    elseif band(func, 0xf0000000) ~= 0 then
        insert(res, format([[
t = rt_C.eval_fnv1a_func(%d, r.b1-r.v[%s].xoff, r.v[%s].xlen)]],
                      rt_C.eval_hash_func(func, '', 0), pos, pos))
        return
    end
    local a = rshift(func, 24)
    local b = band(0xff, rshift(func, 16))
    local c = band(0xff, rshift(func,  8))
    local d = band(0xff, func)
    local stmt = format(emit_compute_hash_func_tab[a],
                        pos, b, pos, c, pos, d, pos)
    if band(a, 0x3) == 0 then
        insert(res, stmt) -- no samples from the string
    else
        local idx_max = band(0xff, rshift(func, 8*(3-band(a, 0x3))))
        insert(res, format('if r.v[%s].xlen > %d then -- %x',
                           pos, idx_max, func))
        insert(res, stmt)
        insert(res, "end")
    end
end

local function emit_instruction(il, o, res, varmap)
    local tab = emit_instruction_tab -- a shorter alias
    if     o.op == opcode.CALLFUNC  then
        insert(res, format('v0%s = f%d(r, v0, %s)',
                            o.ripv == opcode.NILREG and '' or
                            ', '..varref(o.ripv, 0, varmap),
                            il.get_extra(o), varref(o.ipv, o.ipo, varmap)))
    elseif o.op == opcode.MOVE      then
        insert(res, format('%s = %s',
                            varref(o.ripv, 0,     varmap),
                            varref(o.ipv,  o.ipo, varmap)))
    elseif o.op == opcode.SKIP      then
        local pos = varref(o.ipv, o.ipo, varmap)
        insert(res, format('%s = %s+r.v[%s].xoff',
                            varref(o.ripv, 0, varmap), pos, pos))
    elseif o.op == opcode.PSKIP     then
        local pos = varref(o.ipv, o.ipo, varmap)
        insert(res, format('%s = %s+r.b2[r.t[%s]-%d]*(r.v[%s].xoff-1)',
                            varref(o.ripv, 0, varmap),
                            varref(o.ipv, o.ipo + 1, varmap), pos,
                            il.cpool_add('\0\0\0\0\0\0\0\0\0\0\0\1\1'), pos))
    -----------------------------------------------------------
    elseif o.op == opcode.PUTBOOLC  then
        insert(res, format('r.ot[%s] = %d',
                            varref(0, o.offset, varmap), o.ci == 0 and 2 or 3))
    elseif o.op == opcode.PUTINTC then
        local pos = varref(0, o.offset, varmap)
        insert(res, format('r.ot[%s] = 4; r.ov[%s].ival = %d',
                            pos, pos, o.ci))
    elseif o.op == opcode.PUTARRAYC or o.op == opcode.PUTMAPC then
        local pos = varref(0, o.offset, varmap)
        insert(res, format('r.ot[%s] = %d; r.ov[%s].xlen = %d',
                            pos, tab[o.op], pos, o.ci))
    elseif o.op == opcode.PUTLONGC  then
        local pos = varref(0, o.offset, varmap)
        insert(res, format('r.ot[%s] = 4; r.ov[%s].ival = %s',
                            pos, pos, o.cl))
    elseif o.op == opcode.PUTFLOATC or o.op == opcode.PUTDOUBLEC then
        local pos = varref(0, o.offset, varmap)
        insert(res, format('r.ot[%s] = %d; r.ov[%s].dval = %f',
                            pos, tab[o.op], pos, o.cd))
    elseif o.op == opcode.PUTNULC   then
        insert(res, format('r.ot[%s] = 1',
                            varref(0, o.offset, varmap)))
    elseif o.op == opcode.PUTSTRC or o.op == opcode.PUTBINC or
           o.op == opcode.PUTXC     then
        local pos = varref(0, o.offset, varmap)
        local str = il.get_extra(o)
        -- Note: 64 bit constants are slowing down JIT compilation due to
        --       O(n) search in 64 bit const pool, init xlen/xoff separately
        --       instead of doing uval at once
        insert(res, format([[
r.ot[%s] = %d; r.ov[%s].xlen = %d; r.ov[%s].xoff = %d]],
                           pos, tab[o.op],
                           pos, #str, pos, il.cpool_add(str)))
    -----------------------------------------------------------
    elseif o.op == opcode.PUTBOOL   then
        insert(res, format('r.ot[%s] = r.t[%s]',
                            varref(0,     o.offset, varmap),
                            varref(o.ipv, o.ipo,    varmap)))
    elseif o.op >= opcode.PUTINT and o.op <= opcode.PUTBIN2STR then
        local pos = varref(0, o.offset, varmap)
        local opt = tab[o.op]
        insert(res, format('r.ot[%s] = %d; r.ov[%s].%s = r.v[%s].%s',
                            pos, opt[1], pos, opt[2],
                            varref(o.ipv, o.ipo, varmap), opt[3]))
    -----------------------------------------------------------
    elseif o.op == opcode.PUTENUMI2S then
        il.emit_putenumi2s(o, res, varmap)
    -----------------------------------------------------------
    elseif o.op == opcode.PUTENUMS2I then
        il.emit_putenums2i(o, res, varmap)
    -----------------------------------------------------------
    elseif o.op == opcode.ISBOOL or o.op == opcode.ISNULORMAP   then
        local pos = varref(o.ipv, o.ipo, varmap)
        insert(res, format([[
if r.b2[r.t[%s]-%d] == 0 then rt_err_type(r, %s, 0x%x) end]],
                            pos,
                            il.cpool_add(tab[o.op]),
                            pos, o.op))
    elseif o.op == opcode.ISINT     then
        local pos = varref(o.ipv, o.ipo, varmap)
        insert(res, format([[
if r.t[%s] ~= 4 or r.v[%s].uval+0x80000000 > 0xffffffff then rt_err_type(r, %s, 0x%x) end]],
                            pos, pos, pos, opcode.ISINT))
    elseif o.op == opcode.ISFLOAT or o.op == opcode.ISDOUBLE then
        local pos = varref(o.ipv, o.ipo, varmap)
        insert(res, format([[
if r.b2[r.t[%s]-%d] == 0 then rt_err_type(r, %s, 0x%x) end]],
                            pos,
                            il.cpool_add('\0\0\0\0\0\0\1\1\0\0\0\0\0'),
                            pos, o.op))
    elseif o.op >= opcode.ISLONG and o.op <= opcode.ISNUL then
        local pos, t = varref(o.ipv, o.ipo, varmap), tab[o.op]
        insert(res, format('if r.t[%s] ~= %d then rt_err_type(r, %s, 0x%x) end',
                            pos, t, pos, o.op))
    elseif o.op == opcode.LENIS     then
        local pos = varref(o.ipv, o.ipo, varmap)
        insert(res, format([[
if r.v[%s].xlen ~= %d then rt_err_length(r, %s, %d) end]],
                            pos, o.len, pos, o.len))
    elseif o.op == opcode.ISNOTSET  then
        local pos = varref(o.ipv, o.ipo, varmap)
        insert(res, format('if %s ~= 0 then rt_err_duplicate(r, %s) end',
                            pos, pos))
    -----------------------------------------------------------
    elseif o.op == opcode.ENDVAR    then
    -----------------------------------------------------------
    elseif o.op == opcode.CHECKOBUF then
        local expr
        if o.ipv == opcode.NILREG then
            expr = varref(0, o.offset, varmap)
        else
            expr = format('%s+r.v[%s].xlen%s',
                          varref(0, o.offset, varmap),
                          varref(o.ipv, o.ipo, varmap),
                          o.scale == 1 and '' or '*'..o.scale)
        end
        insert(res, format('if %s > r.ot_capacity then rt_buf_grow(r, %s) end',
                            expr, expr))
    -----------------------------------------------------------
    elseif o.op == opcode.ERRVALUEV then
        insert(res, format('rt_err_value(r, %s, 1)',
                           varref(o.ipv, o.ipo, varmap)))
    -----------------------------------------------------------
    elseif o.op == opcode.ISSET     then
        insert(res, format('if %s == 0 then rt_err_missing(r, %s, "%s") end',
                            varref(o.ripv, 0, varmap),
                            varref(o.ipv, o.ipo, varmap),
                            il.get_extra(o)))
    -----------------------------------------------------------
    else
        assert(false)
    end
end

local emit_nested_block

local function emit_if_block(ctx, block, cc, res)
    local varmap  = ctx.varmap
    local head, branch1, branch2 = block[1], block[2], block[3]
    assert(branch1[1].op == opcode.IBRANCH)
    if head.op == opcode.IFNUL then
        insert(res, format('if r.t[%s] %s 1 then',
                            varref(head.ipv, head.ipo, varmap),
                            branch1[1].ci == 0 and '~=' or '=='))
    else
        insert(res, format('if %s %s 0 then',
                            varref(head.ipv, head.ipo, varmap),
                            branch1[1].ci == 0 and '==' or '~='))
    end
    emit_nested_block(ctx, branch1, cc, res)
    if branch2 then
        assert(branch2[1].op == opcode.IBRANCH)
        assert(branch2[1].ci ~= branch1[1].ci)
        insert(res, 'else')
        emit_nested_block(ctx, branch2, cc, res)
    end
    insert(res, 'end')
end

local function emit_intswitch_block(ctx, block, cc, res)
    local varmap = ctx.varmap
    local head   = block[1]
    local pos    = varref(head.ipv, head.ipo, varmap)
    for i = 2, #block do
        local branch = block[i]
        local branch_head = branch[1]
        assert(branch_head.op == opcode.IBRANCH)
        local if_or_elseif = i == 2 and 'if' or 'elseif'

        insert(res, format('%s r.v[%s].ival == %d then',
                            if_or_elseif, pos, branch_head.ci))
        emit_nested_block(ctx, branch, cc, res)
    end
    insert(res, 'else')
    insert(res, format('rt_err_value(r, %s)', pos))
    insert(res, 'end')
end

local function create_strswitch_hash_func(il, block)
    if not il.enable_fast_strings then return 0 end
    local strings = ffi_new('const char *[?]', #block - 1)
    for i = 2, #block do
        local branch = block[i]
        local branch_head = branch[1]
        assert(branch_head.op == opcode.SBRANCH)
        local str = il.get_extra(branch_head)
        strings[i - 2] = str
    end
    return rt_C.create_hash_func(#block - 1, strings,
                                 random_bytes, #random_bytes)
end

local function emit_strswitch_block(ctx, block, cc, res)
    local il     = ctx.il
    local varmap = ctx.varmap
    local head   = block[1]
    local func   = create_strswitch_hash_func(il, block)
    local pos    = varref(head.ipv, head.ipo, varmap)
    if func ~= 0 then
        emit_compute_hash_func(func, pos, res)
    else
        insert(res, format('t = ffi_string(r.b1-r.v[%s].xoff, r.v[%s].xlen)',
                           pos, pos))
    end
    for i = 2, #block do
        local branch = block[i]
        local branch_head = branch[1]
        assert(branch_head.op == opcode.SBRANCH)
        local str = il.get_extra(branch_head)
        local if_or_elseif = i == 2 and 'if' or 'elseif'
        if func ~= 0 then
            insert(res, format('%s t == %d then', if_or_elseif,
                                rt_C.eval_hash_func(func, str, #str)))
            insert(res, format([[
if rt_C.schema_rt_key_eq(r.b2-%d, r.b1-r.v[%s].xoff, %d, r.v[%s].xlen) ~= 0 then]],
                               il.cpool_add(str), pos, #str, pos))
            insert(res, format('rt_err_value(r, %s)\nend', pos))
        else
            insert(res, format('%s t == %q then',
                               if_or_elseif, str))
        end
        emit_nested_block(ctx, branch, cc, res)
    end
    insert(res, 'else')
    insert(res, format('rt_err_value(r, %s)', pos))
    insert(res, 'end')
end

-- Break the current JIT trace in a gentle way.
-- The trace successfully completes and it gets linked with
-- the existing JIT-ed code.
local function break_jit_trace(ctx, res)
    local label = ctx.il.id()
    insert(ctx.jit_trace_breaks, label)
    insert(res, format('s = %d', label))
    insert(res, 'goto continue -- break JIT trace')
    insert(res, format('::l%d::', label))
end

local function emit_objforeach_block(ctx, block, cc, res)
    local il      = ctx.il
    local varmap  = ctx.varmap
    local head    = block[1]
    local itervar = varref(head.ripv, 0, varmap)
    if block.peel or head.step == 0 then
        insert(res, format('%s = %s',
                            itervar, varref(head.ipv, head.ipo + 1 - head.step,
                                            varmap)))
        if il.enable_loop_peeling then
            local label = il.id(); ctx.labelmap[head] = label
            insert(res, format('::l%d::', label))
            break_jit_trace(ctx, res)
            if head.step ~= 0 then
                insert(res, format('%s = %s+%d', itervar,
                                    itervar, head.step))
            end
            local pos = varref(head.ipv, head.ipo, varmap)
            insert(res, format('if %s ~= %s+r.v[%s].xoff then',
                                itervar, pos, pos))
        else -- head.step == 0
            local pos = varref(head.ipv, head.ipo, varmap)
            insert(res, format('while %s ~= %s+r.v[%s].xoff do',
                                itervar, pos, pos))
        end
        emit_nested_block(ctx, block, head, res)
        insert(res, 'end')
        return true -- can FUSE with SKIP
    else
        insert(res, format('for %s = %s, %s+r.v[%s].xoff, %d do',
                            itervar,
                            varref(head.ipv, head.ipo+1, varmap),
                            varref(head.ipv, head.ipo-1, varmap),
                            varref(head.ipv, head.ipo, varmap),
                            head.step))
        emit_nested_block(ctx, block, head, res)
        insert(res, 'end')
    end
end

-- After BEGINVAR, the variable value is 0. If a store follows
-- we can elide the initialization.
local function elide_var_init(block, i)
    local vid = block[i].ipv
    for lookahead = i+1, i+5 do
        local o = block[lookahead]
        if not o then
            return true
        elseif type(o) == 'table' then
            local head = o[1]
            return head.op == opcode.OBJFOREACH and
                   head.ripv == vid
        elseif o.op >= opcode.OBJFOREACH and o.op <= opcode.PSKIP and
               o.ripv == vid then
            return true
        elseif o.ipv == vid then
            return false -- false negatives are ok
        end
    end
    return false
end

-- in OBJFOREACH iteration variable is undefined upon completion;
-- the op is often followed by a SKIP to compute the position of the
-- element following array/map. The later could be fused with OBJFOREACH,
-- if in the resulting code the iteration variable has a well-defined
-- value when the loop completes. (in Lua: consider while vs for loop)
local function fuse_skip(block, i, varmap)
    local head = block[i][1]
    for lookahead = i+1, i+5 do
        local o = block[lookahead]
        if type(o) ~= 'cdata' then
            return i
        elseif o.op == opcode.SKIP and
               o.ipv == head.ipv and o.ipo == head.ipo then
            if o.ripv ~= head.ripv then
                return lookahead + 1, format('%s = %s',
                                             varref(o.ripv, 0, varmap),
                                             varref(head.ripv, 0, varmap))
            end
            return lookahead + 1
        elseif o.op ~= opcode.ENDVAR then
            return i
        end
    end
    return i
end

local function emit_block(ctx, block, cc, res)
    local il =       ctx.il
    local varmap =   ctx.varmap   -- variable name -> local name
    local labelmap = ctx.labelmap -- il object -> label name
    local skiptill = 1
    for i = 2, #block do
        local o = block[i]
        local label = labelmap[o]
        if label then
            insert(res, format('::l%d::', label))
        end
        if type(o) == 'cdata' then
            if o.op == opcode.BEGINVAR  then
                if not elide_var_init(block, i) then
                    insert(res, format('%s = 0', varref(o.ipv, 0, varmap)))
                end
            elseif i >= skiptill then -- FUSE
                emit_instruction(il, o, res, varmap)
            end
        else
            if o.break_jit_trace then break_jit_trace(ctx, res) end
            local head = o[1]
            local link = block[i+1] or cc
            if     head.op == opcode.IFSET or head.op == opcode.IFNUL then
                emit_if_block(ctx, o, link, res)
            elseif head.op == opcode.INTSWITCH then
                emit_intswitch_block(ctx, o, link, res)
            elseif head.op == opcode.STRSWITCH then
                emit_strswitch_block(ctx, o, link, res)
            elseif head.op == opcode.OBJFOREACH then
                local can_fuse = emit_objforeach_block(ctx, o, link, res)
                if can_fuse then -- fuse OBJFOREACH / SKIP
                    local copystmt
                    skiptill, copystmt = fuse_skip(block, i, varmap)
                    insert(res, copystmt)
                end
            else
                assert(false)
            end
        end
    end
end

emit_nested_block = function(ctx, block, cc, res)
    if not block.peel then
        return emit_block(ctx, block, cc, res)
    end
    local il = ctx.il
    local labelmap, queue = ctx.labelmap, ctx.queue
    local label = il.id()
    labelmap[block] = label
    if not labelmap[cc] then
        labelmap[cc] = il.id()
    end
    insert(res, format('goto l%d', label))
    insert(queue, block)
    insert(queue, cc)
end

local locals_tab = {
    [0] = 'local x%d',
    'local x%d, x%d',
    'local x%d, x%d, x%d'
}

local function emit_func_body(il, func, nlocals_min, res)
    local nlocals, varmap = sched_func_variables(func)
    if nlocals_min and nlocals_min > nlocals then
        nlocals = nlocals_min
    end
    for i = 1, nlocals, 4 do
        insert(res, format(locals_tab[nlocals - i] or
                           'local x%d, x%d, x%d, x%d',
                           i, i+1, i+2, i+3))
    end
    if il.enable_loop_peeling then
        peel_annotate(func, 0)
    end
    insert(res, '')
    local patchpos = #res
    local donelabel = il.id()
    local head = func[1]
    local labelmap, jit_trace_breaks = {[head] = donelabel}, {}
    local queue = { func, head }
    local ctx = {
        il = il,
        varmap = varmap,
        labelmap = labelmap,
        jit_trace_breaks = jit_trace_breaks,
        queue = queue
    }
    local emitpos = 0
    while emitpos ~= #queue do
        local n = emitpos + 1
        emitpos = #queue
        for i = n, emitpos, 2 do
            local entry, cc = queue[i], queue[i+1]
            local label = labelmap[entry]
            insert(res, label and format('::l%d::', label))
            emit_block(ctx, entry, cc, res)
            insert(res, format('goto l%s', labelmap[cc]))
        end
    end
    insert(res, format('::l%d::', donelabel))
    return patchpos, jit_trace_breaks
end

local function emit_jump_table(labels, res)
    local kw = 'if'
    for i = #labels, 1, -1 do
        local l = labels[i]
        insert(res, format('%s s == %d then', kw, l))
        insert(res, format('goto l%d', l))
        kw = 'elseif'
    end
    insert(res, 'end')
end

-- result is configurable via opts:
--  .func_decl           - cutomize function declaration
--  .func_locals         - add more locals
--  .func_return         - customize return
--  .conversion_init     - custom code executed before conversion
--  .conversion_complete - custom code executed after conversion
--  .iter_prolog         - custom code executed on every loop iteration
local function emit_func(il, func, res, opts)
    local head = func[1]
    local func_decl = opts and opts.func_decl or
                      format('f%d = function(r, v0, v%d)', head.name, head.ipv)
    local func_locals = opts and opts.func_locals
    local func_return = opts and opts.func_return or
                        format('do return v0, v%d end', head.ipv)
    local conversion_init     = opts and opts.conversion_init
    local conversion_complete = opts and opts.conversion_complete
    local iter_prolog = opts and opts.iter_prolog
    local nlocals_min = opts and opts.nlocals_min

    insert(res, func_decl)
    insert(res, func_locals)
    insert(res, 'local t = 0')
    local tpos = #res
    local patchpos, jit_trace_breaks =
        emit_func_body(il, func, nlocals_min, res)
    res[patchpos] = conversion_init or ''
    if not conversion_complete then
        insert(res, func_return)
    elseif il.enable_loop_peeling --[[ and conversion_complete ]] then
        local label = il.id()
        insert(jit_trace_breaks, 1, label)
        insert(res, format('%s\ns = %d\ngoto continue',
                           conversion_complete, label))
    else --[[ not il.enable_loop_peeling and conversion_complete ]]
        insert(res, conversion_complete)
        insert(res, func_return)
    end
    if next(jit_trace_breaks) then
        local patch = { 'for _ = 1, 1000000000 do' }
        insert(patch, iter_prolog)
        emit_jump_table(jit_trace_breaks, patch)
        if conversion_complete then
            patch[#patch - 1] = func_return
        end
        insert(patch, conversion_init)
        res[patchpos] = concat(patch, '\n')
        res[tpos] = 'local t, s = 0, 0'
        insert(res, '::continue::')
        insert(res, 'end')
    end
    insert(res, 'end')
end
------------------------------------------------------------------------

local function install_backend(il, opts)

    -- cpool (constant pool) state variables
    -- Most of strings used in runtime a cat-ed together resulting in a
    -- blob of data (cpool). Individual elements are accessed with
    -- offsets, the later are relative to the blob's END!
    -- (unparse_msgpack() established this particular representation;
    --  also: fewer upvalues used)
    local cmax = 1000000
    local cmin = 1000001
    local cpos = 0
    local cpoo = {}

    local function cpool_add_raw(data)
        cpoo[cmin - 1] = data
        cmin = cmin - 1
        cpos = cpos + #data
        return cpos
    end

    local function cpool_align(n)
        cpool_add_raw(rep('\0', (-cpos) % n))
    end

    -- store a uint array in cpool; pick the smallest type to fit the values
    -- return an expression (string) to access the array in runtime
    local function cpool_add_uint_array(t, len)
        local v_max = 0
        for i = 0,len-1 do
            local v = t[i] or 0
            v_max = max(v, v_max)
        end
        local item_bits = v_max < 0x100 and 8 or
                          v_max < 0x10000 and 16 or 32
        local array_type = format('uint%d_t [?]', item_bits)
        local buf = ffi_new(array_type, len)
        for i = 0,len-1 do
            buf[i] = t[i] or 0
        end
        cpool_align(4)
        local cpos = cpool_add_raw(ffi_string(buf, ffi_sizeof(array_type, len)))
        if item_bits == 8 then
            return format('r.b2-%d', cpos), 8
        else
            return format('r.b2_%d-%d', item_bits, cpos/(item_bits/8)), item_bits
        end
    end

    -- add a string to cpool and return offset; don't add the same string twice
    local cpool_add_cache = {}
    function il.cpool_add(str)
        str = tostring(str)
        local res = cpool_add_cache[str]
        if res then return res end
        cpool_add_raw(str)
        cpool_add_cache[str] = cpos
        return cpos
    end

    function il.cpool_get_data()
        cpool_align(8)
        return concat(cpoo, '', cmin, cmax)
    end

    -- PUTENUMI2S handling
    -- We assume that several I2S-es may share the same translation table,
    -- hence the caching.
    local i2s_cache = {}
    function il.emit_putenumi2s(o, res, varmap)
        local tab  = il.get_extra(o)
        local emit = i2s_cache[tab]
        if not emit then
            local n, is_sparse, data = #tab, false, {}
            for i = 1, n do
                local str = tab[i]
                is_sparse = is_sparse or str == ''
                data[i*2 - 2] = #str
                data[i*2 - 1] = il.cpool_add(str)
            end
            local cdata = cpool_add_uint_array(data, n*2)
            emit = function(o, res, varmap)
                local pos = varref(o.ipv, o.ipo, varmap)
                insert(res, format([[
if r.v[%s].uval >= %d then rt_err_value(r, %s) end]],
                                   pos, n, pos))
                if is_sparse then
                    insert(res, format([[
if (%s)[r.v[%s].ival*2] == 0 then rt_err_value(r, %s, true) end]],
                                       cdata, pos, pos))
                end
                local output = varref(0, o.offset, varmap)
                insert(res, format([[
r.ot[%s] = 18; r.ov[%s].xlen = (%s)[r.v[%s].ival*2];
r.ov[%s].xoff = (%s)[r.v[%s].ival*2+1];]],
                                   output, output, cdata, pos,
                                   output, cdata, pos))
            end
            i2s_cache[tab] = emit
        end
        emit(o, res, varmap)
    end

    -- Compute data tables for PUTENUMS2I
    --
    -- <str> -(hash_fn)-> <any_int> -(phf_fn)-> index:0..m -(aux_table)-> res
    --
    -- First we build a perfect hash_func to map an input string to an int.
    -- The result is highly dispersed, so we build an integer phf to compress
    -- it into 0..m range (m is typically 1.1x the total number of entries).
    -- Finally, we add aux_table of m*3 elements filled with the triples:
    --   str_len, str_offset, v.
    local function putenums2i_prepare(tab)
        local seed = 0
        local n = 0
        local s = {}
        local n = 0
        local v_max = 0
        local is_sparse = false
        for k, v in pairs(tab) do
            is_sparse = is_sparse or v == -1
            v_max = max(v, v_max)
            s[n] = k
            n = n + 1
        end
        local hash_func = 0
        if il.enable_fast_strings then
            local _s = ffi_new('const char * [?]', n)
            for i = 0, n-1 do
                _s[i] = s[i]
            end
            hash_func = rt_C.create_hash_func(n, _s, random_bytes,
                                              #random_bytes)
        end
        assert(hash_func ~= 0) -- fixme
        -- only use phf if enum is large
        local phf = n > il.phf_threshold and
                    ffi.gc(ffi_new('struct schema_rt_phf'),
                           rt_C.phf_destroy)
        local h = ffi_new('int32_t[?]', n)
        for i = 0, n-1 do
            h[i] = rt_C.eval_hash_func(hash_func, s[i], #s[i])
        end
        local eval_phf_func, m
        if phf then
            local res = rt_C.phf_init_uint32(phf, h, n, 4, 90, seed, 1)
            if res ~= 0 then error('internal error: phf: '..res) end
            rt_C.phf_compact(phf)
            local g_width = byte('#\1#\2#\4', phf.g_op) -- 2:int8 4:int16 6:int32
            cpool_align(4)
            local g_offset = cpool_add_raw(ffi_string(phf.g, phf.r*(g_width)))
            local r
            r, m = tonumber(phf.r), tonumber(phf.m)
            eval_phf_func = format([[
t = rt_C.phf_hash_uint32_band_raw%d(r.b2-%d, t, %d, %d, %d)]],
                                   g_width*8, g_offset, seed, r, m)
        else
            local tab, tab_bits = cpool_add_uint_array(h, n)
            m, eval_phf_func = n, format('t = rt_C.schema_rt_search%d(%s, t, %d)',
                                         tab_bits, tab, n)
        end
        local aux_table = {}
        for i = 0, n-1 do
            local str = s[i]
            local v   = tab[str]
            local index = phf and rt_C.phf_hash_uint32(phf, h[i]) or i
            aux_table[index*3    ] = #str
            aux_table[index*3 + 1] = il.cpool_add(str)
            aux_table[index*3 + 2] = v == -1 and v_max + 1 or v
        end
        return hash_func, eval_phf_func, is_sparse and v_max,
               cpool_add_uint_array(aux_table, m*3)
    end

    -- PUTENUMS2I handling
    -- We assume that several S2I-s may share the same translation table,
    -- hence the cache.
    local s2i_cache = {}
    function il.emit_putenums2i(o, res, varmap)
        local tab  = il.get_extra(o)
        local emit = s2i_cache[tab]
        if not emit then
            local hash_func, eval_phf_func, v_max, aux_table =
                putenums2i_prepare(tab)
            emit = function(o, res, varmap)
                local pos = varref(o.ipv, o.ipo, varmap)
                emit_compute_hash_func(hash_func, pos, res)
                insert(res, eval_phf_func)
                insert(res, format([[
if rt_C.schema_rt_key_eq(r.b2-(%s)[t*3+1], r.b1-r.v[%s].xoff, (%s)[t*3], r.v[%s].xlen) ~= 0 then
    rt_err_value(r, %s)
end]], aux_table, pos, aux_table, pos, pos))
                if v_max then
                    insert(res, format([[
if (%s)[t*3+2] > %d then
    rt_err_value(r, %s, true)
end]], aux_table, v_max, pos))
                end
                local output = varref(0, o.offset, varmap)
                insert(res, format([[
r.ot[%s] = 4; r.ov[%s].ival = (%s)[t*3+2];]],
                                   output, output, aux_table))
            end
            s2i_cache[tab] = emit
        end
        emit(o, res, varmap)
    end

    function il.emit_lua_func(func, res, opts)
        return emit_func(il, func, res, opts)
    end
    
    function il.append_lua_code(code, res)
        local varmap = {}
        for i = 1, #code do
            emit_instruction(il, code[i], res, varmap)
        end
    end

    il.enable_loop_peeling = (opts.enable_loop_peeling ~= false)
    il.enable_fast_strings = (opts.enable_fast_strings ~= false)
    il.phf_threshold       = (opts.phf_threshold or 8)

    return il
end

return {
    install = install_backend
}
