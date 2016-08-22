local random_bytes   = require('digest').base64_decode([[
X1CntcveBDc4inHKyWfyXw5iCjg1f3TmeX88pZ2Galb9tXpTt1uBO0pZrkU5NW1/4Ki9g8fAwElq
B3dRsBscsg==]])

local ffi = require('ffi')
local opcode = ffi.new('struct schema_il_Opcode')
local rt             = require('avro_schema.runtime')

local insert       = table.insert
local rt_C         = ffi.load(rt.C_path)
local band, rshift = bit.band, bit.rshift
local ffi_new      = ffi.new

local size_t = uint64

local RtState = terralib.types.newstruct('schema_rt_State')

local struct schema_rt_Value_ {
    union {
        ival : int64;
        uval : uint64;
        dval : double;
        sval : uint32[2];
    }
}

-- RtState vs. RtState_: avoid warning due to redifinition of a ffi struct
local struct schema_rt_State_ {
    t_capacity   : size_t;
    ot_capacity  : size_t;
    res_capacity : size_t;
    res_size     : size_t;
    res          : &uint8;
    b1           : &uint8;
    b2           : &uint8;
    t            : &uint8;
    v            : &schema_rt_Value_;
    ot           : &uint8;
    ov           : &schema_rt_Value_;
    k            : int32;
    jmp_buf      : intptr[8];
}
local RtState_ = schema_rt_State_

terralib.linklibrary(require('avro_schema.runtime').C_path)

local rt_setjmp, rt_err_type, rt_err_length, rt_err_missing
local rt_err_duplicate, rt_err_value
do
    -- currently it's the only way to attach function attributes in Terra
    -- CAVEATS: (1) depends on clang;
    --          (2) intptr_t incompatible definition;
    --          (3) 'struct Foo' in includec is a unique type
    --              (incompatible with struct Foo in Terra.)
    local M = terralib.includecstring([[
        #include <stdint.h>
        int schema_rt_setjmp(void *) __attribute__((returns_twice));
        void schema_rt_err_type_eh(void *, uint32_t, int)
            __attribute__((noreturn));
        void schema_rt_err_length_eh(void *, uint32_t, uint32_t)
            __attribute__((noreturn));
        void schema_rt_err_missing_eh(void *, uint32_t, uint8_t *)
            __attribute__((noreturn));
        void schema_rt_err_duplicate_eh(void *, uint32_t)
            __attribute__((noreturn));
        void schema_rt_err_value_eh(void *, uint32_t, int)
            __attribute__((noreturn));
    ]])
    rt_setjmp = macro(function (p)
        return `M.schema_rt_setjmp([&opaque](&(p.jmp_buf)))
    end)
    rt_err_type = macro(function (p, pos, op)
        return quote M.schema_rt_err_type_eh([&RtState_](p), pos, op) end
    end)
    rt_err_length = macro(function (p, pos, elen)
        return quote M.schema_rt_err_length_eh([&RtState_](p), pos, elen) end
    end)
    rt_err_missing = macro(function (p, pos, name)
        return quote M.schema_rt_err_missing_eh([&RtState_](p), pos, name) end
    end)
    rt_err_duplicate = macro(function (p, pos)
        return quote M.schema_rt_err_duplicate_eh([&RtState_](p), pos) end
    end)
    rt_err_value = macro(function (p, pos, is_ver_err)
        return quote
            M.schema_rt_err_value_eh([&RtState_](p), pos, [is_ver_err])
        end
    end)
end

local rt_key_eq = terralib.externfunction(
    'schema_rt_key_eq', {&uint8,&uint8,size_t,size_t}->int)
local rt_fnv1a = terralib.externfunction(
    'eval_fnv1a_func', {uint32, &uint8, size_t}->uint32)
local rt_xflatten_done = terralib.externfunction(
    'schema_rt_xflatten_done', {&RtState_, size_t} -> size_t)
local rt_buf_grow = terralib.externfunction(
    'schema_rt_buf_grow_eh', {&RtState_, size_t} -> niltype)

local false_or_true   = terralib.constant('\0\0\1\1\0\0\0\0\0\0\0\0\0')
local float_or_double = terralib.constant('\0\0\0\0\0\0\1\1\0\0\0\0\0')
local array_or_map    = terralib.constant('\0\0\0\0\0\0\0\0\0\0\0\1\1')
local null_or_map     = terralib.constant('\0\1\0\0\0\0\0\0\0\0\0\0\1')

local ival = macro(function(v) return `v.ival end)
local uval = macro(function(v) return `v.uval end)
local dval = macro(function(v) return `v.dval end)
local xlen = macro(function(v) return `v.sval[0] end)
local xoff = macro(function(v) return `v.sval[1] end)

local function compute_hash(hfunc, dest, str, len)
    if band(hfunc, 0xf0000000) ~= 0 then
        return quote dest = rt_fnv1a(hfunc, str, len) end
    end
    local a = rshift(hfunc, 24)
    local b = band(0xff, rshift(hfunc, 16))
    local c = band(0xff, rshift(hfunc,  8))
    local d = band(0xff, hfunc)
    local expr
    if     a == 0x1 then  expr = `str[b]
    elseif a == 0x2 then  expr = `str[b] + str[c]
    elseif a == 0x3 then  expr = `str[b] + str[c] + str[d]
    elseif a == 0x4 then  expr = `len
    elseif a == 0x5 then  expr = `len + str[b]
    elseif a == 0x6 then  expr = `len + str[b] + str[c]
    elseif a == 0x7 then  expr = `len + str[b] + str[c] + str[d]
    elseif a == 0x9 then  expr = `str[b]
    elseif a == 0xa then  expr = `(str[b] <<  8) or str[c]
    elseif a == 0xb then  expr = `(str[b] << 16) or (str[c] << 8) or str[d]
    elseif a == 0xc then  expr = `len
    elseif a == 0xd then  expr = `(len <<  8) or str[a]
    elseif a == 0xe then  expr = `(len << 16) or (str[a] <<  8) or str[b]
    elseif a == 0xf then  expr = `(len << 24) or (str[a] << 16) or (str[b] << 8) or str[d]
    else assert(false)
    end
    if band(a, 0x3) == 0 then
        return quote dest = expr end -- no samples from the string
    else
        local idx_max = band(0xff, rshift(hfunc, 8*(3-band(a, 0x3))))
        return quote
            if len > [idx_max] then dest = expr end
        end
    end
end

local tab = {
    ----------------------- T
    [opcode.PUTNULC    ] =  1,
    [opcode.PUTDUMMYC  ] = 17,
    [opcode.PUTARRAYC  ] = 11,
    [opcode.PUTMAPC    ] = 12,
    [opcode.PUTFLOATC  ] =  6,
    [opcode.PUTDOUBLEC ] =  7,
    [opcode.PUTSTRC    ] = 18,
    [opcode.PUTBINC    ] = 19,
    [opcode.PUTXC      ] = 20,
    ------------------------- T, tofield, fromfield
    [opcode.PUTINT     ] = { t= 4, to=ival, from=ival },
    [opcode.PUTLONG    ] = { t= 4, to=ival, from=ival },
    [opcode.PUTFLOAT   ] = { t= 6, to=dval, from=dval },
    [opcode.PUTDOUBLE  ] = { t= 7, to=dval, from=dval },
    [opcode.PUTSTR     ] = { t= 8, to=uval, from=uval },
    [opcode.PUTBIN     ] = { t= 9, to=uval, from=uval },
    [opcode.PUTARRAY   ] = { t=11, to=xlen, from=xlen },
    [opcode.PUTMAP     ] = { t=12, to=xlen, from=xlen },
    [opcode.PUTINT2LONG] = { t= 4, to=ival, from=ival },
    [opcode.PUTINT2FLT ] = { t= 6, to=dval, from=ival },
    [opcode.PUTINT2DBL ] = { t= 7, to=dval, from=ival },
    [opcode.PUTLONG2FLT] = { t= 6, to=dval, from=ival },
    [opcode.PUTLONG2DBL] = { t= 7, to=dval, from=ival },
    [opcode.PUTFLT2DBL ] = { t= 7, to=dval, from=dval },
    [opcode.PUTSTR2BIN ] = { t= 9, to=uval, from=uval },
    [opcode.PUTBIN2STR ] = { t= 8, to=uval, from=uval },
    ----------------------- T
    [opcode.ISBOOL     ] = false_or_true,
    [opcode.ISNULORMAP ] = null_or_map,
    ----------------------- T
    [opcode.ISNUL      ] =  1,
    [opcode.ISLONG     ] =  4,
    [opcode.ISSTR      ] =  8,
    [opcode.ISBIN      ] =  9,
    [opcode.ISARRAY    ] = 11,
    [opcode.ISMAP      ] = 12
}

local function compile(args, il, il_code, n)
    -- init prototypes
    local by_ordinal, by_name = {}, {}
    for i, func in ipairs(il_code) do
        local terra tfunc :: {&RtState_, uint32, uint32} -> {uint32, uint32}
        by_ordinal[i], by_name[func[1].name] = tfunc, tfunc 
    end
    -- string pool
    local str_pool_bits = {}
    local str_pool_first_idx = 10000001
    local str_pool_last_idx  = 10000000
    local str_pool_pos = 0
    local str_pool_cache = { '' }
    local function str_pool_put(str)
        local cached = str_pool_cache[str]
        if cached then return cached end
        local idx = str_pool_first_idx - 1
        str_pool_bits[idx] = str
        str_pool_first_idx = idx
        local pos = str_pool_pos + #str
        str_pool_pos = pos + 1; str_pool_cache[str] = pos
        return pos
    end
    local terra str_pool_get :: {int} -> &uint8
    -- translate functions
    local function ipvref(I, scope)
        return `[ scope[I.ipv] ] + [I.ipo]
    end
    local function do_block_in_scope(block, scope)
        local S = terralib.newlist()
        local r, v0, _ = scope.r, scope[0], scope._
        local r_t, r_ot = scope.r_t, scope.r_ot
        local r_b1 = scope.r_b1
        local r_v, r_ov = scope.r_v, scope.r_ov
        local skip_till = 2
        for i,I in ipairs(block) do
            local nested_block
            if type(I) == 'table' then I, nested_block = I[1], I end
            local op = I.op
            if i < skip_till or op == opcode.ENDVAR then
                -- skip it
            elseif op == opcode.CALLFUNC then
                local v, rv = ipvref(I, scope), scope[I.ripv]
                if I.k ~= 0 then insert(S, quote r.k = r.k + I.k end) end
                insert(S, quote
                    v0, rv = [ by_name[il.get_extra(I)] ] (r, v0, v)
                    r_ot, r_ov = r.ot, r.ov
                end)
                if I.k ~= 0 then insert(S, quote r.k = r.k - I.k end) end
            elseif op == opcode.MOVE then
                insert(S, quote [ scope[I.ripv] ] = [ ipvref(I, scope) ] end)
            elseif op == opcode.SKIP then
                local v, rv = ipvref(I, scope), scope[I.ripv]
                insert(S, quote rv = v + xoff(r_v[v]) end)
            elseif op == opcode.PSKIP then
                local v, rv = ipvref(I, scope), scope[I.ripv]
                insert(S, quote
                    rv = v + 1 + (xoff(r_v[v]) - 1)*array_or_map[r_t[v]]
                end)
        -----------------------------------------------------------
            elseif op == opcode.PUTBOOLC then
                insert(S, quote
                    r_ot[ v0 + [I.offset] ] = [ I.ci == 0 and 2 or 3 ]
                end)
            elseif op == opcode.PUTINTC then
                local pos = `v0 + [I.offset]
                insert(S, quote r_ot[pos] = 4; r_ov[pos].ival = [I.ci] end)
            elseif op == opcode.PUTINTKC then
                local pos = `v0 + [I.offset]
                insert(S, quote r_ot[pos] = 4; r_ov[pos].ival = r.k + [I.ci] end)
            elseif op == opcode.PUTARRAYC or op == opcode.PUTMAPC then
                local pos = `v0 + [I.offset]
                insert(S, quote
                    r_ot[pos] = [tab[op]]; xlen(r_ov[pos]) = [I.ci]
                end)
            elseif op == opcode.PUTLONGC  then
                local pos = `v0 + [I.offset]
                insert(S, quote r_ot[pos] = 4; r_ov[pos].ival = [I.cl] end)
            elseif op == opcode.PUTFLOATC or op == opcode.PUTDOUBLEC then
                local pos = `v0 + [I.offset]
                insert(S, quote
                    r_ot[pos] = [tab[op]]; r_ov[pos].dval = [I.cd]
                end)
            elseif op == opcode.PUTNULC or op == opcode.PUTDUMMYC then
                insert(S, quote r_ot[v0 + [I.offset]] = [tab[op]] end)
            elseif op == opcode.PUTSTRC or op == opcode.PUTBINC or
                   op == opcode.PUTXC then
                local pos, data = `v0 + [I.offset], il.get_extra(I)
                insert(S, quote
                    r_ot[pos] = [tab[op]]; xlen(r_ov[pos]) = [#data]
                    xoff(r_ov[pos]) = [ str_pool_put(data) ]
                end)
        -----------------------------------------------------------
            elseif op == opcode.PUTBOOL   then
                insert(S, quote
                    r_ot[ v0 + [I.offset] ] = r_t[ [ipvref(I, scope)] ]
                end)
            elseif op >= opcode.PUTINT and op <= opcode.PUTBIN2STR then
                local opts = tab[op]
                insert(S, quote
                    r_ot[ v0 + [I.offset] ] = [ opts.t ]
                    opts.to(r_ov[ v0 + [I.offset] ]) =
                        opts.from(r_v[ [ipvref(I,scope)] ])
                end)
            elseif op == opcode.PUTENUMI2S then
                -- XXX
                local pos = `v0 + [I.offset]
                insert(S, quote
                    r_ot[pos] = 8; xoff(r_ov[pos]) = 0; xlen(r_ov[pos]) = 0
                end)
            elseif op == opcode.PUTENUMS2I then
                -- XXX
                local pos = `v0 + [I.offset]
                insert(S, quote r_ot[pos] = 4; r_ov[pos].ival = 0 end)
        -----------------------------------------------------------
            elseif op == opcode.ISBOOL or op == opcode.ISNULORMAP then
                local v = ipvref(I, scope)
                insert(S, quote
                    if [ tab[op] ][r_t[v]] == 0 then rt_err_type(r, v, op) end
                end)
            elseif op == opcode.ISINT then
                local v = ipvref(I, scope)
                insert(S, quote
                    if r_t[v] ~= 4 or r_v[v].uval+0x80000000 > 0xffffffff then
                        rt_err_type(r, v, op)
                    end
                end)
            elseif op == opcode.ISFLOAT or op == opcode.ISDOUBLE then
                local v = ipvref(I, scope)
                insert(S, quote
                    if float_or_double[r_t[v]] == 0 then
                        if r.t[v] == 4 then -- int->double
                            r_v[v].dval = r_v[v].ival
                        else
                            rt_err_type(r, v, op)
                        end
                    end
                end)
            elseif op >= opcode.ISLONG and op <= opcode.ISNUL then
                local v, tc = ipvref(I, scope), tab[op]
                insert(S, quote
                    if r_t[v] ~= tc then rt_err_type(r, v, op) end
                end)
            elseif op == opcode.LENIS then
                local pos = ipvref(I, scope)
                insert(S, quote
                    if xlen(r_v[pos]) ~= [I.len] then
                        rt_err_length(r, pos, [I.len])
                    end
                end)
            elseif op == opcode.ISNOTSET then
                local pos = ipvref(I, scope)
                insert(S, quote
                    if [scope[I.ipv]]~=0 then
                        rt_err_duplicate(r, pos)
                    end
                end)
        -----------------------------------------------------------
            elseif op == opcode.BEGINVAR then
                local v = terralib.newsymbol(uint32)
                scope[I.ipv] = v
                insert(S, quote var [v] = 0 end)
        -----------------------------------------------------------
            elseif op == opcode.CHECKOBUF then
                local slots_min
                if I.ipv == opcode.NILREG then
                    slots_min = `v0 + [I.offset]
                else
                    local v = ipvref(I, scope)
                    slots_min = `v0 + [I.offset] + xlen(r_v[v]) * [I.scale]
                end
                insert(S, quote
                    if slots_min > r.ot_capacity then
                        rt_buf_grow(r, slots_min); r_ot, r_ov = r.ot, r.ov
                    end
                end)
        -----------------------------------------------------------
            elseif op == opcode.ERRVALUEV then
                insert(S, quote rt_err_value(r, [ipvref(I,scope)], 1) end)
            elseif op == opcode.ISSET     then
                insert(S, quote
                    if [scope[I.ripv]] == 0 then
                        rt_err_missing(r, [ipvref(I,scope)],
                            str_pool_get([str_pool_put(il.get_extra(I))]))
                    end
                end)
        -----------------------------------------------------------
            elseif op == opcode.IFSET or op == opcode.IFNUL then
                local cond
                if op == opcode.IFSET then
                    cond = `[scope[I.ipv]] ~= 0
                else
                    cond = `r_t[ [ipvref(I,scope)] ] == 1
                end
                local branches = {terralib.newlist(), terralib.newlist()}
                for i = 2,3 do
                    local b = nested_block[i]
                    if b then
                        local b_head = b[1]
                        assert(b_head.op == opcode.IBRANCH)
                        branches[b_head.ci == 0 and 2 or 1] =
                            do_block_in_scope(b, scope)
                    end
                end
                insert(S, quote
                    if [cond] then [branches[1]] else [branches[2]] end
                end)
        -----------------------------------------------------------
            elseif op == opcode.INTSWITCH then
                local branch1 = nested_block[2]
                assert(branch1, '!'); assert(branch1[1].op == opcode.IBRANCH)
                local v, value = ipvref(I, scope), terralib.newsymbol(int64)
                local dispatch = quote
                    if value == [branch1[1].ci] then
                        [ do_block_in_scope(branch1, scope) ]
                    else
                        rt_err_value(r, v, 0)
                    end
                end
                for i = 3, #nested_block do
                    local branchN = nested_block[i]
                    assert(branchN[1].op == opcode.IBRANCH)
                    dispatch = quote
                        if value == [branchN[1].ci] then
                            [ do_block_in_scope(branchN, scope) ]
                        else
                            [ dispatch ]
                        end
                    end
                end
                insert(S, quote var [value] = r_v[v].ival; [dispatch] end)
        -----------------------------------------------------------
            elseif op == opcode.STRSWITCH then
                local strings = ffi_new('const char *[?]', #nested_block)
                for i = 2, #nested_block do
                    local branch = nested_block[i]
                    local branch_head = branch[1]
                    assert(branch_head.op == opcode.SBRANCH)
                    local str = il.get_extra(branch_head)
                    strings[i - 2] = str
                end
                local hfunc = rt_C.create_hash_func(#nested_block - 1, strings,
                                                    random_bytes,
                                                    #random_bytes)
                assert(hfunc ~= 0)
                local str = terralib.newsymbol(&uint8)
                local len = terralib.newsymbol(uint32)
                local hv, v = terralib.newsymbol(uint32), ipvref(I, scope)
                local branch1 = nested_block[2]
                assert(branch1, '!'); assert(branch1[1].op == opcode.SBRANCH)
                local str1 = il.get_extra(branch1[1])
                local dispatch = quote
                    if hv == [rt_C.eval_hash_func(hfunc, str1, #str1)] then
                        if rt_key_eq(str,
                                     str_pool_get([str_pool_put(str1)]),
                                     len, [#str1]) ~= 0 then
                            rt_err_value(r, v, 0)
                        end
                        [ do_block_in_scope(branch1, scope) ]
                    else
                        rt_err_value(r, v, 0)
                    end
                end
                for i = 3, #nested_block do
                    local branchN = nested_block[i]
                    assert(branchN[1].op == opcode.SBRANCH)
                    local strN = il.get_extra(branchN[1])
                    dispatch = quote
                        if hv == [rt_C.eval_hash_func(hfunc, strN, #strN)] then
                            if rt_key_eq(str,
                                         str_pool_get([str_pool_put(strN)]),
                                         len, [#strN]) ~= 0 then
                                rt_err_value(r, v, 0)
                            end
                            [ do_block_in_scope(branchN, scope) ]
                        else
                            [ dispatch ]
                        end
                    end
                end
                insert(S, quote
                    var [str], [len] = r_b1 - xoff(r_v[v]), xlen(r_v[v])
                    var [hv] = 0
                    [ compute_hash(hfunc, hv, str, len) ]
                    [ dispatch ]
                end)
        -----------------------------------------------------------
            elseif op == opcode.OBJFOREACH then
                local pos, rv = ipvref(I, scope), scope[I.ripv]
                local next_obj_pos = terralib.newsymbol(uint32)
                insert(S, quote
                    rv = pos + 1
                    var [next_obj_pos] = pos + xoff(r_v[pos])
                    while rv < next_obj_pos do
                        [ do_block_in_scope(nested_block, scope) ]
                        rv = rv + [I.step]
                    end
                end)
                -- fuse skip
                for lookahead = i+1, i+5 do
                    local following_I = block[lookahead]
                    if type(following_I) ~= 'cdata' then
                        break
                    elseif following_I.op == opcode.SKIP then
                        if following_I.ipv == I.ipv and
                           following_I.ipo == I.ipo then
                            insert(S, quote
                                [scope[following_I.ripv]] = next_obj_pos
                            end)
                            skip_till = lookahead+1
                        end
                        break
                    elseif following_I.op ~= opcode.ENDVAR then
                        break
                    end
                end
        -----------------------------------------------------------
            else
                assert(false, 'Unhandled: '..il.opcode_vis(I))
            end
        end
        return S
    end
    for i, func in ipairs(il_code) do
        local r    = terralib.newsymbol(&RtState_)
        local v0   = terralib.newsymbol(uint32)
        local v1   = terralib.newsymbol(uint32)
        local _    = terralib.newsymbol(uint32)
        local r_t  = terralib.newsymbol(&uint8)
        local r_v  = terralib.newsymbol(&schema_rt_Value_)
        local r_b1 = terralib.newsymbol(&uint8)
        local r_ot = terralib.newsymbol(&uint8)
        local r_ov = terralib.newsymbol(&schema_rt_Value_)
        local scope = {}
        scope.r, scope[0], scope[func[1].ipv], scope[opcode.NILREG] = r, v0, v1, _
        local r_ot = terralib.newsymbol(&uint8)
        local scope = {}
        scope.r, scope[0], scope[func[1].ipv], scope[opcode.NILREG] = r, v0, v1, _
        scope.r_t, scope.r_v, scope.r_ot, scope.r_ov = r_t, r_v, r_ot, r_ov
        scope.r_b1 = r_b1
        by_ordinal[i]:resetdefinition(terra ([r], [v0], [v1])
            var [_]
            var [r_t], [r_ot] = r.t, r.ot
            var [r_b1] = r.b1
            var [r_v], [r_ov] = r.v, r.ov
            [ do_block_in_scope(func, scope) ]
            return v0, v1
        end)
    end
    -- finalize string pool
    local str_pool = table.concat(str_pool_bits, '\0',
                                  str_pool_first_idx,
                                  str_pool_last_idx)
    str_pool_get:resetdefinition(terra (idx : int)
        return [&uint8]([ terralib.constant(str_pool) ]) + [ #str_pool ] - idx
    end)
    -- entry points
    by_ordinal[1]:setinlined(true) -- no one uses them, except EP
    by_ordinal[2]:setinlined(true)
    by_ordinal[3]:setinlined(true)
    local flatten = terra (p: &RtState)
        var p = [&RtState_](p);
        if rt_setjmp(p) ~= 0 then return 0 end
        p.b2 = str_pool_get(0)
        var v0, _ = [ by_ordinal[1] ] (p, 0, 0)
        return v0
    end
    local unflatten = terra (p: &RtState)
        var p = [&RtState_](p);
        if rt_setjmp(p) ~= 0 then return 0 end
        p.b2 = str_pool_get(0)
        var v0, _ = [ by_ordinal[2] ] (p, 0, 0)
        return v0
    end
    local xflatten = terra (p: &RtState)
        var p = [&RtState_](p);
        if rt_setjmp(p) ~= 0 then return 0 end
        p.k = n + 1
        p.b2 = str_pool_get(0)
        var v0, _ = [ by_ordinal[3] ] (p, 0, 0)
        return rt_xflatten_done(p, v0)
    end
    return {
        flatten = flatten, unflatten = unflatten, xflatten = xflatten,
        by_ordinal = by_ordinal
    }
end

return { compile = compile }
