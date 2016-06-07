local ffi            = require('ffi')
local json           = require('json')
local msgpack        = require('msgpack')
local rt             = require('avro_schema_rt')
local json_encode    = json and json.encode
local msgpack_decode = msgpack and msgpack.decode
local ffi_new        = ffi.new
local format, rep    = string.format, string.rep
local insert, remove = table.insert, table.remove
local concat         = table.concat

local rtval          = ffi.new('struct schema_rt_Value')

ffi.cdef([[
struct schema_il_Opcode {
    struct {
        uint16_t op;
        union {
            uint16_t scale;
            uint16_t step;
        };
    };
    union {
        uint32_t ripv;
        uint32_t offset;
        uint32_t name;
        uint32_t len;
        uint32_t func;
    };
    union {
        struct {
            uint32_t ipv;
            uint32_t ipo;
        };
        int32_t  ci;
        int64_t  cl;
        double   cd;
        uint32_t cref;
    };

    // block
    static const int CALLFUNC    = 0xc0;
    static const int DECLFUNC    = 0xc1;
    static const int IBRANCH     = 0xc2;
    static const int SBRANCH     = 0xc3;
    static const int IFSET       = 0xc4; // c5, c6 reserved
    static const int STRSWITCH   = 0xc7;
    // ripv ipv ipo
    static const int OBJFOREACH  = 0xc8; // last block
    static const int MOVE        = 0xc9;
    static const int SKIP        = 0xca;
    static const int PSKIP       = 0xcb;

    static const int PUTBOOLC    = 0xcc;
    static const int PUTINTC     = 0xcd;
    static const int PUTLONGC    = 0xce;
    static const int PUTFLOATC   = 0xcf;
    static const int PUTDOUBLEC  = 0xd0;
    static const int PUTSTRC     = 0xd1;
    static const int PUTBINC     = 0xd2;
    static const int PUTARRAYC   = 0xd3;
    static const int PUTMAPC     = 0xd4;
    static const int PUTXC       = 0xd5;
    static const int PUTNULC     = 0xd6;

    static const int PUTBOOL     = 0xd7;
    static const int PUTINT      = 0xd8;
    static const int PUTLONG     = 0xd9;
    static const int PUTFLOAT    = 0xda;
    static const int PUTDOUBLE   = 0xdb;
    static const int PUTSTR      = 0xdc;
    static const int PUTBIN      = 0xdd;
    static const int PUTARRAY    = 0xde;
    static const int PUTMAP      = 0xdf;
    static const int PUTINT2LONG = 0xe0;
    static const int PUTINT2FLT  = 0xe1;
    static const int PUTINT2DBL  = 0xe2;
    static const int PUTLONG2FLT = 0xe3;
    static const int PUTLONG2DBL = 0xe4;
    static const int PUTFLT2DBL  = 0xe5;
    static const int PUTSTR2BIN  = 0xe6;
    static const int PUTBIN2STR  = 0xe7;

    /* rt.err_type depends on these values */
    static const int ISBOOL      = 0xe8;
    static const int ISINT       = 0xe9;
    static const int ISFLOAT     = 0xea;
    static const int ISDOUBLE    = 0xeb;
    static const int ISLONG      = 0xec;
    static const int ISSTR       = 0xed;
    static const int ISBIN       = 0xee;
    static const int ISARRAY     = 0xef;
    static const int ISMAP       = 0xf0;
    static const int ISNUL       = 0xf1;

    static const int LENIS       = 0xf2;

    static const int ISSET       = 0xf3;
    static const int ISNOTSET    = 0xf4;    
    static const int BEGINVAR    = 0xf5;
    static const int ENDVAR      = 0xf6;

    static const int CHECKOBUF   = 0xf7;
    static const int ISSETLABEL  = 0xff;

    static const unsigned NILREG  = 0xffffffff;
};

struct schema_il_V {
    union {
        uint64_t     raw;
        struct {
            uint32_t gen    :30;
            uint32_t islocal:1;
            uint32_t isdead :1;
            uint32_t inc;
        };
    };
};

]])

local opcode = ffi_new('struct schema_il_Opcode')

local op2str = {
    [opcode.CALLFUNC   ] = 'CALLFUNC   ',   [opcode.DECLFUNC   ] = 'DECLFUNC   ',
    [opcode.IBRANCH    ] = 'IBRANCH    ',   [opcode.SBRANCH    ] = 'SBRANCH    ',
    [opcode.IFSET      ] = 'IFSET      ',   [opcode.STRSWITCH  ] = 'STRSWITCH  ',
    [opcode.OBJFOREACH ] = 'OBJFOREACH ',   [opcode.MOVE       ] = 'MOVE       ',
    [opcode.SKIP       ] = 'SKIP       ',   [opcode.PSKIP      ] = 'PSKIP      ',
    [opcode.PUTBOOLC   ] = 'PUTBOOLC   ',   [opcode.PUTINTC    ] = 'PUTINTC    ',
    [opcode.PUTLONGC   ] = 'PUTLONGC   ',   [opcode.PUTFLOATC  ] = 'PUTFLOATC  ',
    [opcode.PUTDOUBLEC ] = 'PUTDOUBLEC ',   [opcode.PUTSTRC    ] = 'PUTSTRC    ',
    [opcode.PUTBINC    ] = 'PUTBINC    ',   [opcode.PUTARRAYC  ] = 'PUTARRAYC  ',
    [opcode.PUTMAPC    ] = 'PUTMAPC    ',   [opcode.PUTXC      ] = 'PUTXC      ',
    [opcode.PUTNULC    ] = 'PUTNULC    ',   [opcode.PUTBOOL    ] = 'PUTBOOL    ',
    [opcode.PUTINT     ] = 'PUTINT     ',   [opcode.PUTLONG    ] = 'PUTLONG    ',
    [opcode.PUTFLOAT   ] = 'PUTFLOAT   ',   [opcode.PUTDOUBLE  ] = 'PUTDOUBLE  ',
    [opcode.PUTSTR     ] = 'PUTSTR     ',   [opcode.PUTBIN     ] = 'PUTBIN     ',
    [opcode.PUTARRAY   ] = 'PUTARRAY   ',   [opcode.PUTMAP     ] = 'PUTMAP     ',
    [opcode.PUTINT2LONG] = 'PUTINT2LONG',   [opcode.PUTINT2FLT ] = 'PUTINT2FLT ',
    [opcode.PUTINT2DBL ] = 'PUTINT2DBL ',   [opcode.PUTLONG2FLT] = 'PUTLONG2FLT',
    [opcode.PUTLONG2DBL] = 'PUTLONG2DBL',   [opcode.PUTFLT2DBL ] = 'PUTFLT2DBL ',
    [opcode.PUTSTR2BIN ] = 'PUTSTR2BIN ',   [opcode.PUTBIN2STR ] = 'PUTBIN2STR ',
    [opcode.ISBOOL     ] = 'ISBOOL     ',   [opcode.ISINT      ] = 'ISINT      ',
    [opcode.ISLONG     ] = 'ISLONG     ',   [opcode.ISFLOAT    ] = 'ISFLOAT    ',
    [opcode.ISDOUBLE   ] = 'ISDOUBLE   ',   [opcode.ISSTR      ] = 'ISSTR      ',
    [opcode.ISBIN      ] = 'ISBIN      ',   [opcode.ISARRAY    ] = 'ISARRAY    ',
    [opcode.ISMAP      ] = 'ISMAP      ',   [opcode.ISNUL      ] = 'ISNUL      ',
    [opcode.LENIS      ] = 'LENIS      ',   [opcode.ISSET      ] = 'ISSET      ',
    [opcode.ISNOTSET   ] = 'ISNOTSET   ',   [opcode.BEGINVAR   ] = 'BEGINVAR   ',
    [opcode.ENDVAR     ] = 'ENDVAR     ',   [opcode.CHECKOBUF  ] = 'CHECKOBUF  ',
    [opcode.ISSETLABEL ] = 'ISSETLABEL '
}

local function opcode_new(op)
    local o = ffi_new('struct schema_il_Opcode')
    if op then o.op = op end
    return o
end

local opcode_NOP = opcode_new(opcode.MOVE); opcode_NOP.ripv = opcode.NILREG

local function opcode_ctor_ipv(op)
    return function(ipv)
        local o = ffi_new('struct schema_il_Opcode')
        o.op = op; o.ipv = ipv
        return o
    end
end

local function opcode_ctor_ipv_ipo(op)
    return function(ipv, ipo)
        local o = ffi_new('struct schema_il_Opcode')
        o.op = op; o.ipv = ipv; o.ipo = ipo
        return o
    end
end

local function opcode_ctor_ripv_ipv_ipo(op)
    return function(ripv, ipv, ipo)
        local o = ffi_new('struct schema_il_Opcode')
        o.op = op; o.ripv = ripv or opcode.NILREG
        o.ipv = ipv; o.ipo = ipo
        return o
    end
end

local function opcode_ctor_offset_ipv_ipo(op)
    return function(offset, ipv, ipo)
        local o = ffi_new('struct schema_il_Opcode')
        o.op = op; o.offset = offset; o.ipv = ipv; o.ipo = ipo
        return o
    end
end

local function opcode_ctor_offset_ci(op)
    return function(offset, ci)
        local o = ffi_new('struct schema_il_Opcode')
        o.op = op; o.offset = offset; o.ci = ci
        return o
    end
end

local il_methods = {
    nop = function() return opcode_NOP end,
    ----------------------------------------------------------------
    callfunc = function(func, ipv, ipo)
        local o = opcode_new(opcode.CALLFUNC)
        o.func = func; o.ipv = ipv; o.ipo = ipo
        return o
    end,
    ----------------------------------------------------------------
    declfunc = function(name, ipv)
        local o = opcode_new(opcode.DECLFUNC)
        o.name = name; o.ipv = ipv
        return o
    end,
    ibranch = function(ci)
        local o = opcode_new(opcode.IBRANCH)
        o.ci = ci
        return o
    end,
    ifset       = opcode_ctor_ipv         (opcode.IFSET),
    strswitch   = opcode_ctor_ipv_ipo     (opcode.STRSWITCH),
    objforeach  = opcode_ctor_ripv_ipv_ipo(opcode.OBJFOREACH),
    ----------------------------------------------------------------
    move        = opcode_ctor_ripv_ipv_ipo(opcode.MOVE),
    skip        = opcode_ctor_ripv_ipv_ipo(opcode.SKIP),
    pskip       = opcode_ctor_ripv_ipv_ipo(opcode.PSKIP),
    ----------------------------------------------------------------
    putboolc    = function(offset, cb)
        local o = opcode_new(opcode.PUTBOOLC)
        o.offset = offset; o.ci = cb and 1 or 0
        return o
    end,
    putintc     = opcode_ctor_offset_ci(opcode.PUTINTC),
    putlongc = function(offset, cl)
        local o = opcode_new(opcode.PUTLONGC)
        o.offset = offset; o.cl = cl
        return o
    end,
    putfloatc   = function(offset, cf)
        local o = opcode_new(opcode.PUTFLOATC)
        o.offset = offset; o.cd = cf
        return o
    end,
    putdoublec  = function(offset, cd)
        local o = opcode_new(opcode.PUTDOUBLEC)
        o.offset = offset; o.cd = cd
        return o
    end,
    putarrayc   = opcode_ctor_offset_ci(opcode.PUTARRAYC),
    putmapc     = opcode_ctor_offset_ci(opcode.PUTMAPC),
    putnulc = function(offset)
        local o = opcode_new(opcode.PUTNULC)
        o.offset = offset
        return o
    end,
    ----------------------------------------------------------------
    putbool     = opcode_ctor_offset_ipv_ipo(opcode.PUTBOOL),
    putint      = opcode_ctor_offset_ipv_ipo(opcode.PUTINT),
    putlong     = opcode_ctor_offset_ipv_ipo(opcode.PUTLONG),
    putfloat    = opcode_ctor_offset_ipv_ipo(opcode.PUTFLOAT),
    putdouble   = opcode_ctor_offset_ipv_ipo(opcode.PUTDOUBLE),
    putstr      = opcode_ctor_offset_ipv_ipo(opcode.PUTSTR),
    putbin      = opcode_ctor_offset_ipv_ipo(opcode.PUTBIN),
    putarray    = opcode_ctor_offset_ipv_ipo(opcode.PUTARRAY),
    putmap      = opcode_ctor_offset_ipv_ipo(opcode.PUTMAP),
    putint2long = opcode_ctor_offset_ipv_ipo(opcode.PUTINT2LONG),
    putint2flt  = opcode_ctor_offset_ipv_ipo(opcode.PUTINT2FLT),
    putint2dbl  = opcode_ctor_offset_ipv_ipo(opcode.PUTINT2DBL),
    putlong2flt = opcode_ctor_offset_ipv_ipo(opcode.PUTLONG2FLT),
    putlong2dbl = opcode_ctor_offset_ipv_ipo(opcode.PUTLONG2DBL),
    putflt2dbl  = opcode_ctor_offset_ipv_ipo(opcode.PUTFLT2DBL),
    putstr2bin  = opcode_ctor_offset_ipv_ipo(opcode.PUTSTR2BIN),
    putbin2str  = opcode_ctor_offset_ipv_ipo(opcode.PUTBIN2STR),
    ----------------------------------------------------------------
    isbool      = opcode_ctor_ipv_ipo(opcode.ISBOOL),
    isint       = opcode_ctor_ipv_ipo(opcode.ISINT),
    islong      = opcode_ctor_ipv_ipo(opcode.ISLONG),
    isfloat     = opcode_ctor_ipv_ipo(opcode.ISFLOAT),
    isdouble    = opcode_ctor_ipv_ipo(opcode.ISDOUBLE),
    isstr       = opcode_ctor_ipv_ipo(opcode.ISSTR),
    isbin       = opcode_ctor_ipv_ipo(opcode.ISBIN),
    isarray     = opcode_ctor_ipv_ipo(opcode.ISARRAY),
    ismap       = opcode_ctor_ipv_ipo(opcode.ISMAP),
    isnul       = opcode_ctor_ipv_ipo(opcode.ISNUL),
    ----------------------------------------------------------------
    lenis = function(ipv, ipo, len)
        local o = opcode_new(opcode.LENIS)
        o.ipv = ipv; o.ipo = ipo; o.len = len
        return o
    end,
    ----------------------------------------------------------------
    isset       = opcode_ctor_ripv_ipv_ipo(opcode.ISSET),
    isnotset    = opcode_ctor_ipv(opcode.ISNOTSET),
    beginvar    = opcode_ctor_ipv(opcode.BEGINVAR),
    endvar      = opcode_ctor_ipv(opcode.ENDVAR),
    ----------------------------------------------------------------
    checkobuf = function(offset, ipv, ipo, scale)
        local o = opcode_new(opcode.CHECKOBUF)
        o.offset = offset; o.ipv = ipv or opcode.NILREG
        o.ipo = ipo or 0; o.scale = scale or 1
        return o
    end
    ----------------------------------------------------------------
    -- sbranch, putstrc, putbinc, putxc and issetlabel are instance methods
}

-- visualize register
local function rvis(reg, inc)
    if reg == opcode.NILREG then
        return '_'
    elseif not inc or inc == 0 then
        return '$'..reg
    else
        return format('$%d+%d', reg, inc)
    end
end

-- visualize constant
local function cvis(cref, objs, decode)
    if objs then
        local c = objs[cref]
        if c then
            local ok, res = true, c
            if decode then
                ok, res = pcall(decode, c)
            end
            if ok then
                ok, res = pcall(json_encode, res)
            end
            if ok then
                return res
            end
        end
    end
    return format('#%d', cref)
end

-- visualize opcode
local function opcode_vis(o, objs)
    local opname = op2str[o.op]
    if o == opcode_NOP then
        return 'NOP'
    elseif o.op == opcode.CALLFUNC then
        return format('%s %d,\t%s', opname,
                      o.func, rvis(o.ipv, o.ipo))
    elseif o.op == opcode.DECLFUNC then
        return format('%s %d,\t%s', opname, o.name, rvis(o.ipv))
    elseif o.op == opcode.IBRANCH then
        return format('%s %d', opname, o.ci)
    elseif o.op == opcode.SBRANCH or o.op == opcode.ISSETLABEL then
        return format('%s %s', opname, cvis(o.cref, objs))
    elseif o.op == opcode.IFSET or (
           o.op >= opcode.ISNOTSET and o.op <= opcode.ENDVAR) then
        return format('%s %s', opname, rvis(o.ipv))
    elseif o.op == opcode.STRSWITCH or (
           o.op >= opcode.ISBOOL and o.op <= opcode.ISNUL) then
        return format('%s [%s]', opname, rvis(o.ipv, o.ipo))
    elseif o.op == opcode.OBJFOREACH then
        return format('%s %s,\t[%s],\t%d', opname, rvis(o.ripv), rvis(o.ipv, o.ipo), o.step)
    elseif o.op == opcode.MOVE or o.op == opcode.ISSET then
        return format('%s %s,\t%s', opname, rvis(o.ripv), rvis(o.ipv, o.ipo))
    elseif o.op == opcode.SKIP or o.op == opcode.PSKIP then
        return format('%s %s,\t[%s]', opname, rvis(o.ripv), rvis(o.ipv, o.ipo))
    elseif o.op == opcode.PUTBOOLC or o.op == opcode.PUTINTC or
           o.op == opcode.PUTARRAYC or o.op == opcode.PUTMAPC then
        return format('%s [%s],\t%d', opname, rvis(0, o.offset), o.ci)
    elseif o.op == opcode.PUTLONGC then
        return format('%s [%s],\t%s', opname, rvis(0, o.offset), o.cl)
    elseif o.op == opcode.PUTFLOATC or o.op == opcode.PUTDOUBLEC then
        return format('%s [%s],\t%f', opname, rvis(0, o.offset), o.cd)
    elseif o.op == opcode.PUTNULC then
        return format('%s [%s]', opname, rvis(0, o.offset))
    elseif o.op == opcode.PUTSTRC or o.op == opcode.PUTBINC then
        return format('%s [%s],\t%s', opname, rvis(0, o.offset), cvis(o.cref, objs))
    elseif o.op == opcode.PUTXC then
        return format('%s [%s],\t%s', opname, rvis(0, o.offset), cvis(o.cref, objs, msgpack_decode))
    elseif o.op >= opcode.PUTBOOL and o.op <= opcode.PUTBIN2STR then
        return format('%s [%s],\t[%s]', opname, rvis(0, o.offset), rvis(o.ipv, o.ipo))
    elseif o.op == opcode.LENIS then
        return format('%s [%s],\t%d', opname, rvis(o.ipv, o.ipo), o.len)
    elseif o.op == opcode.CHECKOBUF then
        return format('%s %s,\t[%s],\t%d', opname, rvis(0, o.offset), rvis(o.ipv, o.ipo), o.scale)
    else
        return format('<opcode: %d>', o.op)
    end
end

-- visualize IL code
local il_vis_helper
il_vis_helper = function(res, il, indentcache, level, object)
    if not object then -- pass
    elseif type(object) == 'table' then
        local start = 1
        if level ~= 0 or type(object[1]) ~= 'table' then
            il_vis_helper(res, il, indentcache, level, object[1])
            start = 2
            level = level + 1
        end
        for i = start, #object do
            il_vis_helper(res, il, indentcache, level, object[i])
        end
    else
        local indent = indentcache[level]
        if not indent then
            indent = '\n'..rep('  ', level)
            indentcache[level] = indent
        end
        insert(res, indent)
        insert(res, il.opcode_vis(object))
    end
end
local function il_vis(il, root)
    local res = {}
    il_vis_helper(res, il, {}, 0, root)
    if res[1] == '\n' then res[1] = '' end
    insert(res, '\n')
    return concat(res)
end

-- cleanup IL:
--  (*) fix incorrect lists nesting
--  (*) remove NOPS and MOVEs assigning to NILREG
--  (*) remove redundant IFs
local il_cleanup_helper
il_cleanup_helper = function(res, object)
    if type(object) == 'table' then
        local head = object[1]
        if type(head) == 'cdata' and
           head.op >= opcode.DECLFUNC and head.op <= opcode.OBJFOREACH then
            local nested = { head }
            for i = 2, #object do
                il_cleanup_helper(nested, object[i])
            end
            if nested[2] or head.op == opcode.DECLFUNC then
                insert(res, nested) -- keep nested block IFF it's not empty
            end
        else
            for i = 1, #object do
                il_cleanup_helper(res, object[i])
            end
        end
    elseif object.op < opcode.MOVE and object.op > opcode.PSKIP or
           object.ripv ~= opcode.NILREG then
        insert(res, object) -- elide NOPs (NOP is MOVE to NILREG)
    end
end
local function il_cleanup(object)
    local res = {}
    il_cleanup_helper(res, object)
    return res
end

-- === Basic optimization engine in less than 450 LOC. ===
--
-- Elide some MOVE $reg, $reg+offset instructions.
-- Combine COBs (CHECKOBUFs) and hoist them out of loops.
--
-- We track variable state in a 'scope' data structure.
-- A scope is a dict keyed by a variable name. Scopes are chained
-- via 'parent' key. The lookup starts with the outter-most scope
-- and walks the chain until the entry is found.
--
-- The root scope is created upon entering DECLFUNC. When optimiser
-- encounters a nested block, it adds another scope on top of the chain
-- and invokes itself recursively. Once the block is done, the child
-- scope is removed.
--
-- When a variable value is updated in a program (e.g. assignment),
-- the updated entry is created in the outter-most scope, shadowing
-- any entry in parent scopes. Once a block is complete, the outermost
-- scope has all variable changes captured. These changes are merged
-- with the parent scope.
--
-- Variable state is <gen, inc, islocal, isdead> tuple. Variables are
-- scoped lexically; if a variable is marked local it is skipped when
-- merging with the parent scope. Codegen may explicitly shorten variable
-- lifetime with a ENDVAR instruction. Once a variable is dead its value
-- is no longer relevant. Valid code MUST never reference a dead variable.
--
-- Note: ENDVAR affects the code following the instruction (in a depth-first
--       tree walk order.) E.g. if the first branch in a conditional
--       declares a variable dead, it affects subsequent branches as well.
--
-- Concerning gen and inc.
--
-- Gen is a generation counter. If a variable X is updated
-- in such a way that new(X) = old(X) + C, the instruction is elided
-- and C is added to inc while gen is unchanged. Those we call 'dirty'
-- variables. If a variable is updated in a different fashion, including
-- but not limited to copying another variable's value, i.e. X = Y, we
-- declare that a new value is unrelated to the old one. Gen is assigned
-- a new unique id, and inc is set to 0.
--
-- Surprisingly, this simple framework is powerful enough to determine
-- if a loop proceeds in fixed increments (capture variable state
-- at the begining of the loop body and at the end and compare gen-s.)

local function vlookup(scope, vid)
    if not scope then
        assert(false, format('$%d not found', vid)) -- assume correct code
    end
    local res = scope[vid]
    if res then
        if res.isdead == 1 then
            assert(false, format('$%d is dead', vid)) -- assume correct code
        end
        return res
    end
    return vlookup(scope.parent, vid)
end

local function vcreate(scope, vid)
    local v = scope[vid]
    if not v then
        v = ffi_new('struct schema_il_V')
        scope[vid] = v
    end
    return v
end

local function vsnapshot(scope, vid)
    local v = scope[vid]
    if v then
        local copy = ffi_new('struct schema_il_V')
        copy.raw = v.raw
        return copy
    else -- in outter scope, virtually immutable
        return vlookup(scope.parent, vid)
    end
end

-- 'execute' an instruction and update scope
local function vexecute(il, scope, o, res)
    assert(type(o)=='cdata')
    if o.op == opcode.MOVE and o.ripv == o.ipv then
       local vinfo = vlookup(scope, o.ipv)
       local vnewinfo = vcreate(scope, o.ipv)
       vnewinfo.gen = vinfo.gen
       vnewinfo.inc = vinfo.inc + o.ipo
       return
    end
    if o.op == opcode.BEGINVAR then
        local vinfo = vcreate(scope, o.ipv)
        vinfo.islocal = 1
        insert(res, o)
        return
    end
    if o.op == opcode.ENDVAR then
        local vinfo = vcreate(scope, o.ipv)
        vinfo.isdead = 1
        insert(res, o)
        return
    end
    local fixipo = 0
    if (o.op == opcode.CALLFUNC or
       o.op >= opcode.STRSWITCH and o.op <= opcode.PSKIP or
       o.op >= opcode.PUTBOOL and o.op <= opcode.ISSET or
       o.op == opcode.CHECKOBUF) and o.ipv ~= opcode.NILREG then

        local vinfo = vlookup(scope, o.ipv)
        fixipo = vinfo.inc
    end
    local fixoffset = 0
    if o.op >= opcode.PUTBOOLC and o.op <= opcode.PUTBIN2STR or
       o.op == opcode.CHECKOBUF then

        local vinfo = vlookup(scope, 0)
        fixoffset = vinfo.inc
    end
    -- spill $0
    if o.op == opcode.CALLFUNC then
        local vinfo = vlookup(scope, 0)
        if vinfo.inc > 0 then
            insert(res, il.move(0, 0, vinfo.inc))
        end
        vinfo = vcreate(scope, 0)
        vinfo.gen = il.id()
        vinfo.inc = 0
    end
    -- apply fixes
    o.ipo = o.ipo + fixipo
    o.offset = o.offset + fixoffset
    insert(res, o)
    if o.op >= opcode.OBJFOREACH and o.op <= opcode.PSKIP and
       o.ripv ~= opcode.NILREG then
        local vinfo = vcreate(scope, o.ripv)
        vinfo.gen = il.id()
        vinfo.inc = 0
    end
end

-- Merge branches (conditional or switch);
-- if branches had diverged irt $r, spill $r
-- (e.g. one did $1 = $1 + 2 while another didn't touch $1 at all)
-- Note: bscopes[1]/bblocks[1] are unused, indices start with 2.
local function vmergebranches(il, bscopes, bblocks)
    local parent = bscopes[2].parent
    local skip = { parent = true } -- vids to skip: dead or diverged
    local maybediverged = {}
    local diverged = {}
    local counters = {} -- how many branches have it, if < #total then diverged
    local nscopes = #bscopes
    for i = 2, nscopes do
        for vid, vinfo in pairs(bscopes[i]) do
            counters[vid] = (counters[vid] or 1) + 1
            if vinfo.islocal == 0 and not skip[vid] then
                local vother = maybediverged[vid]
                if vinfo.isdead == 1 then
                    skip[vid] = true
                    diverged[vid] = nil
                    maybediverged[vid] = nil
                    vcreate(parent, vid).isdead = 1 
                elseif not vother then
                    maybediverged[vid] = vinfo
                elseif vother.raw ~= vinfo.raw then
                    skip[vid] = true
                    maybediverged[vid] = nil
                    diverged[vid] = true
                end
            end
        end
    end
    for vid, vinfo in pairs(maybediverged) do
        if counters[vid] ~= nscopes then
            diverged[vid] = true
        else
            local vpinfo = vcreate(parent, vid)
            vpinfo.gen = vinfo.gen
            vpinfo.inc = vinfo.inc
        end
    end
    for vid, _ in pairs(diverged) do
        local vpinfo = vcreate(parent, vid)
        vpinfo.gen = il.id()
        vpinfo.inc = 0
        for i = 2, nscopes do
            local vinfo = bscopes[i][vid]
            if vinfo and vinfo.inc > 0 then
                insert(bblocks[i], il.move(vid, vid, vinfo.inc))
            end
        end
    end
    return diverged
end

-- Collect variables modified by the given code block.
local vvarschanged_cache = setmetatable({}, { __mode = 'k' })
local vvarschanged
vvarschanged = function(block)
    local res = vvarschanged_cache[block]
    if res then return res end
    assert(type(block) == 'table')
    res = {}
    local locals = {}
    for i = 1, #block do
        local item = block[i]
        if type(item) == 'table' then
            local nested = vvarschanged(item)
            for vid, _ in pairs(nested) do
                res[vid] = true
            end
        elseif item.op == opcode.CALLFUNC then
            res[0] = true
        elseif item.op >= opcode.OBJFOREACH and item.op <= opcode.PSKIP then
            res[item.ripv] = true
        elseif item.op == opcode.BEGINVAR then
            locals[item.ipv] = true
        end
    end
    for vid, _ in pairs(locals) do
        res[vid] = nil
    end
    vvarschanged_cache[block] = res
    return res
end

-- Spill 'dirty' variables that are modified in a loop body
-- before entering the loop.
local function vprepareloop(il, scope, lblock, block)
    local changed = vvarschanged(lblock)
    for vid, _ in pairs(changed) do
        local vinfo = vlookup(scope, vid)
        if vinfo.inc > 0 then
            insert(block, il.move(vid, vid, vinfo.inc))
            local info = vcreate(scope, vid)
            info.gen = il.id()
            info.inc = 0
        end
    end
end

-- Spill 'dirty' variables at the end of a loop body.
local function vmergeloop(il, lscope, lblock)
    local parent = lscope.parent
    for vid, vinfo in pairs(lscope) do
        if vinfo ~= parent and vinfo.islocal == 0 then
            local vpinfo = vcreate(parent, vid)
            vpinfo.gen = il.id()
            vpinfo.inc = 0
            if vinfo.inc > 0 then
                insert(lblock, il.move(vid, vid, vinfo.inc))
            end
        end
    end
end

-- Whether or not it is valid to move a COB across the specified
-- code range. If start is non-nil block[start] is assumed to be
-- another COB we are attempting to merge with.
local vcobmotionvalid
vcobmotionvalid = function(block, cob, start, stop)
    if cob.ipv == opcode.NILREG then
        return true
    else
        local t = block[start]
        if t and t.ipv ~= opcode.NILREG then
            return false -- they won't merge
        end
    end
    for i = start or 1, stop or #block do
        local o = block[i]
        if type(o) == 'table' then
            if not vcobmotionvalid(o, cob) then
                return false
            end
        elseif o.op == opcode.ISMAP or o.op == opcode.ISARRAY then
            -- we lack alias analysis; it's unsafe to move past ANY typecheck
            return false
        elseif o.op >= opcode.OBJFOREACH and o.op <= opcode.PSKIP and
               o.ripv == cob.ipv then
            -- it modifies the variable
            return false
        end
    end
    return true
end

-- Merge 2 COBs, update a.
local function vcobmerge(a, b)
    if a.offset < b.offset then
        a.offset = b.offset
    end
    if b.ipv ~= opcode.NILREG then
        assert(a.ipv == opcode.NILREG)
        a.ipv = b.ipv
        a.ipo = b.ipo
        a.scale = b.scale
    end
end

-- Here be dragons.
local voptimizeblock
voptimizeblock = function(il, scope, block, res)
    -- COB hoisting state
    local entryv0sn = vsnapshot(scope, 0) -- $0 at block start
    local firstcobpos, firstcobv0sn -- first COB (if any), will attempt
                                    -- to pop it into the parent
    local cobpos, cobv0sn, cobfixup -- 'active' COB, when we encounter
                                    -- another COB, we attempt to merge

    for i = 2, #block do -- foreach item in the current block, excl. head
        local o = block[i]
        if type(o) == 'cdata' then -- Opcode
            vexecute(il, scope, o, res)
            if o.op == opcode.CHECKOBUF then
                local v0info = vlookup(scope, 0)
                if not cobv0sn or v0info.gen ~= cobv0sn.gen or
                   not vcobmotionvalid(res, o, cobpos) then
                    -- no active COB or merge imposible: activate current COB
                    cobpos = #res
                    cobv0sn = vsnapshot(scope, 0)
                    if not firstcobpos then
                        firstcobpos = cobpos
                        firstcobv0sn = cobv0sn 
                    end
                    cobfixup = 0
                else
                    -- update active COB and drop the current one
                    o.offset = o.offset + cobfixup
                    vcobmerge(o, res[cobpos])
                    res[cobpos] = o
                    remove(res)
                end
            end
        else
            local head = o[1]
            if head.op == opcode.IFSET or head.op == opcode.STRSWITCH then
                -- branchy things: conditions and switches
                local bscopes = {0}
                local bblocks = {head}
                local cobhoistable, cobmaxoffset = 0, 0
                for i = 2, #o do
                    local branch = o[i]
                    local bscope = { parent = scope }
                    local bblock = { branch[1] }
                    voptimizeblock(il, bscope, branch, bblock)
                    bscopes[i] = bscope
                    bblocks[i] = bblock
                    -- update hoistable COBs counter
                    local cob = bblock[0]
                    if cob and cob.ipv == opcode.NILREG then
                        cobhoistable = cobhoistable + 1
                        if cob.offset > cobmaxoffset then
                            cobmaxoffset = cob.offset
                        end
                    end
                end
                -- a condition has 2 branches, though empty ones are omitted;
                -- if it's the case temporary restore the second branch
                -- for the vmergebranches() to consider this execution path
                if #o == 2 and head.op == opcode.IFSET then
                    bscopes[3] = { parent = scope }
                end
                vmergebranches(il, bscopes, bblocks)
                insert(res, bblocks)
                -- hoist COBs but only if at least half of the branches will
                -- benefit
                if cobhoistable >= #bblocks/2 then
                    for i = 2, #bblocks do
                        local bblock = bblocks[i]
                        local pos = bblock[-1]
                        if pos and bblock[0].ipv == opcode.NILREG then
                            remove(bblock, pos)
                        end
                    end
                    local o = il.checkobuf(cobmaxoffset)
                    local v0info = vlookup(scope, 0)
                    if not cobv0sn or v0info.gen ~= cobv0sn.gen or
                       not vcobmotionvalid(res, o, cobpos) then
                        -- no active COB or merge imposible: activate current
                        cobpos = #res
                        cobv0sn = vsnapshot(scope, 0)
                        if not firstcobpos then
                            firstcobpos = cobpos
                            firstcobv0sn = cobv0sn 
                        end
                        cobfixup = 0
                        insert(res, cobpos, o)
                    else
                        -- update active COB and drop the current one
                        o.offset = o.offset + cobfixup
                        vcobmerge(o, res[cobpos])
                        res[cobpos] = o
                    end
                end
            elseif head.op == opcode.OBJFOREACH then
                -- loops
                local v0bsn = vsnapshot(scope, 0) -- used by COB hoisting
                vprepareloop(il, scope, o, res)
                local v0asn = vsnapshot(scope, 0) -- used by COB hoisting
                local lscope = { parent = scope }
                local lblock = {}
                vexecute(il, lscope, head, lblock)
                local ivinfo = lscope[head.ripv]
                local ivgen = ivinfo.gen
                voptimizeblock(il, lscope, o, lblock)
                if ivinfo.gen == ivgen then
                    -- loop variable incremented in fixed steps
                    head.step = ivinfo.inc
                    lscope[head.ripv] = nil
                    local vinfo = vcreate(scope, head.ripv)
                    vinfo.gen = il.id()
                    vinfo.inc = 0
                end
                vmergeloop(il, lscope, lblock)
                insert(res, lblock)
                local cob = lblock[0]
                if cob then -- attempt COB hoisting
                    local v0info = vlookup(lscope, 0)
                    if v0info.gen ~= v0asn.gen or
                       v0info.inc == v0asn.inc then
                        -- hoisting failed: $0 is non-linear or
                        -- doesn't change at all
                    else
                        remove(lblock, lblock[-1])
                        cob.offset = 0
                        cob.ipv = head.ipv
                        cob.ipo = head.ipo
                        cob.scale = v0info.inc --[[ - v0asn.inc
                            but the later == 0, vprepareloop() spilled it ]]
                        if not cobv0sn or v0bsn.gen ~= cobv0sn.gen or
                           not vcobmotionvalid(res, cob, cobpos) then
                            -- no active COB or merge imposible:
                            -- activate current one
                            cobfixup = 0
                            cobpos = #res
                            cobv0sn = vsnapshot(scope, 0)
                            if not firstcobpos then
                                firstcobpos = cobpos
                                firstcobv0sn = cobv0sn
                            end
                            insert(res, cobpos, cob)
                        else
                            -- update active COB and drop the current one;
                            -- changing cobv0sn here is a hack
                            -- enabling to move COBs across (some) loops.
                            -- Basically when moving COB we must compensate
                            -- for $0 changing. Due to limitations in the
                            -- variable state tracking, it looks as if $0
                            -- value before and after the loop are unrelated.
                            -- In fact, after($0) = before($0) + k*x.
                            cobfixup = v0bsn.inc - cobv0sn.inc
                            cob.offset = cobfixup
                            vcobmerge(cob, res[cobpos])
                            cobv0sn = vsnapshot(scope, 0)
                            res[cobpos] = cob
                        end
                    end
                end
            else
                assert(false)
            end
        end
    end
    -- Add missing ENDVARs.
    for vid, vinfo in pairs(scope) do
        if vid ~= 'parent' and vinfo.islocal == 1 and vinfo.isdead == 0 then
            insert(res, il.endvar(vid))
        end
    end
    -- Attempt to pop the very first COB into the parent.
    if firstcobpos and firstcobv0sn.gen == entryv0sn.gen and
       vcobmotionvalid(res, res[firstcobpos], nil, firstcobpos) then
        -- There was a COB and the code motion was valid.
        -- Create a copy of the COB with adjusted offset and save it in res[0].
        -- If the parent decides to accept, it has to remove a now redundant
        -- COB at firstcobpos.
        local o = res[firstcobpos]
        res[0] = il.checkobuf(o.offset + firstcobv0sn.inc - entryv0sn.inc,
                               o.ipv, o.ipo, o.scale)
        res[-1] = firstcobpos
    end
end

local function voptimizefunc(il, func)
    local head, scope = func[1], {}
    local res = { head }
    local r0 = vcreate(scope, 0)
    local r1 = vcreate(scope, head.ipv)
    voptimizeblock(il, scope, func, res)
    if r0.inc > 0 then
        insert(res, il.move(0, 0, r0.inc))
    end
    return res
end

local function voptimize(il, code)
    local res = {}
    for i = 1, #code do
        res[i] = voptimizefunc(il, code[i])
    end
    return res
end
-----------------------------------------------------------------------
-- Lua codegen

-- Peeling transformation forces us to avoid locals in nested blocks.
-- Use function-level locals instead and reuse them whenever possible.
-- Returns n-locals / mapping var-name -> local-name.
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
            if head.op == opcode.IFSET or head.op == opcode.STRSWITCH then
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

-- After BEGINVAR, the variable value is 0. If a store follows
-- we can elide the initialization.
local function elidevarinit(block, i)
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
local function fuseskip(block, i, varmap)
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

local emit_lua_block_tab = {
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
    [opcode.PUTFLT2DBL ] = {  7, 'dval',  'ival' },
    [opcode.PUTSTR2BIN ] = {  9, 'uval',  'uval' },
    [opcode.PUTBIN2STR ] = {  8, 'uval',  'uval' },
    ----------------------- T
    [opcode.ISLONG     ] =  4,
    [opcode.ISSTR      ] =  8,
    [opcode.ISBIN      ] =  9,
    [opcode.ISARRAY    ] = 11,
    [opcode.ISMAP      ] = 12
}

local emit_nested_lua_block

local function emit_lua_instruction(il, o, res, varmap)
    local tab = emit_lua_block_tab -- just a shorter alias
    if     o.op == opcode.CALLFUNC  then
        insert(res, format('v0 = f%d(r, v0, %s)',
                            o.func, varref(o.ipv, o.ipo, varmap)))
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
        local str = il.cderef(o.cref)
        rtval.xlen = #str
        rtval.xoff = il.cpool_add(str)
        insert(res, format('r.ot[%s] = %d; r.ov[%s].uval = %s',
                            pos, tab[o.op], pos, rtval.uval))
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
    elseif o.op == opcode.ISBOOL    then
        local pos = varref(o.ipv, o.ipo, varmap)
        insert(res, format([[
if r.b2[r.t[%s]-%d] == 0 then rt_err_type(r, %s, 0xe8) end]],
                            pos,
                            il.cpool_add('\0\0\1\1\0\0\0\0\0\0\0\0\0'),
                            pos))
    elseif o.op == opcode.ISINT     then
        local pos = varref(o.ipv, o.ipo, varmap)
        insert(res, format([[
if r.t[%s] ~= 4 or r.v[%s].uval+0x80000000 > 0xffffffff then rt_err_type(r, %s, 0xe9) end]],
                            pos, pos, pos))
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
    elseif o.op == opcode.CHECKOBUF then --[[
        if     o.ipv == opcode.NILREG then
            insert(res, format('t = v0+%d', o.offset))
        elseif o.scale == 1 then
            insert(res, format('t = v0+%d+r.v[%s].xlen',
                                o.offset, varref(o.ipv, o.ipo, varmap)))
        else
            insert(res, format('t = v0+%d+r.v[%s].xlen*%d',
                                o.offset, varref(o.ipv, o.ipo, varmap),
                                o.scale))
        end
        -- TODO
        insert(res, '-- checkobuf(t) ')]]
    -----------------------------------------------------------
    else
        assert(false)
    end
end

local function emit_lua_block(ctx, block, cc, res)
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
        if i < skiptill then -- sometimes we fuse several ops
                             -- and set skiptill
        elseif type(o) == 'cdata' then
            local tab = emit_lua_block_tab -- just a shorter alias
            if o.op == opcode.ISSET     then
                local label
                local nexto = block[i+1]
                if type(nexto) == 'cdata' and nexto.op == opcode.ISSETLABEL then
                    label = il.cderef(nexto.cref)
                    skiptill = i + 2
                end
                insert(res, format('if %s == 0 then rt_err_missing(r, %s, "%s") end',
                                   varref(o.ripv, 0, varmap),
                                   varref(o.ipv, o.ipo, varmap),
                                   label))
            -----------------------------------------------------------
            elseif o.op == opcode.BEGINVAR  then
                if not elidevarinit(block, i) then
                    insert(res, format('%s = 0', varref(o.ipv, 0, varmap)))
                end
            else
                emit_lua_instruction(il, o, res, varmap)
            end
        else
            if o.break_jit_trace then break_jit_trace(ctx, res) end
            local head = o[1]
            local link = block[i+1] or cc
            if     head.op == opcode.IFSET then
                local branch1 = o[2]
                local branch2 = o[3]
                assert(branch1[1].op == opcode.IBRANCH)
                insert(res, format('if %s %s 0 then',
                                   varref(head.ipv, 0, varmap),
                                   branch1[1].ci == 0 and '==' or '~='))
                emit_nested_lua_block(ctx, branch1, link, res)
                if branch2 then
                    assert(branch2[1].op == opcode.IBRANCH)
                    assert(branch2[1].ci ~= branch1[1].ci)
                    insert(res, 'else')
                    emit_nested_lua_block(ctx, branch2, link, res)
                end
                insert(res, 'end')
            elseif head.op == opcode.STRSWITCH then
                local pos = varref(head.ipv, head.ipo, varmap)
                for i = 2, #o do
                    local branch = o[i]
                    local head = branch[1]
                    assert(head.op == opcode.SBRANCH)
                    local str = il.cderef(head.cref)
                    insert(res, format([[
%s r.v[%s].xlen == %d and ffi_C.memcmp(r.b1-r.v[%s].xoff, r.b2-%d, %d) == 0 then]],
                                       i == 2 and 'if' or 'elseif',
                                       pos, #str, pos, il.cpool_add(str), #str))
                    emit_nested_lua_block(ctx, branch, link, res)
                end
                insert(res, 'else')
                insert(res, format('rt_err_value(r, %s)', pos))
                insert(res, 'end')
            elseif head.op == opcode.OBJFOREACH then
                local itervar = varref(head.ripv, 0, varmap)
                if o.peel or head.step == 0 then
                    insert(res, format('%s = %s',
                                       itervar,
                                       varref(head.ipv, head.ipo + 1 - head.step, varmap)))
                    local label = il.id(); labelmap[head] = label
                    insert(res, format('::l%d::', label))
                    break_jit_trace(ctx, res)
                    if head.step ~= 0 then
                        insert(res, format('%s = %s+%d', itervar, itervar, head.step))
                    end
                    local pos = varref(head.ipv, head.ipo, varmap)
                    insert(res, format('if %s ~= %s+r.v[%s].xoff then',
                                        itervar, pos, pos))
                    emit_nested_lua_block(ctx, o, head, res)
                    insert(res, 'end')
                    local copystmt
                    -- fuse OBJFOREACH / SKIP
                    skiptill, copystmt = fuseskip(block, i, varmap)
                    insert(res, copystmt)
                else
                    insert(res, format('for %s = %s, %s+r.v[%s].xoff, %d do', 
                                       itervar,
                                       varref(head.ipv, head.ipo+1, varmap),
                                       varref(head.ipv, head.ipo-1, varmap),
                                       varref(head.ipv, head.ipo, varmap),
                                       head.step))
                    emit_nested_lua_block(ctx, o, head, res)
                    insert(res, 'end')
                end
            else
                assert(false)
            end
        end
    end
end

emit_nested_lua_block = function(ctx, block, cc, res)
    if not block.peel then
        return emit_lua_block(ctx, block, cc, res)
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

local lua_locals_tab = {
    [0] = 'local x%d',
    'local x%d, x%d',
    'local x%d, x%d, x%d'
}

local function emit_lua_func_body(il, func, nlocals_min, res)
    local nlocals, varmap = sched_func_variables(func)
    if nlocals_min and nlocals_min > nlocals then
        nlocals = nlocals_min
    end
    for i = 1, nlocals, 4 do
        insert(res, format(lua_locals_tab[nlocals - i] or
                           'local x%d, x%d, x%d, x%d',
                           i, i+1, i+2, i+3))
    end
    peel_annotate(func, 0)
    insert(res, '')
    local patchpos1 = #res
    local head = func[1]
    local labelmap, jit_trace_breaks = {}, {}
    local queue = { func, head }
    local ctx = {
        il = il,
        varmap = varmap,
        labelmap = labelmap,
        jit_trace_breaks = jit_trace_breaks,
        queue = queue
    }
    local patchpos2
    local emitpos = 0
    while emitpos ~= #queue do
        local n = emitpos + 1
        emitpos = #queue
        for i = n, emitpos, 2 do
            local entry, cc = queue[i], queue[i+1]
            local label = labelmap[entry]
            insert(res, label and format('::l%d::', label))
            emit_lua_block(ctx, entry, cc, res)
            label = labelmap[cc]
            if cc == head then
                insert(res, label and format('::l%d::', label))
                insert(res, '')
                patchpos2 = #res
            else
                insert(res, format('goto l%d', label))
            end
        end
    end
    return patchpos1, patchpos2, jit_trace_breaks
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
local function emit_lua_func(il, func, res, opts)
    local head = func[1]
    local func_decl = opts and opts.func_decl or
                      format('f%d = function(r, v0, v%d)', head.name, head.ipv)
    local func_locals = opts and opts.func_locals
    local func_return = opts and opts.func_return or
                        format('do return v0 end', head.ipv)
    local conversion_init     = opts and opts.conversion_init
    local conversion_complete = opts and opts.conversion_complete
    local iter_prolog = opts and opts.iter_prolog
    local nlocals_min = opts and opts.nlocals_min

    insert(res, func_decl)
    insert(res, func_locals)
    insert(res, 'local t')
    local tpos = #res
    local patchpos1, patchpos2, jit_trace_breaks =
        emit_lua_func_body(il, func, nlocals_min, res)
    res[patchpos1] = conversion_init or ''
    if conversion_complete then
        local label = il.id()
        insert(jit_trace_breaks, 1, label)
        res[patchpos2] = format('%s\ns = %d\ngoto continue', conversion_complete, label)
    else
        res[patchpos2] = func_return
    end
    if next(jit_trace_breaks) then
        local patch = { 'for _ = 1, 1000000000 do' }
        insert(patch, iter_prolog)
        emit_jump_table(jit_trace_breaks, patch)
        if conversion_complete then
            patch[#patch - 1] = func_return
        end
        insert(patch, conversion_init)
        res[patchpos1] = concat(patch, '\n')
        res[tpos] = 'local s, t'
        insert(res, '::continue::')
        insert(res, 'end')
    end
    insert(res, 'end')
end
------------------------------------------------------------------------

local function il_create()
    local objs = {}
    local refs = {}
    local ref  = 0

    local function cref(object)
        local res = refs[object]
        if not res then
            res = ref + 1
            objs[res] = object
            refs[object] = res
            ref = res
        end
        return res
    end

    local cmax = 1000000
    local cmin = 1000001
    local cpos = 0
    local cstr = {}
    local cpoo = {}

    local function cpool_add(str)
        str = tostring(str)
        local res = cstr[str]
        if res then
            return res
        end
        cpoo[cmin - 1] = str
        cmin = cmin - 1
        cpos = cpos + #str
        cstr[str] = cpos
        return cpos
    end

    local id = 10 -- low ids are reserved

    local il
    il = setmetatable({
        sbranch = function(cs)
            local o = opcode_new(opcode.SBRANCH)
            o.cref = cref(cs)
            return o
        end,
        putstrc = function(offset, cs)
            local o = opcode_new(opcode.PUTSTRC)
            o.offset = offset; o.cref = cref(cs)
            return o
        end,
        putbinc = function(offset, cb)
            local o = opcode_new(opcode.PUTBINC)
            o.offset = offset; o.cref = cref(cb)
            return o
        end,
        putxc = function(offset, cx)
            local o = opcode_new(opcode.PUTXC)
            o.offset = offset; o.cref = cref(cx)
            return o
        end,
        issetlabel = function(cs)
            local o = opcode_new(opcode.ISSETLABEL)
            o.cref = cref(cs)
            return o
        end,
    ----------------------------------------------------------------
        id = function() id = id + 1; return id end,
        cref = cref,
        cderef = function(ref)
            return objs[ref]
        end,
        cpool_add = cpool_add,
        cpool_get_data = function() return concat(cpoo, '', cmin, cmax) end,
        vis = function(code) return il_vis(il, code) end,
        opcode_vis = function(o)
            return opcode_vis(o, objs)
        end,
        cleanup = il_cleanup,
        optimize = function(code) return voptimize(il, code) end,
        emit_lua_func = function(func, res, opts)
            return emit_lua_func(il, func, res, opts)
        end,
        append_lua_code = function(code, res)
            local varmap = {}
            for i = 1, #code do
                emit_lua_instruction(il, code[i], res, varmap)
            end
        end
    }, { __index = il_methods })
    return il
end

return {
    il_create = il_create
}
