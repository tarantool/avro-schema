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
emit_patch = function(lir, ir, ipv, ipo, opo)
	local irt = ir_type(ir)
	local lirfuncs = ir2lirfuncs[irt]
	if lirfuncs then
		local isfunc, putfunc = unpack(lirfuncs)
		return {
			lir[isfunc]  (ipv, ipo),
			lir[putfunc] (opo, ipv, ipo)
		}, ipo + 1
	elseif irt == 'FIXED' then
		return {
			lir.isbin(ipv, ipo),
			lir.lenis(ipv, ipo, ir_fixed_size(ir))
		}, ipo + 1
	elseif irt == 'ENUM' then
		assert(false, 'NYI: enum')
	else
		assert(false) -- VLO, can't patch
	end
end

local emit_check
emit_check = function(lir, ir, ipv, ipo)
	local irt = ir_type(ir)
	local lirfuncs = ir2lirfuncs[irt]
	if lirfuncs then
		local isfunc = unpack(lirfuncs)
		return {
			lir[isfunc]  (ipv, ipo),
		}, ipo + 1
	elseif irt == 'FIXED' then
		return {
			lir.isbin(ipv, ipo),
			lir.lenis(ipv, ipo, ir_fixed_size(ir))
		}, ipo + 1
	elseif irt == 'ARRAY' then
		return {
			lir.isarray(ipv, ipo),
			lir.skip(ipv, ipv, ipo)
		}, 0
	elseif irt == 'MAP' then
		return {
			lir.ismap(ipv, ipo),
			lir.skip(ipv, ipv, ipo)
		}, 0
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
emit_validate = function(lir, ir, ipv, ipo)
	return emit_check(lir, ir, ipv, ipo) -- XXX
end

local emit_convert
emit_convert = function(lir, ir, ipv, ipo, opo)
	local irt = ir_type(ir)
	local lirfuncs = ir2lirfuncs[irt]
	if lirfuncs then
		local isfunc, putfunc = unpack(lirfuncs)
		return {
			lir[isfunc]  (ipv, ipo),
			lir.checkobuf(opo),
			lir[putfunc] (opo, ipv, ipo)
		}, ipo + 1, opo + 1
	elseif irt == 'FIXED' then
		return {
			{ lir.isbin(ipv, ipo), lir.lenis(ipv, ipo, ir_fixed_size(ir)) },
			lir.checkobuf(opo),
			lir.putbin(opo, ipv, ipo)
		}, ipo + 1, opo + 1
	elseif irt == 'ARRAY' then
		assert(false, 'NYI: array')
	elseif irt == 'MAP' then
		assert(false, 'NYI: map')
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
emit_convert_unchecked = function(lir, ir, ipv, ipo, opo)
	local res
	res, ipo, opo = emit_convert(lir, ir, ipv, ipo, opo)
	-- get rid of checks - emit_convert must cooperate
	res[1] = lir.nop()
	return res, ipo, opo
end

-----------------------------------------------------------------------
-- emit_rec_flatten(lir, ir, ipv, ipo, opo) -> lir_code, ipo', opo'
local emit_rec_flatten_pass1
local emit_rec_flatten_pass2
local emit_rec_flatten_pass3
local function emit_rec_flatten(lir, ir, ipv, ipo, opo)
	assert(ir_type(ir) == 'RECORD')
	local context = {
		lir = lir,
		ipv = ipv,
		ipo = ipo,
		opo = opo,
		defaults = {},  -- [celli * 2 - 1] lir_put* func,
		                -- [celli * 2]     argument
		var_block = {}, -- variable declarations
		aux_block = {}, -- certain field checks 
		vlocell = nil   -- first cell with a VLO
	}
	-- a shadow tree (keyed by o)
	-- used for inter-pass sharing
	-- after 1st pass contains 'direct write' cell indices,
	-- after 2nd - stores input vars
	-- (either directly or in [0] of a nested table)
	local tree = {}
	context.vlocell = emit_rec_flatten_pass1(context, ir, tree, 1)
	local parser_block = emit_rec_flatten_pass2(context, ir, tree)
	context.opo = opo + context.vlocell
	local generator_block, maxcell = emit_rec_flatten_pass3(context, ir, tree, 1)
	local init_block, defaults = {}, context.defaults
	for i = 1, context.vlocell - 1 do
		local lirfunc = defaults[i * 2 - 1]
		if lirfunc then
			insert(init_block, lirfunc(opo + i, defaults[i * 2]))
		end
	end
	return {
		context.var_block,
		lir.checkobuf(opo + context.vlocell - 1),
		lir.putarrayc(opo, maxcell - 1),
		init_block,
		parser_block,
		context.aux_block,
		generator_block
	}, 0, context.opo
end

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
				defaults[ (curcell + i)*2 - 3 ] = ddata[ i * 2 - 1]
				defaults[ (curcell + i)*2 - 2 ] = ddata[ i * 2 ]
			end
		end
		local fieldir = bc[o2i[o]]
		if not fieldir then
			assert(dcells)
			curcell = curcell + dcells
		else
			local fieldirt
			tree[o] = curcell
::restart::
			fieldirt = ir_type(fieldir)
			if type(fieldir) ~= 'table' or fieldirt == 'FIXED' or fieldirt == 'ENUM' then
				curcell = curcell + 1
			elseif fieldirt == 'RECORD' then
				local childtree, vlofound = {}
				tree[o] = childtree
				curcell, vlofound = emit_rec_flatten_pass1(context, fieldir,
					                                   childtree, curcell)
				if vlofound then
					return curcell, true
				end
			elseif fieldirt == 'UNION' then
				assert(false, 'NYI')
				if ir_union_osimple(fieldir) then
					-- union in source schema mapped to a simple type in target
					fieldir = nil -- XXX
					goto restart
				else
					-- consider it VLO for simplicity (XXX simple optional fields?)
					return curcell, true
				end
			else
				-- ARRAY or tree, it's a VLO
				return curcell, true
			end
		end
	end
	return curcell, false
end

emit_rec_flatten_pass2 = function(context, ir, tree)
	local inames, bc = ir_record_inames(ir), ir_record_bc(ir)
	local i2o = ir_record_i2o(ir)
	local lir, ipv, opo = context.lir, context.ipv, context.opo
	local var_block = context.var_block
	local aux_block = context.aux_block
	local switch = { lir.strswitch(ipv, 0, inames) }
	for i = 1, #inames do
		local fieldir = bc[i]
		local fieldirt = ir_type(fieldir)
		local o = i2o[i]
		local fieldvar = lir.new_var()
		local targetcell = tree[o]
		insert(var_block, lir.ipvar(fieldvar))
		local branch = {
			lir.isnotset(fieldvar),
			lir.move(fieldvar, ipv, 1)
		}
		switch[i + 1] = branch
		-- we aren't going to see this var during pass3
		if not o and not ir_record_ioptional(ir, i) then
			insert(aux_block, lir.isset(fieldvar))
		end
		if fieldirt == 'RECORD' then
			if o then
				if not tree[o] then
					tree[o] = {}
				end
				tree[o][0] = fieldvar
			end
			insert(branch, emit_rec_flatten_pass2(context, fieldir, tree[o]))
		elseif fieldirt == 'UNION' then
			assert(false, 'NYI')
		else
			tree[o] = fieldvar
			local code_block, nipo
			if targetcell then
				code_block, nipo = emit_patch(lir, fieldir, ipv, 1,
					                          opo + targetcell)
			elseif o then
				code_block, nipo = emit_check(lir, fieldir, ipv, 1)
			else
				code_block, nipo = emit_validate(lir, fieldir, ipv, 1)
			end
			insert(branch, code_block)
			if nipo ~= 0 then
				insert(branch, lir.move(ipv, ipv, nipo))
			end
		end
	end
	local res = {
		lir.ismap(ipv, context.ipo),
		{
			lir.mapforeach(ipv, context.ipo),
			lir.isstr(ipv, 0),
			switch
		}
	}
	context.ipo = 0
	return res
end

emit_rec_flatten_pass3 = function(context, ir, tree, curcell)
	return {}, context.vlocell
end

-----------------------------------------------------------------------
local emit_rec_unflatten_pass1
local emit_rec_unflatten_pass2
local function emit_rec_unflatten(lir, ir, ipv, ipo, opo)
	assert(ir_type(ir) == 'RECORD')
	local context = {
		lir = lir,
		ipv = ipv,
		ipo = ipo + 1,
		opo = opo,
		var_block = {} -- variable declarations
	}
	local tree = {}
	local parser_block, maxcell = emit_rec_unflatten_pass1(context, ir, tree, 1)
	local generator_block = emit_rec_unflatten_pass2(context, ir, tree)
	return {
		context.var_block,
		lir.isarray(ipv, ipo),
		lir.lenis(ipv, ipo, maxcell - 1),
		parser_block,
		generator_block
	}, context.ipo, context.opo
end

emit_rec_unflatten_pass1 = function(context, ir, tree, curcell)
	local bc, inames = ir_record_bc(ir), ir_record_inames(ir)
	local i2o = ir_record_i2o(ir)
	local lir = context.lir
	local code = {}
	for i = 1, #inames do
		local fieldir = bc[i]
		local fieldirt = ir_type(fieldir)
		local fieldcode
		if fieldirt == 'RECORD' then
			local childtree = {}
			tree[i] = childtree
			fieldcode, curcell = emit_rec_unflatten_pass1(context, fieldir,
													  childtree, curcell)
		elseif fieldrt == 'UNION' then
			assert(false, 'NYI: union')
		elseif i2o[i] and not ir_record_ohidden(ir, i2o[i]) then
			local var = lir.new_var()
			tree[i] = var
			insert(context.var_block, lir.ipvar(var))
			insert(code, lir.move(var, context.ipv, context.ipo))
			fieldcode, context.ipo = emit_check(context.lir, fieldir,
												context.ipv, context.ipo)
			curcell = curcell + 1
		else
			fieldcode, context.ipo = emit_validate(context.lir, fieldir,
												   context.ipv, context.ipo)
			curcell = curcell + 1
		end
		insert(code, fieldcode)
	end
	return code, curcell
end

emit_rec_unflatten_pass2 = function(context, ir, tree)
	local bc, onames = ir_record_bc(ir), ir_record_onames(ir)
	local o2i = ir_record_o2i(ir)
	local lir, opo = context.lir, context.opo
	local code, maplen = { lir.checkobuf(opo), lir.nop() }, 0
	context.opo = opo + 1
	for o = 1, #onames do
		local i = o2i[o]
		if ir_record_ohidden(ir, o) then
			-- do nothing
		elseif not i then
			-- put defaults
			local schema, val = ir_record_odefault(ir, o)
			local lirfunc, arg = prepare_default(lir, schema, val)
			insert(code, lir.checkobuf(context.opo + 1))
			insert(code, lir.putstrc(context.opo, onames[o]))
			insert(code, lirfunc(context.opo + 1, arg))
			context.opo = context.opo + 2
			maplen = maplen + 1
		else
			local fieldir = bc[i]
			local fieldirt = ir_type(fieldir)
			insert(code, lir.checkobuf(context.opo))
			insert(code, lir.putstrc(context.opo, onames[o]))
			context.opo = context.opo + 1
			if fieldirt == 'RECORD' then
				insert(code, emit_rec_unflatten_pass2(context, fieldir, tree[i]))
			elseif fieldirt == 'UNION' then
				assert(false, 'NYI: union')
			else
				local fieldcode, _
				fieldcode, _, context.opo = emit_convert_unchecked(
					lir, fieldir, tree[i], 0, context.opo)
				insert(code, fieldcode)
			end
			maplen = maplen + 1
		end
	end
	code[2] = lir.putmapc(opo, maplen)
	return code
end

-----------------------------------------------------------------------
local function emit_code(lir, ir)
	-- reserve f001, f002, f003
	lir.new_func()
	lir.new_func()
	lir.new_func()
	local fipv = lir.new_var()
	local fbody, fipo, fopo = emit_rec_flatten(lir, ir, fipv, 0, 0)
	local uipv = lir.new_var()
	local ubody, uipo, uopo = emit_rec_unflatten(lir, ir, uipv, 0, 0)
	return {
		{ lir.cvtfunc(1, fipv, fipo, fopo), fbody },
		{ lir.cvtfunc(2, uipv, uipo, uopo), ubody }
	}
end

-----------------------------------------------------------------------
return {
	emit_code          = emit_code, 
	emit_rec_flatten   = emit_rec_flatten,
	emit_rec_unflatten = emit_rec_unflatten
}
