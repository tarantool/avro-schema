local format = string.format
local insert = table.insert
local concat = table.concat

local function lea(v, offset)
	if not v then
		return offset
	elseif not offset or offset == 0 then
		return format('v%03d', v)
	else
		return format('v%03d+%d', v, offset)
	end
end

local function lua_lir()

	local cmax = 1000000
	local cmin = 1000001
	local cpos = 0
	local cstr = {}
	local cpoo = {}

	local function cstring(str)
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

	local func = 0
	local var = 0

	local lir = {
		cpool_data = function() return concat(cpoo, '', cmin, cmax) end,
		new_var = function() var = var + 1; return var end,
		new_func = function() func = func + 1; return func end,
		func_count = function() return func end,
		-- LIR
		cvtfunc = function(name, ipname, iadd, oadd)
			local decl = format('f%03d = function(r, v000, v%03d)', name, ipname)
			return function(body)
				body[1] = decl
				insert(body, format('return %s, %s', lea(0, oadd), lea(ipname, iadd)))
				insert(body, 'end')
				return body
			end
		end,
		vfunc = function(name, ipname, iadd)
			local decl = format('f%03d = function(r, v%03d)', name, ipname)
			return function(body)
				body[1] = decl
				insert(body, format('return %s', lea(ipname, iadd)))
				insert(body, 'end')
				return body
			end
		end,
		---------------------------------------------------------------
		callcvtfunc = function(name, vname, offset)
			return format('v000, %03d = f%03d(r, v000, %s)', vname, name, lea(vname, offset))
		end,
		callvfunc = function(name, vname, offset)
			return format('%03d = f%03d(r, %s)', vname, name, lea(vname, offset))
		end,
		---------------------------------------------------------------
		variable = function(name)
			return format('local v%03d', name)
		end,
		---------------------------------------------------------------
		checkobuf = function(offset)
			return format('-- checkobuf(r, %s)', lea(0, offset))
		end,
		---------------------------------------------------------------
		putboolc = function(offset, b)
			return format('r.ot[%s] = %d', lea(0, offset), b and 3 or 2)
		end,
		putintc = function(offset, l)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 4', dest),
				format('r.ov[%s].ival = %d', dest, l),
			}
		end,
		putlongc = function(offset, l)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 4', dest),
				format('r.ov[%s].ival = %dLL', dest, l),
			}
		end,
		putfloatc = function(offset, f)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 6', dest),
				format('r.ov[%s].dval = %f', dest, f),
			}
		end,
		putdoublec = function(offset, lf)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 7', dest),
				format('r.ov[%s].dval = %d', dest, lf),
			}
		end,
		putstrc = function(offset, str)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 18', dest),
				format('r.ov[%s].xlen = %d', dest, #str),
				format('r.ov[%s].xoff = %d', dest, cstring(str))
			}
		end,
		putbinc = function(offset, bin)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 19', dest),
				format('r.ov[%s].xlen = %d', dest, #bin),
				format('r.ov[%s].xoff = %d', dest, cstring(bin))
			}
		end,
		putcomplexc = function(offset, complex)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 20', dest),
				format('r.ov[%s].xlen = %d', dest, #complex),
				format('r.ov[%s].xoff = %d', dest, cstring(complex))
			}
		end,
		putarrayc = function(offset, len)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 11', dest),
				format('r.ov[%s].xlen = %d', dest, len)
			}
		end,
		putmapc = function(offset, len)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 12', dest),
				format('r.ov[%s].xlen = %d', dest, len)
			}
		end,
		---------------------------------------------------------------
		putnul = function(offset)
			return format('r.ot[%s] = 1', lea(0, offset))
		end,
		putbool = function(offset, srcvname, srcoffset)
			return format('r.ot[%s] = r.t[%s]', lea(0, offset), lea(srcvname, srcoffset))
		end,
		putlong = function(offset, srcvname, srcoffset)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 4', dest),
				format('r.ov[%s].uval = r.v[%s].uval', dest, lea(srcvname, srcoffset))
			}
		end,
		putfloat = function(offset, srcvname, srcoffset)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 6', dest),
				format('r.ov[%s].dval = r.v[%s].dval', dest, lea(srcvname, srcoffset))
			}
		end,
		putdouble = function(offset, srcvname, srcoffset)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 7', dest),
				format('r.ov[%s].dval = r.v[%s].dval', dest, lea(srcvname, srcoffset))
			}
		end,
		putlong2flt = function(offset, srcvname, srcoffset)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 6', dest),
				format('r.ov[%s].dval = r.v[%s].ival', dest, lea(srcvname, srcoffset))
			}
		end,
		putlong2dbl = function(offset, srcvname, srcoffset)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 7', dest),
				format('r.ov[%s].dval = r.v[%s].ival', dest, lea(srcvname, srcoffset))
			}
		end,
		putstr = function(offset, srcvname, srcoffset)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 8', dest),
				format('r.ov[%s].uval = r.v[%s].uval', dest, lea(srcvname, srcoffset))
			}
		end,
		putbin = function(offset, srcvname, srcoffset)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 9', dest),
				format('r.ov[%s].uval = r.v[%s].uval', dest, lea(srcvname, srcoffset))
			}
		end,
		putarray = function(offset, srcvname, srcoffset)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 11', dest),
				format('r.ov[%s].xlen = r.v[%s].xlen', dest, lea(srcvname, srcoffset))
			}
		end,
		putmap = function(offset, srcvname, srcoffset)
			local dest = lea(0, offset)
			return {
				format('r.ot[%s] = 12', dest),
				format('r.ov[%s].xlen = r.v[%s].xlen', dest, lea(srcvname, srcoffset))
			}
		end,
		---------------------------------------------------------------
		fixlen = function(v, ofs, counter)
			return(format('r.ov[%s].xlen = v%03d', lea(v, ofs), counter))
		end,
		fixoff = function(v, ofs)
			local dest = lea(v, ofs)
			return(format('r.ov[%s].xoff = v000 - (%s)', dest, dest))
		end,
		---------------------------------------------------------------
		mapforeach = function(vname, offset)
			return function(body)
				local src = lea(vname, offset)
				body[1] = {}
				return {
					format('local tmp = %s+r.v[%s].xoff', src, src),
					format('v%03d = %s', vname, lea(vname, offset + 1)),
					format('while v%03d ~= tmp do', vname),
					body,
					'end'
				}
			end
		end,
		---------------------------------------------------------------
		strswitch = function(vname, offset, map)
			return function(body)
				local src = lea(vname, offset)
				local ifelse = [[
%s r.v[%s].xlen == %d and ffi_C.memcmp(r.b1-r.v[%s].xoff, r.b2-%d, %d) == 0 then]]
				local res = {}
				for i = 1, #map do
					local str = map[i]
					res[i * 2 - 1] = format(ifelse,
											i == 1 and 'if' or 'elseif',
											src, #str, src, cstring(str), #str)
					res[i * 2] = body[i + 1]
				end
				insert(res, { 'else', 'error()', 'end' })
				return res
			end
		end,
		---------------------------------------------------------------
		isnul = function(vname, offset)
			return format('if r.t[%s] ~= 1 then error() end', lea(vname, offset))
		end,
		isbool = function(vname, offset)
			local src = lea(vname, offset)
			return format('if r.t[%s] ~= 2 and r[%s].t ~= 3 then error() end', src, src)
		end,
		isint = function(vname, offset)
			local src = lea(vname, offset)
			return format([[
if r.t[%s] ~= 4 or r.v[%s].uval + 0x80000000 > 0xffffffff then error() end]],
					      src, src)
		end,
		islong = function(vname, offset)
			return format('if r.t[%s] ~= 4 then error() end', lea(vname, offset))
		end,
		isfloat = function(vname, offset)
			return format('if r.t[%s] ~= 6 then error() end', lea(vname, offset))
		end,
		isdouble = function(vname, offset)
			return format('if r.t[%s] ~= 7 then error() end', lea(vname, offset))
		end,
		isstr = function(vname, offset)
			return format('if r.t[%s] ~= 8 then error() end', lea(vname, offset))
		end,
		isbin = function(vname, offset)
			return format('if r.t[%s] ~= 9 then error() end', lea(vname, offset))
		end,
		isarray = function(vname, offset)
			return format('if r.t[%s] ~= 11 then error() end', lea(vname, offset))
		end,
		ismap = function(vname, offset)
			return format('if r.t[%s] ~= 12 then error() end', lea(vname, offset))
		end,
		---------------------------------------------------------------
		lenis = function(vname, offset, len)
			return format('if r.v[%s].xlen ~= %d then error() end', lea(vname, offset), len)
		end,
		---------------------------------------------------------------
		move = function(vname, srcvname, srcoffset)
			return format('v%03d = %s', vname, lea(srcvname, srcoffset))
		end,
		---------------------------------------------------------------
		skip = function(vname, srcvname, srcoffset)
			local src = lea(srcvname, srcoffset)
			return format('v%03d = %s + r.v[%s].xoff', vname, src, src)
		end,
		pskip = function(vname, srcvname, srcoffset)
			local src = lea(srcvname, srcoffset)
			return {
				format('if r.t[%s] >= 11 then', src),
				format('v%03d = %s + r.v[%s].xoff', vname, src, src),
				'else',
				format('v%03d = %s', vname, lea(srcvnmae, srcoffset + 1)),
				'end'
			}
		end,
		---------------------------------------------------------------
		isset = function(vname)
			return format('if not v%03d then error() end', vname)
		end,
		isnotset = function(vname)
			return format('if v%03d then error() end', vname)
		end,
		---------------------------------------------------------------
		ifset = function(vname)
			return function(body)
				body[1] = format('if v%03d then', vname)
				insert(body, 'end')
				return body
			end
		end,
		ifnotset = function(vname)
			return function(body)
				body[1] = format('if not v%03d then', vname)
				insert(body, 'end')
				return body
			end
		end,
		---------------------------------------------------------------
		ifnul = function(vname, offset)
			local dest = lea(vname, offset)
			return function(body)
				body[1] = format('if r.t[%s] == 1 then')
				insert(body, 'end')
				return body
			end
		end,
		ifnotnul = function(vname, offset)
			local dest = lea(vname, offset)
			return function(body)
				body[1] = format('if r.t[%s] ~= 1 then')
				insert(body, 'end')
				return body
			end
		end,
		---------------------------------------------------------------
		nop = function()
			return {}
		end
	}
	lir.arrayforeach = lir.mapforeach
	lir.putint = lir.putlong
	lir.putint2long = lir.putlong
	lir.putint2flt = lir.putlong2flt
	lir.putint2dbl = lir.putlong2dbl
	lir.putflt2dbl = lir.putdbl
	lir.putstr2bin = lir.putbin
	lir.putbin2str = lir.putstr
	return lir
end

local lua_lir_to_code_helper
lua_lir_to_code_helper = function(code, res)
	if type(code) == 'table' then
		if type(code[1]) == 'function' then
			return lua_lir_to_code_helper(code[1](code), res)
		end
		for i = 1, #code do
			local item = code[i]
			if type(item) == 'string' then
				insert(res, item)
			else
				lua_lir_to_code_helper(item, res)
			end
		end
	else
		insert(res, code)
	end
end

local function lua_lir_to_code(lir, code)
	local res = {}
	for i = 1, lir.func_count() do
		insert(res, format('local f%03d', i))
	end
	lua_lir_to_code_helper(code, res)
	return concat(res, '\n')
end

return {
	lua_lir         = lua_lir,
	lua_lir_to_code = lua_lir_to_code
}
