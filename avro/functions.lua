--------------------------------------------------------------------------------
--- A Lua submodule
--------------------------------------------------------------------------------

local function func(a, b)
    -- Check arguments for all public functions
    if a == nil or b == nil then
        error('Usage: functions.func(a: number, b: number)')
    end
    return a + b
end

-- result is returned from require('modulekit.functions')
return {
    func = func;
}
-- vim: syntax=lua ts=4 sts=4 sw=4 et
