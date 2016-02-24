--------------------------------------------------------------------------------
--- An example of Lua module for Tarantool
--------------------------------------------------------------------------------

--
-- Dependencies
--

local log = require('log') -- some Tarantool module

-- Lua submodule
local functions = require('modulekit.functions')
-- Lua/C submodule
local cfunctions = require('modulekit.cfunctions')
-- Now you can use functions.* and cfunctions.* in your code

--
-- Constants
--

-- local variables are only visible from this file.
local SOME_CONSTANT = 10

--
-- Functions
--

-- A some module function
local function test(x)
    log.info("test() called with x=%s", x)
    return x + SOME_CONSTANT
end

--
-- Export functions
--

    -- result is returned from require('example')
return {
    test = test;
    functions = functions;
    cfunctions = cfunctions;
}
-- vim: ts=4 sts=4 sw=4 et
