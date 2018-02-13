local function table_contains(t, xval)
    for k, val in ipairs(t) do
        if type(k) == "number" and val == xval then
            return true
        end
    end
    return false
end

return {
    table_contains = table_contains
}