-- null

t {
    schema = '"null"',
    validate = 'null',
    validate_only = true
}

t {
    schema = '"null"',
    validate = '42',
    validate_error = 'Not a null: 42'
}

-- boolean

t {
    schema = '"boolean"',
    validate = 'true',
    validate_only = true
}

t {
    schema = '"boolean"',
    validate = 'false',
    validate_only = true
}

t {
    schema = '"boolean"',
    validate = '100500',
    validate_error = 'Not a boolean: 100500'
}

t {
    schema = '"boolean"',
    validate = '"100500"',
    validate_error = 'Not a boolean: 100500'
}

-- int

t {
    schema = '"int"',
    validate = '42',
    validate_only = true
}

t {
    schema = '"int"',
    validate = '42.1',
    validate_error = 'Not a int: 42.1'
}

t {
    schema = '"int"',
    validate = '"Hello!"',
    validate_error = 'Not a int: Hello!'
}

t {
    schema = '"int"',
    validate = '2147483647',
    validate_only = true
}

t {
    schema = '"int"',
    validate = '-2147483648',
    validate_only = true
}

t {
    schema = '"int"',
    validate = '2147483648',
    validate_error = 'Not a int: 2147483648'
}

t {
    schema = '"int"',
    validate = '-2147483649',
    validate_error = 'Not a int: -2147483649'
}

-- long

t {
    schema = '"long"',
    validate = '42',
    validate_only = true
}

t {
    schema = '"long"',
    validate = '42.1',
    validate_error = 'Not a long: 42.1'
}

t {
    schema = '"long"',
    validate = '"Hello!"',
    validate_error = 'Not a long: Hello!'
}

t {
    schema = '"long"',
    validate = 9223372036854775807LL,
    validate_only = true
}

t {
    schema = '"long"',
    validate = 9223372036854775808LL,
    validate_only = true
}

-- note: IEEE 754 double precision floating-point numbers encode
--       fraction with 52 bits, hence when the value is 2^63,
--       the delta must be at least 2^11 (2048) to make a difference.
t {
    schema = '"long"',
    validate = 9223372036854775808 - 2048,
    validate_only = true
}

t {
    schema = '"long"',
    validate = -9223372036854775808,
    validate_only = true
}

t {
    schema = '"long"',
    validate = 9223372036854775808,
    validate_error = 'Not a long: 9.2233720368548e+18'
}

t {
    schema = '"long"',
    validate = -9223372036854775808 - 2048,
    validate_error = 'Not a long: -9.2233720368548e+18'
}

-- float
-- double
-- string
-- bytes
-- array
-- map
-- union
-- fixed
-- enum
-- record
