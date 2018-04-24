local card_face = [[
    {"name": "card_face", "type": "enum", "symbols":
        ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}
]]

local card_face_nullable = [[
    {"name": "card_face_nullable", "type": "enum*", "symbols":
        ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}
]]

t {
    schema = card_face,
    func = 'flatten',
    input = '"SPADES"', output = '[0]'
}

t {
    schema = card_face,
    func = 'unflatten',
    input = '[0]', output = '"SPADES"'
}

t {
    schema = card_face,
    func = 'flatten',
    input = '"HEARTS"', output = '[1]'
}

t {
    schema = card_face,
    func = 'unflatten',
    input = '[1]', output = '"HEARTS"'
}
t {
    schema = card_face,
    func = 'flatten',
    input = '"DIAMONDS"', output = '[2]'
}

t {
    schema = card_face,
    func = 'unflatten',
    input = '[2]', output = '"DIAMONDS"'
}

t {
    schema = card_face,
    func = 'flatten',
    input = '"CLUBS"', output = '[3]'
}

t {
    schema = card_face,
    func = 'unflatten',
    input = '[3]', output = '"CLUBS"'
}

-- validation errors
t {
    error  = 'Expecting STR, encountered LONG',
    schema = card_face,
    func = 'flatten', input = '42'
}

t {
    error  = 'Bad value: "Jizz"',
    schema = card_face,
    func = 'flatten', input = '"Jizz"'
}

t {
    error  = '1: Expecting INT, encountered DOUBLE',
    schema = card_face,
    func = 'unflatten', input = '[3.14]'
}

t {
    error  = '1: Bad value: 4',
    schema = card_face,
    func = 'unflatten', input = '[4]'
}

t {
    error  = '1: Bad value: 1000',
    schema = card_face,
    func = 'unflatten', input = '[1000]'
}

t {
    error  = '1: Bad value: -1',
    schema = card_face,
    func = 'unflatten', input = '[-1]'
}

t {
    schema = card_face_nullable,
    func = 'flatten',
    input = '"HEARTS"', output = '[1]'
}

t {
    schema = card_face_nullable,
    func = 'flatten',
    input = 'null', output = '[null]'
}
