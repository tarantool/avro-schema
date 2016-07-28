local default_false = [[
{
    "name": "subscriber_contract_info",
    "type": "record",
    "fields": [
        { "name": "msisdn", "type": "string"},
        { "name": "register_ind", "type": "boolean",  "default": false}
    ]
}]]

t {
    schema = default_false,
    func = 'flatten', input = '{ "msisdn": "79099421523"}', output = '["79099421523", false]'
}
