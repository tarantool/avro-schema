#! /usr/bin/env python3
# Order-preserving JSON->msgpack conversion.
# Mongodb-inspired {"$binary": "FFFF"} representation for MsgPack BIN-s
import sys
import msgpack
import simplejson as json
from collections import OrderedDict
from base64 import b64decode as base64_decode
from binascii import hexlify

def msgpack_pairs_hook(pairs):
    return OrderedDict((
        (k,{'$binary': hexlify(v)}) if isinstance(v, bytes) else (k,v))
        for k, v in pairs)

def msgpack_list_hook(items):
    return list(({'$binary': hexlify(v)} if isinstance(v, bytes) else v)
                for v in items)

def json_pairs_hook(pairs):
    for k,v in pairs:
        if k == '$binary':
            return bytes.fromhex(v)
    return OrderedDict(pairs)

def msgpack_to_json(data):
    res = msgpack.loads(data, object_pairs_hook=msgpack_pairs_hook,
                        list_hook=msgpack_list_hook,
                        encoding='utf-8')
    if isinstance(res, bytes):
        res = { '$binary': hexlify(res) }
    return json.dumps(res)

def json_to_msgpack(data):
    data = data.decode('utf-8') if isinstance(data, bytes) else data
    single_precision = False
    if data.startswith('!'):
        data = data[1:]
        single_precision = True
    return msgpack.dumps(json.loads(data, encoding='utf-8',
                                    object_pairs_hook=json_pairs_hook),
                         use_bin_type=True, use_single_float=single_precision)

def sanity_check():
    json_data = '{"a": 1, "b": 2, "c": 3}'
    msgpack_data = b'\x83\xa1a\x01\xa1b\x02\xa1c\x03'
    assert(msgpack_data == json_to_msgpack(json_data))
    assert(json_data == msgpack_to_json(msgpack_data))
    ordered_map_samples = [
        '{"a": 1, "b": 2, "c": 3}',
        '{"a": 1, "c": 2, "b": 3}',
        '{"b": 1, "a": 2, "c": 3}',
        '{"b": 1, "c": 2, "a": 3}',
        '{"c": 1, "a": 2, "b": 3}',
        '{"c": 1, "b": 2, "a": 3}'
    ]
    for sample in ordered_map_samples:
        assert(sample == msgpack_to_json(json_to_msgpack(sample)))

if __name__ == '__main__':
    sanity_check()
    from argparse import ArgumentParser
    parser = ArgumentParser(description=
                            'Order-preserving JSON->msgpack conversion.')
    parser.add_argument('-D', dest='func', action='store_const',
                        const=msgpack_to_json, default=json_to_msgpack,
                        help='decode msgpack (default: encode)')
    parser.add_argument('base64_input', nargs='?',
                        help='if missing, reads stdin')
    args = parser.parse_args()
    try:
        res = args.func(args.base64_input and base64_decode(args.base64_input)
                        or sys.stdin.buffer.read())
        sys.stdout.buffer.write(res.encode('utf-8')
                                if isinstance(res, str) else res)
    except Exception as e:
        sys.stderr.write(str(e)+'\n')
        sys.exit(-1)
