#! /usr/bin/env python2.7
# Order-preserving JSON->msgpack conversion.
import sys
import msgpack
import simplejson as json
from collections import OrderedDict
from argparse import ArgumentParser
from base64 import b64decode as base64_decode

def msgpack_to_json(data):
    return json.dumps(msgpack.loads(data, object_pairs_hook=OrderedDict))

def json_to_msgpack(data):
    return msgpack.dumps(json.loads(data, object_pairs_hook=OrderedDict))

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

sanity_check()
parser = ArgumentParser(description=
                        'Order-preserving JSON->msgpack conversion.')
parser.add_argument('-D', dest='func', action='store_const',
                    const=msgpack_to_json, default=json_to_msgpack,
                    help='decode msgpack (default: encode)')
parser.add_argument('base64_input', nargs='?', help='if missing, reads stdin')
args = parser.parse_args()
try:
    input = args.base64_input
    sys.stdout.write(args.func(input and base64_decode(input) or
                               sys.stdin.read()))
except Exception as e:
    sys.stderr.write(str(e)+'\n')
    sys.exit(-1)
