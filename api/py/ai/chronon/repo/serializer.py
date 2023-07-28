import json
from ai.chronon.utils import JsonDiffer
from thrift.Thrift import TType
from thrift.protocol.TJSONProtocol import TSimpleJSONProtocolFactory, TJSONProtocolFactory

from thrift import TSerialization


class ThriftJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        self._thrift_class = kwargs.pop('thrift_class')
        super(ThriftJSONDecoder, self).__init__(*args, **kwargs)

    def decode(self, json_str):
        if isinstance(json_str, dict):
            dct = json_str
        else:
            dct = super(ThriftJSONDecoder, self).decode(json_str)
        return self._convert(dct, TType.STRUCT,
                             (self._thrift_class, self._thrift_class.thrift_spec))

    def _convert(self, val, ttype, ttype_info):
        if ttype == TType.STRUCT:
            (thrift_class, thrift_spec) = ttype_info
            ret = thrift_class()
            for field in thrift_spec:
                if field is None:
                    continue
                (_, field_ttype, field_name, field_ttype_info, dummy) = field
                if field_name not in val:
                    continue
                converted_val = self._convert(val[field_name], field_ttype, field_ttype_info)
                setattr(ret, field_name, converted_val)
        elif ttype == TType.LIST:
            (element_ttype, element_ttype_info, _) = ttype_info
            ret = [self._convert(x, element_ttype, element_ttype_info) for x in val]
        elif ttype == TType.SET:
            (element_ttype, element_ttype_info) = ttype_info
            ret = set([self._convert(x, element_ttype, element_ttype_info) for x in val])
        elif ttype == TType.MAP:
            (key_ttype, key_ttype_info, val_ttype, val_ttype_info, _) = ttype_info
            ret = dict([(self._convert(k, key_ttype, key_ttype_info),
                         self._convert(v, val_ttype, val_ttype_info)) for (k, v) in val.items()])
        elif ttype == TType.STRING:
            ret = str(val)
        elif ttype == TType.DOUBLE:
            ret = float(val)
        elif ttype == TType.I64:
            ret = int(val)
        elif ttype == TType.I32 or ttype == TType.I16 or ttype == TType.BYTE:
            ret = int(val)
        elif ttype == TType.BOOL:
            ret = bool(val)
        else:
            raise TypeError('Unrecognized thrift field type: %d' % ttype)
        return ret


def json2thrift(json_str, thrift_class):
    return json.loads(json_str, cls=ThriftJSONDecoder, thrift_class=thrift_class)


def file2thrift(path, thrift_class):
    try:
        with open(path, 'r') as file:
            return json2thrift(file.read(), thrift_class)
    except json.decoder.JSONDecodeError as e:
        raise Exception(f"Error decoding file into a {thrift_class.__name__}:  {path}. " +
                        f"Please double check that {path} represents a valid {thrift_class.__name__}.") from e


def thrift_json(obj):
    return TSerialization.serialize(obj, protocol_factory=TJSONProtocolFactory())


def thrift_simple_json(obj):
    simple = TSerialization.serialize(obj, protocol_factory=TSimpleJSONProtocolFactory())
    parsed = json.loads(simple)
    return json.dumps(parsed, indent=2)


def thrift_simple_json_protected(obj, obj_type) -> str:
    serialized = thrift_simple_json(obj)
    # ensure that reversal works - we will use this reversal during deployment
    thrift_obj = json.loads(serialized, cls=ThriftJSONDecoder, thrift_class=obj_type)
    actual = thrift_simple_json(thrift_obj)
    differ = JsonDiffer()
    diff = differ.diff(serialized, actual)
    assert len(diff) == 0, f"""Serialization can't be reversed
diff: \n{diff}
original: \n{serialized}
"""
    differ.clean()
    return serialized
