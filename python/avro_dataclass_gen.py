# generate dataclasses and enum classes based on avro
# TODO: make this a standalone pip package


import json
from typing import Any, Dict, Union


example = """
    {
        "type": "record",
        "name": "Join",
        "namespace": "ai.zipline.api.Config",
        "fields": [
            {
                "name": "stagingQuery",
                "type": "string"
            },
            {
                "name": "scanQuery",
                "type": {
                    "type": "record",
                    "name": "ScanQuery",
                    "fields": [
                        {
                            "name": "selects",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "string",
                                    "name": null
                                }
                            }
                        },
                        {
                            "name": "wheres",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "string",
                                    "name": null
                                }
                            }
                        },
                        {
                            "name": "startPartition",
                            "type": "string"
                        },
                        {
                            "name": "endPartition",
                            "type": "string"
                        },
                        {
                            "name": "timeExpression",
                            "type": "string"
                        }
                    ],
                    "namespace": "ai.zipline.api.Config"
                }
            },
            {
                "name": "table",
                "type": "string"
            },
            {
                "name": "dataModel",
                "type": {
                    "type": "enum",
                    "name": "DataModel",
                    "symbols": [
                        "Entities",
                        "Events"
                    ],
                    "namespace": "ai.zipline.api.Config"
                }
            },
            {
                "name": "startPartition",
                "type": "string"
            },
            {
                "name": "joinParts",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "JoinPart",
                        "fields": [
                            {
                                "name": "groupBy",
                                "type": {
                                    "type": "record",
                                    "name": "GroupBy",
                                    "fields": [
                                        {
                                            "name": "sources",
                                            "type": {
                                                "type": "array",
                                                "items": {
                                                    "type": "record",
                                                    "name": "DataSource",
                                                    "fields": [
                                                        {
                                                            "name": "scanQuery",
                                                            "type": "ScanQuery"
                                                        },
                                                        {
                                                            "name": "table",
                                                            "type": "string"
                                                        },
                                                        {
                                                            "name": "dataModel",
                                                            "type": "DataModel"
                                                        },
                                                        {
                                                            "name": "topic",
                                                            "type": "string"
                                                        },
                                                        {
                                                            "name": "mutationTable",
                                                            "type": "string"
                                                        },
                                                        {
                                                            "name": "mutationTimeExpression",
                                                            "type": "string",
                                                            "default": "mutation_ts"
                                                        },
                                                        {
                                                            "name": "reversalExpression",
                                                            "type": "string",
                                                            "default": "is_before"
                                                        }
                                                    ],
                                                    "namespace": "ai.zipline.api.Config"
                                                }
                                            }
                                        },
                                        {
                                            "name": "keys",
                                            "type": {
                                                "type": "array",
                                                "items": {
                                                    "type": "string",
                                                    "name": null
                                                }
                                            }
                                        },
                                        {
                                            "name": "aggregations",
                                            "type": {
                                                "type": "array",
                                                "items": {
                                                    "type": "record",
                                                    "name": "Aggregation",
                                                    "fields": [
                                                        {
                                                            "name": "type",
                                                            "type": {
                                                                "type": "enum",
                                                                "name": "AggregationType",
                                                                "symbols": [
                                                                    "ApproxDistinctCount",
                                                                    "Average",
                                                                    "BottomK",
                                                                    "Count",
                                                                    "First",
                                                                    "FirstK",
                                                                    "Last",
                                                                    "LastK",
                                                                    "Max",
                                                                    "Min",
                                                                    "Sum",
                                                                    "TopK"
                                                                ],
                                                                "namespace": "ai.zipline.api.Config"
                                                            }
                                                        },
                                                        {
                                                            "name": "inputColumn",
                                                            "type": "string"
                                                        },
                                                        {
                                                            "name": "windows",
                                                            "type": {
                                                                "type": "array",
                                                                "items": {
                                                                    "type": "record",
                                                                    "name": "Window",
                                                                    "fields": [
                                                                        {
                                                                            "name": "length",
                                                                            "type": "int"
                                                                        },
                                                                        {
                                                                            "name": "unit",
                                                                            "type": {
                                                                                "type": "record",
                                                                                "name": "TimeUnit",
                                                                                "fields": [
                                                                                    {
                                                                                        "name": "millis",
                                                                                        "type": "long"
                                                                                    },
                                                                                    {
                                                                                        "name": "str",
                                                                                        "type": "string"
                                                                                    }
                                                                                ],
                                                                                "namespace": "ai.zipline.api.Config"
                                                                            }
                                                                        }
                                                                    ],
                                                                    "namespace": "ai.zipline.api.Config"
                                                                }
                                                            }
                                                        },
                                                        {
                                                            "name": "args",
                                                            "type": {
                                                                "type": "map",
                                                                "values": "string"
                                                            },
                                                            "default": "Map()"
                                                        }
                                                    ],
                                                    "namespace": "ai.zipline.api.Config"
                                                }
                                            }
                                        },
                                        {
                                            "name": "metadata",
                                            "type": {
                                                "type": "record",
                                                "name": "MetaData",
                                                "fields": [
                                                    {
                                                        "name": "name",
                                                        "type": "string"
                                                    },
                                                    {
                                                        "name": "team",
                                                        "type": "string"
                                                    },
                                                    {
                                                        "name": "dependencies",
                                                        "type": {
                                                            "type": "array",
                                                            "items": {
                                                                "type": "string",
                                                                "name": null
                                                            }
                                                        },
                                                        "default": "List()"
                                                    },
                                                    {
                                                        "name": "online",
                                                        "type": "boolean",
                                                        "default": false
                                                    },
                                                    {
                                                        "name": "production",
                                                        "type": "boolean",
                                                        "default": false
                                                    }
                                                ],
                                                "namespace": "ai.zipline.api.Config"
                                            }
                                        }
                                    ],
                                    "namespace": "ai.zipline.api.Config"
                                }
                            },
                            {
                                "name": "keyRenaming",
                                "type": {
                                    "type": "map",
                                    "values": "string"
                                }
                            },
                            {
                                "name": "accuracy",
                                "type": {
                                    "type": "enum",
                                    "name": "Accuracy",
                                    "symbols": [
                                        "Snapshot",
                                        "Temporal"
                                    ],
                                    "namespace": "ai.zipline.api.Config"
                                }
                            },
                            {
                                "name": "prefix",
                                "type": "string"
                            }
                        ],
                        "namespace": "ai.zipline.api.Config"
                    }
                }
            },
            {
                "name": "metadata",
                "type": "MetaData"
            },
            {
                "name": "timeExpression",
                "type": "string",
                "default": "ts"
            }
        ]
    }
    """


primitives = {
    "boolean": "bool",
    "int": "int",
    "long": "int",
    "float": "float",
    "double": "float",
    "bytes": "bytes",
    "string": "str"
}


global_structs: Dict[str, Any] = {}


def parse(schema: Union[str, dict]) -> str:
    if isinstance(schema, str):
        if schema in primitives:
            return primitives[schema]
        else:
            assert schema in global_structs, f"Encountered a definition of {schema} without prior declaration"
            return schema

    assert isinstance(schema, dict), f"schema can only be either str or a dict, but found {type(schema)}"

    typ = schema["type"]
    print(typ)
    if typ == "record":
        fields = schema["fields"]
        record_type = schema["name"]
        record_schema = []
        for field in fields:
            field_type = parse(field["type"])
            field_name = field["name"]
            record_schema.append((field_type, field_name))
        global_structs[record_type] = record_schema  # json.dumps(record_schema)
        return record_type
    if typ == "enum":
        enum_symbols = schema["symbols"]
        enum_type = schema["name"]
        global_structs[enum_type] = enum_symbols  # json.dumps(enum_symbols)
        return enum_type
    if typ == "array":
        elem_type = parse(schema["items"])
        return f"List[{elem_type}]"
    if typ == "map":
        value_type = parse(schema["values"])
        return f"Dict[str, {value_type}]"
    elif typ in primitives:
        return primitives[typ]
    else:
        raise Exception(f"Unknown type {typ}")


parse(json.loads(example))

print(json.dumps(global_structs, indent=2))
