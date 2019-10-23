local large = [[{
    "type": "record",
    "name": "type_1",
    "fields": [
        {
            "name": "field_1",
            "type": "long"
        },
        {
            "name": "field_2",
            "type": "long"
        },
        {
            "name": "field_3",
            "type": "string"
        },
        {
            "name": "field_4",
            "type": "string"
        },
        {
            "name": "field_5",
            "type": {
                "type": "record",
                "name": "type_2",
                "fields": [
                    {
                        "name": "field_6",
                        "type": "string"
                    },
                    {
                        "name": "field_7",
                        "type": "string"
                    }
                ]
            }
        },
        {
            "name": "field_8",
            "type": "string"
        },
        {
            "name": "field_9",
            "type": {
                "type": "array",
                "items": "type_1"
            }
        },
        {
            "name": "field_10",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "type_3",
                    "fields": [
                        {
                            "name": "field_11",
                            "type": "string"
                        },
                        {
                            "name": "field_12",
                            "type": {
                                "type": "record",
                                "name": "type_4",
                                "fields": [
                                    {
                                        "name": "field_13",
                                        "type": "long"
                                    },
                                    {
                                        "name": "field_14",
                                        "type": {
                                            "type": "record",
                                            "name": "type_5",
                                            "fields": [
                                                {
                                                    "name": "field_15",
                                                    "type": "long"
                                                },
                                                {
                                                    "name": "field_16",
                                                    "type": "string"
                                                },
                                                {
                                                    "name": "field_17",
                                                    "type": "string"
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "name": "field_18",
                            "type": "boolean"
                        },
                        {
                            "name": "field_19",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "type_6",
                                    "fields": [
                                        {
                                            "name": "field_20",
                                            "type": {
                                                "type": "string"
                                            }
                                        },
                                        {
                                            "name": "field_21",
                                            "type": {
                                                "type": "string"
                                            }
                                        },
                                        {
                                            "name": "field_22",
                                            "type": "boolean"
                                        },
                                        {
                                            "name": "field_23",
                                            "type": "string"
                                        },
                                        {
                                            "name": "field_24",
                                            "type": {
                                                "type": "record",
                                                "name": "type_7",
                                                "fields": [
                                                    {
                                                        "name": "field_25",
                                                        "type": "long"
                                                    },
                                                    {
                                                        "name": "field_26",
                                                        "type": "string"
                                                    }
                                                ]
                                            }
                                        },
                                        {
                                            "name": "field_27",
                                            "type": "type_2"
                                        },
                                        {
                                            "name": "field_28",
                                            "type": "string"
                                        },
                                        {
                                            "name": "field_29",
                                            "type": {
                                                "type": "record",
                                                "name": "type_8",
                                                "fields": [
                                                    {
                                                        "name": "field_30",
                                                        "type": "long"
                                                    },
                                                    {
                                                        "name": "field_31",
                                                        "type": "string"
                                                    }
                                                ]
                                            }
                                        },
                                        {
                                            "name": "field_32",
                                            "type": {
                                                "type": "record",
                                                "name": "type_9",
                                                "fields": [
                                                    {
                                                        "name": "field_33",
                                                        "type": "long"
                                                    },
                                                    {
                                                        "name": "field_34",
                                                        "type": {
                                                            "type": "string"
                                                        }
                                                    },
                                                    {
                                                        "name": "field_35",
                                                        "type": {
                                                            "type": "string"
                                                        }
                                                    },
                                                    {
                                                        "name": "field_36",
                                                        "type": "string"
                                                    }
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "name": "field_37",
                            "type": "long"
                        },
                        {
                            "name": "field_38",
                            "type": "boolean"
                        },
                        {
                            "name": "field_39",
                            "type": "long"
                        },
                        {
                            "name": "field_40",
                            "type": "long"
                        },
                        {
                            "name": "field_41",
                            "type": "boolean"
                        },
                        {
                            "name": "field_42",
                            "type": "long"
                        },
                        {
                            "name": "field_43",
                            "type": "type_7"
                        },
                        {
                            "name": "field_44",
                            "type": "type_2"
                        },
                        {
                            "name": "field_45",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "type_10",
                                    "fields": [
                                        {
                                            "name": "field_46",
                                            "type": "string"
                                        },
                                        {
                                            "name": "field_47",
                                            "type": "string"
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        },
        {
            "name": "field_48",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "type_11",
                    "fields": [
                        {
                            "name": "field_49",
                            "type": "long"
                        },
                        {
                            "name": "field_50",
                            "type": "string"
                        },
                        {
                            "name": "field_51",
                            "type": {
                                "type": "string"
                            }
                        },
                        {
                            "name": "field_52",
                            "type": "type_7"
                        },
                        {
                            "name": "field_53",
                            "type": {
                                "type": "record",
                                "name": "type_12",
                                "fields": [
                                    {
                                        "name": "field_54",
                                        "type": {
                                            "type": "string"
                                        }
                                    },
                                    {
                                        "name": "field_55",
                                        "type": {
                                            "type": "string"
                                        }
                                    },
                                    {
                                        "name": "field_56",
                                        "type": {
                                            "type": "string"
                                        }
                                    },
                                    {
                                        "name": "field_57",
                                        "type": "boolean"
                                    },
                                    {
                                        "name": "field_58",
                                        "type": "string"
                                    },
                                    {
                                        "name": "field_59",
                                        "type": "type_2"
                                    },
                                    {
                                        "name": "field_60",
                                        "type": "string"
                                    },
                                    {
                                        "name": "field_61",
                                        "type": "type_8"
                                    },
                                    {
                                        "name": "field_62",
                                        "type": "type_7"
                                    }
                                ]
                            }
                        },
                        {
                            "name": "field_63",
                            "type": {
                                "type": "record",
                                "name": "type_13",
                                "fields": [
                                    {
                                        "name": "field_64",
                                        "type": "long"
                                    },
                                    {
                                        "name": "field_65",
                                        "type": "long"
                                    },
                                    {
                                        "name": "field_66",
                                        "type": "long"
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        },
        {
            "name": "field_67",
            "type": {
                "type": "record",
                "name": "type_14",
                "fields": [
                    {
                        "name": "field_68",
                        "type": "string"
                    },
                    {
                        "name": "field_69",
                        "type": "string"
                    },
                    {
                        "name": "field_70",
                        "type": "long"
                    }
                ]
            }
        },
        {
            "name": "field_71",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "type_15",
                    "fields": [
                        {
                            "name": "field_72",
                            "type": "string"
                        },
                        {
                            "name": "field_73",
                            "type": "string"
                        }
                    ]
                }
            }
        },
        {
            "name": "field_74",
            "type": {
                "type": "record",
                "name": "type_16",
                "fields": [
                    {
                        "name": "field_75",
                        "type": "boolean"
                    },
                    {
                        "name": "field_76",
                        "type": "string"
                    }
                ]
            }
        },
        {
            "name": "field_77",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "type_17",
                    "fields": [
                        {
                            "name": "field_78",
                            "type": {
                                "type": "string"
                            }
                        },
                        {
                            "name": "field_79",
                            "type": "type_4"
                        },
                        {
                            "name": "field_80",
                            "type": "type_4"
                        },
                        {
                            "name": "field_81",
                            "type": "boolean"
                        },
                        {
                            "name": "field_82",
                            "type": "boolean"
                        },
                        {
                            "name": "field_83",
                            "type": "type_2"
                        },
                        {
                            "name": "field_84",
                            "type": "long"
                        },
                        {
                            "name": "field_85",
                            "type": "string"
                        },
                        {
                            "name": "field_86",
                            "type": {
                                "type": "record",
                                "name": "type_18",
                                "fields": [
                                    {
                                        "name": "field_87",
                                        "type": "long"
                                    },
                                    {
                                        "name": "field_88",
                                        "type": "string"
                                    }
                                ]
                            }
                        },
                        {
                            "name": "field_89",
                            "type": "type_7"
                        },
                        {
                            "name": "field_90",
                            "type": "boolean"
                        },
                        {
                            "name": "field_91",
                            "type": "string"
                        }
                    ]
                }
            }
        },
        {
            "name": "field_92",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "type_19",
                    "fields": [
                        {
                            "name": "field_93",
                            "type": "string"
                        },
                        {
                            "name": "field_94",
                            "type": "long"
                        },
                        {
                            "name": "field_95",
                            "type": "type_4"
                        },
                        {
                            "name": "field_96",
                            "type": "type_4"
                        },
                        {
                            "name": "field_97",
                            "type": "boolean"
                        },
                        {
                            "name": "field_98",
                            "type": "boolean"
                        },
                        {
                            "name": "field_99",
                            "type": "string"
                        },
                        {
                            "name": "field_100",
                            "type": "type_18"
                        },
                        {
                            "name": "field_101",
                            "type": "type_7"
                        },
                        {
                            "name": "field_102",
                            "type": {
                                "type": "record",
                                "name": "type_20",
                                "fields": [
                                    {
                                        "name": "field_103",
                                        "type": "long"
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
    ]
}]]

-- gh-124: we should not reach LuaJIT's 200 local variables limit
t {
    schema = large,
    compile_only = true,
}
