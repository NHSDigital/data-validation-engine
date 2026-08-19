import json
import pytest
from dve.core_engine.configuration.v1 import V1EngineConfig

CONFIG_WITHOUT_LINKAGE = """{
    "contract": {
        "schemas": {},
        "datasets": {
            "animals": {
                "fields": {
                    "name": "str",
                    "height": "float",
                    "weight": "float",
                    "region": "str"
                },
                "reader_config": {
                    ".xml": {
                        "reader": "DuckDBXMLStreamReader",
                        "kwargs": {
                            "record_tag": "animal",
                            "root_tag": "animals"
                        }
                    }
                },
                "mandatory_fields": [
                    "name"
                ]
            }
        }
    },
    "transformations": {
        "filters": [
            {
                "entity": "animals",
                "name": "check_valid_region",
                "expression": "lower(region) in ('africa', 'asia')",
                "error_code": "ANE01",
                "failure_message": "Record rejected - `{{ region }}` is not in a valid region."
            },
            {
                "entity": "animals",
                "name": "check_for_pets",
                "expression": "lower(name) != 'human'",
                "error_code": "ANE02",
                "failure_message": "Submission Rejected - 'Human' is not a valid animal.",
                "failure_type": "submission"
            },
            {
                "entity": "animals",
                "name": "check_valid_weight",
                "expression": "weight > 0",
                "error_code": "ANE03",
                "failure_message": "Warning - `{{ weight }}` is below zero.",
                "is_informational": true
            }
        ]
    }
}"""

CONFIG_WITH_LINKAGE = """{
    "contract": {
        "schemas": {},
        "datasets": {
            "ds_001": {
                "fields": {
                    "ds_001_id": "str",
                    "patient_id": "str",
                    "address": "str",
                    "name": "str"
                },
                "reader_config": {
                    ".xml": {
                        "reader": "DuckDBXMLStreamReader",
                        "kwargs": {
                            "record_tag": "001",
                            "root_tag": "header"
                        }
                    }
                },
                "mandatory_fields": [
                    "ds_001_id",
                    "patient_id",
                    "address",
                    "name"
                ]
            },
            "ds_002": {
                "fields": {
                    "ds_002_id": "str",
                    "gp_name": "str",
                    "gp_address": "str"
                },
                "reader_config": {
                    ".xml": {
                        "reader": "DuckDBXMLStreamReader",
                        "kwargs": {
                            "record_tag": "002",
                            "root_tag": "header"
                        }
                    }
                },
                "mandatory_fields": [
                    "ds_002_id",
                    "gp_name",
                    "gp_address"
                ]
            },
        "ds_003": {
                "fields": {
                    "ds_003_id": "str",
                    "ds_001_id": "str",
                    "total_income": "int"
                },
                "reader_config": {
                    ".xml": {
                        "reader": "DuckDBXMLStreamReader",
                        "kwargs": {
                            "record_tag": "003",
                            "root_tag": "header"
                        }
                    }
                },
                "mandatory_fields": [
                    "ds_003_id",
                    "ds_001_id"
                ]
            },
             "ds_101": {
                "fields": {
                    "ds_001_id": "str",
                    "referral_id": "int",
                    "consultant_name": "str"
                },
                "reader_config": {
                    ".xml": {
                        "reader": "DuckDBXMLStreamReader",
                        "kwargs": {
                            "record_tag": "101",
                            "root_tag": "header"
                        }
                    }
                },
                "mandatory_fields": [
                    "referral_id",
                    "ds_001_id"
                ]
            },
             "ds_201": {
                "fields": {
                    "ds_201_id": "str",
                    "referral_id": "str",
                    "contact_date": "date"
                },
                "reader_config": {
                    ".xml": {
                        "reader": "DuckDBXMLStreamReader",
                        "kwargs": {
                            "record_tag": "201",
                            "root_tag": "header"
                        }
                    }
                },
                "mandatory_fields": [
                    "referral_id",
                    "ds_201_id",
                    "contact_date"
                ]
            },
             "ds_202": {
                "fields": {
                    "ds_202_id": "str",
                    "ds_201_id": "str",
                    "contact_name": "str"
                },
                "reader_config": {
                    ".xml": {
                        "reader": "DuckDBXMLStreamReader",
                        "kwargs": {
                            "record_tag": "202",
                            "root_tag": "header"
                        }
                    }
                },
                "mandatory_fields": [
                    "ds_202_id",
                    "ds_201_id"
                ]
            }
        }
    },
    "transformations": {
        "filters": [
            {
                "entity": "001",
                "name": "check_name",
                "expression": "len(name) > 2",
                "error_code": "CHECK1",
                "failure_message": "Record rejected - `{{ name }}` is not valid."
            }
        ]
    },
    "entity_relationships": {
            "ds_003": {
                    "parent_entity": "ds_001",
                    "join_fields": ["ds_001_id"],
                    "mandatory": false
                },
            "ds_101": {
                    "parent_entity": "ds_001",
                    "join_fields": ["ds_001_id"],
                    "mandatory_entity": true
                },
            "ds_201": {
                    "parent_entity": "ds_101",
                    "join_fields": ["referral_id"],
                    "mandatory": false
                },
            "ds_202": {
                    "parent_entity": "ds_201",
                    "join_fields": ["ds_201_id"],
                    "mandatory": true
                }
        }
}"""

def test_no_linkage_config_load():
    config = V1EngineConfig(location="", 
                            **json.loads(CONFIG_WITHOUT_LINKAGE))
    assert len(config.contract.datasets) == 1
    hierarchy = config.get_entity_hierarchy()
    assert len(hierarchy) == 1
    assert not hierarchy.get("animals").children


def test_linkage_config_load():
    config = V1EngineConfig(location="", 
                            **json.loads(CONFIG_WITH_LINKAGE))
    assert len(config.contract.datasets) == 6
    hierarchy = config.get_entity_hierarchy()
    assert len(hierarchy) == 2
    assert not hierarchy.get("ds_002").children
    assert len(hierarchy.get("ds_001").get_descendents()) == 4
    children_001 = sorted(hierarchy.get("ds_001").children, key=lambda x: x.entity_name)
    dict_rep_001 = hierarchy.get("ds_001").as_dict()
    assert len(children_001) == 2
    assert children_001[0].entity_name == "ds_003"
    assert not children_001[0].children
    assert children_001[1].entity_name == "ds_101"
    assert dict_rep_001 == {'ds_001': {'children': {'ds_003': {'children': {}, 'mandatory': False, 'join_fields': ['ds_001_id']}, 'ds_101': {'children': {'ds_201': {'children': {'ds_202': {'children': {}, 'mandatory': True, 'join_fields': ['ds_201_id']}}, 'mandatory': False, 'join_fields': ['referral_id']}}, 'mandatory': False, 'join_fields': ['ds_001_id']}}, 'mandatory': False}}
    children_101 = children_001[1].children
    dict_rep_101 = dict_rep_001["ds_001"]["children"]["ds_101"]
    assert len(children_101) == 1
    assert children_101[0].entity_name == "ds_201"
    assert children_101[0].children[0].entity_name == "ds_202"
    assert not children_101[0].children[0].children
    assert dict_rep_101 == {'children': {'ds_201': {'children': {'ds_202': {'children': {}, 'mandatory': True, 'join_fields': ['ds_201_id']}}, 'mandatory': False, 'join_fields': ['referral_id']}}, 'mandatory': False, 'join_fields': ['ds_001_id']}
    