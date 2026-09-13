"""Constant values used in mutiple places."""

RECORD_INDEX_COLUMN_NAME: str = "__record_index__"
"""The name of the column containing the record index for each entity."""

CONTRACT_ERROR_VALUE_FIELD_NAME: str = "__error_value"
"""The name of the field that can be used to extract the field value that caused
   a pydantic validation error"""

ORPHANED_RECORD_ENTITY_NAME: str = "orphaned_record_tracker"
"""Name of entity to keep track of records where there is a missing parent record"""
