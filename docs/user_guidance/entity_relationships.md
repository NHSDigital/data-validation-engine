---
title: Entity Relationships
tags:
    - Linkage
    - Relationships
    - Missing
    - Parent
    - Group
    - Rejections
---

Sometimes a user may choose to use the file transformation stage to `normalise` a heavily nested dataset into separate entities during the initial reading of data. This would be done by specifying different entities in the dataset section of the contract configuration in the `dischema` file. This allows for easier interaction when customising errors in the data contract or writing transformations in the business rules. However, if the dataset being processed requires more complex validation, for example removing orphaned records or implementing group rejections, then how to link normalised entities needs to be provided. This can be provided in the `entity_relationships` section of the `dischema`

## Entity Relationships Content

To allow the DVE to link between normalised assets, the following information should be provided (per linkable entity):

- parent_entity: the immediate parent of the entity
- join_fields: how to join the entity with its parent in dictionary form (parent_field_name: child_field_name)
- mandatory: whether the child entity is a mandatory field in the immediate parent

There is also the functionality to customise errors related to either missing parent or group rejections:

- missing_parent_id_error_code: the error code to display if a record is rejected as it hs no valid parent record
- missing_parent_id_error_message: the error message to display if a record is rejected as it hs no valid parent record
- no_valid_records_error_code: the error code to display if parent records are removed due to no valid children in a mandatory field
- no_valid_records_error_message: the error message to display if parent records are removed due to no valid children in a mandatory field

## Entity Hierarchy Object

The details provided in the entity_relationships section of the dischema are used to create an EntityHierarchy object.
Please refer to [Advanced User Guidance: Entity Hierarchy](../advanced_guidance/package_documentation/entity_hierarchy.md).
