---
title: Frequently Asked Questions (FAQ)
tags:
    - FAQ
    - Business Rules
    - Error Reports
---

??? question "The `id` field in the error report is missing when it should be present?"

    If the `id` field isn't populated then it will be due to the fact that the key field is not present in the entity you are looking up against. The `key_fields`/`key_fields` is sourced from a dictionary lookup sourced in the data contract, so new entities defined within the business_rules won't have this lookup and it will result in the column not being populated. To get around this issue for entities without a defined `key_field` you can use the `original_entity_override` key to define the entity you want to source your `key_field`/`key_fields` from. You may find an example below.

    ??? example

        ```json
        "filters": [
            {
                "entity": "entity_not_defined_in_data_contract",
                "name": "my rule",
                "expression": "a < b",
                "original_entity_override": "entity_defined_in_data_contract"
            }
        ]
        ```
