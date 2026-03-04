---
tags:
    - Auditing
---

The Auditing objects within the DVE are used to help control and store information about a given submission and what stage it's currently at. In addition to the above, it's also used to store statistics about the submission and the number of validations it has triggered etc. So, for users not interested in using the Error reports stage, you could source information directly from the audit tables.

## Audit Tables
Currently, these are the audit tables that can be accessed within the DVE:

| Table Name            | Purpose |
| --------------------- | ------- |
| processing_status     | Contains information about the submission and what the current processing status is. |
| submission_info       | Contains information about the submitted file. |
| submission_statistics | Contains validation statistics for each submission. |

## Audit Objects

You can use the the following methods to help you interact with the tables above or you can query the table via `sql`.

<hr>

::: src.dve.core_engine.backends.base.auditing.BaseAuditingManager
    options:
        heading_level: 3
        members:
            - get_submission_info
            - get_submission_statistics
            - get_submission_status
            - get_all_file_transformation_submissions
            - get_all_data_contract_submissions
            - get_all_business_rule_submissions
            - get_all_error_report_submissions
            - get_current_processing_info
