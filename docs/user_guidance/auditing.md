---
tags:
    - Auditing
---

The Auditing objects within the DVE are used to help control and store information about submitted data and what stage it's currently at. In addition to the above, it's also used to store statistics about the submission and the number of validations it has triggered etc. So, for users not interested in using the Error reports stage, you could source information directly from the audit tables.

## Audit Tables

Currently, these are the audit tables that can be accessed within the DVE:

| Table Name              | Purpose | When Available |
| ----------------------- | ------- | -------------- |
| `processing_status`     | Contains information about the submission and what the current processing status is. | >= File Transformation |
| `submission_info`       | Contains information about the submitted file. | >= File Transformation |
| `submission_statistics` | Contains validation statistics for each submission. | >= Error Reports |
| `aggregates`            | Contains aggregate counts of errors triggered for a submission | >= Error Reports |

## Audit Objects

You can use the the following methods to help you interact with the tables above or you can query the table via `sql`.

You can read more about how to interact with the Audit Objects [here](../advanced_guidance/package_documentation/auditing.md).
