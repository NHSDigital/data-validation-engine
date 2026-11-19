---
title: Customising your Error Report
tags:
    - Feedback
    - Error Reports
---

!!! note

    This feature is WIP and functionality will be expanded over time.

This feature will allow you to customise elements of the error report generated. To use this functionality simply add the following to your dischema document:

```json
{
    "contract": {},
    "transformations": {},
    "error_report": {
        "record_rejection_reporting": false
    }
}
```

## Current features available

| Feature | Purpose | Default |
| ------- | ------- | ------- |
| record_rejection_reporting | Whether to show record rejection figures in the Data Summary Tab. This will remove "Total Number of Record Rejections" from the headlines figures and it will remove the "Record Rejection" type from the errors raised per entity. | True |
