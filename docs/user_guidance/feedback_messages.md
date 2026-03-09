---
title: Feedback Messages
tags:
    - Feedback
    - Messages
---

During the processing of a submission through the DVE, Feedback Messages will be produced.

These messages are generated when...

1. the data is structurely incorrect during the [File Transformation](./file_transformation.md) stage.
2. the data has failed during the modelling and casting steps during the [Data Contract](./data_contract.md) stage.
3. the data has failed one of the validation rules defined during the [Business Rules](./business_rules.md) stage.

The messages are compiled into a `jsonl` file associated with the stage it failed in.

The `jsonl` files produced will be in the same folder as your submission under a folder called `errors/`.

## Processing Errors

In situations where the DVE cannot continue (critical failure), a message will be produced and stored in the submissions folder under a name called `processing_errors/`.

The processing error `jsonl` file will contain information regarding why the DVE could not continue.

If the DVE is crashing and the error message is either unreadable or is crashing when it should be working, please [raise an issue](https://github.com/NHSDigital/data-validation-engine/issues) on our GitHub.


## Feedback Message Models

Please refer to [Advanced User Guidance: Feedback Messages](../advanced_guidance/package_documentation/feedback_messages.md).
