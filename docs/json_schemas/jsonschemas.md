# JSON Schemas

These JSON schemas define the

For autocomplete support in VS Code, alter `settings.json` and add new entries to the
`json.schemas` array (or create this value if it's missing)

```json
{
    ...,
    "json.schemas": [
        {
            "fileMatch": [
                "*.dischema.json"
            ],
            "url": "./json_schemas/dataset.schema.json"
        },
        {
            "fileMatch": [
                "*.rulestore.json",
                "*_ruleset.json"
            ],
            "url": "./json_schemas/rule_store.schema.json"
        }
    ]
}
```

Data Ingest JSON schemas (when saved with file_name `dataset.dischema.json`) should then have
autocomplete support.

# Components
[https://github.com/NHSDigital/data-validation-engine/tree/main/docs/json_schemas](https://github.com/NHSDigital/data-validation-engine/tree/main/docs/json_schemas)
