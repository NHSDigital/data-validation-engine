# JSON Schemas

JSON schemas define how the rules within the dischema document should be written. We also include components to help write the rulestore or ruleset documents as well.

You can download a copy of the json schemas [here](https://github.com/NHSDigital/data-validation-engine/tree/main/docs/advanced_guidance/json_schemas).

For autocomplete support in VS Code, you can alter the `.vscode/settings.json` and add new entries to the
`json.schemas` key. If not present, simply copy & paste the code shown below into your `settings.json`:

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

Your dischema will then have autocomplete and syntax suggestion support.

# Components

The DVE rules are built on a number of components. You can view the components [here](https://github.com/NHSDigital/data-validation-engine/tree/main/docs/advanced_guidance/json_schemas).
