# Text ingestion of BNL data

- styles are valid at level of page, but not unique if aggregated at issue level


- Implement a function to parse the logical structure of the mets file
to be found in `<structMap ID="DTL27" LABEL="Logical Structure" TYPE="LOGICAL">`
Ultimately it should return a list of mappings

```
{
    "MODSMD_ARTICLE9": [
        {
            "type": "title",
            "ptr": "P3_TB00008"
        },
        {
            "type": "subheadline",
            "ptr": "P3_TB00009"
        },
        {
            "type": "body",
            "ptr": "P3_CB00001"
        }
    ]
}
```
