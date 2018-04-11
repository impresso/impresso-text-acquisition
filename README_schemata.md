# JSON Schemata

## Page schema

To be found at [`text_importer/schemas/article.schema`](text_importer/schemas/article.schema).

Meanings of keys:

- `r`: regions
- `c`: coordinates
- `p`: paragraphs
- `l`: lines
- `t`: tokens
- `tx`: text
- `nf`: normalized form
- `s`: style reference
- `pOf`: part of

```json
{
   "r": [
      {
         "c": [
            82,
            113,
            442,
            365
         ],
         "p": [
            {
               "l": [
                  {
                     "c": [
                        192,
                        140,
                        349,
                        168
                     ],
                     "t": [
                        {
                           "c": [
                              192,
                              140,
                              349,
                              168
                           ],
                           "tx": "R\u00c9DACTION",
                           "s": 3
                        }
                     ]
                  }
               ]
            }
         ],
         "n": "1",
         "pOf": "GDL-1900-01-02-a-i0001"
      }
    ]
}
```

## Issue schema

To be found at [`text_importer/schemas/issue.schema`](text_importer/schemas/issue.schema).

Meanings of keys:

- `i`: items
- `l`: legacy (this is strictly not part of the schema, and should be used to store any legacy information, e.g. IDs)
- `m`: metadata
- `id`: canonical ID of the item
- `l`: language
- `pp`: page numbers
- `pub`: publication (i.e. acronym of newspaper)
- `t`: title
- `tp`: type (e.g. article or ad)

Example:

```json
{
   "i": [
      {
         "l": {
            "id": "Ar00106",
            "source": "103-GDL-1900-01-02-0001.pdf"
         },
         "m": {
            "id": "GDL-1900-01-02-a-i0001",
            "l": "french",
            "pp": [
               1
            ],
            "pub": "GDL",
            "t": "Untitled Article",
            "tp": "article"
         }
      },
      {
         "l": {
            "id": "Ad00311",
            "source": "103-GDL-1900-01-02-0001.pdf"
         },
         "m": {
            "id": "GDL-1900-01-02-a-i0032",
            "l": "french",
            "pp": [
               3
            ],
            "pub": "GDL",
            "t": "Untitled Ad",
            "tp": "ad"
         }
      }
    ]
}
```
