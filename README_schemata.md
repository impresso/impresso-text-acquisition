# JSON Schemata

## Page schema

To be found at [`text_importer/schemas/article.schema`](text_importer/schemas/article.schema).

Keys' meaning:

- `r`: regions
- `c`: coordinates
- `p`: paragraphs
- `l`: lines
- `t`: tokens
- `tx`: text
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

TBD
