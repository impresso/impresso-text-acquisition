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

- paragraph information is available only for text that is part of an article (while
  in Olive it's encoded at the page level)

## getting to IIIF

https://persist.lu/ark:/70795/hnpwc4

https://iiif.eluxemburgensia.lu/iiif/2/ark:%2f70795%2fhnpwc4%2fpages%2f1/info.json

```python
img_props = self.issue.image_properties[self.number]
    x_resolution = img_props['x_resolution']
    y_resolution = img_props['y_resolution']
    coordinates = convert_coordinates(
        hpos,
        vpos,
        height,
        width,
        x_resolution,
        y_resolution
    )
    iiif_link = self.data['iiif']
    iiif_link = iiif_link.replace("info.json", ",".join([str(c) for c in coordinates]))
    """
```
