# NB: labels for content item types need to be consistent with the JSON schema
# https://impresso.github.io/impresso-schemas/json/newspaper/issue.schema.json
CONTENTITEM_TYPE_ARTICLE = 'article'
CONTENTITEM_TYPE_ADVERTISEMENT = 'ad'
CONTENTITEM_TYPE_OBITUARY = 'death_notice'
CONTENTITEM_TYPE_TABLE = 'table'
CONTENTITEM_TYPE_WEATHER = 'weather'
CONTENTITEM_TYPE_IMAGE = 'image'

CONTENTITEM_TYPES = [
    CONTENTITEM_TYPE_ARTICLE,
    CONTENTITEM_TYPE_ADVERTISEMENT,
    CONTENTITEM_TYPE_IMAGE,
    CONTENTITEM_TYPE_OBITUARY,
    CONTENTITEM_TYPE_TABLE,
    CONTENTITEM_TYPE_WEATHER
]
