from text_importer.importers import CONTENTITEM_TYPE_IMAGE

NON_ARTICLE = ["advertisement", "death_notice"]


def convert_coordinates(hpos, vpos, width, height, x_res, y_res):
    """
    x =   (coordinate['xResolution']/254.0) * coordinate['hpos']

    y =   (coordinate['yResolution']/254.0) * coordinate['vpos']

    w =  (coordinate['xResolution']/254.0) * coordinate['width']

    h =  (coordinate['yResolution']/254.0) * coordinate['height']
    """
    x = (x_res / 254) * hpos
    y = (y_res / 254) * vpos
    w = (x_res / 254) * width
    h = (y_res / 254) * height
    return [int(x), int(y), int(w), int(h)]


def encode_ark(ark):
    """Replaces (encodes) backslashes in the Ark identifier."""
    return ark.replace('/', '%2f')


def div_has_body(div, body_type='body'):
    """ Returns True if the given `div` has a body in it's direct children
    
    :param div:
    :param body_type:
    :return:
    """
    children_types = set()
    for i in div.findChildren('div', recursive=False):
        child_type = i.get('TYPE')
        if child_type is not None:
            children_types.add(child_type.lower())
    return body_type in children_types


def section_is_article(section_div):
    """ Returns True if the given div's children are all `ad_type`, except for "BODY" and "BODY_CONTENT"
    
    :param section_div:
    :return:
    """
    types = []
    for c in section_div.findChildren('div'):
        _type = c.get('TYPE').lower()
        if not (_type == 'body' or _type == 'body_content'):
            types.append(_type)
    return not all(t in NON_ARTICLE for t in types)


def find_section_articles(section_div, content_items):
    """
    Given a section div, parses all the articles inside of it, and searches the content items for that article.
    :param section_div:
    :param content_items:
    :return:
    """
    articles_lid = []
    for d in section_div.findChildren("div", {"TYPE": "ARTICLE"}):
        article_id = d.get('DMDID')
        if article_id is not None:
            articles_lid.append(article_id)
    
    children_art = []
    # Then search for corresponding content item
    for i in articles_lid:
        for ci in content_items:
            if i == ci['l']['id']:
                children_art.append(ci['m']['id'])
    return children_art


def remove_section_cis(content_items, sections):
    """
    Given all the content items and the formed sections, filters out those that are contained in a section and returns the rest
    :param content_items:
    :param sections:
    :return:
    """
    to_remove = [j for i in sections for j in i['l']['canonical_parts']]
    if len(to_remove) == 0:
        return content_items, []
        
    to_remove = set(to_remove)
    new_cis = []
    removed = []
    for ci in content_items:
        if ci['m']['id'] not in to_remove or ci['m']['tp'] == CONTENTITEM_TYPE_IMAGE:
            new_cis.append(ci)
            removed.append(ci['m']['id'])
    
    return new_cis, to_remove


