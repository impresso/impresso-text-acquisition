import bs4
from typing import List, Dict


def distill_coordinates(element) -> List[float]:
    hpos = int(element.get('HPOS'))
    vpos = int(element.get('VPOS'))
    width = int(element.get('WIDTH'))
    height = int(element.get('HEIGHT'))
    
    # NB: these coordinates need to be converted
    return [hpos, vpos, width, height]


def parse_textline(element) -> dict:
    line = {}
    line['c'] = distill_coordinates(element)
    tokens = []
    
    for child in element.children:
        
        if isinstance(child, bs4.element.NavigableString):
            continue
        
        if child.name == 'String':
            token = {
                    'c': distill_coordinates(child),
                    'tx': child.get('CONTENT')
                    }
            
            if child.get('SUBS_TYPE') == "HypPart1":
                # token['tx'] += u"\u00AD"
                token['tx'] += "-"
                token['hy'] = True
            elif child.get('SUBS_TYPE') == "HypPart2":
                token['nf'] = child.get('SUBS_CONTENT')
            
            tokens.append(token)
    
    line['t'] = tokens
    return line


def parse_printspace(element, mappings: Dict[str, str]) -> List[dict]:
    """Parses the `<PrintSpace>` element of an ALTO XML document."""
    
    regions = []
    
    for block in element.children:
        
        if isinstance(block, bs4.element.NavigableString):
            continue
        
        block_id = block.get('ID')
        if block_id in mappings:
            part_of_contentitem = mappings[block_id]
        else:
            part_of_contentitem = None
        
        coordinates = distill_coordinates(block)
        
        lines = [
                parse_textline(line_element)
                for line_element in block.findAll('TextLine')
                ]
        
        paragraph = {
                "c": coordinates,
                "l": lines
                }
        
        region = {
                "c": coordinates,
                "p": [paragraph]
                }
        
        if part_of_contentitem:
            region['pOf'] = part_of_contentitem
        regions.append(region)
    return regions
